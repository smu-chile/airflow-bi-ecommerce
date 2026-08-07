from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pendulum
from utils.slack_utils import dag_success_slack, dag_failure_slack

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

def fetch_page(session, url, headers, page, page_size):
    page_headers = headers.copy()
    page_headers["x-janis-page-size"] = str(page_size)
    page_headers["x-janis-page"] = str(page)
    response = session.get(url, headers=page_headers, timeout=15)
    response.raise_for_status()
    return page, response.json()

def fetch_single_sku(session, url, headers, sku_ref_id):
    try:
        response = session.get(url, headers=headers, timeout=10)
        
        # Janis API devuelve un HTTP 400 con JSON especificando que el SKU no existe
        if response.status_code == 400:
            try:
                err_data = response.json()
                if err_data.get("code") == 14 or "not exists" in err_data.get("message", ""):
                    print(f"SKU {sku_ref_id} no existe en Janis (API retornó código 14). Marcando como not_found.")
                    return sku_ref_id, 0, False
            except Exception:
                pass
                
        response.raise_for_status()
        data = response.json()
        stock = 0
        found = False
        if data and isinstance(data, list) and len(data) > 0:
            stock = data[0].get("stock", 0)
            found = True
        return sku_ref_id, stock, found
    except Exception as e:
        print(f"Error fetching SKU {sku_ref_id} from Janis API: {e}")
        return sku_ref_id, 0, False

def _fetch_janis_api_stock(**kwargs):
    import requests
    import pandas as pd
    import sqlalchemy
    import time
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from airflow.models import Variable
    from requests.adapters import HTTPAdapter
    from urllib3.util import Retry

    # 1. Credentials
    api_key = Variable.get("JANIS_API_KEY")
    api_secret = Variable.get("JANIS_API_SECRET")
    client = Variable.get("JANIS_CLIENT")
    base_url = Variable.get("JANIS_API_URL")

    # 2. Setup Session with Retries/Throttling
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=20))

    headers = {
        "janis-api-key": api_key,
        "janis-api-secret": api_secret,
        "janis-client": client,
        "Connection": "keep-alive"
    }

    warehouse_ref_id = "193949d"
    all_stock = []
    seen_ref_ids = set()
    
    # 3. Get active catalog ref_ids from Postgres
    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)
    
    catalog_ref_ids = set()
    with engine.connect() as connection:
        result = connection.execute("""
            SELECT DISTINCT CONCAT(material, '-', umv) AS ref_id 
            FROM ecommdata.lista8 
            WHERE id_tienda = '0917'
              AND excluido IS FALSE
              AND bloq_centro IS NULL
              AND bloq_formato IS NULL
              AND catalogado IS TRUE;
        """)
        catalog_ref_ids = set([row[0] for row in result.fetchall()])
    
    print(f"Total active catalog SKUs in lista8: {len(catalog_ref_ids)}")
    
    # Parámetros de Paralelismo Cauteloso
    batch_size = 15              # Lotes de 15 páginas
    max_workers = 15             # Máximo 15 hilos en paralelo
    sleep_between_batches = 0.2  # Pausa de 200ms entre lotes
    
    page = 1
    page_size = 100
    stop_fetching = False

    print(f"Starting API fetch for warehouse: {warehouse_ref_id} (Cautious Parallelism: batch_size={batch_size}, max_workers={max_workers})")

    while not stop_fetching:
        pages_to_fetch = list(range(page, page + batch_size))
        url = f"{base_url}stock?warehouseRefId={warehouse_ref_id}"
        
        batch_results = {}
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(fetch_page, session, url, headers, p, page_size): p 
                for p in pages_to_fetch
            }
            
            for future in as_completed(futures):
                p_num = futures[future]
                try:
                    p_num, data = future.result()
                    batch_results[p_num] = data
                except Exception as e:
                    print(f"Error fetching page {p_num}: {e}")
                    raise e
        
        # Procesamos los resultados en orden correlativo estricto
        for p_num in sorted(pages_to_fetch):
            data = batch_results.get(p_num)
            
            if not data or not isinstance(data, list):
                print(f"Finished: Page {p_num} is empty or invalid. Stopping extraction.")
                stop_fetching = True
                break
                
            print(f"Page {p_num} processed. Items: {len(data)}")
            for item in data:
                ref_id = item.get("skuRefId")
                stock = item.get("stock", 0)
                if ref_id:
                    material = ref_id.split('-')[0]
                    # Map to check if it matches catalog ref_ids
                    for possible_ref_id in (ref_id, f"{material}-KG", f"{material}-KGV"):
                        if possible_ref_id in catalog_ref_ids:
                            if possible_ref_id not in seen_ref_ids:
                                seen_ref_ids.add(possible_ref_id)
                                all_stock.append({
                                    "ref_id": possible_ref_id,
                                    "stock": stock,
                                    "not_found": False
                                })
                            break
                    
        page += batch_size
        
        if not stop_fetching:
            time.sleep(sleep_between_batches)

    # 4. Check for skipped/missing active catalog SKUs (Pagination drift safety net)
    missing_ref_ids = catalog_ref_ids - seen_ref_ids
    print(f"Skipped/Missing SKUs to check individually: {len(missing_ref_ids)}")
    
    if 0 < len(missing_ref_ids) <= 500:
        print(f"Starting individual fetch for {len(missing_ref_ids)} missing SKUs...")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for sku_ref_id in missing_ref_ids:
                material = sku_ref_id.split('-')[0]
                # Consultamos la API con la versión -UN (ya que Janis V1 no tiene -KG/-KGV)
                sku_query_id = f"{material}-UN"
                single_url = f"{base_url}stock?warehouseRefId={warehouse_ref_id}&skuRefId={sku_query_id}"
                futures.append(
                    executor.submit(fetch_single_sku, session, single_url, headers, sku_ref_id)
                )
            
            for future in as_completed(futures):
                sku_ref_id, stock, found = future.result()
                if found:
                    all_stock.append({
                        "ref_id": sku_ref_id,
                        "stock": stock,
                        "not_found": False
                    })
                    print(f"Resolved missing SKU {sku_ref_id}: stock {stock}")
                else:
                    all_stock.append({
                        "ref_id": sku_ref_id,
                        "stock": 0,
                        "not_found": True
                    })
                    print(f"Missing SKU {sku_ref_id} NOT found in Janis stock. Defaulted to 0 with not_found=True.")
    elif len(missing_ref_ids) > 500:
        print(f"Warning: Too many missing SKUs ({len(missing_ref_ids)}). Skipping individual checks to avoid API lock.")
        # Default all missing SKUs to 0 and not_found=True for safety
        for sku_ref_id in missing_ref_ids:
            all_stock.append({
                "ref_id": sku_ref_id,
                "stock": 0,
                "not_found": True
            })

    print(f"Total items fetched from Janis API (including resolved): {len(all_stock)}")

    # 5. Save to database
    with engine.begin() as connection:
        # Drop table if exists to ensure schema updates are applied
        connection.execute("DROP TABLE IF EXISTS staging.stock_unimarc_api;")
        # Create staging table (incorporating not_found column)
        connection.execute("""
            CREATE TABLE staging.stock_unimarc_api (
                ref_id VARCHAR(50) PRIMARY KEY,
                stock INTEGER NOT NULL,
                not_found BOOLEAN NOT NULL DEFAULT FALSE
            );
        """)
        
        # Batch insert
        if all_stock:
            df = pd.DataFrame(all_stock)
            # Remove any last duplicates to ensure unique primary key before database copy
            df = df.drop_duplicates(subset=["ref_id"], keep="first")
            df.to_sql(
                name="stock_unimarc_api",
                con=connection,
                schema="staging",
                if_exists="append",
                index=False,
                chunksize=1000,
                method="multi"
            )
            print("Successfully populated staging.stock_unimarc_api table.")

    # Retornamos la cantidad de skus recuperados para el log
    return len(all_stock)

def _publish_janis_v2_stock(**kwargs):
    import requests
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text
    from airflow.models import Variable
    from requests.adapters import HTTPAdapter
    from urllib3.util import Retry

    # 1. Credentials
    api_key = Variable.get("JANIS_V2_UNIMARC_KEY")
    api_secret = Variable.get("JANIS_V2_UNIMARC_SECRET")
    client = Variable.get("JANIS_V2_UNIMARC_CLIENT")

    # 2. Database connection
    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    # 3. Mark not_found items as published/processed immediately since they cannot be injected into Janis
    with engine.begin() as connection:
        res_update = connection.execute(text("""
            UPDATE ecommdata.stock_trapenses_v2
            SET publicado_janis = TRUE,
                fecha_actualizacion = CURRENT_TIMESTAMP
            WHERE publicado_janis = FALSE
              AND not_found = TRUE;
        """))
        print(f"Skipped and marked as published {res_update.rowcount} SKUs that do not exist in Janis (not_found = TRUE).")

    # 4. Query pending items (only not_found = FALSE)
    with engine.connect() as connection:
        result = connection.execute(text("""
            SELECT ref_id, stock
            FROM ecommdata.stock_trapenses_v2
            WHERE publicado_janis = FALSE
              AND not_found = FALSE
              AND intentos_janis < 5;
        """))
        pending_items = result.fetchall()

    if not pending_items:
        print("No pending stock updates for Janis v2 WMS.")
        return

    print(f"Found {len(pending_items)} pending updates for Janis v2 WMS.")

    # 4.5 Cargar factores de conversión de pesables (multiplicador_unidad_medida)
    multipliers_dict = {}
    with engine.connect() as connection:
        result_multipliers = connection.execute(text("""
            SELECT DISTINCT split_part(ref_id, '-', 1) AS material, COALESCE(multiplicador_unidad_medida, 1.0)
            FROM ecommdata.skus
            WHERE ref_id ILIKE '%-KG' OR ref_id ILIKE '%-KGV';
        """))
        multipliers_dict = {row[0]: float(row[1]) for row in result_multipliers.fetchall()}

    # 5. Setup API session
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries, pool_connections=15, pool_maxsize=15))

    headers = {
        "janis-api-key": api_key,
        "janis-api-secret": api_secret,
        "janis-client": client,
        "Content-Type": "application/json",
        "Connection": "keep-alive"
    }

    url = "https://wms.janis.in/api/stored-good-batch"

    # Build payload and map database ref_ids
    items_to_send = []
    for ref_id, stock in pending_items:
        material = ref_id.split('-')[0]
        umv = ref_id.split('-')[1] if '-' in ref_id else 'UN'
        
        quantity = int(stock)
        sku_api = ref_id
        
        if umv in ('KG', 'KGV'):
            factor = multipliers_dict.get(material, 1.0)
            # Calculamos el peso correspondiente en kilos
            quantity = round(int(stock) * factor, 2)
            # Reemplazamos el sufijo a -UN para Janis v2 WMS
            sku_api = f"{material}-UN"
            print(f"[Janis WMS v2] Conversión Pesable: SKU {ref_id} -> {sku_api} | Unidades: {stock} | Factor: {factor} | Kilos: {quantity}")
            
        items_to_send.append({
            "payload": {
                "quantity": quantity,
                "skuReferenceId": sku_api,
                "warehouseReferenceId": "unimarc-0917",
                "operation": "set"
            },
            "original_ref_id": ref_id
        })

    def chunk_list(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    # Send in batches of 500
    for chunk in chunk_list(items_to_send, 500):
        batch_payload = [item["payload"] for item in chunk]
        batch_ref_ids = [item["payload"]["skuReferenceId"] for item in chunk]
        batch_original_ref_ids = [item["original_ref_id"] for item in chunk]
        
        try:
            print(f"📤 [Janis WMS v2] Sending batch of {len(chunk)} items. SKUs: {batch_ref_ids}")
            response = session.post(url, headers=headers, json=batch_payload, timeout=15)
            
            if response.status_code >= 400:
                print(f"❌ [Janis WMS v2] Error response body: {response.text}")
                
            response.raise_for_status()
            print(f"✅ [Janis WMS v2] Batch successfully sent. Status: {response.status_code}")
            
            with engine.begin() as connection:
                connection.execute(text("""
                    UPDATE ecommdata.stock_trapenses_v2
                    SET publicado_janis = TRUE,
                        intentos_janis = 0,
                        fecha_actualizacion = CURRENT_TIMESTAMP
                    WHERE ref_id = ANY(:ref_ids);
                """), {"ref_ids": list(batch_original_ref_ids)})
        except Exception as e:
            print(f"❌ [Janis WMS v2] Exception sending batch: {e}")
            with engine.begin() as connection:
                connection.execute(text("""
                    UPDATE ecommdata.stock_trapenses_v2
                    SET intentos_janis = intentos_janis + 1,
                        fecha_actualizacion = CURRENT_TIMESTAMP
                    WHERE ref_id = ANY(:ref_ids);
                """), {"ref_ids": list(batch_original_ref_ids)})

def send_single_vtex_stock(session, url, headers, ref_id, vtex_id, stock):
    payload = {
        "unlimitedQuantity": False,
        "dateUtcOnBalanceSystem": None,
        "quantity": int(stock)
    }
    try:
        print(f"📤 [VTEX] Sending SKU: {vtex_id} | RefId: {ref_id} | Stock: {stock}")
        response = session.put(url, headers=headers, json=payload, timeout=10)
        
        if response.status_code >= 400:
            print(f"❌ [VTEX] Error response body for SKU {vtex_id}: {response.text}")
            
        response.raise_for_status()
        print(f"✅ [VTEX] Stock update success. SKU: {vtex_id} | Stock: {stock}")
        return ref_id, True, None
    except Exception as e:
        return ref_id, False, str(e)

def _publish_vtex_stock(**kwargs):
    import requests
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from airflow.models import Variable
    from requests.adapters import HTTPAdapter
    from urllib3.util import Retry

    # 1. Credentials
    app_key = Variable.get("X_VTEX_API_AppKey", default_var=None) or Variable.get("VTEX_API_KEY", default_var=None)
    app_token = Variable.get("X_VTEX_API_AppToken", default_var=None) or Variable.get("VTEX_API_TOKEN", default_var=None)
    account_name = Variable.get("VTEX_ACCOUNT_NAME", default_var="unimarc")
    env = Variable.get("VTEX_ENV", default_var="vtexcommercestable")

    if not app_key or not app_token:
        print("VTEX AppKey or AppToken not configured. Skipping VTEX stock update.")
        return

    # 2. Database connection
    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    # 3. Query pending items
    with engine.connect() as connection:
        result = connection.execute(text("""
            SELECT v.ref_id, v.stock, s.vtex_id
            FROM ecommdata.stock_trapenses_v2 v
            INNER JOIN ecommdata.skus s ON v.ref_id = s.ref_id
            WHERE v.publicado_vtex = FALSE
              AND v.intentos_vtex < 5
              AND s.vtex_id IS NOT NULL;
        """))
        pending_items = result.fetchall()

    if not pending_items:
        print("No pending stock updates for VTEX.")
        return

    print(f"Found {len(pending_items)} pending updates for VTEX.")

    # 4. Setup API session
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=20))

    headers = {
        "X-VTEX-API-AppKey": app_key,
        "X-VTEX-API-AppToken": app_token,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    # 5. Parallel sending
    max_workers = 10
    success_ids = []
    failed_ids = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for ref_id, stock, vtex_id in pending_items:
            url = f"https://{account_name}.{env}.com.br/api/logistics/pvt/inventory/skus/{vtex_id}/warehouses/0917"
            futures.append(
                executor.submit(send_single_vtex_stock, session, url, headers, ref_id, vtex_id, stock)
            )
        
        for future in as_completed(futures):
            ref_id, success, err_msg = future.result()
            if success:
                success_ids.append(ref_id)
            else:
                failed_ids.append(ref_id)
                print(f"Error publishing SKU {ref_id} to VTEX: {err_msg}")

    print(f"VTEX Sync Completed. Success: {len(success_ids)} | Failed: {len(failed_ids)}")

    # 6. Update database states
    if success_ids:
        with engine.begin() as connection:
            connection.execute(text("""
                UPDATE ecommdata.stock_trapenses_v2
                SET publicado_vtex = TRUE,
                    intentos_vtex = 0,
                    fecha_actualizacion = CURRENT_TIMESTAMP
                WHERE ref_id = ANY(:ref_ids);
            """), {"ref_ids": list(success_ids)})

    if failed_ids:
        with engine.begin() as connection:
            connection.execute(text("""
                UPDATE ecommdata.stock_trapenses_v2
                SET intentos_vtex = intentos_vtex + 1,
                    fecha_actualizacion = CURRENT_TIMESTAMP
                WHERE ref_id = ANY(:ref_ids);
            """), {"ref_ids": list(failed_ids)})

with DAG(
    'etl_stock_trapenses_v2_comparison',
    default_args=default_args,
    description="Monitoreo y cálculo horario de cambios de stock para Los Trapenses v2 obteniendo stock en tiempo real desde la API de Janis",
    schedule_interval="0 * * * *",
    start_date=pendulum.datetime(2026, 7, 31, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["DATA", "janis", "stock", "trapenses", "monitoreo"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Cálculo de delta de stock por hora para tienda Los Trapenses consultando directamente la API de Janis (Warehouse 193949d).
    Obtiene el stock en tiempo real, lo guarda en la tabla staging.stock_unimarc_api y calcula los cambios.
    """

    fetch_api_stock = PythonOperator(
        task_id="fetch_janis_api_stock",
        python_callable=_fetch_janis_api_stock,
    )

    calculate_changes = PostgresOperator(
        task_id="calculate_and_apply_changes",
        postgres_conn_id="postgresql_conn",
        sql="sql/stock_trapenses_v2_comparison.sql"
    )

    publish_janis_v2 = PythonOperator(
        task_id="publish_janis_v2_stock",
        python_callable=_publish_janis_v2_stock,
    )

    publish_vtex = PythonOperator(
        task_id="publish_vtex_stock",
        python_callable=_publish_vtex_stock,
    )

    fetch_api_stock >> calculate_changes >> [publish_janis_v2, publish_vtex]
