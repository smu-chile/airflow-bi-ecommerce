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
                    if ref_id not in seen_ref_ids:
                        seen_ref_ids.add(ref_id)
                        # El tercer elemento es "not_found = False"
                        all_stock.append((ref_id, stock, False))
                    
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
                single_url = f"{base_url}stock?warehouseRefId={warehouse_ref_id}&skuRefId={sku_ref_id}"
                futures.append(
                    executor.submit(fetch_single_sku, session, single_url, headers, sku_ref_id)
                )
            
            for future in as_completed(futures):
                sku_ref_id, stock, found = future.result()
                if found:
                    all_stock.append((sku_ref_id, stock, False))
                    print(f"Resolved missing SKU {sku_ref_id}: stock {stock}")
                else:
                    all_stock.append((sku_ref_id, 0, True)) # Not found in Janis stock list, default to 0 and not_found=True
                    print(f"Missing SKU {sku_ref_id} NOT found in Janis stock. Defaulted to 0 with not_found=True.")
    elif len(missing_ref_ids) > 500:
        print(f"Warning: Too many missing SKUs ({len(missing_ref_ids)}). Skipping individual checks to avoid API lock.")
        # Default all missing SKUs to 0 and not_found=True for safety
        for sku_ref_id in missing_ref_ids:
            all_stock.append((sku_ref_id, 0, True))

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
            df = pd.DataFrame(all_stock, columns=["ref_id", "stock", "not_found"])
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

    fetch_api_stock >> calculate_changes
