from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from utils.slack_utils import upload_bytes_to_slack, dag_success_slack, dag_failure_slack
import pendulum
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import pandas as pd
import io
import time
import sqlalchemy
from utils.postgres_utils import query_to_df

def fetch_products_page_manual(session, url, headers, page, page_size):
    page_headers = headers.copy()
    page_headers["X-Janis-Page"] = str(page)
    page_headers["X-Janis-Page-Size"] = str(page_size)
    
    response = session.get(url, headers=page_headers, timeout=60)
    if response.status_code != 200:
        raise Exception(f"Fallo al consultar la API de Janis. Código: {response.status_code} | Respuesta: {response.text}")
    return page, response.json()

def sync_catalogo_api_manual():
    # 1. Obtener credenciales de Janis Alvi
    janis_api_key = Variable.get("JANIS_ALVI_API_KEY")
    janis_api_secret = Variable.get("JANIS_ALVI_API_SECRET")
    janis_client = Variable.get("JANIS_ALVI_CLIENT")
    janis_base_url = Variable.get("JANIS_API_URL")
    
    if not all([janis_api_key, janis_api_secret, janis_client, janis_base_url]):
        raise Exception("Faltan variables de configuración de Janis (JANIS_API_URL) o exclusivas de Alvi (JANIS_ALVI_API_KEY, JANIS_ALVI_API_SECRET, JANIS_ALVI_CLIENT).")
        
    url = f"{janis_base_url.rstrip('/')}/products"
    
    print(f"Iniciando sincronización manual multihilo de catálogo desde la API: {url}")
    
    # Configurar sesión HTTP con retries y pool de conexiones
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries, pool_connections=15, pool_maxsize=15))
    session.mount('http://', HTTPAdapter(max_retries=retries, pool_connections=15, pool_maxsize=15))
    
    headers = {
        "janis-api-key": janis_api_key,
        "janis-api-secret": janis_api_secret,
        "janis-client": janis_client,
        "Connection": "keep-alive"
    }
    
    products_extracted = []
    page = 1
    page_size = 100
    batch_size = 10
    max_workers = 10
    stop_fetching = False
    
    while not stop_fetching:
        pages_to_fetch = list(range(page, page + batch_size))
        batch_results = {}
        
        print(f"Descargando lote de páginas en paralelo: {pages_to_fetch[0]} a {pages_to_fetch[-1]} (workers={max_workers})...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(fetch_products_page_manual, session, url, headers, p, page_size): p 
                for p in pages_to_fetch
            }
            
            for future in as_completed(futures):
                p_num = futures[future]
                try:
                    p_num, data = future.result()
                    batch_results[p_num] = data
                except Exception as e:
                    print(f"Error extrayendo la página {p_num}: {e}")
                    raise e
                    
        for p_num in sorted(pages_to_fetch):
            data = batch_results.get(p_num)
            if not data or len(data) == 0:
                print(f"Página vacía {p_num} recibida. Extracción finalizada.")
                stop_fetching = True
                break
                
            print(f"Recibidos {len(data)} productos en la página {p_num}.")
            
            for prod in data:
                product_active = prod.get("IsActive", False)
                show_without_stock = prod.get("ShowWithoutStock", False)
                stores = prod.get("Stores", [])
                items = prod.get("Items", [])
                
                for item in items:
                    sku_ref_id = item.get("IdSku")
                    sku_active = item.get("IsActive", False)
                    is_active = bool(product_active and sku_active)
                    
                    if sku_ref_id:
                        if len(stores) > 0:
                            for store in stores:
                                products_extracted.append({
                                    "ref_id": sku_ref_id,
                                    "id_tienda": str(store).strip(),
                                    "activo": is_active,
                                    "show_without_stock": bool(show_without_stock)
                                })
                        else:
                            products_extracted.append({
                                "ref_id": sku_ref_id,
                                "id_tienda": "",
                                "activo": False,
                                "show_without_stock": bool(show_without_stock)
                            })
                            
        page += batch_size
        if not stop_fetching:
            time.sleep(0.1)
        
    print(f"Total registros extraídos: {len(products_extracted)}")
    if len(products_extracted) == 0:
        return
        
    df = pd.DataFrame(products_extracted)
    df = df.dropna(subset=['ref_id', 'id_tienda'])
    df = df.drop_duplicates(subset=['ref_id', 'id_tienda'])
    
    print(f"Total registros únicos a cargar: {len(df.index)}")
    
    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)
    
    print("Limpiando y guardando catálogo en ecommdata_alvi.productos_janis_api...")
    with engine.begin() as conn:
        conn.execute("TRUNCATE TABLE ecommdata_alvi.productos_janis_api")
        df.to_sql(
            name="productos_janis_api",
            con=conn,
            schema="ecommdata_alvi",
            if_exists="append",
            index=False,
            chunksize=10000,
            method="multi"
        )

def generate_reconciliation_csvs(ds):
    fecha_str = str(pendulum.now("America/Santiago").date())
    
    # 1. Cargar datos necesarios
    # Consultar tiendas activas
    tiendas_query = "select id from ecommdata_alvi.tiendas where status = 1 and id <> '1'"
    df_tiendas = query_to_df(tiendas_query)
    series_active_stores = df_tiendas['id'].unique()
    
    # Consultar la lista8 de hoy/mañana
    # Query lista8 que incluye excluidos y promociones fijas
    promociones_query = """select distinct concat(material,'-',umv) as ref_id, id_tienda, excluido
                from ecommdata_alvi.lista8 l"""
    df_lista8 = query_to_df(promociones_query)
    df_lista8.columns = ["ref_id", "id_tienda", "excluido"]
    df_lista8 = df_lista8[df_lista8['id_tienda'].isin(series_active_stores)]
    
    # Separar en permitidos y excluidos (manejando nulos como False)
    df_lista8['excluido'] = df_lista8['excluido'].fillna(False)
    df_lista8_active = df_lista8[df_lista8['excluido'] == False].copy()
    
    # Agrupar las tiendas autorizadas de la lista8
    df_lista8_active = df_lista8_active.sort_values(by=['ref_id', 'id_tienda'])
    df_active_grouped = df_lista8_active.groupby('ref_id')['id_tienda'].apply(
        lambda x: ','.join(sorted(x.dropna().unique()))
    ).reset_index(name='stores_target')
    
    # Consultar todos los SKUs registrados en Janis (nuestra tabla réplica)
    df_janis_skus = query_to_df("select distinct ref_id from ecommdata_alvi.productos_janis_api")
    
    # Filtro de SKUs inválidos
    query_invalid_skus = """
        select ref_id from ecommdata_alvi.productos p
        left join ecommdata_alvi.skus s using (ref_id)
        where s.nombre_sku is null
    """
    df_invalid_skus = query_to_df(query_invalid_skus)
    lista_invalid_skus = df_invalid_skus['ref_id'].to_list() if not df_invalid_skus.empty else []

    # 2. Generar el 100% de los registros para Reconciliación
    reconciled_products = []
    reconciled_skus = []
    
    for _, row in df_janis_skus.iterrows():
        sku_ref_id = row['ref_id']
        
        # Ignorar SKUs inválidos de seguridad
        if sku_ref_id in lista_invalid_skus:
            continue
            
        # Buscar si tiene tiendas permitidas en la lista8
        match_active = df_active_grouped[df_active_grouped['ref_id'] == sku_ref_id]
        
        if not match_active.empty:
            # Caso A: SKU permitido en al menos una tienda
            stores_str = match_active.iloc[0]['stores_target']
            reconciled_products.append({
                "refId": sku_ref_id,
                "stores": stores_str,
                "publish": 1,
                "updatePending": 1,
                "visible": 1,
                "active": 1,
                "showUnavailable": 0
            })
            reconciled_skus.append({
                "refId": sku_ref_id,
                "publish": 1,
                "updatePending": 1,
                "active": 1
            })
        else:
            # Caso B & C: SKU no tiene tiendas permitidas hoy
            reconciled_products.append({
                "refId": sku_ref_id,
                "stores": "3188",
                "publish": 1,
                "updatePending": 1,
                "visible": 0,
                "active": 0,
                "showUnavailable": 0
            })
            reconciled_skus.append({
                "refId": sku_ref_id,
                "publish": 1,
                "updatePending": 1,
                "active": 0
            })
            
    df_final_prod = pd.DataFrame(reconciled_products)
    df_final_skus = pd.DataFrame(reconciled_skus)
    
    # 3. Exportar y enviar a Slack
    buf_prod = io.StringIO()
    buf_skus = io.StringIO()
    
    df_final_prod.to_csv(buf_prod, sep=';', index=False)
    df_final_skus.to_csv(buf_skus, sep=';', index=False)
    
    bytes_prod = buf_prod.getvalue().encode("utf-8")
    bytes_skus = buf_skus.getvalue().encode("utf-8")
    
    file_prod = f"carga_productos_conciliacion_total_{fecha_str}.csv"
    file_skus = f"carga_skus_conciliacion_total_{fecha_str}.csv"
    
    comment = "⚠️ [MANUAL - CONCILIACIÓN TOTAL] ¡Ya se puede cargar el archivo maestro de conciliación total! :cat0:"
    
    upload_bytes_to_slack(
        file_name=file_prod,
        data_bytes=bytes_prod,
        channel_var_name="token_slack_carga_tiendas",
        initial_comment=comment.format(name=file_prod),
    )
    
    upload_bytes_to_slack(
        file_name=file_skus,
        data_bytes=bytes_skus,
        channel_var_name="token_slack_carga_tiendas",
        initial_comment=comment.format(name=file_skus),
    )
    
    print(f"CSVs de conciliación enviados: {file_prod}, {file_skus}")

def post_stock_chunk_manual(session, url, headers, chunk, batch_index):
    payload_json = json.dumps(chunk)
    print(f"Enviando lote manual {batch_index} ({len(chunk)} registros)...")
    response = session.post(url, headers=headers, data=payload_json, timeout=60)
    if response.status_code != 200:
        print(f"⚠️ Error en lote {batch_index}. Código: {response.status_code} | Respuesta: {response.text}")
    else:
        print(f"Lote {batch_index} enviado exitosamente: {response.status_code}")
    return batch_index, response.status_code

def send_excluded_stock_0_manual():
    # 1. Obtener credenciales de Janis Alvi
    janis_api_key = Variable.get("JANIS_ALVI_API_KEY")
    janis_api_secret = Variable.get("JANIS_ALVI_API_SECRET")
    janis_client = Variable.get("JANIS_ALVI_CLIENT")
    janis_base_url = Variable.get("JANIS_API_URL")
    
    if not all([janis_api_key, janis_api_secret, janis_client, janis_base_url]):
        raise Exception("Faltan variables de configuración de Janis (JANIS_API_URL) o exclusivas de Alvi (JANIS_ALVI_API_KEY, JANIS_ALVI_API_SECRET, JANIS_ALVI_CLIENT).")
        
    url = f"{janis_base_url.rstrip('/')}/stock"
    
    # 2. Consultar stock real de Janis que debería estar en 0 (excluidos)
    # Buscamos en la réplica de stock de Janis los productos que tienen stock > 0,
    # pero que no figuran en la lista8 de esa tienda o están marcados como excluidos.
    sql_stock_0 = """
        SELECT DISTINCT 
            s.ref_id, 
            t.id AS id_tienda
        FROM staging.stock_janis_alvi st
        JOIN ecommdata_alvi.skus s 
            ON st.item_id = s.id
        JOIN ecommdata_alvi.tiendas t 
            ON st.store_id = t.id_janis::bigint
        JOIN ecommdata_alvi.lista8 l 
            ON s.ref_id = (l.material::text || '-' || l.umv::text) 
           AND t.id = l.id_tienda
        WHERE st.stock > 0
          AND t.status = 1 
          AND t.id <> '1'
          AND l.excluido IS TRUE
          AND l.umv IN ('UN', 'KG', 'KGV')
    """
    df_stock_0 = query_to_df(sql_stock_0)
    
    if df_stock_0.empty:
        print("No se encontraron discrepancias de stock físico a corregir en Janis.")
        return
        
    stock_payload = []
    for _, row in df_stock_0.iterrows():
        sku_code = row['ref_id']
        material = str(sku_code.split('-')[0]).zfill(18)
        id_tienda = str(row['id_tienda']).zfill(4)
        stock_payload.append({
            "IdSku": material,
            "Quantity": 0,
            "Store": id_tienda,
            "Type": 1
        })
        
    print(f"Se inyectarán stock 0 multihilo a {len(stock_payload)} combinaciones de producto-tienda...")
    
    headers = {
        "janis-api-key": janis_api_key,
        "janis-api-secret": janis_api_secret,
        "janis-client": janis_client,
        "Content-Type": "application/json",
        "Connection": "keep-alive"
    }
    
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=10))
    session.mount('http://', HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=10))
    
    chunk_size = 500
    chunks = [stock_payload[i:i + chunk_size] for i in range(0, len(stock_payload), chunk_size)]
    
    max_workers = 5
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(post_stock_chunk_manual, session, url, headers, chunk, idx + 1)
            for idx, chunk in enumerate(chunks)
        ]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error enviando lote en paralelo: {e}")

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    "etl_manual_conciliacion_catalogo_alvi",
    default_args=default_args,
    description="Conciliación e inyección manual masiva de catálogo y stock cero en Janis",
    schedule_interval=None, # Ejecución manual únicamente
    start_date=pendulum.datetime(2026, 8, 1, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "Janis", "Alvi", "Manual", "MAURICIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    t0 = PythonOperator(
        task_id="sync_catalogo_api_manual",
        python_callable=sync_catalogo_api_manual
    )
    
    t1 = PythonOperator(
        task_id="generate_reconciliation_csvs",
        python_callable=generate_reconciliation_csvs
    )
    
    t2 = PythonOperator(
        task_id="send_excluded_stock_0_manual",
        python_callable=send_excluded_stock_0_manual
    )

    t0 >> t1 >> t2
