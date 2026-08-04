from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from utils.slack_utils import dag_failure_slack, dag_success_slack
import pendulum
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import pandas as pd
import sqlalchemy
import time

def fetch_products_page(session, url, headers, page, page_size):
    page_headers = headers.copy()
    page_headers["X-Janis-Page"] = str(page)
    page_headers["X-Janis-Page-Size"] = str(page_size)
    
    response = session.get(url, headers=page_headers, timeout=60)
    if response.status_code != 200:
        raise Exception(f"Fallo al consultar la API de Janis. Código: {response.status_code} | Respuesta: {response.text}")
    return page, response.json()

def sync_catalogo_api():
    # 1. Obtener credenciales de Janis Alvi
    janis_api_key = Variable.get("JANIS_ALVI_API_KEY")
    janis_api_secret = Variable.get("JANIS_ALVI_API_SECRET")
    janis_client = Variable.get("JANIS_ALVI_CLIENT")
    janis_base_url = Variable.get("JANIS_API_URL")
    
    if not all([janis_api_key, janis_api_secret, janis_client, janis_base_url]):
        raise Exception("Faltan variables de configuración de Janis (JANIS_API_URL) o exclusivas de Alvi (JANIS_ALVI_API_KEY, JANIS_ALVI_API_SECRET, JANIS_ALVI_CLIENT).")
        
    url = f"{janis_base_url.rstrip('/')}/products"
    
    print(f"Iniciando sincronización multihilo del catálogo desde la API de Janis: {url}")
    
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
    
    # 2. Bucle de paginación multihilo (lotes paralelos)
    while not stop_fetching:
        pages_to_fetch = list(range(page, page + batch_size))
        batch_results = {}
        
        print(f"Consultando lote de páginas en paralelo: {pages_to_fetch[0]} a {pages_to_fetch[-1]} (workers={max_workers})...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(fetch_products_page, session, url, headers, p, page_size): p 
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
                    
        # Procesamos los resultados en orden correlativo de página
        for p_num in sorted(pages_to_fetch):
            data = batch_results.get(p_num)
            
            if not data or len(data) == 0:
                print(f"Página vacía {p_num} recibida. Finalizando extracción.")
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
                    
                    # Un SKU se considera activo si tanto el producto como el SKU están activos en Janis
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
                            # Si no tiene tiendas asociadas, guardamos con id_tienda vacío y activo False
                            products_extracted.append({
                                "ref_id": sku_ref_id,
                                "id_tienda": "",
                                "activo": False,
                                "show_without_stock": bool(show_without_stock)
                            })
                            
        page += batch_size
        if not stop_fetching:
            time.sleep(0.1)
            
    print(f"Total registros a cargar: {len(products_extracted)}")
    
    if len(products_extracted) == 0:
        print("No se encontraron registros para cargar.")
        return
        
    df = pd.DataFrame(products_extracted)
    df = df.dropna(subset=['ref_id', 'id_tienda'])
    df = df.drop_duplicates(subset=['ref_id', 'id_tienda'])
    
    print(f"Total registros únicos a cargar: {len(df.index)}")
    
    # 3. Guardar en Postgres (TRUNCATE - INSERT)
    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)
    
    print("Truncando tabla ecommdata_alvi.productos_janis_api e insertando datos nuevos...")
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
        
    print("Sincronización finalizada exitosamente.")

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    "etl_sync_catalogo_janis_alvi",
    default_args=default_args,
    description="Sincronización del catálogo en tiempo real desde la API de Janis a Postgres",
    schedule_interval="30 4 * * *", # 4:30 AM
    start_date=pendulum.datetime(2026, 8, 1, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "Janis", "Alvi", "API", "MAURICIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    t0 = PythonOperator(
        task_id="sync_catalogo_api",
        python_callable=sync_catalogo_api
    )
