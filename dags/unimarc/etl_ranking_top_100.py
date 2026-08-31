from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pendulum
import requests
import pandas as pd
import json
import time
from io import BytesIO

# Configuración VTEX y Colección Fija (Carrusel Unimarc)
COLLECTION_ID = 10385

# from utils.slack_utils import dag_success_slack, dag_failure_slack

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

def get_vtex_headers():
    X_VTEX_API_AppKey = Variable.get("X_VTEX_API_AppKey", default_var="vtexappkey-unimarc-QTILMS")
    X_VTEX_API_AppToken = Variable.get("X_VTEX_API_AppToken", default_var="XKJEUCQRZQCQYRDBBENGIVIIOOFBMPRMVTJSYBRDSNLCPXEKWCVKFHVBCAPGWPFKYPVEHLCANKOUKFTRJUFJTHGNYIPXLKGCIBEZCZDLZVWVSARWGXXBKWQQIHFZVFOD")
    return {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        "X-VTEX-API-AppKey": X_VTEX_API_AppKey,
        "X-VTEX-API-AppToken": X_VTEX_API_AppToken,
    }

def get_collection_skus(id_coleccion, account_name, environment):
    """Obtiene el conjunto de SKUs actuales de una colección en VTEX"""
    skus = set()
    page = 1
    headers_api = get_vtex_headers()
    while True:
        url = f"https://{account_name}.{environment}.com.br/api/catalog/pvt/collection/{id_coleccion}/products?page={page}&pageSize=50"
        r = requests.get(url, headers=headers_api)
        if r.status_code == 200:
            data = r.json()
            items = data.get('Data', []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
            if not items:
                break
            for item in items:
                sku_id = item.get('SkuId') or item.get('id') or item.get('SKU')
                if sku_id:
                    try:
                        skus.add(str(int(float(sku_id))))
                    except (ValueError, TypeError):
                        pass
            page += 1
            if page > 100:
                break
        else:
            break
    return skus

def remove_skus_from_collection(vtex_ids, id_coleccion, account_name, environment):
    """Excluye la lista de SKUs obsoletos de la colección VTEX utilizando el endpoint oficial importexclude"""
    X_VTEX_API_AppKey = Variable.get("X_VTEX_API_AppKey", default_var="vtexappkey-unimarc-QTILMS")
    X_VTEX_API_AppToken = Variable.get("X_VTEX_API_AppToken", default_var="XKJEUCQRZQCQYRDBBENGIVIIOOFBMPRMVTJSYBRDSNLCPXEKWCVKFHVBCAPGWPFKYPVEHLCANKOUKFTRJUFJTHGNYIPXLKGCIBEZCZDLZVWVSARWGXXBKWQQIHFZVFOD")
    headers_multiform = {
        'X-VTEX-API-AppKey': X_VTEX_API_AppKey,
        'X-VTEX-API-AppToken': X_VTEX_API_AppToken,
        'Accept': 'application/json'
    }
    vtex_ids = list(set(vtex_ids))
    if not vtex_ids:
        return

    max_length = 1000
    product_batches = [vtex_ids[i:i + max_length] for i in range(0, len(vtex_ids), max_length)]

    for batch in product_batches:
        batch_list = [[int(vtex_id), '', '', ''] for vtex_id in batch]
        df = pd.DataFrame(batch_list, columns=['SKU', 'PRODUCT', 'SKUREFID', 'PRODUCTREFID'])
        df = df.drop_duplicates(subset=["SKU"])
        
        output = BytesIO()
        df.to_excel(output, index=False)
        output.seek(0)
        
        files = {'file': ('collection_products.xls', output, 'application/vnd.ms-excel')}
        url_exclude = f"https://{account_name}.{environment}.com.br/api/catalog/pvt/collection/{id_coleccion}/stockkeepingunit/importexclude"
        
        print(f"🧹 Enviando POST a {url_exclude} (importexclude) con {len(batch)} SKUs a excluir...")
        r = requests.post(url_exclude, headers=headers_multiform, files=files)
        time.sleep(5)
        print(f"Status importexclude: {r.status_code} | Response: {r.text[:300]}")

def load_collection(vtex_ids, id_coleccion, account_name, environment):
    """Envía la lista de SKUs a la colección VTEX especificada utilizando el endpoint oficial importinsert"""
    X_VTEX_API_AppKey = Variable.get("X_VTEX_API_AppKey", default_var="vtexappkey-unimarc-QTILMS")
    X_VTEX_API_AppToken = Variable.get("X_VTEX_API_AppToken", default_var="XKJEUCQRZQCQYRDBBENGIVIIOOFBMPRMVTJSYBRDSNLCPXEKWCVKFHVBCAPGWPFKYPVEHLCANKOUKFTRJUFJTHGNYIPXLKGCIBEZCZDLZVWVSARWGXXBKWQQIHFZVFOD")
    headers_multiform = {
        'X-VTEX-API-AppKey': X_VTEX_API_AppKey,
        'X-VTEX-API-AppToken': X_VTEX_API_AppToken,
        'Accept': 'application/json'
    }
    vtex_ids = list(dict.fromkeys(vtex_ids))
    if not vtex_ids:
        print(f"No hay SKUs válidos para cargar en la colección {id_coleccion}")
        return

    max_length = 1000
    product_batches = [vtex_ids[i:i + max_length] for i in range(0, len(vtex_ids), max_length)]

    for batch in product_batches:
        batch_list = [[int(vtex_id), '', '', ''] for vtex_id in batch]
        df = pd.DataFrame(batch_list, columns=['SKU', 'PRODUCT', 'SKUREFID', 'PRODUCTREFID'])
        df = df.drop_duplicates(subset=["SKU"])
        
        output = BytesIO()
        df.to_excel(output, index=False)
        output.seek(0)
        
        files = {'file': ('collection_products.xls', output, 'application/vnd.ms-excel')}
        url_load_collection = f"https://{account_name}.{environment}.com.br/api/catalog/pvt/collection/{id_coleccion}/stockkeepingunit/importinsert"
        
        collection_not_loaded = True
        retries = 0
        while collection_not_loaded and (retries < 5):
            print(f"Enviando POST a {url_load_collection} (importinsert) con {len(batch)} SKUs (intento {retries+1})")
            r = requests.post(url_load_collection, headers=headers_multiform, files=files)
            time.sleep(10)
            print(f"Status: {r.status_code} | Response: {r.text[:300]}")
            
            try:
                if r.text and r.text.strip():
                    try:
                        node = json.loads(r.text)
                        if isinstance(node, dict) and node.get('TotalProductsProcessed') == len(batch):
                            print(f"✅ COLECCIÓN {id_coleccion} CARGADA EXITOSAMENTE.")
                            collection_not_loaded = False
                        elif r.status_code in [200, 201, 202, 204]:
                            print(f"✅ COLECCIÓN {id_coleccion} CARGADA EXITOSAMENTE (Status {r.status_code}).")
                            collection_not_loaded = False
                        else:
                            retries += 1
                    except json.JSONDecodeError:
                        if r.status_code in [200, 201, 202, 204]:
                            print(f"✅ COLECCIÓN {id_coleccion} CARGADA EXITOSAMENTE (Status {r.status_code}).")
                            collection_not_loaded = False
                        else:
                            retries += 1
                elif r.status_code in [200, 201, 202, 204]:
                    print(f"✅ COLECCIÓN {id_coleccion} CARGADA EXITOSAMENTE (Status {r.status_code}).")
                    collection_not_loaded = False
                else:
                    retries += 1
            except Exception as err:
                if r.status_code in [200, 201, 202, 204]:
                    print(f"✅ COLECCIÓN {id_coleccion} CARGADA EXITOSAMENTE (Status {r.status_code}).")
                    collection_not_loaded = False
                else:
                    print(f"Error al procesar respuesta: {err}")
                    retries += 1

def sync_ranking_top_100_vtex_collection(**kwargs):
    """Obtiene los vtex_id del ranking top 100 y sincroniza la colección en VTEX."""
    account_name = Variable.get("VTEX_ACCOUNT_NAME", default_var="unimarc")
    environment = Variable.get("VTEX_ENV", default_var="vtexcommercestable")
    collection_id = int(Variable.get("RANKING_TOP_100_COLLECTION_ID", default_var=COLLECTION_ID))

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    query = """
        SELECT vtex_id
        FROM ecommdata.ranking_top_100
        WHERE vtex_id IS NOT NULL AND TRIM(vtex_id) <> ''
        ORDER BY ranking ASC;
    """
    records = pg_hook.get_records(query)
    
    # Preservar el orden según el ranking (1 al 100) sin duplicados
    vtex_ids_ordenados = []
    for r in records:
        if r[0]:
            try:
                sku_id = str(int(float(r[0])))
                if sku_id not in vtex_ids_ordenados:
                    vtex_ids_ordenados.append(sku_id)
            except (ValueError, TypeError):
                pass

    # Garantizar que el SKU 61690 (café) esté sí o sí en el Top 5 (posición 1)
    target_sku = "61690"
    if target_sku in vtex_ids_ordenados:
        vtex_ids_ordenados.remove(target_sku)
    vtex_ids_ordenados.insert(0, target_sku)

    print(f"📦 SKUs únicos a cargar en colección {collection_id}: {len(vtex_ids_ordenados)}")
    
    if not vtex_ids_ordenados:
        print(f"⚠️ No se encontraron SKUs válidos en ecommdata.ranking_top_100 para actualizar la colección {collection_id}.")
        return

    vtex_ids_set = set(vtex_ids_ordenados)

    # 1. Obtener SKUs actuales de la colección en VTEX
    skus_actuales = get_collection_skus(collection_id, account_name, environment)
    print(f"🔎 SKUs actualmente en la colección {collection_id}: {len(skus_actuales)}")

    # 2. Excluir SKUs obsoletos que ya no estén en el Top 100
    skus_a_excluir = skus_actuales - vtex_ids_set
    if skus_a_excluir:
        print(f"🧹 Excluyendo {len(skus_a_excluir)} SKUs obsoletos de la colección {collection_id}...")
        remove_skus_from_collection(list(skus_a_excluir), collection_id, account_name, environment)
    else:
        print(f"✨ No hay SKUs obsoletos a excluir en la colección {collection_id}.")

    # 3. Importar SKUs vigentes del Top 100 mediante importinsert.
    # Invertimos la lista para que la inserción secuencial de VTEX (LIFO) posicione el SKU #1 en el puesto 1.
    vtex_ids_para_cargar = list(reversed(vtex_ids_ordenados))
    print(f"🚀 Insertando {len(vtex_ids_para_cargar)} SKUs vigentes en la colección {collection_id} (orden de ranking en VTEX)...")
    load_collection(vtex_ids_para_cargar, collection_id, account_name, environment)


with DAG(
    'etl_ranking_top_100',
    default_args=default_args,
    description="Carga del ranking top 100 de productos con promociones vigentes en ecommdata.ranking_top_100 y sincronización con colección VTEX",
    schedule_interval="0 7 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["DATA", "ecommdata", "ranking", "promociones", "Unimarc", "vtex", "colecciones"],
    # on_success_callback=dag_success_slack,
    # on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Carga de ranking top 100 productos en promociones vigentes en la tabla ecommdata.ranking_top_100 y actualización de la colección VTEX Carrusel Unimarc (ID 10385).
    """

    t0 = PostgresOperator(
        task_id="load_ranking_top_100",
        postgres_conn_id="postgresql_conn",
        sql="sql/ranking_top_100.sql",
    )

    t1 = PythonOperator(
        task_id="sync_ranking_top_100_vtex_collection",
        python_callable=sync_ranking_top_100_vtex_collection,
    )

    t0 >> t1
