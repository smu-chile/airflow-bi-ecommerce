from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from utils.slack_utils import dag_success_slack, dag_failure_slack
from datetime import datetime, timedelta
import pendulum
import requests
import time
import logging

def query_to_df(query):
    import pandas as pd
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query)
    column_names = [desc[0] for desc in cursor.description]
    results = cursor.fetchall()
    results = pd.DataFrame(results, columns=column_names)
    cursor.close()
    pg_connection.close()
    return results

def get_vtex_headers():
    return {
        'Accept': "application/json",
        'Content-Type': "application/json",
        "X-VTEX-API-AppKey": Variable.get("X_VTEX_API_AppKey"),
        "X-VTEX-API-AppToken": Variable.get("X_VTEX_API_AppToken")
    }

def retry_request(method, url, max_retries=8, **kwargs):
    backoff_factor = 2
    for i in range(max_retries):
        try:
            if method == 'GET':
                resp = requests.get(url, **kwargs)
            elif method == 'POST':
                resp = requests.post(url, **kwargs)
            elif method == 'PUT':
                resp = requests.put(url, **kwargs)
            elif method == 'DELETE':
                resp = requests.delete(url, **kwargs)
            else:
                raise ValueError("Method not supported")

            if resp.status_code in [200, 201, 204]:
                return resp
            else:
                logging.warning(f"VTEX API returned {resp.status_code} for {url}. Text: {resp.text}")
                if resp.status_code in [400, 404]: # No point in retrying bad requests
                    return resp
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {e}")
        
        if i < max_retries - 1:
            sleep_time = backoff_factor ** (i + 1)
            logging.info(f"Retrying in {sleep_time} seconds...")
            time.sleep(sleep_time)
            
    raise Exception(f"Max retries exceeded for {method} {url}")

def get_base_price(vtex_id):
    url = f"https://api.vtex.com/unimarc/pricing/prices/{vtex_id}"
    resp = retry_request('GET', url, headers=get_vtex_headers(), timeout=30)
    if resp.status_code == 200:
        data = resp.json()
        return data.get("basePrice")
    else:
        raise Exception(f"Failed to get price for {vtex_id}. Status: {resp.status_code}")

def update_bundle_prices():
    import pandas as pd
    
    # 1. Obtener mapeo de bundles activos y sus vtex_ids
    query = """
        SELECT 
            b.sku_original,
            s_orig.vtex_id as vtex_id_original,
            b.sku_bundle,
            s_bund.vtex_id as vtex_id_bundle
        FROM ecommdata.sku_bundles_retornables b
        INNER JOIN ecommdata.skus s_orig ON b.sku_original = s_orig.ref_id
        INNER JOIN ecommdata.skus s_bund ON b.sku_bundle = s_bund.ref_id
        WHERE b.active = true
    """
    df_bundles = query_to_df(query)
    
    if df_bundles.empty:
        logging.info("No active bundles found.")
        return
        
    headers = get_vtex_headers()
    
    for _, row in df_bundles.iterrows():
        vtex_id_original = str(row['vtex_id_original'])
        vtex_id_bundle = str(row['vtex_id_bundle'])
        
        logging.info(f"Procesando Bundle: {vtex_id_bundle} (Original: {vtex_id_original})")
        
        try:
            # 2. Get original base price
            original_base_price = get_base_price(vtex_id_original)
            if original_base_price is None:
                raise Exception(f"Original SKU {vtex_id_original} has no basePrice.")
                
            # 3. Get kit components
            kit_url = f"https://unimarc.myvtex.com/api/catalog/pvt/stockkeepingunitkit?parentSkuId={vtex_id_bundle}"
            resp_kit = retry_request('GET', kit_url, headers=headers, timeout=30)
            if resp_kit.status_code != 200:
                raise Exception(f"Failed to get kit components for {vtex_id_bundle}")
                
            components = resp_kit.json()
            if len(components) != 2:
                logging.error(f"ALERTA: El bundle {vtex_id_bundle} tiene {len(components)} componentes en lugar de 2. Omitiendo.")
                continue
                
            # Identificar envase y bebida dentro del kit
            envase_comp = None
            bebida_comp = None
            
            for comp in components:
                if str(comp['StockKeepingUnitId']) == vtex_id_original:
                    bebida_comp = comp
                else:
                    envase_comp = comp
                    
            if not envase_comp or not bebida_comp:
                logging.error(f"ALERTA: El bundle {vtex_id_bundle} no contiene el componente original {vtex_id_original} de forma clara. Omitiendo.")
                continue
                
            vtex_id_envase = str(envase_comp['StockKeepingUnitId'])
            
            # 4. Get envase base price
            envase_base_price = get_base_price(vtex_id_envase)
            if envase_base_price is None:
                raise Exception(f"Envase SKU {vtex_id_envase} has no basePrice.")
                
            # 5. Calcular precio residual
            target_bebida_price = original_base_price - envase_base_price
            
            if target_bebida_price < 0:
                logging.error(f"ALERTA CRÍTICA: El residual para {vtex_id_bundle} es negativo ({target_bebida_price}). Total={original_base_price}, Envase={envase_base_price}. Omitiendo.")
                continue
                
            # 6. Actualizar componentes si es necesario
            current_envase_price = envase_comp.get('UnitPrice')
            current_bebida_price = bebida_comp.get('UnitPrice')
            
            # Helper for updating a component
            def update_component(comp, new_price):
                sku_id = comp['StockKeepingUnitId']
                # Delete
                del_url = f"https://unimarc.myvtex.com/api/catalog/pvt/stockkeepingunitkit?parentSkuId={vtex_id_bundle}&skuId={sku_id}"
                resp_del = retry_request('DELETE', del_url, headers=headers, timeout=30)
                if resp_del.status_code not in [200, 204]:
                    raise Exception(f"Fallo crítico al hacer DELETE del componente {sku_id} en el bundle {vtex_id_bundle}. RESP: {resp_del.text}")
                    
                # Post
                post_url = "https://unimarc.myvtex.com/api/catalog/pvt/stockkeepingunitkit"
                payload = {
                    "StockKeepingUnitParent": int(vtex_id_bundle),
                    "StockKeepingUnitId": int(sku_id),
                    "Quantity": 1,
                    "UnitPrice": new_price
                }
                resp_post = retry_request('POST', post_url, json=payload, headers=headers, timeout=30)
                if resp_post.status_code not in [200, 201]:
                    # Lógica de desastre
                    error_msg = f"URGENTE: El Bundle ID {vtex_id_bundle} quedó INCOMPLETO. VTEX rechazó la inserción del componente {sku_id}. RESP: {resp_post.text}. Requiere revisión manual INMEDIATA."
                    logging.error(error_msg)
                    raise Exception(error_msg)
            
            # Evaluar envase
            if current_envase_price != envase_base_price:
                logging.info(f"Actualizando envase {vtex_id_envase} en bundle {vtex_id_bundle} de {current_envase_price} a {envase_base_price}")
                update_component(envase_comp, envase_base_price)
            else:
                logging.info(f"El envase {vtex_id_envase} ya tenía el precio correcto ({envase_base_price}). No se requiere actualización.")
                
            # Evaluar bebida
            if current_bebida_price != target_bebida_price:
                logging.info(f"Actualizando bebida {vtex_id_original} en bundle {vtex_id_bundle} de {current_bebida_price} a {target_bebida_price}")
                update_component(bebida_comp, target_bebida_price)
            else:
                logging.info(f"La bebida {vtex_id_original} ya tenía el precio correcto ({target_bebida_price}). No se requiere actualización.")
                
            # 7. Actualizar precio maestro del bundle
            put_url = f"https://api.vtex.com/unimarc/pricing/prices/{vtex_id_bundle}"
            put_payload = {
                "itemId": vtex_id_bundle,
                "listPrice": None,
                "basePrice": original_base_price,
                "costPrice": original_base_price
            }
            resp_put = retry_request('PUT', put_url, json=put_payload, headers=headers, timeout=30)
            if resp_put.status_code not in [200, 204]:
                 raise Exception(f"Fallo al actualizar el precio final del bundle {vtex_id_bundle}. RESP: {resp_put.text}")
                 
            logging.info(f"Bundle {vtex_id_bundle} sincronizado exitosamente con total {original_base_price}.")
            
        except Exception as e:
            logging.error(f"Error procesando bundle {vtex_id_bundle}: {str(e)}")
            # Dependiendo de si queremos que el DAG falle completo o siga con el resto de bundles
            if "INCOMPLETO" in str(e):
                raise e # Falla el DAG entero por criticidad para alertar a Slack
            continue 

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    'etl_precios_bundles_retornables',
    default_args=default_args,
    description="Sincroniza y cuadra el precio de los bundles retornables restando el costo del envase al precio de la bebida original.",
    schedule_interval="0 4 * * *",
    start_date=pendulum.datetime(2022, 1, 1, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "VTEX", "ecommdata", "precios", "bundles", "MAURICIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    **ETL Precios Bundles Retornables**
    
    A las 04:00 AM, este DAG revisa la tabla `ecommdata.sku_bundles_retornables`.
    Para cada bundle activo:
    1. Obtiene el precio total de la bebida original de VTEX.
    2. Obtiene el precio del envase de VTEX.
    3. Garantiza que los componentes del bundle sumen el total, ajustando el precio artificial de la bebida dentro del kit.
    4. Setea el precio total del bundle en VTEX para que haga match perfecto.
    
    Protecciones incorporadas:
    - Retries exponenciales para peticiones HTTP.
    - Aborto seguro si el bundle tiene != 2 componentes o si el precio diferencial da negativo.
    - Prevención de desarmado masivo mediante Try/Catch aislado.
    """ 
    
    update_task = PythonOperator(
        task_id = "update_bundle_prices",
        python_callable = update_bundle_prices,
    )
