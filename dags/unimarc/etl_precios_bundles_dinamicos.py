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
                if resp.status_code in [400, 404]:
                    return resp
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {e}")
        
        if i < max_retries - 1:
            sleep_time = backoff_factor ** (i + 1)
            logging.info(f"Retrying in {sleep_time} seconds...")
            time.sleep(sleep_time)
            
    raise Exception(f"Max retries exceeded for {method} {url}")

def get_price_info(vtex_id):
    url = f"https://api.vtex.com/unimarc/pricing/prices/{vtex_id}"
    resp = retry_request('GET', url, headers=get_vtex_headers(), timeout=30)
    if resp.status_code == 200:
        data = resp.json()
        return {
            "basePrice": data.get("basePrice"),
            "listPrice": data.get("listPrice")
        }
    elif resp.status_code == 404:
        return None
    else:
        raise Exception(f"Failed to get price for {vtex_id}. Status: {resp.status_code}")

def update_dynamic_bundle_prices():
    # 1. Obtener bundles activos
    query = """
        SELECT 
            b.sku_bundle as vtex_id_bundle,
            COALESCE(b.discount_multiplier, 1.0) as discount_multiplier
        FROM ecommdata.sku_bundles_dinamicos b
        WHERE b.active = true
    """
    try:
        df_bundles = query_to_df(query)
    except Exception as e:
        logging.error(f"Error querying active bundles. Es posible que la tabla no exista: {e}")
        raise e
    
    if df_bundles.empty:
        logging.info("No active dynamic bundles found.")
        return
        
    headers = get_vtex_headers()
    
    for _, row in df_bundles.iterrows():
        vtex_id_bundle = str(row['vtex_id_bundle'])
        logging.info(f"Procesando Bundle Dinámico: {vtex_id_bundle}")
        
        try:
            # 2. Obtener componentes del kit (bundle)
            kit_url = f"https://unimarc.myvtex.com/api/catalog/pvt/stockkeepingunitkit?parentSkuId={vtex_id_bundle}"
            resp_kit = retry_request('GET', kit_url, headers=headers, timeout=30)
            if resp_kit.status_code != 200:
                raise Exception(f"Failed to get kit components for {vtex_id_bundle}")
                
            components = resp_kit.json()
            if not components:
                logging.error(f"ALERTA: El bundle {vtex_id_bundle} no tiene componentes en VTEX. Omitiendo.")
                continue
            
            total_base_price = 0.0
            total_list_price = 0.0
            has_valid_list_price = True
            
            components_to_update = []
            
            # 3. Iterar componentes, buscar sus precios y calcular totales
            for comp in components:
                sku_id = str(comp['StockKeepingUnitId'])
                quantity = int(comp.get('Quantity', 1))
                kit_rel_id = comp.get('id') or comp.get('Id')
                current_unit_price = comp.get('UnitPrice')
                
                prices = get_price_info(sku_id)
                if not prices or prices['basePrice'] is None:
                    raise Exception(f"El componente {sku_id} no tiene basePrice en VTEX.")
                    
                comp_base = float(prices['basePrice'])
                comp_list = prices.get('listPrice')
                
                total_base_price += (comp_base * quantity)
                
                if comp_list is not None:
                    total_list_price += (float(comp_list) * quantity)
                else:
                    has_valid_list_price = False
                
                components_to_update.append({
                    "sku_id": sku_id,
                    "kit_rel_id": kit_rel_id,
                    "quantity": quantity,
                    "current_price": current_unit_price,
                    "target_price": comp_base
                })
                
            # 4. Actualizar los componentes del kit
            for comp in components_to_update:
                sku_id = comp['sku_id']
                kit_rel_id = comp['kit_rel_id']
                target_price = comp['target_price']
                
                # Check with a small tolerance for floating point differences
                if comp['current_price'] is not None and abs(float(comp['current_price']) - target_price) < 0.01:
                    logging.info(f"Componente {sku_id} ya tiene el precio correcto ({target_price}).")
                    continue
                    
                logging.info(f"Actualizando componente {sku_id} en bundle {vtex_id_bundle} a {target_price}")
                
                if kit_rel_id:
                    del_url = f"https://unimarc.myvtex.com/api/catalog/pvt/stockkeepingunitkit/{kit_rel_id}"
                else:
                    del_url = f"https://unimarc.myvtex.com/api/catalog/pvt/stockkeepingunitkit?parentSkuId={vtex_id_bundle}&skuId={sku_id}"
                    
                resp_del = retry_request('DELETE', del_url, headers=headers, timeout=30)
                if resp_del.status_code not in [200, 204]:
                    raise Exception(f"Fallo crítico al borrar componente {sku_id}. RESP: {resp_del.text}")
                    
                post_url = "https://unimarc.myvtex.com/api/catalog/pvt/stockkeepingunitkit"
                payload = {
                    "StockKeepingUnitParent": int(vtex_id_bundle),
                    "StockKeepingUnitId": int(sku_id),
                    "Quantity": comp['quantity'],
                    "UnitPrice": target_price
                }
                resp_post = retry_request('POST', post_url, json=payload, headers=headers, timeout=30)
                if resp_post.status_code not in [200, 201]:
                    error_msg = f"URGENTE: Bundle {vtex_id_bundle} quedó INCOMPLETO. Falló inserción de {sku_id}."
                    logging.error(error_msg)
                    raise Exception(error_msg)
            
            # 5. Aplicar descuento y actualizar precio maestro del bundle
            discount_multiplier = float(row.get('discount_multiplier', 1.0))
            
            final_base_price = int(round(total_base_price * discount_multiplier))
            
            # Si hay descuento, el precio de lista (tachado) debería ser el precio original sin descuento
            if discount_multiplier < 1.0:
                final_list_price = int(round(total_base_price))
            else:
                final_list_price = int(round(total_list_price)) if has_valid_list_price else None
                
            put_url = f"https://api.vtex.com/unimarc/pricing/prices/{vtex_id_bundle}"
            
            put_payload = {
                "itemId": vtex_id_bundle,
                "basePrice": final_base_price,
                "costPrice": final_base_price,
                "listPrice": final_list_price
            }
                
            resp_put = retry_request('PUT', put_url, json=put_payload, headers=headers, timeout=30)
            if resp_put.status_code not in [200, 204]:
                 raise Exception(f"Fallo al actualizar precio final del bundle {vtex_id_bundle}. RESP: {resp_put.text}")
                 
            logging.info(f"Bundle {vtex_id_bundle} sincronizado. Multiplier: {discount_multiplier}. Base: {final_base_price}, List: {final_list_price}")
            
        except Exception as e:
            logging.error(f"Error procesando bundle dinámico {vtex_id_bundle}: {str(e)}")
            if "INCOMPLETO" in str(e):
                raise e
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
    'etl_precios_bundles_dinamicos',
    default_args=default_args,
    description="Sincroniza el precio de los bundles calculando dinámicamente el valor a partir de los precios actuales de sus componentes en VTEX.",
    schedule_interval="30 4 * * *",
    start_date=pendulum.datetime(2022, 1, 1, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "VTEX", "ecommdata", "precios", "bundles"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    **ETL Precios Bundles Dinámicos**
    
    A las 04:30 AM, este DAG revisa la tabla `ecommdata.sku_bundles_dinamicos`.
    Para cada bundle activo:
    1. Consulta a VTEX la estructura del kit (componentes y cantidades).
    2. Consulta los precios (basePrice, listPrice) de cada componente.
    3. Actualiza el valor interno del componente (UnitPrice) en el kit usando DELETE/POST para evitar bugs de VTEX.
    4. Setea el precio total maestro del bundle mediante la suma ponderada de sus componentes.
    """
    
    update_task = PythonOperator(
        task_id = "update_dynamic_bundle_prices",
        python_callable = update_dynamic_bundle_prices,
    )
