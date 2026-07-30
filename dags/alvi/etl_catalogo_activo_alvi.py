from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from utils.slack_utils import dag_success_slack, dag_failure_slack
from utils.janis_alvi_utils import _execute_mariadb_query
import pendulum
import pandas as pd
import requests
import json
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

def format_unixtime(ts):
    if pd.isna(ts) or ts == 0:
        return '2100-12-31 23:59'
    try:
        dt = pendulum.from_timestamp(float(ts), tz="America/Santiago")
        return dt.strftime('%Y-%m-%d %H:%M')
    except:
        return '2100-12-31 23:59'

def format_janis_date(ts):
    if pd.isna(ts) or ts == 0:
        return '31-12-2100 23:59'
    try:
        dt = pendulum.from_timestamp(float(ts), tz="America/Santiago")
        return dt.strftime('%d-%m-%Y %H:%M')
    except:
        return '31-12-2100 23:59'

def safe_int(val, default=0):
    if pd.isna(val) or val is None:
        return default
    try:
        return int(float(val))
    except:
        return default

def _audit_vtex_prices(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    
    # 1. Fetch data from Postgres
    print("Obteniendo catálogo y skus desde Postgres...")
    df_catalog = pg_hook.get_pandas_df("SELECT vtex_id, ref_id FROM ecommdata_alvi.catalogo_activo_alvi WHERE categoria_valida IS TRUE AND vtex_id IS NOT NULL")
    if df_catalog.empty:
        print("No hay productos válidos en el catálogo activo para auditar.")
        return
        
    df_skus = pg_hook.get_pandas_df("SELECT id AS id_sku_janis, ref_id FROM ecommdata_alvi.skus")
    
    # 2. Fetch data from MariaDB (Janis)
    print("Obteniendo precios en vivo desde MariaDB (Janis)...")
    query_maria = """
        SELECT item_id as id_sku_janis, store_id as id_tienda_janis, 
               sku_min_quantity as skuminquantity, price, list_price, 
               cost_price, valid_from, valid_to 
        FROM janis_alvicl.price
    """
    results, columns = _execute_mariadb_query(query_maria)
    df_prices = pd.DataFrame(results, columns=columns)
    
    # Convert numerical columns
    num_cols = ['id_sku_janis', 'id_tienda_janis', 'skuminquantity', 'price', 'list_price', 'cost_price', 'valid_from', 'valid_to']
    for col in num_cols:
        df_prices[col] = pd.to_numeric(df_prices[col], errors='coerce')
        
    # Filter by date validity
    current_time = int(time.time())
    df_prices = df_prices[
        (df_prices['valid_from'] <= current_time) & 
        ((df_prices['valid_to'] >= current_time) | (df_prices['valid_to'] == 0) | df_prices['valid_to'].isna())
    ]
    
    # 2.5 Filter df_prices by active stores (status = 1)
    df_tiendas_db = pg_hook.get_pandas_df("SELECT id_janis FROM ecommdata_alvi.tiendas WHERE status = 1 AND id <> '1'")
    active_janis_store_ids = df_tiendas_db['id_janis'].dropna().unique()
    df_prices = df_prices[df_prices['id_tienda_janis'].isin(active_janis_store_ids)]
    
    # 3. Merge and Homologate
    df_active_prices = df_prices.merge(df_skus, on="id_sku_janis").merge(df_catalog, on="ref_id")
    
    # Pajaritos (id_tienda_janis == 9)
    df_pajaritos = df_active_prices[df_active_prices['id_tienda_janis'] == 9]
    
    # Huerfanos
    pajaritos_vtex_ids = df_pajaritos['vtex_id'].unique()
    df_huerfanos = df_active_prices[~df_active_prices['vtex_id'].isin(pajaritos_vtex_ids)]
    
    if not df_huerfanos.empty:
        scale_counts = df_huerfanos.groupby(['vtex_id', 'id_tienda_janis']).size().reset_index(name='cant_escalas')
        base_prices = df_huerfanos[df_huerfanos['skuminquantity'] == 1].groupby(['vtex_id', 'id_tienda_janis'])['price'].max().reset_index(name='precio_base')
        
        ranking = scale_counts.merge(base_prices, on=['vtex_id', 'id_tienda_janis'], how='left').fillna(0)
        ranking = ranking.sort_values(by=['vtex_id', 'cant_escalas', 'precio_base'], ascending=[True, False, False])
        ganadores = ranking.groupby('vtex_id').first().reset_index()[['vtex_id', 'id_tienda_janis']]
        
        df_huerfanos_ganadores = df_huerfanos.merge(ganadores, on=['vtex_id', 'id_tienda_janis'])
    else:
        df_huerfanos_ganadores = pd.DataFrame(columns=df_pajaritos.columns)
        
    df_expected = pd.concat([df_pajaritos, df_huerfanos_ganadores])
    df_expected['validfrom_str'] = df_expected['valid_from'].apply(format_unixtime)
    df_expected['validto_str'] = df_expected['valid_to'].apply(format_unixtime)
    
    # 3.5 Calcular tiendas igualadas en Janis
    df_active_prices['price_tuple'] = df_active_prices['skuminquantity'].astype(str) + '-' + df_active_prices['price'].astype(str) + '-' + df_active_prices['list_price'].astype(str)
    store_profiles = df_active_prices.sort_values(by=['vtex_id', 'id_tienda_janis', 'skuminquantity']).groupby(['vtex_id', 'id_tienda_janis'])['price_tuple'].apply(lambda x: '|'.join(x)).reset_index()
    profile_counts = store_profiles.groupby('vtex_id')['price_tuple'].nunique().reset_index(name='unique_profiles')
    profile_counts['tiendas_igualadas_janis'] = profile_counts['unique_profiles'] == 1
    
    # 4. Build Expected JSONs
    expected_data = {}
    expected_igualadas_map = {}
    if not df_expected.empty:
        grouped = df_expected.groupby('vtex_id')
        for vtex_id, group in grouped:
            base_record = group.sort_values(by='skuminquantity').iloc[0]
            fecha_inicio = base_record['validfrom_str']
            fecha_termino = base_record['validto_str']
            precio_lista = str(int(base_record['list_price']) if pd.notnull(base_record['list_price']) else 0)
            
            group_escalas = group[group['skuminquantity'] > 1].sort_values(by='skuminquantity')
            if not group_escalas.empty:
                escalas_dict = {}
                for idx, row in enumerate(group_escalas.itertuples()):
                    nivel_key = f"nivel{idx+1}"
                    escalas_dict[nivel_key] = {
                        "precio": str(int(row.price) if pd.notnull(row.price) else 0),
                        "cantidad": str(int(row.skuminquantity) if pd.notnull(row.skuminquantity) else 1)
                    }
                escala_json_obj = {
                    "escalas": [{
                        "fechaInicio": fecha_inicio,
                        "fechaTermino": fecha_termino,
                        **escalas_dict
                    }]
                }
                # Escala normalizada como JSON lineal
                json_escala_precio = json.dumps(escala_json_obj)
            else:
                json_escala_precio = None
                
            igualadas = profile_counts[profile_counts['vtex_id'] == vtex_id]['tiendas_igualadas_janis']
            is_igualadas = bool(igualadas.iloc[0]) if not igualadas.empty else False

            expected_data[str(vtex_id)] = {
                "precio_lista": precio_lista,
                "escala_precio": json_escala_precio,
                "tiendas_igualadas_janis": is_igualadas
            }
            expected_igualadas_map[str(vtex_id)] = is_igualadas
            
    unique_vtex_ids = df_catalog['vtex_id'].dropna().unique()
    print(f"Auditando {len(unique_vtex_ids)} productos en VTEX...")
    
    vtex_app_key = Variable.get("X_VTEX_ALVI_API_Appkey")
    vtex_app_token = Variable.get("X_VTEX_ALVI_API_Apptoken")
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-VTEX-API-AppKey": vtex_app_key,
        "X-VTEX-API-AppToken": vtex_app_token
    }
    
    def fetch_and_compare(vtex_id):
        vtex_id_str = str(vtex_id)
        url = f"https://alvicl.myvtex.com/api/catalog_system/pvt/products/{vtex_id_str}/specification"
        
        vtex_precio_lista = None
        vtex_escala_precio = None
        tiene_precio = False
        tiene_escala = False
        
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code == 200:
                specs = resp.json()
                for spec in specs:
                    if spec.get("Name") == "Precio Lista":
                        val = spec.get("Value")
                        vtex_precio_lista = val[0] if val and len(val) > 0 else None
                        tiene_precio = True
                    elif spec.get("Name") == "Escala Precios":
                        val = spec.get("Value")
                        raw_escala = val[0] if val and len(val) > 0 else None
                        tiene_escala = True
                        if raw_escala:
                            try:
                                vtex_escala_precio = json.dumps(json.loads(raw_escala))
                            except Exception:
                                vtex_escala_precio = raw_escala
        except Exception as e:
            print(f"Error consultando VTEX para product {vtex_id_str}: {e}")
            
        exp_info = expected_data.get(vtex_id_str, {})
        exp_precio = exp_info.get("precio_lista")
        exp_escala = exp_info.get("escala_precio")
        exp_igualadas = exp_info.get("tiendas_igualadas_janis", False)
        
        coincide_precio = (vtex_precio_lista == exp_precio)
        
        coincide_escala = False
        if exp_escala and vtex_escala_precio:
            try:
                coincide_escala = (json.loads(exp_escala) == json.loads(vtex_escala_precio))
            except Exception:
                coincide_escala = (vtex_escala_precio == exp_escala)
        elif not exp_escala and not vtex_escala_precio:
            coincide_escala = True
            
        return (
            vtex_precio_lista, vtex_escala_precio, 
            tiene_precio, tiene_escala,
            exp_precio, exp_escala, 
            coincide_precio, coincide_escala,
            exp_igualadas,
            vtex_id_str
        )

    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        for res in executor.map(fetch_and_compare, unique_vtex_ids):
            results.append(res)
            
    if results:
        # Build update and backup payloads for VTEX S3
        updates_data = []
        backups_data = []
        cnt_modificados = 0
        cnt_precio_act = 0
        cnt_escala_act = 0

        for res in results:
            (vtex_precio_lista, vtex_escala_precio, _, _, 
             exp_precio, exp_escala, coincide_precio, coincide_escala, 
             _, vtex_id_str) = res
            
            if not coincide_precio or not coincide_escala:
                cnt_modificados += 1
                update_specs = []
                backup_specs = []
                
                if not coincide_precio:
                    cnt_precio_act += 1
                    update_specs.append({
                        "Value": [exp_precio] if exp_precio else [],
                        "Id": 33,
                        "Name": "Precio Lista"
                    })
                    backup_specs.append({
                        "Value": [vtex_precio_lista] if vtex_precio_lista else [],
                        "Id": 33,
                        "Name": "Precio Lista"
                    })
                    
                if not coincide_escala:
                    cnt_escala_act += 1
                    update_specs.append({
                        "Value": [exp_escala] if exp_escala else [],
                        "Id": 28,
                        "Name": "Escala Precios"
                    })
                    backup_specs.append({
                        "Value": [vtex_escala_precio] if vtex_escala_precio else [],
                        "Id": 28,
                        "Name": "Escala Precios"
                    })
                    
                updates_data.append({
                    "vtex_id": vtex_id_str,
                    "url": f"https://alvicl.myvtex.com/api/catalog_system/pvt/products/{vtex_id_str}/specification",
                    "body": update_specs
                })
                
                backups_data.append({
                    "vtex_id": vtex_id_str,
                    "url": f"https://alvicl.myvtex.com/api/catalog_system/pvt/products/{vtex_id_str}/specification",
                    "body": backup_specs
                })

        # Build Janis price homogenization updates and backups (Categorized: insert & update)
        janis_inserts = []
        janis_updates = []
        
        janis_backups_updates = []
        
        cnt_janis_skus = 0

        # Load active stores mapping from Postgres (id is SAP store code, id_janis is Janis store code)
        df_tiendas = pg_hook.get_pandas_df("SELECT id AS id_tienda, id_janis FROM ecommdata_alvi.tiendas WHERE status = 1")
        df_tiendas = df_tiendas[(df_tiendas['id_tienda'] != '1') & df_tiendas['id_janis'].notnull()]

        for vtex_id, is_igualadas in expected_igualadas_map.items():
            if not is_igualadas:
                # Get the expected price scales from df_expected (the winning store profile)
                group = df_expected[df_expected['vtex_id'].astype(str) == str(vtex_id)].sort_values(by='skuminquantity')
                if group.empty:
                    continue
                
                # Build winner store tiers map: min_quantity -> row
                winner_tiers = {safe_int(row.skuminquantity): row for row in group.itertuples()}
                
                ref_id = group.iloc[0]['ref_id']
                parts = ref_id.split('-')
                material = parts[0]
                umv = parts[1] if len(parts) > 1 else 'UN'
                
                sku_had_mismatch = False
                
                # Fetch current prices in Janis for this product
                df_curr_prod = df_active_prices[df_active_prices['vtex_id'].astype(str) == str(vtex_id)]
                
                # Get the Janis store IDs that currently have prices for this SKU and are active
                active_janis_store_ids = set(df_tiendas['id_janis'].unique())
                product_stores = df_curr_prod[df_curr_prod['id_tienda_janis'].isin(active_janis_store_ids)]
                unique_stores = product_stores['id_tienda_janis'].unique()
                
                for store_janis_id in unique_stores:
                    df_curr_store = product_stores[product_stores['id_tienda_janis'] == store_janis_id]
                    store_tiers = {safe_int(row.skuminquantity): row for row in df_curr_store.itertuples()}
                    
                    store_code = df_tiendas[df_tiendas['id_janis'] == store_janis_id].iloc[0]['id_tienda']
                    
                    # Evaluate winner scales against store scales (Inserts & Updates)
                    for min_qty, winner_row in winner_tiers.items():
                        winner_price = safe_int(winner_row.price)
                        winner_list_price = safe_int(winner_row.list_price) if pd.notnull(winner_row.list_price) else winner_price
                        winner_cost_price = safe_int(winner_row.cost_price) if pd.notnull(winner_row.cost_price) else winner_list_price
                        
                        if min_qty not in store_tiers:
                            sku_had_mismatch = True
                            janis_inserts.append({
                                "IdSku": material,
                                "Store": str(store_code),
                                "Price": winner_price,
                                "MinQuantity": min_qty,
                                "MeasurementUnit": umv,
                                "ValidDateFrom": format_janis_date(winner_row.valid_from),
                                "ValidDateTo": format_janis_date(winner_row.valid_to),
                                "ListPrice": winner_list_price,
                                "CostPrice": winner_cost_price
                            })
                        else:
                            curr_row = store_tiers[min_qty]
                            curr_price = safe_int(curr_row.price)
                            curr_list_price = safe_int(curr_row.list_price) if pd.notnull(curr_row.list_price) else curr_price
                            if curr_price != winner_price or curr_list_price != winner_list_price:
                                sku_had_mismatch = True
                                janis_updates.append({
                                    "IdSku": material,
                                    "Store": str(store_code),
                                    "Price": winner_price,
                                    "MinQuantity": min_qty,
                                    "MeasurementUnit": umv,
                                    "ValidDateFrom": format_janis_date(winner_row.valid_from),
                                    "ValidDateTo": format_janis_date(winner_row.valid_to),
                                    "ListPrice": winner_list_price,
                                    "CostPrice": winner_cost_price
                                })
                                
                                curr_cost_price = safe_int(curr_row.cost_price) if pd.notnull(curr_row.cost_price) else curr_list_price
                                janis_backups_updates.append({
                                    "IdSku": material,
                                    "Store": str(store_code),
                                    "Price": curr_price,
                                    "MinQuantity": min_qty,
                                    "MeasurementUnit": umv,
                                    "ValidDateFrom": format_janis_date(curr_row.valid_from),
                                    "ValidDateTo": format_janis_date(curr_row.valid_to),
                                    "ListPrice": curr_list_price,
                                    "CostPrice": curr_cost_price
                                })

                if sku_had_mismatch:
                    cnt_janis_skus += 1

        # Chunking helper function
        def chunk_list(lst, n=500):
            for i in range(0, len(lst), n):
                yield lst[i:i + n]

        janis_updates_data = []
        for chunk in chunk_list(janis_inserts, 500):
            janis_updates_data.append({
                "tipo_accion": "insert",
                "descripcion": "Precios y escalas nuevas a insertar",
                "url": "https://janis.in/api/price",
                "body": chunk
            })
        for chunk in chunk_list(janis_updates, 500):
            janis_updates_data.append({
                "tipo_accion": "update",
                "descripcion": "Precios y escalas existentes con cambio de valor",
                "url": "https://janis.in/api/price",
                "body": chunk
            })

        janis_backups_data = []
        for chunk in chunk_list(janis_backups_updates, 500):
            janis_backups_data.append({
                "tipo_accion": "update",
                "descripcion": "Respaldo de precios y escalas antes del cambio",
                "url": "https://janis.in/api/price",
                "body": chunk
            })

        cnt_insert = len(janis_inserts)
        cnt_update = len(janis_updates)
        cnt_total_janis = cnt_insert + cnt_update

        if cnt_modificados > 0 or cnt_janis_skus > 0:
            from airflow.hooks.S3_hook import S3Hook
            try:
                s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
                s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
                
                logical_date = kwargs.get('logical_date') or kwargs.get('execution_date')
                if logical_date:
                    if hasattr(logical_date, 'in_timezone'):
                        dt_exec = logical_date.in_timezone("America/Santiago")
                    else:
                        dt_exec = pendulum.instance(logical_date).in_timezone("America/Santiago")
                else:
                    dt_exec = pendulum.now("America/Santiago")
                
                year = dt_exec.strftime('%Y')
                month = dt_exec.strftime('%m')
                day = dt_exec.strftime('%d')
                timestamp = dt_exec.strftime('%H%M%S')
                
                if cnt_modificados > 0:
                    update_payload = {
                        "skus_modificados": cnt_modificados,
                        "listas_de_precios_actualizadas": cnt_precio_act,
                        "escalas_de_precios_actualizadas": cnt_escala_act,
                        "data": updates_data
                    }
                    backup_payload = {
                        "skus_modificados": cnt_modificados,
                        "listas_de_precios_actualizadas": cnt_precio_act,
                        "escalas_de_precios_actualizadas": cnt_escala_act,
                        "data": backups_data
                    }
                    update_key = f"audit_vtex_prices/year={year}/month={month}/day={day}/update/vtex_updates_{timestamp}.json"
                    backup_key = f"audit_vtex_prices/year={year}/month={month}/day={day}/backup/vtex_backup_{timestamp}.json"
                    
                    s3_hook.load_string(json.dumps(update_payload), key=update_key, bucket_name=s3_bucket, replace=True)
                    s3_hook.load_string(json.dumps(backup_payload), key=backup_key, bucket_name=s3_bucket, replace=True)
                    print(f"✅ Se subieron los payloads de VTEX a S3.")
                    print(f"   Update VTEX: {update_key}")
                    print(f"   Backup VTEX: {backup_key}")
                    
                if cnt_janis_skus > 0:
                    janis_update_payload = {
                        "skus_modificados": cnt_janis_skus,
                        "precios_totales_a_enviar": cnt_total_janis,
                        "precios_insertados": cnt_insert,
                        "precios_actualizados": cnt_update,
                        "chunks_totales": len(janis_updates_data),
                        "data": janis_updates_data
                    }
                    janis_backup_payload = {
                        "skus_modificados": cnt_janis_skus,
                        "precios_totales_a_enviar": len(janis_backups_updates),
                        "precios_actualizados": len(janis_backups_updates),
                        "chunks_totales": len(janis_backups_data),
                        "data": janis_backups_data
                    }
                    janis_update_key = f"audit_janis_prices/year={year}/month={month}/day={day}/update/janis_updates_{timestamp}.json"
                    janis_backup_key = f"audit_janis_prices/year={year}/month={month}/day={day}/backup/janis_backup_{timestamp}.json"
                    
                    s3_hook.load_string(json.dumps(janis_update_payload), key=janis_update_key, bucket_name=s3_bucket, replace=True)
                    s3_hook.load_string(json.dumps(janis_backup_payload), key=janis_backup_key, bucket_name=s3_bucket, replace=True)
                    print(f"✅ Se subieron los payloads de Janis a S3.")
                    print(f"   Update Janis: {janis_update_key}")
                    print(f"   Backup Janis: {janis_backup_key}")
                    
            except Exception as e:
                print(f"❌ Error al subir los archivos a S3: {e}")

        from psycopg2.extras import execute_values
        update_query = """
            UPDATE ecommdata_alvi.catalogo_activo_alvi AS c
            SET vtex_precio_lista = v.vtex_precio_lista,
                vtex_escala_precio = v.vtex_escala_precio,
                tiene_precio_lista = CAST(v.tiene_precio_lista AS boolean),
                tiene_escala = CAST(v.tiene_escala AS boolean),
                calculated_precio_lista = v.calculated_precio_lista,
                calculated_escala_precio = v.calculated_escala_precio,
                coincide_precio_lista = CAST(v.coincide_precio_lista AS boolean),
                coincide_escala = CAST(v.coincide_escala AS boolean),
                tiendas_igualadas_janis = CAST(v.tiendas_igualadas_janis AS boolean)
            FROM (VALUES %s) AS v(
                vtex_precio_lista, vtex_escala_precio, tiene_precio_lista, tiene_escala,
                calculated_precio_lista, calculated_escala_precio, coincide_precio_lista, coincide_escala,
                tiendas_igualadas_janis,
                vtex_id
            )
            WHERE c.vtex_id = v.vtex_id;
        """
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        execute_values(cursor, update_query, results)
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Se actualizaron {len(results)} registros de auditoría en la tabla.")

with DAG(
    'etl_catalogo_activo_alvi',
    default_args=default_args,
    description="Actualiza la tabla de catálogo activo de Alvi y audita los precios usando MariaDB y VTEX.",
    schedule_interval="0 6 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["DATA", "Alvi", "ecommdata_alvi", "catalogo", "MATIAS", "auditoria"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    ## Catálogo Activo Alvi y Auditoría de Precios
    1. Cruza los productos únicos de `ecommdata_alvi.lista8` con `ecommdata_alvi.productos` 
    y `ecommdata_alvi.categorias` para obtener el catálogo activo.
    2. Ejecuta un script en Python que consulta a la BD MariaDB de Janis por los precios en vivo, procesando la lógica de tienda ganadora usando Pandas en memoria.
    3. Consulta la API de VTEX para verificar los precios actuales.
    4. Guarda los resultados de la auditoría actualizando la misma tabla del catálogo.
    """ 

    t1 = PostgresOperator(
        task_id="truncate_and_load_catalogo_activo",
        postgres_conn_id="postgresql_conn",
        sql="sql/catalogo_activo_alvi.sql",
    )
    
    t2 = PythonOperator(
        task_id="audit_vtex_prices",
        python_callable=_audit_vtex_prices,
    )

    t1 >> t2
