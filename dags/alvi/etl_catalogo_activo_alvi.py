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
               valid_from, valid_to 
        FROM janis_alvicl.price
    """
    results, columns = _execute_mariadb_query(query_maria)
    df_prices = pd.DataFrame(results, columns=columns)
    
    # Convert numerical columns
    num_cols = ['id_sku_janis', 'id_tienda_janis', 'skuminquantity', 'price', 'list_price', 'valid_from', 'valid_to']
    for col in num_cols:
        df_prices[col] = pd.to_numeric(df_prices[col], errors='coerce')
        
    # Filter by date validity
    current_time = int(time.time())
    df_prices = df_prices[
        (df_prices['valid_from'] <= current_time) & 
        ((df_prices['valid_to'] >= current_time) | (df_prices['valid_to'] == 0) | df_prices['valid_to'].isna())
    ]
    
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
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                specs = response.json()
                for spec in specs:
                    name = spec.get("Name", "")
                    if name == "Precio Lista" and spec.get("Value"):
                        vtex_precio_lista = str(spec["Value"][0])
                        tiene_precio = True
                    elif name == "Escala Precios" and spec.get("Value"):
                        vtex_escala_precio = str(spec["Value"][0])
                        tiene_escala = True
            else:
                print(f"Error {response.status_code} API VTEX para ID {vtex_id_str}")
        except Exception as e:
            print(f"Request failed para ID {vtex_id_str}: {e}")
            
        # Expected from Janis
        exp_precio = None
        exp_escala = None
        exp_igualadas = False
        if vtex_id_str in expected_data:
            exp_precio = expected_data[vtex_id_str]["precio_lista"]
            exp_escala = expected_data[vtex_id_str]["escala_precio"]
            exp_igualadas = expected_data[vtex_id_str]["tiendas_igualadas_janis"]
            
        # Comparison logic
        coincide_precio = False
        if exp_precio is not None and vtex_precio_lista is not None:
            coincide_precio = (vtex_precio_lista == exp_precio)
        elif exp_precio is None and vtex_precio_lista is None:
            coincide_precio = True
        
        coincide_escala = False
        if exp_escala and vtex_escala_precio:
            try:
                dict_vtex = json.loads(vtex_escala_precio)
                dict_exp = json.loads(exp_escala)
                coincide_escala = (dict_vtex == dict_exp)
            except:
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
