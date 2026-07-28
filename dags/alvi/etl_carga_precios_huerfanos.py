from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import pendulum
from datetime import datetime, timedelta
import pandas as pd
import requests
import json
from concurrent.futures import ThreadPoolExecutor

from utils.slack_utils import dag_success_slack, dag_failure_slack

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

def process_orphan_prices(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    
    # 1. Fetch data from SQL
    with open("/opt/airflow/dags/alvi/sql/precios_huerfanos.sql", "r") as f:
        sql_query = f.read()
    
    df = pg_hook.get_pandas_df(sql_query)
    
    if df.empty:
        print("No orphan prices found.")
        return
        
    unique_vtex_ids = df['vtex_product_id'].dropna().unique()
    print(f"Encontrados {len(unique_vtex_ids)} productos huérfanos únicos.")
    
    vtex_app_key = Variable.get("X_VTEX_ALVI_API_Appkey")
    vtex_app_token = Variable.get("X_VTEX_ALVI_API_Apptoken")
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-VTEX-API-AppKey": vtex_app_key,
        "X-VTEX-API-AppToken": vtex_app_token
    }
    
    def check_vtex_spec(vtex_id):
        url = f"https://alvicl.myvtex.com/api/catalog_system/pvt/products/{vtex_id}/specification"
        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                specs = response.json()
                has_escala = False
                has_precio_lista = False
                for spec in specs:
                    name = spec.get("Name", "")
                    if name == "Escala Precios":
                        has_escala = True
                    if name == "Precio Lista":
                        has_precio_lista = True
                return vtex_id, has_escala, has_precio_lista
            else:
                print(f"Error {response.status_code} for vtex_id {vtex_id}: {response.text}")
                return vtex_id, False, False
        except Exception as e:
            print(f"Request failed for {vtex_id}: {e}")
            return vtex_id, False, False

    spec_results = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        for result in executor.map(check_vtex_spec, unique_vtex_ids):
            spec_results[result[0]] = {"has_escala": result[1], "has_precio_lista": result[2]}
            
    # Valid products are those without BOTH specifications
    valid_vtex_ids = [vid for vid, res in spec_results.items() if not res["has_escala"] and not res["has_precio_lista"]]
    print(f"Productos válidos tras check de especificaciones: {len(valid_vtex_ids)}")
    
    df_valid = df[df['vtex_product_id'].isin(valid_vtex_ids)]
    if df_valid.empty:
        print("Todos los productos ya tienen sus especificaciones de precio en VTEX.")
        return
        
    vtex_insert_records = []
    janis_insert_records = []
    
    grouped = df_valid.groupby('vtex_product_id')
    for vtex_id, group in grouped:
        # Base list price and dates (using the base record, usually skuminquantity = 1)
        base_record = group.sort_values(by='skuminquantity').iloc[0]
        fecha_inicio = str(base_record['validfrom'])[:16]
        fecha_termino = str(base_record['validto'])[:16]
        precio_lista = str(int(base_record['listprice']) if pd.notnull(base_record['listprice']) else 0)
        
        # Only consider scales for quantities > 1
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
        
        vtex_insert_records.append({
            "vtex_product_id": vtex_id,
            "json_precio_lista": precio_lista,
            "json_escala_precio": json_escala_precio
        })
        
        for row in group.itertuples():
            janis_insert_records.append((
                row.id,
                row.store,
                row.skurefid,
                row.skuminquantity,
                row.price,
                row.listprice,
                row.costprice,
                row.validfrom,
                row.validto,
                row.locked,
                row.updatepending,
                row.active
            ))
            
    # Insert VTEX specs table
    df_vtex = pd.DataFrame(vtex_insert_records)
    df_vtex = df_vtex.where(pd.notnull(df_vtex), None)
    if not df_vtex.empty:
        records_vtex = list(df_vtex.itertuples(index=False, name=None))
        insert_query_vtex = """
            INSERT INTO ecommdata_alvi.precios_huerfanos_a_vtex 
            (vtex_product_id, json_precio_lista, json_escala_precio)
            VALUES (%s, %s, %s)
        """
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.executemany(insert_query_vtex, records_vtex)
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Insertados {len(records_vtex)} registros en precios_huerfanos_a_vtex")
        
    # Insert Janis homologation table
    df_janis = pd.DataFrame(janis_insert_records, columns=[
        "id", "store", "skurefid", "skuminquantity", "price", "listprice", "costprice",
        "validfrom", "validto", "locked", "updatepending", "active"
    ])
    df_janis = df_janis.where(pd.notnull(df_janis), None)
    
    if not df_janis.empty:
        records_janis = list(df_janis.itertuples(index=False, name=None))
        insert_query_janis = """
            INSERT INTO ecommdata_alvi.precios_huerfanos
            (id, store, "skuRefId", "skuMinQuantity", price, "listPrice", "costPrice", "validFrom", "validTo", locked, updatepending, active)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.executemany(insert_query_janis, records_janis)
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Insertados {len(records_janis)} registros en precios_huerfanos")

with DAG(
    'etl_carga_precios_huerfanos',
    default_args=default_args,
    description="Carga de tabla de precios para productos huérfanos que no están en Pajaritos (Alvi).",
    schedule_interval="30 7 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["precios", "alvi", "huerfanos"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Carga de tabla de precios para productos huérfanos en Alvi.
    Detecta productos que no están en la lista8 de Pajaritos (SAP ID 3092), elige la tienda ganadora según reglas comerciales.
    Luego revisa en VTEX si el producto carece de 'Escala Precios' o 'Precio Lista'. Si faltan, genera los JSON para inyectar en VTEX
    e inyecta la homologación a las tiendas activas excluyendo Pajaritos en `ecommdata_alvi.precios_huerfanos`.
    """
    
    t0 = PostgresOperator(
        task_id="truncate_table_precios_huerfanos",
        postgres_conn_id="postgresql_conn",
        sql="""
        TRUNCATE ecommdata_alvi.precios_huerfanos;
        """,
    )

    t0_5 = PostgresOperator(
        task_id="truncate_table_precios_huerfanos_a_vtex",
        postgres_conn_id="postgresql_conn",
        sql="""
        CREATE TABLE IF NOT EXISTS ecommdata_alvi.precios_huerfanos_a_vtex (
            vtex_product_id VARCHAR,
            json_precio_lista TEXT,
            json_escala_precio TEXT,
            fecha_insercion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        TRUNCATE ecommdata_alvi.precios_huerfanos_a_vtex;
        """,
    )

    t1 = PythonOperator(
        task_id="process_orphan_prices",
        python_callable=process_orphan_prices,
    )
    
    [t0, t0_5] >> t1
