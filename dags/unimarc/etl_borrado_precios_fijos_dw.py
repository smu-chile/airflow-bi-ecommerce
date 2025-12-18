from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.netezza_utils import load_custom_query_to_s3
from utils.slack_utils import dag_success_slack, dag_failure_slack

from datetime import datetime

import pendulum

def _delete_fixed_prices_from_vtex(ti, ds):
    import pandas as pd
    import sqlalchemy
    import requests
    
    price_file = ti.xcom_pull(key="return_value", task_ids=["load_custom_query_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: " + price_file)
    if not s3_hook.check_for_key(price_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % price_file)

    price_object = s3_hook.get_key(price_file, bucket_name=s3_bucket)
    column_types = {
        "REF_ID": "str",
        "PRICE_TABLE": "str"
    }

    df_precios_fijos = pd.read_csv(price_object.get()["Body"], dtype=column_types)
    print(f"Number of records found: {len(df_precios_fijos.index)}")

    column_names = {
        "REF_ID": "ref_id",
        "PRICE_TABLE": "id_lista_precios"
    }

    df_precios_fijos = df_precios_fijos.rename(columns=column_names)

    list_ref_id = tuple(df_precios_fijos['ref_id'].tolist())

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    print("Getting vtex_ids of products in WP with Fixed Price")
    query = f"""SELECT DISTINCT s.ref_id, s.vtex_id
                FROM ecommdata.skus s
                WHERE s.ref_id IN {list_ref_id};"""
    cursor.execute(query)
    results = cursor.fetchall()
    df_vtex_id = pd.DataFrame(results, columns=['ref_id', 'vtex_id'])
    cursor.close()
    pg_connection.close()

    merged_df = pd.merge(df_precios_fijos, df_vtex_id, on='ref_id', how='inner')

    X_VTEX_API_AppKey = Variable.get("X_VTEX_API_AppKey")
    X_VTEX_API_AppToken = Variable.get("X_VTEX_API_AppToken")
    accountName = Variable.get("VTEX_ACCOUNT_NAME")
    headers = {
        'Accept': "application/json",
        'Content-Type': "application/json",
        "X-VTEX-API-AppKey": X_VTEX_API_AppKey,
        "X-VTEX-API-AppToken": X_VTEX_API_AppToken,
        "Connection": "keep-alive"
    }
    for _, row in merged_df.iterrows():
        endpoint = f"https://api.vtex.com/{accountName}/pricing/prices/{row['vtex_id']}/fixed/{row['id_lista_precios']}"
        print(endpoint)
        response = requests.delete(endpoint, headers=headers)
        if response.status_code == 200:
            print(f"Deleted price for VTEX ID: {row['vtex_id']} and Price Table ID: {row['id_lista_precios']}")
        else:
            print(f"Failed to delete price for VTEX ID: {row['vtex_id']}. Status code: {response.status_code}")
            print(response.text)

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_borrado_precios_fijos_dw',
    default_args=default_args,
    description="Extracción y carga de tiendas desde DW hasta Workspace.",
    schedule_interval="30 9 * * *",
    start_date=pendulum.datetime(2022, 2, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["DW", "ecommdata", "VTEX", "PRECIOS FIJOS", "LISTA DE PRECIOS", "SERGIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    borrado de precios fijos caducados en Data Warehouse.
    """ 
    
    t0 = PythonOperator(
        task_id = "load_custom_query_to_s3",
        python_callable = load_custom_query_to_s3,
        op_kwargs = {
            "query": """SELECT DISTINCT LPAD(MATERIAL,18,'0') || '-' || CASE
                    WHEN UN_MEDIDA_VENTA = 'ST'::text THEN 'UN'
                    WHEN UN_MEDIDA_VENTA = 'CS'::text THEN 'CJ'
                    ELSE UN_MEDIDA_VENTA
            END AS REF_ID,
            TRANSLATE(NOMBRE_PROMOCION, ' ,.', '') AS PRICE_TABLE
        FROM NZ_BU.ECOMERCE.VW_WORKFLOW 
        WHERE ORGANIZACION_VENTAS = '1000'
        AND CANAL_DISTRIBUCION in ('10','70')
        AND ID_EVENTO <> '572'
        AND FECHA_INICIO_DE_PROMOCION <= TO_DATE('{{execution_date.strftime('%Y-%m-%d')}}', 'YYYY-MM-DD')
        AND FECHA_FIN_DE_PROMOCION >= TO_DATE('{{execution_date.strftime('%Y-%m-%d')}}', 'YYYY-MM-DD') 
        AND SKU_CANCEL = 'X'
        AND TIPO_PROMOCION = 4;
            """,
            "query_name": "borrado_precios_fijos",
        }
    )

    t1 = PythonOperator(
        task_id = "_delete_fixed_prices_from_vtex",
        python_callable = _delete_fixed_prices_from_vtex
    )

    t0 >> t1
