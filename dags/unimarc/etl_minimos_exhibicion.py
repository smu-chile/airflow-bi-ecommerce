from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

from utils.netezza_utils import load_custom_query_to_s3

import pendulum

from datetime import datetime, timedelta

def minimos_exhibicion_to_s3(ti,ds):
    import pandas as pd
    import numpy as np
    import io
    from io import StringIO

    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"minimos_exhibicion_/{exec_date}/"

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    filename = ti.xcom_pull(key="return_value", task_ids=["extract_data_from_dw"])[0]

    print("Searching file: "+filename)
    if not s3_hook.check_for_key(filename, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % filename)

    ventas_sala_dw_object = s3_hook.get_key(filename, bucket_name=s3_bucket)

    column_types = {
    
    "SUPPLIER_RETAIL": "str",
    "NUESTRO_100": "str",
    "MARCA_PROPIA": "str"
    }

    df = pd.read_csv(ventas_sala_dw_object.get()["Body"], dtype=column_types)
    print(f"Number of records found: {len(df.index)}")

    if len(df.index) == 0:
        print("There are no new records to load. Task will exit as successfull.")
        return
    df.info()

    buffer = io.StringIO()
    df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"minimos_exhibicion_/{exec_date}/minimos_exhibicion_{date_aux}.csv"
    buffer.seek(0)
    print("se logro transformar el dataframe a un archivo .csv")
    print(f"con fecha {ds} y nombre de filename como {filename}")
    s3_hook.load_string(buffer.getvalue(),
                key=filename,
                bucket_name=s3_bucket,
                replace=True,
                encrypt=False)
    
    print(f"File load on S3: {prefix}")

    return filename

def minimos_exhibicion_to_postgresql():
    print("check")
    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_minimos_exhibicion',
    default_args=default_args,
    description="cargar minimos de exhibicion a tabla en postgresql",
    schedule_interval="30 7 * * *",
    start_date=pendulum.datetime(2024, 2, 19, tz="America/Santiago"),
    catchup=False,
    tags=["DATA","minimos_exhibicion", "unimarc", "DW", "PATRICIO"],
) as dag:
    

    dag.doc_md = """
    Carga minimos de exhibicion nivel sku tienda ultima semana, proviene el dato de DW \n
    guardar en S3, postgres.
    """ 
    t0 = PythonOperator(
        task_id = "extract_data_from_dw",
        python_callable = load_custom_query_to_s3,
        op_kwargs = {
            "query": """
                SELECT h.SKU_PRODUCT, STORE_H.STORE_ID, STORE_H."STORE", STORE_H.ORG_IP , h.SKU_NM , s.MINIMO_EXHIBICION, s.PLANOGRAMADO_FLG,
                s.STOCK_SEGURIDAD, s.IN_OUT , s.CATALOGADO, s.PLANOGRAMADO_CANTIDAD
                FROM DWC_SMU.SMU.VW_DIM_ou_sku s
                LEFT JOIN DWC_SMU.SMU.VW_DIM_SKU_HIERARCHY h
                ON s.SKU_KEY = h.SKU_KEY 
                LEFT JOIN DWC_SMU.SMU.VW_DIM_STORE_HIERARCHY STORE_H
                ON STORE_H.STORE_KEY = s.STORE_KEY  
                WHERE STORE_H.ORG_IP ='Unimarc'
                AND h.SKU_PRODUCT <> '000000000000000000'
                AND s.CATALOGADO = 1
                ORDER BY s.MINIMO_EXHIBICION desc
            """,
            "query_name": "minimos_exhibicion"
        },
        retries = 2,
        retry_delay = timedelta(minutes=1),
        execution_timeout = timedelta(minutes=60),
        pool = "backfill_pool"
    )

    t1 = PythonOperator(
        task_id='minimos_exhibicion_to_s3',
        python_callable=minimos_exhibicion_to_s3,
    )
    
    t2 = PythonOperator(
        task_id = "minimos_exhibicion_to_postgresql",
        python_callable = minimos_exhibicion_to_postgresql,
    )

    t0 >> t1 >> t2