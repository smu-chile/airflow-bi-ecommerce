from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

from utils.bigquery_utils import load_custom_bq_query_to_s3
from utils.slack_utils import dag_success_slack, dag_failure_slack

import pendulum

from datetime import datetime, timedelta


def minimos_exhibicion_to_postgresql(ti):
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text

    filename = ti.xcom_pull(key="return_value", task_ids=["extract_data_from_dw"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+filename)
    if not s3_hook.check_for_key(filename, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % filename)

    s_stock_object = s3_hook.get_key(filename, bucket_name=s3_bucket)

    column_types = {
    "SKU_PRODUCT": "str",
    "UMB": "str",
    "STORE_ID": "str", 
    "STORE": "str", 
    "ORG_IP": "str" , 
    "SKU_NM": "str" , 
    "MINIMO_EXHIBICION": "float", 
    "PLANOGRAMADO_FLG": "str",
    "STOCK_SEGURIDAD": "float",
    "IN_OUT" :"str",
    "CATALOGADO": "str",
    "PLANOGRAMADO_CANTIDAD": "str"
    }

    df = pd.read_csv(s_stock_object.get()["Body"], dtype=column_types)
    print(f"Number of records found: {len(df.index)}")
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")
    df.columns = ["material",
                  "umv",
                  "id_tienda",
                  "tienda",
                  "org_id",
                  "nombre_sku",
                  "minimo_exhibicion",
                  "planogramado",
                  "stock_seguridad",
                  "in_out",
                  "catalogado",
                  "planogramado_cant"]
    
    df['in_out'] = df['in_out'].map(lambda x: True if x == 'X' else False if x is not None else False)
    df['planogramado'] = df['planogramado'].map(lambda x: True if x == '1.0' else False if x is not None else False)
    df['catalogado'] = df['catalogado'].map(lambda x: True if x == '1' else False if x is not None else False)
    df['umv'] = df['umv'].replace('ST', 'UN')
    df.info()

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    with engine.begin() as conn:
        conn.execute("TRUNCATE ecommdata.minimos_exhibicion_in_out")
        df.to_sql(name="minimos_exhibicion_in_out",
                    con=conn,         
                    schema="ecommdata",         
                    if_exists='append',         
                    index=False,         
                    chunksize=20000,         
                    method='multi')

    print("Data saved to PostgreSQL.")

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
    schedule_interval="0 8 * * *",
    start_date=pendulum.datetime(2024, 2, 19, tz="America/Santiago"),
    catchup=False,
    tags=["DATA","minimos_exhibicion", "unimarc", "DW", "PATRICIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:
    

    dag.doc_md = """
    Carga minimos de exhibicion nivel sku tienda ultima semana, proviene el dato de DW \n
    guardar en S3, postgres.
    """ 
    t0 = PythonOperator(
        task_id = "extract_data_from_dw",
        python_callable = load_custom_bq_query_to_s3,
        op_kwargs = {
            "query": """
                SELECT h.SKU_PRODUCT,h.UMB, STORE_H.STORE_ID, STORE_H.STORE, STORE_H.ORG_IP , h.SKU_NM , s.MINIMO_EXHIBICION, s.PLANOGRAMADO_FLG,
                s.STOCK_SEGURIDAD, s.IN_OUT , s.CATALOGADO, s.PLANOGRAMADO_CANTIDAD
                FROM `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_OU_SKU` s
                LEFT JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_SKU_HIERARCHY` h
                ON s.SKU_KEY = h.SKU_KEY 
                LEFT JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_STORE_HIERARCHY` STORE_H
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
        task_id = "minimos_exhibicion_to_postgresql",
        python_callable = minimos_exhibicion_to_postgresql,
    )

    t0 >> t1