from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

from utils.janis_utils import _execute_mariadb_query

import pendulum

from datetime import datetime, timedelta

def orden_comentario_to_s3(ti,ds):
    import pandas as pd
    query = """
    
    """

    results, columns = _execute_mariadb_query(query)

    df = pd.DataFrame(results, columns=columns)

    df.info()
    return 

def orden_comentario_to_postgresql(ti):
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text

    filename = ti.xcom_pull(key="return_value", task_ids=["orden_comentario_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+filename)
    if not s3_hook.check_for_key(filename, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % filename)

    s_stock_object = s3_hook.get_key(filename, bucket_name=s3_bucket)

    df = pd.read_csv(s_stock_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")

    df.info()

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    with engine.begin() as conn:
        df.to_sql(name="orden_comentario",
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
    'etl_ordenes_comentarios',
    default_args=default_args,
    description="cargar comentarios de ordenes a tabla en postgresql",
    schedule_interval="0 10 * * *",
    start_date=pendulum.datetime(2024, 2, 19, tz="America/Santiago"),
    catchup=False,
    tags=["DATA","ordenes", "unimarc", "DW","janis", "PATRICIO"],
) as dag:
    

    dag.doc_md = """
    Carga comentarios de ordenes proviene el dato de janis-jackie \n
    guardar en S3, postgres.
    """ 

    t0 = PythonOperator(
        task_id='orden_comentario_to_s3',
        python_callable=orden_comentario_to_s3,
    )
    
    t1 = PythonOperator(
        task_id = "orden_comentario_to_postgresql",
        python_callable = orden_comentario_to_postgresql,
    )

    t0 >> t1