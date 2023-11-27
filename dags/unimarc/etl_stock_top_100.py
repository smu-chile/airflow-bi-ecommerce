from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime, timedelta

import pendulum

def load_top_100_to_s3(ds):
    import pandas as pd
    import numpy as np
    import io
    import os
    from io import StringIO

    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"top_100/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()

    curr_working_directory = os.getcwd()
    print(os.getcwd())

    with open(curr_working_directory+f"/dags/unimarc/sql/stock_top100.sql", "r") as query_file:
        stock_top100_query = query_file.read()
    
    stock_top100_query = stock_top100_query.replace("{ds}", ds)

    print("Base query:")
    print(stock_top100_query)

    df_promotions = pd.read_sql_query(stock_top100_query, pg_connection)
    buffer = io.StringIO()
    df_promotions.to_csv(buffer, header=True, index=False, encoding="utf-8")

    filename = f"top_100/{exec_date}/top_100_stock_{date_aux}.csv"
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

def load_stock_top100_to_postgres(ti):
    import pandas as pd
    import numpy as np
    import sqlalchemy

    stock_top100_file = ti.xcom_pull(key="return_value", task_ids=["load_top_100_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+stock_top100_file)
    if not s3_hook.check_for_key(stock_top100_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % stock_top100_file)

    s_stock_object = s3_hook.get_key(stock_top100_file, bucket_name=s3_bucket)

    df = pd.read_csv(s_stock_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")
    print(df.info())

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    with engine.begin() as conn:
        df.to_sql(name="stock_top100",
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
    'etl_stock_top_100',
    default_args=default_args,
    description="Extracción de datos de tabla ventas_ecommerce_dw y posterior carga de stock de top 100 SKUs segmentados por tienda",
    schedule_interval="0 7 * * *",
    start_date=pendulum.datetime(2022, 8, 11, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "ecommdata", "stock", "Unimarc", "ventas_ecommerce_dw"],
) as dag:

    dag.doc_md = """
    Extracción de datos de tabla ventas_ecommerce_dw y posterior carga de stock de top 100 SKUs segmentados por tienda\n
    """ 
    t0 = PythonOperator(
        task_id = "load_top_100_to_s3",
        python_callable = load_top_100_to_s3,
    )

    t1 = PostgresOperator(
        task_id = "truncate_table",
        postgres_conn_id="postgresql_conn",
        sql="""
        truncate ecommdata.stock_top100
        """,
    )

    t2 = PythonOperator(
        task_id = "load_stock_top100_to_postgres",
        python_callable = load_stock_top100_to_postgres,
    )

    t0 >> t1 >> t2