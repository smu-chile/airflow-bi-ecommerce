from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable

from utils.slack_utils import dag_success_slack, dag_failure_slack

import pendulum

def create_and_load_s3(ds):
    import pandas as pd
    import numpy as np
    import os
    import io
    from io import StringIO

    exec_date = ds.replace("-", "/")
    prefix = f"promociones_vtex/{exec_date}/"
    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    curr_working_directory = os.getcwd()
    print(os.getcwd())

    with open(curr_working_directory+f"/dags/unimarc/sql/promociones_diarias.sql", "r") as query_file:
        promociones_query = query_file.read()
    
    promociones_query = promociones_query.replace("{ds}", ds)

    print("Base query:")
    print(promociones_query)

    pg_hook = PostgresHook(conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()

    df_promotions = pd.read_sql_query(promociones_query, pg_connection)
    buffer = io.StringIO()
    df_promotions.to_csv(buffer, header=True, index=False, encoding="utf-8")

    filename = f"promociones_vtex/{exec_date}/promociones_diarias.csv"

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
    
def truncate_and_load_postgres(ti):
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text

    filename = ti.xcom_pull(key="return_value", task_ids=["create_and_load_s3"])[0]

    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
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
    print(df.info())
    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    # Save to PostgreSQL:

    with engine.begin() as conn:
        conn.execute("TRUNCATE ecommdata.promociones_diarias") 
        df.to_sql(name="promociones_diarias",
                    con=conn,         
                    schema="ecommdata",         
                    if_exists='append',         
                    index=False,         
                    chunksize=20000,         
                    method='multi')

    print("Data loaded to Postgres: ecommdata.promociones_diarias")
    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_promociones_diarias',
    default_args=default_args,
    description="crear y cargar promociones que estan activas en workflow y VTEX",
    schedule="50 8,15 * * *",
    start_date=pendulum.datetime(2023, 6, 1, tz="America/Santiago"),
    catchup=False,
    tags=["ecommdata", "VTEX", "promociones", "unimarc", "workflow", "SERGIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:
    
    dag.doc_md = """
    construir y cargar promociones diarias de VTEX. \n
    Upsert en tabla ecommdata.promociones_diarias.
    """ 

    t0 = PythonOperator(
        task_id = "create_and_load_s3",
        python_callable = create_and_load_s3,
    )

    t1 = PythonOperator(
        task_id = "truncate_and_load_postgres",
        python_callable = truncate_and_load_postgres,
    )
    
    t0 >> t1