from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from utils.janis_alvi_utils import load_full_table_to_s3
from utils.slack_utils import dag_failure_slack, dag_success_slack

from datetime import datetime

import pendulum

def _brands_table_full_load(ti):
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text
    
    brands_file = ti.xcom_pull(key="return_value", task_ids=["load_full_table_to_s3"])[0]

    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+brands_file)
    if not s3_hook.check_for_key(brands_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % brands_file)

    brands_object = s3_hook.get_key(brands_file, bucket_name=s3_bucket)

    column_types = {
        "erp_id": "string",
        "name": "string",
    } 

    df = pd.read_csv(brands_object.get()["Body"], dtype=column_types)
    df = df[["id", "ref_id", "erp_id", "name", "date_created", "date_modified"]]  

    # Ensure correct datatypes:
    df["id"] = df["id"].astype("int", errors="ignore")
    df["date_created"] = df["date_created"].astype("int", errors="ignore")
    df["date_modified"] = df["date_modified"].astype("int", errors="ignore")
    
    # Fix date types and timezone:
    print("Fixing date datatype columns...")
    df["date_created"] = pd.to_datetime(df["date_created"], errors="ignore", unit="s")
    df["date_modified"] = pd.to_datetime(df["date_modified"], errors="ignore", unit="s")

    columns_rename = {
		"name": "nombre",
		"date_created": "fecha_creacion",
		"date_modified": "fecha_modificacion"
    }

    df = df.rename(columns=columns_rename)

    print("Number of records to be loaded: "+str(len(df.index)))

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    connection = engine.connect()
    truncate_query = "TRUNCATE TABLE ecommdata_alvi.marcas"
    connection.execute(text(truncate_query))
    connection.close()

    # Save to PostgreSQL:
    df.to_sql(name="marcas",
                con=engine,         
                schema="ecommdata_alvi",         
                if_exists='append',         
                index=False,         
                chunksize=20000,         
                method='multi')

    print("Data saved to PostgreSQL. Table: ecommdata_alvi.marcas")

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_marcas_alvi_full_load',
    default_args=default_args,
    description="Extracción y carga de tabla marcas desde Janis Replica Alvi hasta Workspace.",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2022, 4, 1, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "Janis", "ecommdata_alvi", "marcas", "Alvi", "MATIAS"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla de marcas de Janis Alvi.
    """ 
    t0 = PythonOperator(
        task_id = "load_full_table_to_s3",
        python_callable = load_full_table_to_s3,
        op_kwargs = {"table_name": "brands"}
    )

    t1 = PythonOperator(
        task_id = "brands_table_full_load",
        python_callable = _brands_table_full_load
    )

    t0 >> t1
