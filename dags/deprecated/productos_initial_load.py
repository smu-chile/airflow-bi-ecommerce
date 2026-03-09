from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from utils.janis_utils import load_full_table_to_s3

from datetime import datetime

def _create_initial_products_table(ti):
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text
    
    products_file = ti.xcom_pull(key="return_value", task_ids=["load_full_table_to_s3"])[0]

    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+products_file)
    if not s3_hook.check_for_key(products_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % products_file)

    products_object = s3_hook.get_key(products_file, bucket_name=s3_bucket)

    column_types = {
        "ref_id": "string",
        "ref_code": "string",
        "name": "string",
    } 

    df = pd.read_csv(products_object.get()["Body"], dtype=column_types)
    df = df[["id", "ref_id", "vtex_id", "ref_code", "name", "category", "brand", "date_created", "date_modified"]]  

    # Ensure correct datatypes:
    df["id"] = df["id"].astype("int", errors="ignore")
    df["vtex_id"] = df["vtex_id"].astype("int", errors="ignore")
    df["category"] = df["category"].astype("int", errors="ignore")
    df["brand"] = df["brand"].astype("int", errors="ignore")
    df["date_created"] = df["date_created"].astype("int", errors="ignore")
    df["date_modified"] = df["date_modified"].astype("int", errors="ignore")
    
    # Fix date types and timezone:
    print("Fixing date datatype columns...")
    df["date_created"] = pd.to_datetime(df["date_created"], errors="ignore", unit="s")
    df["date_created"] = df["date_created"].dt.tz_localize('UTC').dt.tz_convert('America/Santiago')
    df["date_modified"] = pd.to_datetime(df["date_modified"], errors="ignore", unit="s")
    df["date_modified"] = df["date_modified"].dt.tz_localize('UTC').dt.tz_convert('America/Santiago')

    columns_rename = {
        "ref_code": "material",
		"name": "nombre",
		"category": "id_categoria",
		"brand": "id_marca",
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
    truncate_query = "TRUNCATE TABLE ecommdata.productos"
    connection.execute(text(truncate_query))
    connection.close()

    # Save to PostgreSQL:
    df.to_sql(name="productos",
                con=engine,         
                schema="ecommdata",         
                if_exists='append',         
                index=False,         
                chunksize=20000,         
                method='multi')

    print("Data saved to PostgreSQL. Table: ecommdata.productos")

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_productos_initial_load',
    default_args=default_args,
    description="Extracción y carga de tabla productos desde Janis Replica hasta Workspace.",
    schedule=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["DATA", "Janis", "ecommdata", "productos"],
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla de productos de Janis.
    """ 
    t0 = PythonOperator(
        task_id = "load_full_table_to_s3",
        python_callable = load_full_table_to_s3,
        op_kwargs = {"table_name": "products"}
    )

    t1 = PythonOperator(
        task_id = "create_initial_products_table",
        python_callable = _create_initial_products_table
    )

    t0 >> t1
