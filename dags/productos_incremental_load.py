from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.janis_utils import incremental_load_table_s3
from utils.postgres_utils import get_max_updated_at_value
from utils.slack_utils import dag_success_slack, dag_failure_slack

from datetime import datetime

import pendulum

def _incremental_load_products_table(ti):
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text
    
    products_file = ti.xcom_pull(key="return_value", task_ids=["incremental_load_table_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
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
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
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

    columns = [
        "ref_id",
        "vtex_id",
        "material",
		"nombre",
		"id_categoria",
		"id_marca",
		"fecha_creacion",
		"fecha_modificacion"
    ]
    columns_query = ",".join(columns)
    excluded_query = ",".join(["EXCLUDED."+column for column in columns])
    values_query = "%s,"+",".join(["%s" for column in columns])
    df = df.fillna("NULL")
    records = list(df.to_records(index=False))
    
    # Change data types to native python types
    fixed_records = []
    for record in records:
        fixed_record = []
        for value in record:
            if isinstance(value, np.generic):
                fixed_record.append(value.item())
            elif value == "NULL":
                fixed_record.append(None)
            else:
                fixed_record.append(value)
        fixed_records.append(tuple(fixed_record))
    print(f"Number of records: {str(len(fixed_records))}")
    incremental_query = """
        INSERT INTO ecommdata.productos (id,"""+columns_query+""") 
        VALUES ("""+values_query+""")
        ON CONFLICT (id)
        DO UPDATE SET ("""+columns_query+""") = ("""+excluded_query+""") 
    """
    print(incremental_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.executemany(incremental_query, fixed_records)
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    print("Data loaded to Postgres")

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_productos_incremental_load',
    default_args=default_args,
    description="Extracción y carga de tabla productos desde Janis Replica hasta Workspace.",
    schedule_interval="0 3 * * *",
    start_date=pendulum.datetime(2022, 1, 1, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "Janis", "ecommdata", "productos", "MATIAS"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla de productos de Janis a Workspace.
    UPSERT incremental basado en fecha_modificacion.
    """ 
    t0 = PythonOperator(
        task_id = "get_max_updated_at_date",
        python_callable = get_max_updated_at_value,
        op_kwargs = {
            "schema": "ecommdata",
            "table_name": "productos", 
            "updated_at_field": "fecha_modificacion"
        }
    )

    t1 = PythonOperator(
        task_id = "incremental_load_table_to_s3",
        python_callable = incremental_load_table_s3,
        op_kwargs = {
            "table_name": "products", 
            "xcom_updated_date_task_id": "get_max_updated_at_date", 
            "updated_column": "date_modified",
            "from_unixtime": True
        }
    )

    t2 = PythonOperator(
        task_id = "incremental_load_products_table",
        python_callable = _incremental_load_products_table
    )

    t0 >> t1 >> t2
