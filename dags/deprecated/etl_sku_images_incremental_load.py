from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.janis_alvi_utils import load_custom_query_to_s3
from utils.postgres_utils import get_max_updated_at_value

from datetime import datetime

import pendulum 

def _incremental_unixtime_custom_query_load_to_s3(ti, ts):
    max_date_modified_unixtime = ti.xcom_pull(key="return_value", task_ids=["get_max_updated_at_date"])[0]
    max_date_modified_unixtime = 0 if max_date_modified_unixtime is None else max_date_modified_unixtime
    query_name = "sku_images"
    custom_query = f"""
        SELECT si.*, s.ref_id
        FROM janis_alvicl.sku_images AS si 
        LEFT JOIN janis_alvicl.skus AS s
        ON si.sku = s.id
        WHERE si.date_modified > {max_date_modified_unixtime};
    """
    file_name = load_custom_query_to_s3(ts, custom_query, query_name)
    return file_name

def _sku_images_incremental_load(ti):
    import numpy as np
    import pandas as pd
    
    sku_images_file = ti.xcom_pull(key="return_value", task_ids=["incremental_unixtime_custom_query_load_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+sku_images_file)
    if not s3_hook.check_for_key(sku_images_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % sku_images_file)

    sku_images_object = s3_hook.get_key(sku_images_file, bucket_name=s3_bucket)
    df = pd.read_csv(sku_images_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")

    # Select only relevant columns:
    df = df[[
        "id",
        "vtex_id",
        "ref_id",
        "image",
        "ord",
        "date_modified",
        "date_created"
    ]]

    # Rename columns to match workspace schema:
    columns_rename = {
        "id": "id",
        "vtex_id": "sku_vtex_id",
        "ref_id": "sku_ref_id",
        "image": "imagen",
        "ord": "ord",
        "date_modified": "fecha_modificacion",
        "date_created": "fecha_creacion"
    }
    df = df.rename(columns=columns_rename)

    # Calculate extra columns:
    df["fecha_creacion"] = pd.to_datetime(df["fecha_creacion"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["fecha_modificacion_unixtime"] = df["fecha_modificacion"]
    df["fecha_modificacion"] = pd.to_datetime(df["fecha_modificacion"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")

    df = df.astype({
        "id": "int",
        "sku_vtex_id": "int",
        "sku_ref_id": "str",
        "imagen": "str",
        "ord": "int",
        "fecha_modificacion": "str",
        "fecha_creacion": "str",
        "fecha_modificacion_unixtime": "int"
    }, errors="ignore")

    columns = [
        "sku_vtex_id",
        "sku_ref_id",
        "imagen",
        "ord",
        "fecha_modificacion",
        "fecha_creacion",
        "fecha_modificacion_unixtime"
    ]

    df = df[["id"]+columns]

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
    print(f"Number of records to load: {str(len(fixed_records))}")
    incremental_query = """
        INSERT INTO ecommdata_alvi.sku_imagenes (id,"""+columns_query+""") 
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
    print("Data loaded to Postgres in table ecommdata_alvi.sku_images")

    return


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_sku_imagenes_alvi_incremental_load',
    default_args=default_args,
    description="Extracción y carga de tabla sku_imagenes desde Janis Alvi Replica hasta Workspace.",
    schedule_interval="0 * * * *",
    start_date=pendulum.datetime(2022, 6, 16, tz="America/Santiago"),
    max_active_runs=1,
    catchup=False,
    tags=["DATA", "Janis", "ecommdata_alvi", "sku_imagenes", "alvi"],
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla de sku_imagenes de Janis Alvi a Workspace. \n
    UPSERT incremental basado en fecha_modificacion_unixtime.
    """ 
    t0 = PythonOperator(
        task_id = "get_max_updated_at_date",
        python_callable = get_max_updated_at_value,
        op_kwargs = {
            "schema": "ecommdata_alvi",
            "table_name": "sku_imagenes", 
            "updated_at_field": "fecha_modificacion_unixtime",
            "is_unixtime": True
        }
    )

    t1 = PythonOperator(
        task_id = "incremental_unixtime_custom_query_load_to_s3",
        python_callable = _incremental_unixtime_custom_query_load_to_s3
    )

    t2 = PythonOperator(
        task_id = "sku_images_incremental_load_",
        python_callable = _sku_images_incremental_load
    )



    t0 >> t1 >> t2
