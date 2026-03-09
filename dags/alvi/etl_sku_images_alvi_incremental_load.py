from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.janis_alvi_utils import incremental_unixtime_load_table_s3
from utils.postgres_utils import get_max_updated_at_value
from utils.slack_utils import dag_failure_slack, dag_success_slack

from datetime import datetime

import pendulum

def _incremental_load_sku_images_table(ti):
    import numpy as np
    import pandas as pd
    
    sku_images_file = ti.xcom_pull(key="return_value", task_ids=["incremental_unixtime_load_table_to_s3"])[0]

    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
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
    df = df[["id",
            "sku",
            "image",
            "label",
            "ord",
            "date_scheduled",
            "user_created",
            "user_modified",
            "date_created",
            "date_modified",
            ]]

    # Rename columns to match workspace schema:
    columns_rename = {
        "id": "id",
        "sku": "id_sku_janis",
        "image": "imagen",
        "label": "etiqueta",
        "ord": "orden",
        "date_scheduled": "fecha_programada",
        "user_created": "creacion_usuario",
        "user_modified": "modificacion_usuario",
        "date_created": "fecha_creacion",
        "date_modified": "fecha_modificacion"
    }
    df = df.rename(columns=columns_rename)

    # Calculate extra columns:
    df["ref_id"] = ""
    df["nombre_producto"] = ""
    df["fecha_programada"] = pd.to_datetime(df["fecha_programada"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["fecha_creacion"] = pd.to_datetime(df["fecha_creacion"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["fecha_modificacion_unixtime"] = df["fecha_modificacion"]
    df["fecha_modificacion"] = pd.to_datetime(df["fecha_modificacion"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")

    # Cast numeric values to int

    df = df.astype({
        "id": "int",
        "id_sku_janis": "int",
        "imagen": "string",
        "etiqueta": "string",
        "orden": "int",
        "fecha_creacion": "string",
        "fecha_modificacion": "string",
        "fecha_programada": "string",
        "creacion_usuario": "bool",
        "modificacion_usuario": "bool"
    }, errors="ignore")

    columns = [
        "ref_id",
        "nombre_producto",
        "id_sku_janis",
        "imagen",
        "etiqueta",
        "orden",
        "fecha_programada",
        "creacion_usuario",
        "modificacion_usuario",
        "fecha_creacion",
        "fecha_modificacion",
        "fecha_modificacion_unixtime"
    ]

    df = df[["id",
        "ref_id",
        "nombre_producto",
        "id_sku_janis",
        "imagen",
        "etiqueta",
        "orden",
        "fecha_programada",
        "creacion_usuario",
        "modificacion_usuario",
        "fecha_creacion",
        "fecha_modificacion",
        "fecha_modificacion_unixtime"
    ]]
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
        INSERT INTO ecommdata_alvi.imagenes_sku (id,"""+columns_query+""") 
        VALUES ("""+values_query+""")
        ON CONFLICT (id)
        DO UPDATE SET ("""+columns_query+""") = ("""+excluded_query+""");
    """
    update_query = """
        UPDATE ecommdata_alvi.imagenes_sku isk
        SET ref_id = s.ref_id, nombre_producto = p.nombre
        FROM ecommdata_alvi.skus s
        LEFT JOIN ecommdata_alvi.productos p on s.ref_id = p.ref_id
        WHERE isk.id_sku_janis = s.id;
    """
    print(incremental_query)
    pg_hook = PostgresHook(conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.executemany(incremental_query, fixed_records)
    cursor.execute(update_query)
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
    'etl_imagenes_sku_alvi_incremental_load',
    default_args=default_args,
    description="Extracción y carga de tabla imagenes_sku desde Janis Alvi Replica hasta Workspace.",
    schedule="30 * * * *",
    start_date=pendulum.datetime(2022, 7, 1, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "Janis", "ecommdata_alvi", "imagenes_sku", "alvi", "MATIAS"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla de imagenes sku de Janis Alvi a Workspace. \n
    UPSERT incremental basado en fecha_modificacion_unixtime.
    """ 
    t0 = PythonOperator(
        task_id = "get_max_updated_at_date",
        python_callable = get_max_updated_at_value,
        op_kwargs = {
            "schema": "ecommdata_alvi",
            "table_name": "imagenes_sku", 
            "updated_at_field": "fecha_modificacion_unixtime",
            "is_unixtime": True
        }
    )

    t1 = PythonOperator(
        task_id = "incremental_unixtime_load_table_to_s3",
        python_callable = incremental_unixtime_load_table_s3,
        op_kwargs = {
            "table_name": "sku_images", 
            "xcom_updated_date_task_id": "get_max_updated_at_date", 
            "updated_column": "date_modified"
        }
    )

    t2 = PythonOperator(
        task_id = "incremental_load_sku_images_table",
        python_callable = _incremental_load_sku_images_table
    )

    t0 >> t1 >> t2
