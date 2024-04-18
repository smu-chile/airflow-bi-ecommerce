from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.janis_utils import incremental_unixtime_load_table_s3
from utils.postgres_utils import get_max_updated_at_value

from datetime import datetime

def _incremental_load_attributes_table(ti):
    import numpy as np
    import pandas as pd
    
    attributes_file = ti.xcom_pull(key="return_value", task_ids=["incremental_unixtime_load_table_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+attributes_file)
    if not s3_hook.check_for_key(attributes_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % attributes_file)

    attributes_object = s3_hook.get_key(attributes_file, bucket_name=s3_bucket)

    df = pd.read_csv(attributes_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")

    # Select only relevant columns:
    df = df[["id",
            "name",
            "category",
            "update_pending",
            "update_error",
            "user_created",
            "user_modified",
            "date_created",
            "date_modified",
            ]]

    # Rename columns to match workspace schema:
    columns_rename = {
        "id": "id",
        "name": "nombre",
        "category": "id_categoria",
        "update_pending": "actualizacion_pendiente",
        "update_error": "error_actualizacion",
        "user_created": "creacion_usuario",
        "user_modified": "modificacion_usuario",
        "date_created": "fecha_creacion",
        "date_modified": "fecha_modificacion"
    }
    df = df.rename(columns=columns_rename)

    # Calculate extra columns:
    df["nombre_categoria"] = ""
    df["fecha_creacion"] = pd.to_datetime(df["fecha_creacion"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["fecha_modificacion_unixtime"] = df["fecha_modificacion"]
    df["fecha_modificacion"] = pd.to_datetime(df["fecha_modificacion"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")

    # Cast numeric values to int

    df = df.astype({
        "id_categoria": "int",
        "fecha_creacion": "string",
        "fecha_modificacion": "string",
        "actualizacion_pendiente": "bool",
        "error_actualizacion": "bool",
        "creacion_usuario": "bool",
        "modificacion_usuario": "bool"
    }, errors="ignore")

    columns = [
        "nombre",
        "id_categoria",
        "nombre_categoria",
        "actualizacion_pendiente",
        "error_actualizacion",
        "creacion_usuario",
        "modificacion_usuario",
        "fecha_creacion",
        "fecha_modificacion",
        "fecha_modificacion_unixtime"
    ]

    df = df[["id",
        "nombre",
        "id_categoria",
        "nombre_categoria",
        "actualizacion_pendiente",
        "error_actualizacion",
        "creacion_usuario",
        "modificacion_usuario",
        "fecha_creacion",
        "fecha_modificacion",
        "fecha_modificacion_unixtime"]]
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
    print(f"Number of records to lo.ad: {str(len(fixed_records))}")
    incremental_query = """
        BEGIN TRANSACTION;
        INSERT INTO ecommdata.atributos (id,"""+columns_query+""") 
        VALUES ("""+values_query+""")
        ON CONFLICT (id)
        DO UPDATE SET ("""+columns_query+""") = ("""+excluded_query+""");
        UPDATE ecommdata.atributos a
        SET nombre_categoria = c.n1
        FROM ecommdata.categorias c
        WHERE a.id_categoria = c.id;
        COMMIT;
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
    'etl_atributos_incremental_load',
    default_args=default_args,
    description="Extracción y carga de tabla atributos desde Janis Unimarc Replica hasta Workspace.",
    schedule_interval="30 * * * *",
    start_date=datetime(2022, 7, 1),
    catchup=False,
    max_active_runs = 1,
    tags=["DATA", "Janis", "ecommdata", "atributos", "Unimarc", "MATIAS"],
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla de atributos de Janis Unimarc a Workspace. \n
    UPSERT incremental basado en fecha_modificacion_unixtime.
    """ 
    t0 = PythonOperator(
        task_id = "get_max_updated_at_date",
        python_callable = get_max_updated_at_value,
        op_kwargs = {
            "schema": "ecommdata",
            "table_name": "atributos", 
            "updated_at_field": "fecha_modificacion_unixtime",
            "is_unixtime": True
        }
    )

    t1 = PythonOperator(
        task_id = "incremental_unixtime_load_table_to_s3",
        python_callable = incremental_unixtime_load_table_s3,
        op_kwargs = {
            "table_name": "attributes", 
            "xcom_updated_date_task_id": "get_max_updated_at_date", 
            "updated_column": "date_modified"
        }
    )

    t2 = PythonOperator(
        task_id = "incremental_load_attributes_table",
        python_callable = _incremental_load_attributes_table
    )

    t0 >> t1 >> t2
