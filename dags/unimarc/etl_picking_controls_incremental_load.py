from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.janis_utils import load_full_table_to_s3
from utils.postgres_utils import get_max_updated_at_value
from utils.slack_utils import dag_success_slack, dag_failure_slack

from datetime import datetime

import pendulum

def _incremental_load_picking_control_table(ti):
    import numpy as np
    import pandas as pd
    
    picking_controls_file = ti.xcom_pull(key="return_value", task_ids=["load_full_table_to_s3"])[0]

    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+picking_controls_file)
    if not s3_hook.check_for_key(picking_controls_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % picking_controls_file)

    picking_controls_object = s3_hook.get_key(picking_controls_file, bucket_name=s3_bucket)

    df = pd.read_csv(picking_controls_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")

    # Select only relevant columns:
    df = df[["id",
            "store_id",
            "entity",
            "entity_id",
            "controller",
            "date_start",
            "date_end",
            "status",
            "date_created",
            "user_created",
            "date_modified",
            "user_modified"
            ]]

    # Rename columns to match workspace schema:
    columns_rename = {
        "id": "id",
        "store_id": "id_tienda",
        "entity": "entidad",
        "entity_id": "id_entidad",
        "controller": "controlador",
        "date_start": "fecha_inicio",
        "date_end": "fecha_fin",
        "status": "estado",
        "date_created": "fecha_creacion",
        "user_created": "creacion_usuario",
        "date_modified": "fecha_modificacion",
        "user_modified": "modificacion_usuario"
    }
    df = df.rename(columns=columns_rename)

    # Calculate extra columns:
    df["fecha_inicio"] = pd.to_datetime(df["fecha_inicio"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["fecha_fin"] = pd.to_datetime(df["fecha_fin"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["fecha_creacion"] = pd.to_datetime(df["fecha_creacion"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["fecha_modificacion_unixtime"] = df["fecha_modificacion"]
    df["fecha_modificacion"] = pd.to_datetime(df["fecha_modificacion"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")

    # Cast numeric values to int

    df = df.astype({
        "id": "int",
        "id_tienda": "string",
        "entidad": "string",
        "id_entidad": "int",
        "controlador": "int",
        "fecha_inicio": "string",
        "fecha_fin": "string",
        "estado": "bool",
        "fecha_creacion": "string",
        "fecha_modificacion": "string",
        "fecha_modificacion_unixtime": "int",
        "creacion_usuario": "bool",
        "modificacion_usuario": "bool"
    }, errors="ignore")

    columns = [
        "id_tienda",
        "entidad",
        "id_entidad",
        "controlador",
        "fecha_inicio",
        "fecha_fin",
        "estado",
        "fecha_creacion",
        "fecha_modificacion",
        "fecha_modificacion_unixtime",
        "creacion_usuario",
        "modificacion_usuario"
    ]

    df = df[[
        "id",
        "id_tienda",
        "entidad",
        "id_entidad",
        "controlador",
        "fecha_inicio",
        "fecha_fin",
        "estado",
        "fecha_creacion",
        "fecha_modificacion",
        "fecha_modificacion_unixtime",
        "creacion_usuario",
        "modificacion_usuario"
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
    print(f"Number of records to lo.ad: {str(len(fixed_records))}")
    incremental_query = """
        INSERT INTO ecommdata.control_picking (id,"""+columns_query+""") 
        VALUES ("""+values_query+""")
        ON CONFLICT (id)
        DO UPDATE SET ("""+columns_query+""") = ("""+excluded_query+""")
    """
    print(incremental_query)
    pg_hook = PostgresHook(conn_id="postgresql_conn")
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
    'etl_control_picking_incremental_load',
    default_args=default_args,
    description="Extracción y carga de tabla control picking desde Janis Unimarc Replica hasta Workspace.",
    schedule="30 * * * *",
    start_date=pendulum.datetime(2022, 7, 1, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "Janis", "ecommdata", "control_picking", "Unimarc", "MATIAS"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla de control picking de Janis Unimarc a Workspace. \n
    UPSERT incremental basado en fecha_modificacion_unixtime.
    """ 

    t0 = PythonOperator(
        task_id = "load_full_table_to_s3",
        python_callable = load_full_table_to_s3,
        op_kwargs = {
            "table_name": "picking_controls"
        }
    )

    t1 = PythonOperator(
        task_id = "incremental_load_picking_control_table",
        python_callable = _incremental_load_picking_control_table
    )

    t0 >> t1
