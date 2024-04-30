from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.janis_utils import incremental_unixtime_load_table_s3
from utils.postgres_utils import get_max_updated_at_value

from datetime import datetime

def _incremental_load_order_status_changes(ti):
    import pandas as pd
    import numpy as np
    import sqlalchemy
    
    order_status_changes_file = ti.xcom_pull(key="return_value", task_ids=["incremental_unixtime_load_table_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+order_status_changes_file)
    if not s3_hook.check_for_key(order_status_changes_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % order_status_changes_file)

    order_status_changes_object = s3_hook.get_key(order_status_changes_file, bucket_name=s3_bucket)

    df = pd.read_csv(order_status_changes_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")

    # Select only relevant columns:
    df = df[[
            "id",
            "order_id",
            "old_status",
            "new_status",
            "user_created",
            "date_created"
            ]]

    # Rename columns to match workspace schema:
    columns_rename = {
        "order_id": "id_orden",
        "old_status": "estado_anterior",
        "new_status": "estado_nuevo",
        "user_created": "creado_por",
        "date_created": "fecha_creacion_unixtime"
    }
    df = df.rename(columns=columns_rename)

    # Calculate extra columns:
    df["fecha_creacion"] = pd.to_datetime(df["fecha_creacion_unixtime"], unit="s")

    df = df.astype({
        "id": "int",
        "id_orden": "int",
        "estado_anterior": "int",
        "estado_nuevo": "int",
        "fecha_creacion_unixtime": "int",
        "fecha_creacion": "string"
    })

    columns = [
        "id_orden",
        "estado_anterior",
        "estado_nuevo",
        "creado_por",
        "fecha_creacion_unixtime",
        "fecha_creacion"
    ]

    print("Number of records to be loaded: "+str(len(df.index)))

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
        INSERT INTO ecommdata.orden_cambios_de_estado (id,"""+columns_query+""") 
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
    'etl_ordenes_janis_cambios_de_estado_incremental_load',
    default_args=default_args,
    description="Extracción y carga de tabla de cambios de estado de ordenes desde Janis Replica hasta Workspace.",
    schedule_interval="*/30 * * * *",
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["DATA", "Janis", "ecommdata", "orden_cambios_de_estado", "unimarc", "cyber", "MATIAS"],
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla de cambios de estado de ordenes de Janis a Workspace. \n
    INSERT incremental basado en fecha_creacion_unixtime.
    """ 
    t0 = PythonOperator(
        task_id = "get_max_updated_at_date",
        python_callable = get_max_updated_at_value,
        op_kwargs = {
            "schema": "ecommdata",
            "table_name": "orden_cambios_de_estado", 
            "updated_at_field": "fecha_creacion_unixtime",
            "is_unixtime": True
        }
    )

    t1 = PythonOperator(
        task_id = "incremental_unixtime_load_table_to_s3",
        python_callable = incremental_unixtime_load_table_s3,
        op_kwargs = {
            "table_name": "wms_order_status_changes", 
            "xcom_updated_date_task_id": "get_max_updated_at_date", 
            "updated_column": "date_created",
            "inclusive": True
        }
    )

    t2 = PythonOperator(
        task_id = "incremental_load_order_status_changes",
        python_callable = _incremental_load_order_status_changes
    )

    t0 >> t1 >> t2
