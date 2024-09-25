from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.janis_utils import incremental_unixtime_load_table_s3
from utils.postgres_utils import get_max_updated_at_value

from datetime import datetime

def _incremental_load_invoices_table(ti):
    import numpy as np
    import pandas as pd
    
    invoices_file = ti.xcom_pull(key="return_value", task_ids=["incremental_unixtime_load_table_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+invoices_file)
    if not s3_hook.check_for_key(invoices_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % invoices_file)

    invoices_object = s3_hook.get_key(invoices_file, bucket_name=s3_bucket)

    df = pd.read_csv(invoices_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")

    # Select only relevant columns:
    df = df[["id",
            "order_id",
            "number",
            "amount",
            "date_invoiced",
            "url",
            "invoice_key",
            "tracking_number",
            "type",
            "invoice_extra_info",
            "status",
            "user_created",
            "user_modified",
            "date_created",
            "date_modified"
            ]]

    # Rename columns to match workspace schema:
    columns_rename = {
        "id": "id",
        "order_id": "id_orden",
        "number": "numero",
        "amount": "monto",
        "date_invoiced": "fecha_facturacion",
        "url": "url",
        "invoice_key": "llave_factura",
        "tracking_number": "numero_seguimiento",
        "type": "tipo",
        "invoice_extra_info": "info_extra_factura",
        "status": "estado",
        "user_created": "creacion_usuario",
        "user_modified": "modificacion_usuario",
        "date_created": "fecha_creacion",
        "date_modified": "fecha_modificacion"
    }
    df = df.rename(columns=columns_rename)

    # Calculate extra columns:
    df["fecha_facturacion"] = pd.to_datetime(df["fecha_facturacion"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["fecha_creacion"] = pd.to_datetime(df["fecha_creacion"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["fecha_modificacion_unixtime"] = df["fecha_modificacion"]
    df["fecha_modificacion"] = pd.to_datetime(df["fecha_modificacion"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["tipo"] = df["tipo"].astype("str")
    df.loc[df["tipo"]=="1.0", "tipo"] = "factura"
    df.loc[df["tipo"]=="1.0", "tipo"] = "boleta"


    # Cast numeric values to int

    df = df.astype({
        "id": "int",
        "id_orden": "int",
        "numero": "int",
        "monto": "int",
        "fecha_facturacion": "string",
        "url": "string",
        "llave_factura": "string",
        "numero_seguimiento": "int",
        "tipo": "string",
        "info_extra_factura": "string",
        "estado": "bool",
        "creacion_usuario": "bool",
        "modificacion_usuario": "bool",
        "fecha_creacion": "string",
        "fecha_modificacion": "string"
    }, errors="ignore")

    columns = [
        "id_orden",
        "numero",
        "monto",
        "fecha_facturacion",
        "url",
        "llave_factura",
        "numero_seguimiento",
        "tipo",
        "info_extra_factura",
        "estado",
        "creacion_usuario",
        "modificacion_usuario",
        "fecha_creacion",
        "fecha_modificacion",
        "fecha_modificacion_unixtime"
    ]

    df = df[["id",
        "id_orden",
        "numero",
        "monto",
        "fecha_facturacion",
        "url",
        "llave_factura",
        "numero_seguimiento",
        "tipo",
        "info_extra_factura",
        "estado",
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
        INSERT INTO ecommdata.facturas (id,"""+columns_query+""") 
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
    'etl_facturas_incremental_load',
    default_args=default_args,
    description="Extracción y carga de tabla facturas desde Janis Unimarc Replica hasta Workspace.",
    schedule_interval="30 * * * *",
    start_date=datetime(2022, 7, 1),
    catchup=False,
    max_active_runs = 1,
    tags=["DATA", "Janis", "ecommdata", "facturas", "Unimarc", "MATIAS"],
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla de facturas de Janis Unimarc a Workspace. \n
    UPSERT incremental basado en fecha_modificacion_unixtime.
    """ 
    t0 = PythonOperator(
        task_id = "get_max_updated_at_date",
        python_callable = get_max_updated_at_value,
        op_kwargs = {
            "schema": "ecommdata",
            "table_name": "facturas", 
            "updated_at_field": "fecha_modificacion_unixtime",
            "is_unixtime": True
        }
    )

    t1 = PythonOperator(
        task_id = "incremental_unixtime_load_table_to_s3",
        python_callable = incremental_unixtime_load_table_s3,
        op_kwargs = {
            "table_name": "invoices", 
            "xcom_updated_date_task_id": "get_max_updated_at_date", 
            "updated_column": "date_modified"
        }
    )

    t2 = PythonOperator(
        task_id = "incremental_load_invoices_table",
        python_callable = _incremental_load_invoices_table
    )

    t0 >> t1 >> t2
