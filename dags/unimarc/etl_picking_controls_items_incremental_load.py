from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.janis_utils import load_full_table_to_s3
from utils.postgres_utils import get_max_updated_at_value

from datetime import datetime

def _incremental_load_picking_control_items_table(ti, ds):
    import numpy as np
    import pandas as pd
    
    picking_control_items_file = ti.xcom_pull(key="return_value", task_ids=["load_full_table_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+picking_control_items_file)
    if not s3_hook.check_for_key(picking_control_items_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % picking_control_items_file)

    picking_control_items_object = s3_hook.get_key(picking_control_items_file, bucket_name=s3_bucket)

    df = pd.read_csv(picking_control_items_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")

    # Select only relevant columns:
    df = df[["id",
            "picking_control_id",
            "order_item_id",
            "reason_id",
            "basket",
            "seal",
            "ean",
            "control_result",
            "quantity_to_audit",
            "quantity_audited",
            "comment",
            "status"
            ]]

    # Rename columns to match workspace schema:
    columns_rename = {
        "id": "id",
        "picking_control_id": "id_control_picking",
        "order_item_id": "id_orden_producto",
        "reason_id": "id_razon",
        "basket": "canasta",
        "seal": "sello",
        "ean": "ean",
        "control_result": "resultado_control",
        "quantity_to_audit": "cantidad_a_auditar",
        "quantity_audited": "cantidad_auditada",
        "comment": "comentarios",
        "status": "estado"
    }
    df = df.rename(columns=columns_rename)

    # Cast numeric values to int

    df = df.astype({
        "id": "int",
        "id_control_picking": "int",
        "id_orden_producto": "int",
        "id_razon": "int",
        "canasta": "int",
        "sello": "int",
        "ean": "string",
        "resultado_control": "int",
        "cantidad_a_auditar": "int",
        "cantidad_auditada": "int",
        "comentarios": "string",
        "estado": "int",
    }, errors="ignore")

    columns = [
        "id_control_picking",
        "id_razon",
        "id_orden_producto",
        "canasta",
        "sello",
        "ean",
        "resultado_control",
        "cantidad_a_auditar",
        "cantidad_auditada",
        "comentarios",
        "estado"
    ]

    df = df[[
        "id",
        "id_control_picking",
        "id_razon",
        "id_orden_producto",
        "canasta",
        "sello",
        "ean",
        "resultado_control",
        "cantidad_a_auditar",
        "cantidad_auditada",
        "comentarios",
        "estado"
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
        BEGIN TRANSACTION;
        INSERT INTO ecommdata.control_picking_productos (id,"""+columns_query+""") 
        VALUES ("""+values_query+""")
        ON CONFLICT (id)
        DO UPDATE SET ("""+columns_query+""") = ("""+excluded_query+""");
        UPDATE ecommdata.control_picking_productos cpp
        SET descripcion = op.descripcion
        FROM ecommdata.orden_productos op
        WHERE cpp.id_orden_producto = op.id and cpp.descripcion is NULL;
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
    'etl_control_picking_productos_incremental_load',
    default_args=default_args,
    description="Extracción y carga de tabla control picking productos desde Janis Unimarc Replica hasta Workspace.",
    schedule_interval="30 * * * *",
    start_date=datetime(2022, 7, 1),
    catchup=False,
    tags=["DATA", "Janis", "ecommdata", "control_picking", "Unimarc", "MATIAS"],
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla de control picking productos de Janis Unimarc a Workspace. \n
    UPSERT incremental basado en fecha_modificacion_unixtime.
    """ 

    t0 = PythonOperator(
        task_id = "load_full_table_to_s3",
        python_callable = load_full_table_to_s3,
        op_kwargs = {
            "table_name": "picking_controls_items"
        }
    )

    t1 = PythonOperator(
        task_id = "incremental_load_picking_control_items_table",
        python_callable = _incremental_load_picking_control_items_table
    )

    t0 >> t1
