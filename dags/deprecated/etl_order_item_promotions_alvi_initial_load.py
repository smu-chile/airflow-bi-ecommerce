from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.janis_alvi_utils import load_custom_query_to_s3

from datetime import datetime


def _get_order_item_promotions_from_janis(ts):
    # Search based on wms_orders.id
    query = f"""
        SELECT woip.*
        FROM janis_alvicl.wms_orders AS wo
        INNER JOIN janis_alvicl.wms_order_items AS woi
        ON wo.id = woi.order_id
        INNER JOIN janis_alvicl.wms_order_item_promotions AS woip
        ON woi.id = woip.order_item 
    """
    print(query)
    s3_object_name = load_custom_query_to_s3(ts, query, "wms_order_item_promotions")
    return s3_object_name

def _order_item_promotions_table_incremental_load(ts, ti):
    import numpy as np
    import pandas as pd
    
    order_item_proms_file = ti.xcom_pull(key="return_value", task_ids=["get_order_item_promotions_from_janis"])[0]

    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+order_item_proms_file)
    if not s3_hook.check_for_key(order_item_proms_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % order_item_proms_file)

    order_item_proms_object = s3_hook.get_key(order_item_proms_file, bucket_name=s3_bucket)

    df = pd.read_csv(order_item_proms_object.get()["Body"])
    df = df[[
        "id", 
        "order_item", 
        "name", 
        "quantity", 
        "value"
    ]]  

    # # Ensure correct datatypes:
    df["id"] = df["id"].astype("int")
    df["order_item"] = df["order_item"].astype("int")
    df["name"] = df["name"].astype("str", errors="ignore")
    df["quantity"] = df["quantity"].astype("int", errors="ignore")
    df["value"] = df["value"].astype("float", errors="ignore")

    columns_rename = {
        "order_item": "orden_producto",
        "name": "nombre",
        "quantity": "cantidad",
        "value": "valor"
    }

    df = df.rename(columns=columns_rename)

    columns = ["orden_producto", "nombre", "cantidad", "valor"]

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
        INSERT INTO ecommdata_alvi.orden_producto_promociones (id,"""+columns_query+""") 
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
    'etl_orden_producto_promociones_alvi_initial_load',
    default_args=default_args,
    description="Extracción y carga de tabla orden_producto_promociones desde Janis Replica Alvi hasta Workspace.",
    schedule=None,
    start_date=datetime(2022, 2, 1),
    catchup=False,
    max_active_runs = 1,
    tags=["DATA", "Janis", "ecommdata_alvi", "orden_producto_promociones", "alvi", "cyber"],
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla de orden_producto_promociones de Janis a Workspace. \n
    Carga Inicial.
    """ 

    t0 = PythonOperator(
        task_id = "get_order_item_promotions_from_janis",
        python_callable = _get_order_item_promotions_from_janis
    )

    t1 = PythonOperator(
        task_id = "orden_producto_promociones_incremental_load",
        python_callable = _order_item_promotions_table_incremental_load,
        trigger_rule = "none_failed"
    )

    t0 >> t1
