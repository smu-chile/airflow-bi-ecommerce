from airflow import DAG
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pendulum

from utils.janis_alvi_utils import load_custom_query_to_s3

from datetime import datetime

def _get_new_orders_from_s3(ts):
    import pandas as pd

    curr_datetime = ts[:16].replace("-", "/").replace("T", "/").replace(":", "")
    orders_file = f"janis/replica_alvi/wms_orders/{curr_datetime}_wms_orders.csv"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+orders_file)
    if not s3_hook.check_for_key(orders_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % orders_file)

    orders_object = s3_hook.get_key(orders_file, bucket_name=s3_bucket)

    df = pd.read_csv(orders_object.get()["Body"])
    print(f"Number of records found: {len(df.index)}")

    return df

def _get_order_item_promotions_from_janis(ts):
    # Search based on wms_orders.id
    df = _get_new_orders_from_s3(ts)
    order_ids = df["id"].tolist()
    query_order_ids = "(" + ",".join([str(order_id) for order_id in order_ids]) + ")"
    query = f"""
        SELECT woip.*
        FROM janis_alvicl.wms_orders AS wo
        INNER JOIN janis_alvicl.wms_order_items AS woi
        ON wo.id = woi.order_id
        INNER JOIN janis_alvicl.wms_order_item_promotions AS woip
        ON woi.id = woip.order_item
        WHERE wo.id IN {query_order_ids} 
    """
    print(query)
    s3_object_name = load_custom_query_to_s3(ts, query, "wms_order_item_promotions")
    return s3_object_name

def _order_item_promotions_table_incremental_load(ts, ti):
    import numpy as np
    import pandas as pd
    
    order_item_proms_file = ti.xcom_pull(key="return_value", task_ids=["get_order_item_promotions_from_janis"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
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
    'etl_orden_producto_promociones_alvi_incremental_load',
    default_args=default_args,
    description="Extracción y carga de tabla orden_producto_promociones desde Janis Replica Alvi hasta Workspace.",
    schedule_interval="30 * * * *",
    start_date=pendulum.datetime(2023, 7, 11, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["DATA", "Janis", "ecommdata_alvi", "orden_producto_promociones", "alvi", "cyber", "MATIAS"],
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla de orden_producto_promociones de Janis a Workspace. \n
    UPSERT incremental basado registros creados por el etl de la tabla ordenes.
    """ 
    t0 = S3KeySensor(
        task_id = "wait_for_orders_s3_file",
        bucket_key = "janis/replica_alvi/wms_orders/{{execution_date.strftime('%Y/%m/%d/%H%M')}}_wms_orders.csv",
        bucket_name = Variable.get("AWS_S3_BUCKET_NAME"),
        aws_conn_id = "aws_s3_connection",
        timeout = 1800
    )

    t1 = PythonOperator(
        task_id = "get_order_item_promotions_from_janis",
        python_callable = _get_order_item_promotions_from_janis
    )

    t2 = PythonOperator(
        task_id = "orden_producto_promociones_incremental_load",
        python_callable = _order_item_promotions_table_incremental_load,
        trigger_rule = "none_failed"
    )

    t0 >> t1 >> t2
