from airflow import DAG
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.janis_utils import load_custom_query_to_s3
from utils.slack_utils import dag_success_slack, dag_failure_slack

from datetime import datetime

import pendulum

def _get_new_orders_from_s3(ts):
    import pandas as pd

    curr_datetime = ts[:16].replace("-", "/").replace("T", "/").replace(":", "")
    orders_file = f"janis/replica/wms_orders/{curr_datetime}_wms_orders.csv"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+orders_file)
    if not s3_hook.check_for_key(orders_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % orders_file)

    orders_object = s3_hook.get_key(orders_file, bucket_name=s3_bucket)

    df = pd.read_csv(orders_object.get()["Body"])
    print(f"Number of records found: {len(df.index)}")

    return df

def _get_order_item_promotion_additional_info_from_janis(ts):
    # Search based on wms_orders.id
    df = _get_new_orders_from_s3(ts)
    order_ids = df["id"].tolist()
    query_order_ids = "(" + ",".join([str(order_id) for order_id in order_ids]) + ")"
    query = f"""
        SELECT woipai.*
        FROM janis_jackie.wms_orders AS wo
        JOIN janis_jackie.wms_order_items AS woi
        ON woi.order_id = wo.id
        JOIN janis_jackie.wms_order_item_promotions AS woip
        ON woip.order_item = woi.id
        JOIN janis_jackie.wms_order_item_promotions_additional_info woipai
        ON woipai.order_item_promotion = woip.id
        WHERE wo.id IN {query_order_ids} 
    """
    print(query)
    s3_object_name = load_custom_query_to_s3(ts, query, "wms_order_item_promotions_additional_info")
    return s3_object_name

def _order_item_promo_additional_info_incremental_load(ts, ti):
    import numpy as np
    import pandas as pd
    
    order_item_prom_add_info_file = ti.xcom_pull(key="return_value", task_ids=["get_order_item_promotions_from_janis"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+order_item_prom_add_info_file)
    if not s3_hook.check_for_key(order_item_prom_add_info_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % order_item_prom_add_info_file)

    order_item_prom_add_info_object = s3_hook.get_key(order_item_prom_add_info_file, bucket_name=s3_bucket)

    df = pd.read_csv(order_item_prom_add_info_object.get()["Body"])
    df = df[[
        "id", 
        "order_item_promotion", 
        "field",  
        "value" 
    ]]  

    # # Ensure correct datatypes:
    df["id"] = df["id"].astype("int")
    df["order_item_promotion"] = df["order_item_promotion"].astype("int")
    df["field"] = df["field"].astype("str", errors="ignore")
    df["value"] = df["value"].astype("str", errors="ignore")

    # Ignore invalid (non-numeric) values:
    df["value"] = df["value"].str.strip()
    df = df[
        ~(df["field"].isin(["ID", "WORKFLOWID"])) |
        (df["value"].str.isnumeric())
    ]

    columns_rename = {
        "order_item_promotion": "orden_producto_promocion",
        "field": "campo",
        "value": "valor"
    }

    df = df.rename(columns=columns_rename)

    columns = ["orden_producto_promocion", "campo", "valor"]

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
        INSERT INTO ecommdata.orden_producto_promocion_extrainfo (id,"""+columns_query+""") 
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
    print("Data loaded to Postgres: ecommdata.orden_producto_promocion_extrainfo")

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_orden_producto_promocion_extrainfo_incremental_load',
    default_args=default_args,
    description="Extracción y carga de tabla orden_producto_promocion_extrainfo desde Janis Replica hasta Workspace.",
    schedule_interval="*/30 * * * *",
    start_date=pendulum.datetime(2022, 2, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["DATA", "Janis", "ecommdata", "orden_producto_promocion_extrainfo", "unimarc", "cyber", "MATIAS"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla de orden_producto_promocion_extrainfo de Janis a Workspace. \n
    UPSERT incremental basado registros creados por el etl de la tabla ordenes.
    """ 
    t0 = S3KeySensor(
        task_id = "wait_for_orders_s3_file",
        bucket_key = "janis/replica/wms_orders/{{execution_date.strftime('%Y/%m/%d/%H%M')}}_wms_orders.csv",
        bucket_name = Variable.get("AWS_S3_BUCKET_NAME"),
        aws_conn_id = "aws_s3_connection",
        timeout = 1800
    )

    t1 = PythonOperator(
        task_id = "get_order_item_promotions_from_janis",
        python_callable = _get_order_item_promotion_additional_info_from_janis
    )

    t2 = PythonOperator(
        task_id = "order_item_promo_additional_info_incremental_load",
        python_callable = _order_item_promo_additional_info_incremental_load
    )

    t0 >> t1 >> t2