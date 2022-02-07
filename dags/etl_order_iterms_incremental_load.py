from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.janis_utils import load_custom_query_to_s3
# from utils.postgres_utils import get_max_updated_at_value

from datetime import datetime

def _get_new_order_ids_from_s3(ts):
    import numpy as np
    import pandas as pd

    curr_datetime = ts[:16].replace("-", "/").replace("T", "/").replace(":", "")
    orders_file = f"/janis/replica/wms_orders/{curr_datetime}_wms_orders.csv",
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+orders_file)
    if not s3_hook.check_for_key(orders_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % orders_file)

    orders_object = s3_hook.get_key(orders_file, bucket_name=s3_bucket)

    df = pd.read_csv(orders_object.get()["Body"])
    print(f"Number of records found: {len(df.index)}")
    order_ids = df["seq_id"].tolist()

    return order_ids

def _get_order_items_from_janis(ts, ti):
    order_ids = ti.xcom_pull(key="return_value", task_ids=["incremental_load_table_to_s3"])[0]
    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_orden_productos_incremental_load',
    default_args=default_args,
    description="Extracción y carga de tabla orden_productos desde Janis Replica hasta Workspace.",
    schedule_interval="30 * * * *",
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["DATA", "Janis", "ecommdata", "orden_productos"],
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla de orden_productos de Janis a Workspace. \n
    UPSERT incremental basado registros creados por el etl de la tabla ordenes.
    """ 
    t0 = S3KeySensor(
        task_id = "wait_for_orders_s3_file",
        bucket_key = "/janis/replica/wms_orders/{{execution_date.format('YYYY/mm/dd/HHMM')}}_wms_orders.csv",
        bucket_name = "s3-bi-ecommerce-develop",
        aws_conn_id = "aws_s3_connection",
        timeout = 1800
    )

    t1 = PythonOperator(
        task_id = "get_new_order_ids_from_s3",
        python_callable = _get_new_order_ids_from_s3
    )

    t0 >> t1
