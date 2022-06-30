from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

from utils.postgres_utils import get_max_updated_at_value

from datetime import datetime, timedelta

def _get_orders_meli_documents(ti, ts):
    from bson.json_util import dumps
    max_updated_at_value = ti.xcom_pull(key="return_value", task_ids=["get_max_updated_at_date"])[0]
    if max_updated_at_value is None:
        max_updated_at_value = "1970-01-01T00:00:00"
    mongo_hook = MongoHook(conn_id="mongodb_meli_conn")
    order_documents = mongo_hook.find(
        mongo_collection="orders",
        query={"last_updated": {"$gt": max_updated_at_value}} 
    )

    list_order_documents = list(order_documents)
    print(f"Number of documents found: {len(list_order_documents)}")
    json_order_documents = dumps(list_order_documents)

    curr_datetime = ts[:16].replace("-", "/").replace("T", "/").replace(":", "")
    file_name = "meli/mongodb/orders/"+curr_datetime+".json"

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
    s3_hook.load_string(json_order_documents,
                  key=file_name,
                  bucket_name=s3_bucket,
                  replace=True,
                  encrypt=False)

    return file_name

def _load_meli_orders_to_workspace(ti, ts):
    import pandas as pd
    json_order_documents_key = ti.xcom_pull(key="return_value", task_ids=["extract_orders_from_mongodb"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+json_order_documents_key)
    if not s3_hook.check_for_key(json_order_documents_key, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % json_order_documents_key)

    new_orders_object = s3_hook.get_key(json_order_documents_key, bucket_name=s3_bucket)

    df = pd.read_json(new_orders_object.get()["Body"], orient="records")
    print(f"Number of records found: {len(df.index)}")

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    "etl_ordenes_mercado_libre_incremental_load",
    default_args=default_args,
    description="Extracción periodica de ordenes de Unimarc a través de MercadoLibre.",
    schedule_interval="0 */4 * * *",
    start_date=datetime(2021, 9, 21),
    catchup=True,
    max_active_runs=1,
    concurrency=2,
    tags=["DATA", "mongodb", "workspace", "ecommdata_unimarc", "ordenes_meli", "unimarc"],
) as dag:

    dag.doc_md = """
    Extracción periodica de ordenes de Unimarc a través de MercadoLibre. \n
    Método de carga incremental: UPSERT sobre campo last_updated \n
    MongoDB -> S3 -> Workspace (Postgresql)
    """ 
    t0 = PythonOperator(
        task_id = "get_max_updated_at_date",
        python_callable = get_max_updated_at_value,
        op_kwargs = {
            "schema": "ecommdata_unimarc",
            "table_name": "ordenes_meli", 
            "updated_at_field": "fecha_modificacion"
        },
        depends_on_past = True,
        pool = "backfill_pool"
    )
    
    t1 = PythonOperator(
        task_id = "extract_orders_from_mongodb",
        python_callable = _get_orders_meli_documents,
        retries = 2,
        retry_delay = timedelta(minutes=1),
        depends_on_past = True,
        pool = "backfill_pool"
    )

    t2 = PythonOperator(
        task_id = "load_meli_orders_to_workspace",
        python_callable = _load_meli_orders_to_workspace
        retries = 2,
        retry_delay = timedelta(minutes=1),
        depends_on_past = True,
        pool = "backfill_pool"
    )

    t0 >> t1 >> t2
