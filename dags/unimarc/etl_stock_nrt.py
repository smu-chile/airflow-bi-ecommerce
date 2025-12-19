from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

from utils.postgres_utils import get_max_updated_at_value
from utils.slack_utils import dag_success_slack, dag_failure_slack

from datetime import datetime, timedelta

import pendulum

def _get_stock_nrt_documents(ds, ts):
    from bson.json_util import dumps
    max_updated_at_value = ds
    mongo_hook = MongoHook(conn_id="mongodb_nrt_conn")
    order_documents = mongo_hook.find(
        mongo_collection="movements",
        query={"createAt": {"$gt": max_updated_at_value}} 
    )

    list_order_documents = list(order_documents)
    print(f"Number of documents found: {len(list_order_documents)}")
    json_order_documents = dumps(list_order_documents)

    curr_datetime = ts[:16].replace("-", "/").replace("T", "/").replace(":", "")
    file_name = "stock_nrt/mongodb/BD_STOCK_NRT/"+curr_datetime+".json"

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
    s3_hook.load_string(json_order_documents,
                  key=file_name,
                  bucket_name=s3_bucket,
                  replace=True,
                  encrypt=False)

    return file_name

def _load_stock_nrt_to_workspace(ti, ts):
    import json
    import numpy as np
    import pandas as pd
    json_stock_nrt_documents_key = ti.xcom_pull(key="return_value", task_ids=["get_stock_nrt_documents"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+json_stock_nrt_documents_key)
    if not s3_hook.check_for_key(json_stock_nrt_documents_key, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % json_stock_nrt_documents_key)

    new_stock_nrt_object = s3_hook.get_key(json_stock_nrt_documents_key, bucket_name=s3_bucket)

    list_stock_nrt_document = json.loads(new_stock_nrt_object.get()["Body"].read().decode('utf-8'))
    print(f"Number of records found: {len(list_stock_nrt_document)}")

    new_documents = []
    for document in list_stock_nrt_document:
        new_document = {}
        new_document["id"] = document["_id"]["$oid"]
        new_document["fecha_hora"] = document["createAt"]
        new_document["sku_id"] = document["IdSku"]
        new_document["UMV"] = document.get("MeasurementUnit", None)
        new_document["id_tienda"] = document["Store"]
        new_document["tipo"] = document["Type"]
        new_document["cantidad"] = document["Quantity"]
        new_documents.append(new_document)

    df = pd.DataFrame(new_documents)
    columns = [
        "fecha_hora",
        "sku_id",
        "UMV",
        "id_tienda",
        "tipo",
        "cantidad"
    ]

    df = df[["id"]+columns]

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
        INSERT INTO ecommdata.stock_nrt (id,"""+columns_query+""") 
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
    "etl_stock_nrt_incremental_load",
    default_args=default_args,
    description="Extracción periodica de Stock NRT.",
    schedule_interval="0 * * * *",
    start_date=pendulum.datetime(2022, 9, 20, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    concurrency=2,
    tags=["DATA", "mongodb", "ecommdata", "stock_nrt", "MATIAS"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Extracción periodica de Stock NRT. \n
    Método de carga incremental: UPSERT sobre campo createAt \n
    MongoDB -> S3 -> Workspace (Postgresql)
    """ 
    
    t0 = PythonOperator(
        task_id = "get_stock_nrt_documents",
        python_callable = _get_stock_nrt_documents,
        retries = 2,
        retry_delay = timedelta(minutes=1)
    )

    t1 = PythonOperator(
        task_id = "load_stock_nrt_to_workspace",
        python_callable = _load_stock_nrt_to_workspace,
        retries = 2,
        retry_delay = timedelta(minutes=1)
    )

    t2 = PostgresOperator(
        task_id = "delete_old_data",
        postgres_conn_id="postgresql_conn",
        sql="""
        delete from ecommdata.stock_nrt
        where fecha_hora::date < '{{ds}}'::date - interval '21 days'
        """,
    )

    t0 >> t1 >> t2
