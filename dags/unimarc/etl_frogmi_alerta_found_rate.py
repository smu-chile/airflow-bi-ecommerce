from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

from utils.postgres_utils import get_max_updated_at_value

from datetime import datetime, timedelta

def _get_json_frogmi(ds):
    import requests
    import json

    FROGMI_API_URL = Variable.get("FROGMI_API_URL")
    FROGMI_COMPANY_UUID_SECRET = Variable.get("FROGMI_COMPANY_UUID_SECRET")
    FROGMI_API_TOKEN_SECRET = Variable.get("FROGMI_API_TOKEN_SECRET")

    url = f"{FROGMI_API_URL}/api/v3/tasks_management/results?filters[period][from]={ds}&filters[period][to]={ds}&filters[activity][]=a6dbc4bd-64e6-4628-bb6b-66902cba3a7e&per_page=100&include=stores,events"

    payload={}
    headers = {
    'Authorization': FROGMI_API_TOKEN_SECRET,
    'X-Company-UUID': FROGMI_COMPANY_UUID_SECRET,
    'Content-Type': 'application/vnd.api+json'
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    res = json.loads(response.text)

    file_name = f"frogmi/alerta_found_rate/{ds}.json"

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
    s3_hook.load_string(res,
                  key=file_name,
                  bucket_name=s3_bucket,
                  replace=True,
                  encrypt=False)

    return file_name

def _load_stock_nrt_to_workspace(ti, ts):
    import json
    import numpy as np
    import pandas as pd
    json_frogmi_documents_key = ti.xcom_pull(key="return_value", task_ids=["get_json_frogmi"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+json_frogmi_documents_key)
    if not s3_hook.check_for_key(json_frogmi_documents_key, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % json_frogmi_documents_key)

    new_frogmi_object = s3_hook.get_key(json_frogmi_documents_key, bucket_name=s3_bucket)

    list_frogmi_document = json.loads(new_frogmi_object.get()["Body"].read().decode('utf-8'))
    print(f"Number of records found: {len(list_frogmi_document)}")

    new_documents = []
    for document in list_frogmi_document:
        new_document = {}
        new_document["id"] = document["_id"]["$oid"]
        new_document[""] = document["createAt"]
        new_document["sku_id"] = document["IdSku"]
        new_document["UMV"] = document["MeasurementUnit"]
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
    schedule_interval="0 10 * * *",
    start_date=datetime(2022, 9, 20),
    catchup=False,
    max_active_runs=1,
    concurrency=2,
    tags=["DATA", "mongodb", "ecommdata", "stock_nrt"],
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
        retry_delay = timedelta(minutes=1),
        depends_on_past = True
    )

    t1 = PythonOperator(
        task_id = "load_stock_nrt_to_workspace",
        python_callable = _load_stock_nrt_to_workspace,
        retries = 2,
        retry_delay = timedelta(minutes=1),
        depends_on_past = True
    )

    t0 >> t1
