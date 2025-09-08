from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

from utils.postgres_utils import get_max_updated_at_value

from datetime import datetime, timedelta

import pendulum

def _get_orders_meli_documents(ti, ts):
    from bson.json_util import dumps
    max_updated_at_value = ti.xcom_pull(key="return_value", task_ids=["get_max_updated_at_date"])[0]
    if max_updated_at_value is None:
        max_updated_at_value = "1970-01-01T00:00:00"
    max_updated_at_value = max_updated_at_value.replace(" ", "T")
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
    import json
    import numpy as np
    import pandas as pd
    json_order_documents_key = ti.xcom_pull(key="return_value", task_ids=["extract_orders_from_mongodb"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+json_order_documents_key)
    if not s3_hook.check_for_key(json_order_documents_key, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % json_order_documents_key)

    new_orders_object = s3_hook.get_key(json_order_documents_key, bucket_name=s3_bucket)

    list_order_document = json.loads(new_orders_object.get()["Body"].read().decode('utf-8'))
    print(f"Number of records found: {len(list_order_document)}")

    new_documents = []
    for document in list_order_document:
        new_document = {}
        new_document["id"] = document["_id"]["$oid"]
        new_document["id_orden"] = document["id"]
        new_document["id_pack"] = document.get("pack_id", None)
        if document.get("order_items", False):
            new_document["ref_id_sku"] = document["order_items"][0]["item"]["seller_sku"]
            new_document["id_meli_producto"] = document["order_items"][0]["item"]["id"]
            new_document["descripcion"] = document["order_items"][0]["item"]["title"]
            new_document["precio_lista_unitario"] = document["order_items"][0]["full_unit_price"]
            new_document["precio_venta_unitario"] = document["order_items"][0]["unit_price"]
            new_document["comision_unitaria"] = document["order_items"][0]["sale_fee"]
            new_document["unidades_solicitadas"] = document["order_items"][0]["requested_quantity"]["value"]
            new_document["unidades_pickeadas"] = document["order_items"][0]["quantity"]
            new_document["id_meli_categoria"] = document["order_items"][0]["item"]["category_id"]
        new_document["fecha_creacion"] = document["date_created"]
        new_document["fecha_modificacion"] = document["last_updated"]
        new_document["estado"] = document["status"]
        new_documents.append(new_document)

        if document.get("cancel_detail", False):
            new_document["fecha_cancelacion"] = document["cancel_detail"].get("date", None)
        else:
            new_document["fecha_cancelacion"] = None

    df = pd.DataFrame(new_documents)
    df = df[df["id_meli_producto"].notna()]
    columns = [
        "id_orden",
        "id_pack",
        "ref_id_sku",
        "id_meli_producto",
        "descripcion",
        "precio_lista_unitario",
        "precio_venta_unitario",
        "comision_unitaria",
        "unidades_solicitadas",
        "unidades_pickeadas",
        "id_meli_categoria",
        "fecha_creacion",
        "fecha_modificacion",
        "fecha_cancelacion",
        "estado",
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
        INSERT INTO ecommdata_meli.ordenes (id,"""+columns_query+""") 
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
    "etl_ordenes_mercado_libre_incremental_load",
    default_args=default_args,
    description="Extracción periodica de ordenes de Unimarc a través de MercadoLibre.",
    schedule_interval="0 7 * * *",
    start_date=pendulum.datetime(2022, 6, 29, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    concurrency=2,
    tags=["DATA", "mongodb", "workspace", "ecommdata_meli", "ordenes", "mercadolibre", "MATIAS"],
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
            "schema": "ecommdata_meli",
            "table_name": "ordenes", 
            "updated_at_field": "fecha_modificacion"
        },
        depends_on_past = True
    )
    
    t1 = PythonOperator(
        task_id = "extract_orders_from_mongodb",
        python_callable = _get_orders_meli_documents,
        retries = 2,
        retry_delay = timedelta(minutes=1),
        depends_on_past = True
    )

    t2 = PythonOperator(
        task_id = "load_meli_orders_to_workspace",
        python_callable = _load_meli_orders_to_workspace,
        retries = 2,
        retry_delay = timedelta(minutes=1),
        depends_on_past = True
    )

    t0 >> t1 >> t2
