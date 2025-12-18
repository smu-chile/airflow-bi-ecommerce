from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.models import Variable

from utils.slack_utils import dag_success_slack, dag_failure_slack

from datetime import datetime, timedelta

import pendulum

def get(order_id, responses, failed_request_attempts, session, headers):
    params = {
            "sort": "date_created",
            "criteria": "desc",
            "external_reference": order_id
        }
    api_url = "https://api.mercadopago.com/v1/payments/search"
    response = session.get(api_url, params=params, headers=headers)

    if response.status_code != 200:
        print(f"Status code: {response.status_code}")
        print(f"Meli Order id: {order_id}")
        print(f"Response: {response}")
        failed_request_attempts.append(order_id)
    else:
        try:
            response_json = response.json()
            responses.append({"order_id": order_id, "results": response_json["results"]})
        except Exception as e:
            print(e)
            print(f"Meli Order id: {order_id}")
            failed_request_attempts.append(order_id)

def bulk_get(order_id_sublist, responses, failed_request_attempts, session, headers):
    for order_id in order_id_sublist:
        get(order_id, responses, failed_request_attempts, session, headers)
    return

def _get_pagos_from_mepa_api(ti, ts):
    import json
    import requests
    from threading import Thread

    curr_datetime = ts[:16].replace("-", "/").replace("T", "/").replace(":", "")
    file_name = "meli/mongodb/orders/"+curr_datetime+".json"

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+file_name)
    if not s3_hook.check_for_key(file_name, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % file_name)

    new_orders_object = s3_hook.get_key(file_name, bucket_name=s3_bucket)

    list_order_document = json.loads(new_orders_object.get()["Body"].read().decode('utf-8'))
    print(f"Number of records found: {len(list_order_document)}")

    id_list = []
    for document in list_order_document:
        if not (document["id"] == "" or document["id"] is None):
            id_list.append(int(float(document["id"])))
        else:
            print(f"Skipping unsuable id: {document['id']} from mongo ObjectId: {document['_id']}")
    
    print(f"Number of ids to request: {len(id_list)}")
    responses = []
    failed_request_attempts = []
    s = requests.Session()

    api_url = "https://api.mercadopago.com/v1/payments/search"
    bearer_token = Variable.get("MERCADOPAGO_API_TOKEN_SECRET")
    headers = {"Authorization": f"Bearer {bearer_token}"}

    session = requests.session()
    thread_num = 20
    task_num = len(id_list)//thread_num # division entera
    adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=thread_num)
    session.mount(api_url, adapter)
    thread_tasks = []
    count = 0

    for thr in range(thread_num):
        new_task = Thread(target=bulk_get, args=[id_list[task_num*count:task_num*(count+1)], responses, failed_request_attempts, session, headers], daemon=True)
        new_task.start()
        thread_tasks.append(new_task)
        count = count + 1
    # tareas resagadas:
    if task_num*thread_num != len(id_list):
        new_task = new_task = Thread(target=bulk_get, args=[id_list[task_num*thread_num:], responses, failed_request_attempts, session, headers], daemon=True)
        new_task.start()
        thread_tasks.append(new_task)
    for task in thread_tasks:
        task.join()
        thread_tasks = []

    json_pagos = json.dumps(responses)
    json_retries = json.dumps(failed_request_attempts)

    pagos_s3_path = "meli/mepa_api/get_pagos/"+curr_datetime+".json"
    failed_requests_s3_path = "meli/mepa_api/failed_get_pagos/"+curr_datetime+".json"
    # retries_s3_path = "meli/mepa_api/get_pagos_retires/"+curr_datetime+".json"

    s3_hook.load_string(json_pagos,
                  key=pagos_s3_path,
                  bucket_name=s3_bucket,
                  replace=True,
                  encrypt=False)
    ti.xcom_push(key="pagos_json_s3_path", value=pagos_s3_path)

    s3_hook.load_string(json_retries,
                  key=failed_requests_s3_path,
                  bucket_name=s3_bucket,
                  replace=True,
                  encrypt=False)
    ti.xcom_push(key="failed_requests_s3_path", value=failed_requests_s3_path)

    return

def _retry_get_request(ti, ts):
    import json
    import requests

    curr_datetime = ts[:16].replace("-", "/").replace("T", "/").replace(":", "")
    file_name = ti.xcom_pull(key="failed_requests_s3_path", task_ids=["get_pagos_from_mercadopago_api"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+file_name)
    if not s3_hook.check_for_key(file_name, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % file_name)

    order_ids_object = s3_hook.get_key(file_name, bucket_name=s3_bucket)

    order_ids_list = json.loads(order_ids_object.get()["Body"].read().decode('utf-8'))
    print(f"Number of records found: {len(order_ids_list)}")
    
    responses = []

    api_url = "https://api.mercadopago.com/v1/payments/search"
    bearer_token = Variable.get("MERCADOPAGO_API_TOKEN_SECRET")
    headers = {"Authorization": f"Bearer {bearer_token}"}
    s = requests.Session()

    for order_id in order_ids_list:
        params = {
            "sort": "date_created",
            "criteria": "desc",
            "external_reference": order_id
        }
        response = s.get(api_url, params=params, headers=headers)
        if response.status_code != 200:
            print(f"Status code: {response.status_code}")
            print(f"Meli Order id: {order_id}")
            raise Exception(f"Request faild with status : {response.status_code}.")
        else:
            try:
                results = response.json()["results"]
                responses.append({"order_id": order_id, "results": results})
            except Exception as e:
                print(e)
                print(f"Meli Order id: {order_id}")
                raise Exception(f"Process failed on response parsing : {response}.")

    json_pagos = json.dumps(responses)

    retries_s3_path = "meli/mepa_api/get_pagos_retires/"+curr_datetime+".json"

    s3_hook.load_string(json_pagos,
                  key=retries_s3_path,
                  bucket_name=s3_bucket,
                  replace=True,
                  encrypt=False)
    ti.xcom_push(key="retries_json_s3_path", value=retries_s3_path)
    return

def _load_mepa_pagos_to_workspace(ti):
    import json
    import numpy as np
    import pandas as pd
    
    json_pagos_documents_key = ti.xcom_pull(key="pagos_json_s3_path", task_ids=["get_pagos_from_mercadopago_api"])[0]
    json_pagos_retires_documents_key = ti.xcom_pull(key="retries_json_s3_path", task_ids=["retry_failed_get_requests"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+json_pagos_documents_key)
    if not s3_hook.check_for_key(json_pagos_documents_key, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % json_pagos_documents_key)

    pagos_object = s3_hook.get_key(json_pagos_documents_key, bucket_name=s3_bucket)

    pagos_list = json.loads(pagos_object.get()["Body"].read().decode('utf-8'))
    print(f"Number of records found: {len(pagos_list)}")

    # Get retries:
    print("Searching file: "+json_pagos_retires_documents_key)
    if not s3_hook.check_for_key(json_pagos_retires_documents_key, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % json_pagos_retires_documents_key)

    pagos_retires_object = s3_hook.get_key(json_pagos_retires_documents_key, bucket_name=s3_bucket)

    pagos_retires_list = json.loads(pagos_retires_object.get()["Body"].read().decode('utf-8'))
    print(f"Number of records found on retires: {len(pagos_retires_list)}")

    full_pagos_list = pagos_list + pagos_retires_list
    print(f"Total number of records found: {len(full_pagos_list)}")

    new_documents = []
    for pago_response in full_pagos_list:
        if len(pago_response["results"]) != 0:
            for element in pago_response["results"]:
                new_document_header = {}
                new_document_header.update({"tipo_operacion": element["operation_type"]})
                new_document_header.update({"fecha_aprobacion": element["date_approved"]})
                new_document_header.update({"total_pagado_cliente": element["transaction_details"]["total_paid_amount"]})
                new_document_header.update({"monto_neto_recibido": element["transaction_details"]["net_received_amount"]})
                new_document_header.update({"id_orden_meli": element["external_reference"]})
                new_document_header.update({"id_cargo": element["id"]})
                new_document_header.update({"detalle_estado": element["status_detail"]})
                new_document_header.update({"estado": element["status"]})
                new_document_header.update({"monto_devolucion_cliente": element["transaction_amount_refunded"]})
                new_document_header.update({"monto_original_transaccion": element["transaction_amount"]})
                new_document_header.update({"fecha_liberacion": element["money_release_date"]})
                new_document_header.update({"fecha_modificacion": element["date_last_updated"]})
                new_document_header.update({"fecha_creacion": element["date_created"]})
                new_document_header.update({"monto_despacho": element["shipping_amount"]})
                new_document_header.update({"modo_procesamiento": element["processing_mode"]})
                new_document_header.update({"moneda": element["currency_id"]})
                new_document_header.update({"costo_despacho": element["shipping_cost"]})
                charges_detail = element["charges_details"]
                for charge in charges_detail:
                    new_document = {}
                    refund_charges = charge.get("refund_charges", [])
                    refund_amount = 0
                    refund_last_date = None
                    for refund in refund_charges:
                        refund_amount = refund_amount + refund["amount"]
                        if refund_last_date is None or refund["date_created"] > refund_last_date:
                            refund_last_date = refund["date_created"] 
                    
                    new_document.update({"monto_devolucion_comision": refund_amount})
                    new_document.update({"fecha_ultima_devolucion_comision": refund_last_date})
                    new_document.update({"monto_comision": charge["amounts"]["original"]})
                    new_document.update({"nombre_cargo": charge["name"]})
                    new_document.update({"id": charge["id"]})
                    new_document.update({"tipo_cargo": charge["type"]})
                    new_document.update(new_document_header)

                    new_documents.append(new_document)
                if len(charges_detail) == 0:
                    new_document_header.update({"id": element["id"]})
                    new_documents.append(new_document_header)
        else:
            print(f"Empty response for Order ID: {pago_response['order_id']}")

    df = pd.DataFrame(new_documents)

    columns = [
        "id_cargo",
        "id_orden_meli",
        "tipo_operacion",
        "tipo_cargo",
        "estado",
        "detalle_estado",
        "nombre_cargo",
        "modo_procesamiento",
        "moneda",
        "monto_neto_recibido",
        "monto_original_transaccion",
        "monto_comision",
        "monto_devolucion_comision",
        "total_pagado_cliente",
        "monto_devolucion_cliente",
        "costo_despacho",
        "monto_despacho",
        "fecha_liberacion",
        "fecha_aprobacion",
        "fecha_creacion",
        "fecha_modificacion",
        "fecha_ultima_devolucion_comision",
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
        INSERT INTO ecommdata_meli.pagos (id,"""+columns_query+""") 
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
    print("Data loaded to Postgres: ecommdata_meli.pagos")

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    "etl_pagos_mercadopago_incremental_load",
    default_args=default_args,
    description="Extracción periodica de pagos de MercadoPago a SMU a través de API Rest.",
    schedule_interval="0 7 * * *",
    start_date=pendulum.datetime(2022, 7, 20, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    concurrency=2,
    tags=["DATA", "api", "workspace", "ecommdata_meli", "pagos", "MATIAS"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Extracción periodica de ordenes pagos de MercadoPago a SMU a través de API Rest. \n
    Método de carga incremental: UPSERT en base a delta de carga de ordenes. \n
    Obtención de datos mediante API Rest de MercadoPago utilizando ids de ordenes actualizadas. Se realiza una GET request por cada id de orden. \n
    MercadoPago (API) -> S3 -> Workspace (Postgresql)
    """ 

    t0 = S3KeySensor(
        task_id = "wait_for_ordenes_meli_file",
        bucket_key = "meli/mongodb/orders/{{execution_date.strftime('%Y/%m/%d/%H%M')}}.json",
        bucket_name = Variable.get("AWS_S3_BUCKET_NAME"),
        aws_conn_id = "aws_s3_connection",
        timeout = 60,
        retries = 3,
        retry_delay = timedelta(minutes=1),
    )

    t1 = PythonOperator(
        task_id = "get_pagos_from_mercadopago_api",
        python_callable = _get_pagos_from_mepa_api
    )

    t2 = PythonOperator(
        task_id = "retry_failed_get_requests",
        python_callable = _retry_get_request
    )

    t3 = PythonOperator(
        task_id = "load_mepa_pagos_to_workspace",
        python_callable = _load_mepa_pagos_to_workspace
    )

    t0 >> t1 >> t2 >> t3
