from airflow import DAG
from airflow import macros
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow import macros

from utils.slack_utils import dag_success_slack, dag_failure_slack

from datetime import datetime

import pendulum

def _load_json_to_s3(ts, ds):
    import requests
    import json
    import pandas as pd
    from io import StringIO
    import boto3

    sds = ds
    fds = macros.ds_add(ds, 1)

    accountName = Variable.get("VTEX_ACCOUNT_NAME")
    env = Variable.get("VTEX_ENV")

    url = f"https://{accountName}.{env}.com.br/api/oms/pvt/orders"

    X_VTEX_API_AppKey = Variable.get("X_VTEX_API_AppKey")
    X_VTEX_API_AppToken = Variable.get("X_VTEX_API_AppToken")

    headers = {
    "X-VTEX-API-AppKey" : X_VTEX_API_AppKey,
    "X-VTEX-API-AppToken" :  X_VTEX_API_AppToken
    }

    page = 1
    page_cond = True
    lista_lineas = []

    while(page_cond):
        parameters = {
            "incompleteOrders" : "true",
            "orderBy" : "creationDate,desc",
            "f_creationDate" : f"creationDate:[{sds}T00:00:00.00Z TO {fds}T00:00:00.00Z]",
            "per_page" : "100",
            "page" : str(page),
        }

        print(parameters)

        

        response = requests.request("GET", url, headers=headers, params=parameters)

        res = json.loads(response.text)
        print(response.text)

        for linea in res['list']:
            print("pagina "+str(page))
            orderId = linea['orderId']
            creationDate = linea['creationDate']
            clientName = linea['clientName']
            totalValue = linea['totalValue']
            paymentNames = linea['paymentNames']
            status = linea['status']
            statusDescription = linea['statusDescription']
            marketPlaceOrderId = linea['marketPlaceOrderId']
            sequence = linea['sequence']
            salesChannel = linea['salesChannel']
            affiliateId = linea['affiliateId']
            origin = linea['origin']
            workflowInErrorState = linea['workflowInErrorState']
            workflowInRetry = linea['workflowInRetry']
            lastMessageUnread = linea['lastMessageUnread']
            ShippingEstimatedDate = linea['ShippingEstimatedDate']
            ShippingEstimatedDateMax = linea['ShippingEstimatedDateMax']
            ShippingEstimatedDateMin = linea['ShippingEstimatedDateMin']
            orderIsComplete = linea['orderIsComplete']
            authorizedDate = linea['authorizedDate']
            callCenterOperatorName = linea['callCenterOperatorName']
            totalItems = linea['totalItems']
            lastChange = linea['lastChange']
            isAllDelivered = linea['isAllDelivered']
            isAnyDelivered = linea['isAnyDelivered']
            giftCardProviders = linea['giftCardProviders']
            orderFormId = linea['orderFormId']
            paymentApprovedDate = linea['paymentApprovedDate']
            readyForHandlingDate = linea['readyForHandlingDate']
            deliveryDates = linea['deliveryDates']

            order_url = f'{url}/{orderId}'
            order_response = requests.request("GET", order_url, headers=headers)
            print("------------")
            print(order_url)
            print("------------")
            print(order_response.text)
            order_res = json.loads(order_response.text)

            try:
                utm_source = order_res['marketingData']['utmSource']
                utm_medium = order_res['marketingData']['utmMedium']
            except:
                utm_source = None
                utm_medium = None

            lista_lineas.append([orderId,
                                 creationDate,
                                 clientName,
                                 totalValue,
                                 paymentNames,
                                 status,
                                 statusDescription,
                                 marketPlaceOrderId,
                                 sequence,
                                 salesChannel,
                                 affiliateId,
                                 origin,
                                 workflowInErrorState,
                                 workflowInRetry,
                                 lastMessageUnread,
                                 ShippingEstimatedDate,
                                 ShippingEstimatedDateMax,
                                 ShippingEstimatedDateMin,
                                 orderIsComplete,
                                 authorizedDate,
                                 callCenterOperatorName,
                                 totalItems,
                                 lastChange,
                                 isAllDelivered,
                                 isAnyDelivered,
                                 giftCardProviders,
                                 orderFormId,
                                 paymentApprovedDate,
                                 readyForHandlingDate,
                                 deliveryDates,
                                 utm_source,
                                 utm_medium])
        page += 1
        print("p_total "+str(res['paging']['pages']))
        print('checking '+str(page)+'>'+str(res['paging']['pages']))
        if page > res['paging']['pages']:
            page_cond = False
    df = pd.DataFrame(lista_lineas, columns = ['orderId',
                                 'creationDate',
                                 'clientName',
                                 'totalValue',
                                 'paymentNames',
                                 'status',
                                 'statusDescription',
                                 'marketPlaceOrderId',
                                 'sequence',
                                 'salesChannel',
                                 'affiliateId',
                                 'origin',
                                 'workflowInErrorState',
                                 'workflowInRetry',
                                 'lastMessageUnread',
                                 'ShippingEstimatedDate',
                                 'ShippingEstimatedDateMax',
                                 'ShippingEstimatedDateMin',
                                 'orderIsComplete',
                                 'authorizedDate',
                                 'callCenterOperatorName',
                                 'totalItems',
                                 'lastChange',
                                 'isAllDelivered',
                                 'isAnyDelivered',
                                 'giftCardProviders',
                                 'orderFormId',
                                 'paymentApprovedDate',
                                 'readyForHandlingDate',
                                 'deliveryDates',
                                 'utm_source',
                                 'utm_medium'])
    
    df = df.astype({
        'orderId': 'string',
        'creationDate': 'string',
        'clientName': 'string',
        'totalValue': 'int',
        'paymentNames': 'string',
        'status': 'string',
        'statusDescription': 'string',
        'marketPlaceOrderId': 'string',
        'sequence': 'int',
        'salesChannel': 'int',
        'affiliateId': 'string',
        'origin': 'string',
        'workflowInErrorState': 'string',
        'workflowInRetry': 'string',
        'lastMessageUnread': 'string',
        'ShippingEstimatedDate': 'string',
        'ShippingEstimatedDateMax': 'string',
        'ShippingEstimatedDateMin': 'string',
        'orderIsComplete': 'string',
        'authorizedDate': 'string',
        'callCenterOperatorName': 'string',
        'totalItems': 'int',
        'lastChange': 'string',
        'isAllDelivered': 'string',
        'isAnyDelivered': 'string',
        'giftCardProviders': 'string',
        'orderFormId': 'string',
        'paymentApprovedDate': 'string',
        'readyForHandlingDate': 'string',
        'deliveryDates': 'string',
        'utm_source': 'string',
        'utm_medium': 'string'
    }, errors="ignore")
    
    curr_datetime = ts[:16].replace("-", "/").replace("T", "/").replace(":", "")
    file_name = f"vtex/ordenes_incompletas/{curr_datetime}_ordenes_incompletas.csv"
    buffer = StringIO()

    df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    buffer.seek(0)

    access_key = Variable.get("AWS_ACCESS_KEY")
    secret_key = Variable.get("AWS_SECRET_KEY")
    bucket_name = Variable.get("AWS_S3_BUCKET_NAME")
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name = "us-east-1"
    )
    response = s3_client.put_object(
        Bucket=bucket_name, Key=file_name, Body=buffer.getvalue()
    )

    return file_name

def _get_table_oi_from_S3(ti):
    import pandas as pd

    ordenes_file = ti.xcom_pull(key="return_value", task_ids=["load_json_to_s3"])[0]
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+ordenes_file)
    if not s3_hook.check_for_key(ordenes_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % ordenes_file)

    ordenes_object = s3_hook.get_key(ordenes_file, bucket_name=s3_bucket)

    df = pd.read_csv(ordenes_object.get()["Body"])
    print(f"Number of records found: {len(df.index)}")

    df = df.astype({
        'orderId': 'string',
        'creationDate': 'string',
        'clientName': 'string',
        'totalValue': 'int',
        'paymentNames': 'string',
        'status': 'string',
        'statusDescription': 'string',
        'marketPlaceOrderId': 'string',
        'sequence': 'int',
        'salesChannel': 'int',
        'affiliateId': 'string',
        'origin': 'string',
        'workflowInErrorState': 'string',
        'workflowInRetry': 'string',
        'lastMessageUnread': 'string',
        'ShippingEstimatedDate': 'string',
        'ShippingEstimatedDateMax': 'string',
        'ShippingEstimatedDateMin': 'string',
        'orderIsComplete': 'string',
        'authorizedDate': 'string',
        'callCenterOperatorName': 'string',
        'totalItems': 'int',
        'lastChange': 'string',
        'isAllDelivered': 'string',
        'isAnyDelivered': 'string',
        'giftCardProviders': 'string',
        'orderFormId': 'string',
        'paymentApprovedDate': 'string',
        'readyForHandlingDate': 'string',
        'deliveryDates': 'string',
        'utm_source': 'string',
        'utm_medium': 'string'
    }, errors="ignore")

    return df

def _save_table_oi(ts, ti, ds):
    import pandas as pd
    import sqlalchemy

    df = _get_table_oi_from_S3(ti)
    df = df[['orderId',
            'creationDate',
            'clientName',
            'totalValue',
            'paymentNames',
            'status',
            'statusDescription',
            'marketPlaceOrderId',
            'sequence',
            'salesChannel',
            'affiliateId',
            'origin',
            'workflowInErrorState',
            'workflowInRetry',
            'lastMessageUnread',
            'ShippingEstimatedDate',
            'ShippingEstimatedDateMax',
            'ShippingEstimatedDateMin',
            'orderIsComplete',
            'authorizedDate',
            'callCenterOperatorName',
            'totalItems',
            'lastChange',
            'isAllDelivered',
            'isAnyDelivered',
            'giftCardProviders',
            'orderFormId',
            'paymentApprovedDate',
            'readyForHandlingDate',
            'deliveryDates',
            'utm_source',
            'utm_medium']]

    columns_rename = {
            'orderId':'id_orden',
            'creationDate':'fecha_creacion',
            'clientName':'nombre_cliente',
            'totalValue':'valor_total',
            'paymentNames':'nombres_pagos',
            'status':'estado',
            'statusDescription':'descripcion_estado',
            'marketPlaceOrderId':'id_orden_marketplace',
            'sequence':'sequence',
            'salesChannel':'canal_de_ventas',
            'affiliateId':'id_afiliado',
            'origin':'origen',
            'workflowInErrorState':'workflow_en_estado_error',
            'workflowInRetry':'workflow_en_reintento',
            'lastMessageUnread':'ultimo_mensaje_no_leido',
            'ShippingEstimatedDate':'estimacion_fecha_shipping',
            'ShippingEstimatedDateMax':'estimacion_fecha_shipping_max',
            'ShippingEstimatedDateMin':'estimacion_fecha_shipping_min',
            'orderIsComplete':'orden_completa',
            'authorizedDate':'fecha_autorizacion',
            'callCenterOperatorName':'nombre_operador_callcenter',
            'totalItems':'items_totales',
            'lastChange':'fecha_modificacion',
            'isAllDelivered':'todo_entregado',
            'isAnyDelivered':'parcialmente_entregado',
            'giftCardProviders':'proveedor_giftcard',
            'orderFormId':'id_forma_orden',
            'paymentApprovedDate':'fecha_aprobacion_pago',
            'readyForHandlingDate':'fecha_ready_for_handling',
            'deliveryDates':'fechas_entrega',
            'utm_source':'utm_source',
            'utm_medium':'utm_medium'
    }
    df = df.rename(columns=columns_rename)
    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)
    df.to_sql(name="ordenes_incompletas_vtex",
            con=engine,         
            schema="ecommdata",         
            if_exists='append',         
            index=False,         
            chunksize=20000,         
            method='multi')

    return



default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'etl_ordenes_incompletas_vtex',
    default_args=default_args,
    description="Extracción y carga de tablas ordenes_incompletas desde API.",
    schedule_interval="0 10 * * *",
    start_date=pendulum.datetime(2024, 11, 1, tz="America/Santiago"),
    catchup=True,
    max_active_runs = 1,
    tags=["vtex", "ordenes_incompletas", "MATIAS"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Extracción y carga de tablas ordenes_incompletas desde API.
    """ 

    
    t0 = PythonOperator(
        task_id = "load_json_to_s3",
        python_callable = _load_json_to_s3
    )

    t1 = PythonOperator(
        task_id = "save_table_oi",
        python_callable = _save_table_oi
    )

t0 >> t1
