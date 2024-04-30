from airflow import DAG
from airflow import macros
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.S3_hook import S3Hook

from datetime import datetime

import pendulum

def get(url, responses, session, exception_cases, X_VTEX_API_AppKey, X_VTEX_API_AppToken):
    r = session.get(url, headers = {'Accept': "application/json", 'Content-Type': "application/json", "X-VTEX-API-AppKey" : X_VTEX_API_AppKey, "X-VTEX-API-AppToken" : X_VTEX_API_AppToken, "Connection": "keep-alive"})
    try:
        responses.append({'json':r.json(), 'url':url})
    except Exception as e:
        print(e)
        print(url)
        print(r)
        print(r.status_code)
        exception_cases.append(url)


def bulk_get(url_sublist, responses, session, exception_cases, X_VTEX_API_AppKey, X_VTEX_API_AppToken):
    for url in url_sublist:
        get(url, responses, session, exception_cases, X_VTEX_API_AppKey, X_VTEX_API_AppToken)
    return

def _load_vtex_order_status_to_s3(ti, ds, ts):
    import pandas as pd
    import numpy as np
    import requests
    import json
    from io import StringIO
    import boto3
    from threading import Thread

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    print("Getting vtex_ids of orders")
    query = f"""select oj.vtex_id 
        from ecommdata.ordenes_janis oj
        where oj.fecha_modificacion >= '{ds}'::date - interval '365 days'  """
    cursor.execute(query)
    results = cursor.fetchall()
    list_vtex_id = [result[0] for result in results]
    cursor.close()
    pg_connection.close()
    print(f"Se obtuvieron {len(list_vtex_id)} vtex_id de ordenes")

    if len(list_vtex_id) == 0:
        print('the list of vtex id was empty')
        return

    accountName = Variable.get("VTEX_ACCOUNT_NAME")
    env = Variable.get("VTEX_ENV")
    url_total_list = [f"https://{accountName}.{env}.com.br/api/oms/pvt/orders/{i}" for i in list_vtex_id]
    
    url_list_array = np.array_split(url_total_list, 300)
    s3_file_list = []
    aux_count = 0

    for url_list in url_list_array:

        session = requests.session()
        thread_num = 10
        task_num = len(url_list)//thread_num # division entera
        adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=thread_num)
        session.mount('https://', adapter)
        thread_tasks = []
        count = 0
        responses = []
        exception_cases = []

        X_VTEX_API_AppKey = Variable.get("X_VTEX_API_AppKey")
        X_VTEX_API_AppToken = Variable.get("X_VTEX_API_AppToken")

        for thr in range(thread_num):
            new_task = Thread(target=bulk_get, args=[url_list[task_num*count:task_num*(count+1)], responses, session, exception_cases, X_VTEX_API_AppKey, X_VTEX_API_AppToken], daemon=True)
            new_task.start()
            thread_tasks.append(new_task)
            count = count + 1
        # tareas resagadas:
        if task_num*thread_num != len(url_list):
            new_task = new_task = Thread(target=bulk_get, args=[url_list[task_num*thread_num:], responses, session, exception_cases, X_VTEX_API_AppKey, X_VTEX_API_AppToken], daemon=True)
            new_task.start()
            thread_tasks.append(new_task)
        for task in thread_tasks:
            task.join()
            thread_tasks = []
        
        final_responses = []

        for response in responses:
            try:
                order_id = response['json']['orderId']
                state = response['json']['status']
                status_description = response['json']['statusDescription']
                lastState = None
                lastChange = response['json']['lastChange']
                value = response['json']['value']
                totals = response['json']['totals'][0]['value']
                discount = response['json']['totals'][1]['value']
                email = response['json']['clientProfileData']['email']
                rut = response['json']['clientProfileData']['document']
                linea = [order_id, state, status_description, lastState, lastChange, value, totals, discount, email, rut]
                final_responses.append(linea)
            except KeyError as e:
                print(e)
                print(response)
                exception_cases.append(response['url'])

        print(exception_cases)
        df = pd.DataFrame(final_responses, columns =['order_id', 'state', 'status_description', 'lastState', 'lastChange', 'value', 'totals', 'discount', 'email', 'rut'])
        df = df.astype({
            "order_id": "string",
            "state": "string",
            "status_description": "string",
            "lastState": "string",
            "lastChange": "string",
            "value": "int",
            "totals": "int",
            "discount": "int",
            "email": "string",
            "rut": "string"
        }, errors="ignore")
        
        curr_datetime = ts[:16].replace("-", "/").replace("T", "/").replace(":", "")
        file_name = f"vtex/vtex_state/initial_load/{curr_datetime}_{aux_count}_vtex_state.csv"
        aux_count += 1
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
        print(f"file {aux_count} out of 300 done with name {file_name}")
        s3_file_list.append(file_name)
    return s3_file_list

def _get_table_vtex_order_status(ti):
    import pandas as pd

    alerta_found_rate_file_list = ti.xcom_pull(key="return_value", task_ids=["load_vtex_order_status_to_s3"])[0]
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    df_list = []

    for alerta_found_rate_file in alerta_found_rate_file_list:

        print("Searching file: "+alerta_found_rate_file)
        if not s3_hook.check_for_key(alerta_found_rate_file, bucket_name=s3_bucket):
            raise Exception("Key %s does not exist." % alerta_found_rate_file)

        alerta_found_rate_object = s3_hook.get_key(alerta_found_rate_file, bucket_name=s3_bucket)

        df = pd.read_csv(alerta_found_rate_object.get()["Body"])
        print(f"Number of records found: {len(df.index)}")

        df = df.astype({
            "order_id": "string",
            "state": "string",
            "status_description": "string",
            "lastState": "string",
            "lastChange": "string",
            "value": "int",
            "totals": "int",
            "discount": "int",
            "email": "string",
            "rut": "string"
        }, errors="ignore")

        df_list.append(df)
    df_final = pd.concat(df_list)
    return df_final

def _upload_order_status(ts, ti, ds):
    import pandas as pd
    import sqlalchemy

    df = _get_table_vtex_order_status(ti)
    df = df[['order_id', 'state', 'status_description', 'lastState', 'lastChange', 'value', 'totals', 'discount', 'email', 'rut']]

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")

    exec_date = ds

    exec_datetime = datetime.strptime(ts[:16], "%Y-%m-%dT%H:%M")
    exec_datetime_utc = pendulum.timezone("utc").convert(exec_datetime)
    local_tz = pendulum.timezone("America/Santiago")
    exec_datetime_local = local_tz.convert(exec_datetime_utc)
    exec_datetime_local_str = exec_datetime_local.strftime("%Y-%m-%dT%H:%M")
    print(exec_datetime_local_str)
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)
    with engine.begin() as conn:
        df.to_sql(name="vtex_order_status",
                con=engine,         
                schema="ecommdata",         
                if_exists='append',         
                index=False,         
                chunksize=20000,         
                method='multi')
        conn.close


    
    return


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'etl_vtex_order_status_initial_load',
    default_args=default_args,
    description="Extracción y carga de la tabla vtex_order_status desde API.",
    schedule_interval=None,
    start_date=pendulum.datetime(2023, 6, 6, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["vtex", "orders", "status", "MATIAS"],
) as dag:

    dag.doc_md = """
    Extracción y carga de la tabla vtex_order_status desde API.
    """

    t0 = PythonOperator(
        task_id="load_vtex_order_status_to_s3",
        python_callable=_load_vtex_order_status_to_s3
    )

    t1 = PythonOperator(
        task_id="upload_order_status",
        python_callable=_upload_order_status
    )

    t0 >> t1
