from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from utils.janis_alvi_utils import load_full_table_to_s3
from utils.slack_utils import dag_failure_slack, dag_success_slack

from datetime import datetime

import pendulum

def get(url, responses, session, exception_cases, X_VTEX_API_AppKey, X_VTEX_API_AppToken):
    r = session.get(url, headers = {"X-VTEX-API-AppKey" : X_VTEX_API_AppKey, "X-VTEX-API-AppToken" : X_VTEX_API_AppToken})
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

def load_full_table_from_staging_to_s3(table_name, df, ts):
    from io import StringIO
    import boto3
    
    curr_datetime = ts[:16].replace("-", "/").replace("T", "/").replace(":", "")
    prefix = "staging/"+table_name+"/"+curr_datetime
    file_name = prefix+table_name+".csv"

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

def _load_final_responses_to_postgres(final_responses, ts, file_name):
    import pandas as pd
    import sqlalchemy

    df = pd.DataFrame(final_responses)

    df.columns = [
        "skuId",
        "warehouseId",
        "totalQuantity",
        "reservedQuantity",
        "hasUnlimitedQuantity"
    ]
    
    df = df[[
        "skuId",
        "warehouseId",
        "totalQuantity",
        "reservedQuantity",
        "hasUnlimitedQuantity"
    ]]

    df["skuId"] = df["skuId"].astype("str")
    df["warehouseId"] = df["warehouseId"].astype("str")
    df["totalQuantity"] = df["totalQuantity"].astype("int")
    df["reservedQuantity"] = df["reservedQuantity"].astype("int")
    df["hasUnlimitedQuantity"] = df["hasUnlimitedQuantity"].astype("bool")

    columns_rename = {
        "skuId": "vtex_id",
        "warehouseId": "id_warehouse",
        "totalQuantity": "cantidad_total",
        "reservedQuantity": "cantidad_reservada",
        "hasUnlimitedQuantity": "cantidad_ilimitada"
    }

    df = df.rename(columns=columns_rename)

    df = df.drop_duplicates(subset=['vtex_id', 'id_warehouse'], keep='last')

    print(df)


    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    df.to_sql(name="stock_vtex_alvi",
                con=engine,         
                schema="staging",         
                if_exists='append',         
                index=False,         
                chunksize=20000,         
                method='multi')

    load_full_table_from_staging_to_s3(file_name, df, ts)

    return

def ids_vtex():
    #lista de id Vtex para buscar stock
    query = """
        select s.vtex_id
            from ( select CONCAT(l.material, '-', l.umv) as ref_id, l.material, l.umv
                from ecommdata_alvi.lista8 l) _t
            inner join ecommdata_alvi.skus s on _t.ref_id = s.ref_id
            where s.vtex_id is not null
            UNION
            select distinct s.vtex_id
            from staging.stock_janis_alvi sa
            inner join ecommdata_alvi.skus s on s.id = sa.item_id
            where sa.stock > 0 and s.vtex_id is not null;
        """
    print(query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    pg_connection.close()
    return results

def _get_table_stock_janis_from_S3(ts,ti):
    import pandas as pd

    stock_file = ti.xcom_pull(key="return_value", task_ids=["load_full_table_to_s3"])[0]
    print(stock_file)
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+stock_file)
    if not s3_hook.check_for_key(stock_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % stock_file)

    orders_object = s3_hook.get_key(stock_file, bucket_name=s3_bucket)

    df = pd.read_csv(orders_object.get()["Body"])
    print(f"Number of records found: {len(df.index)}")

    return df

def _save_table_stock_janis(ts, ti):
    import pandas as pd
    import sqlalchemy
    import numpy as np
    print("iniciando load table janis alvi")
    df = _get_table_stock_janis_from_S3(ts, ti)
    df = df[['id',
            'item_id',
            'store_id',
            'warehouse_id',
            'stock',
            'min_stock',
            'infinite_stock',
            'date_modified',
            'date_published',
            'operation_type']]
    df = df.loc[df['stock'] > 0]
    print(df["date_published"])
    df["date_published"] = pd.to_datetime(df["date_published"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["date_modified"] = pd.to_datetime(df["date_modified"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    print("paso por la transformacion de date date_published y date_modified")
    print(df["date_published"])
    print(df.columns)
    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    df_array = np.array_split(df,5)

    for i in df_array:

        i.to_sql(name="stock_janis_alvi",
                    con=engine,         
                    schema="staging",         
                    if_exists='append',         
                    index=False,         
                    chunksize=20000,         
                    method='multi')
    
    return

def _save_vtex_stock_in_ecommdata(ti, ts):
    import requests
    from threading import Thread
    import pandas as pd
    import sqlalchemy
    
    vtex_ids = ids_vtex()
    df_vtex_ids=pd.DataFrame(vtex_ids)
    df_vtex_ids.columns = ["vtex_id"]
    sku_list = df_vtex_ids["vtex_id"].tolist()
    print(sku_list)
    vtex_account_name = {
        "alvicl004": Variable.get("VTEX_ALVI3074_ACCOUNT_NAME"),
        #"alvicl003": Variable.get("VTEX_ALVI3082_ACCOUNT_NAME"),
        "alvicl008": Variable.get("VTEX_ALVI3089_ACCOUNT_NAME"),
        "alvicl001": Variable.get("VTEX_ALVI3092_ACCOUNT_NAME"),
        "alvicl010": Variable.get("VTEX_ALVI3093_ACCOUNT_NAME"),
        "alvicl005": Variable.get("VTEX_ALVI3098_ACCOUNT_NAME"),
        "alvicl009": Variable.get("VTEX_ALVI3172_ACCOUNT_NAME"),
        "alvicl002": Variable.get("VTEX_ALVI3180_ACCOUNT_NAME"),
        "alvicl006": Variable.get("VTEX_ALVI3181_ACCOUNT_NAME"),
        "alvicl007": Variable.get("VTEX_ALVI3187_ACCOUNT_NAME"),
        "alvicl011": Variable.get("VTEX_ALVI3188_ACCOUNT_NAME"),
        "alvitobalaba3193": Variable.get("VTEX_ALVI3193_ACCOUNT_NAME"),
        "alvicl012": Variable.get("VTEX_ALVI3088_ACCOUNT_NAME"),
        "alvicl013": Variable.get("VTEX_ALVI3094_ACCOUNT_NAME"),
        "alvicl014": Variable.get("VTEX_ALVI3086_ACCOUNT_NAME"),
        "alvichillan3091": Variable.get("VTEX_ALVI3091_ACCOUNT_NAME"),
        "alvilosandes3206": Variable.get("VTEX_ALVI3206_ACCOUNT_NAME"),
        "alvibelloto3085": Variable.get("VTEX_ALVI3085_ACCOUNT_NAME"),
        "alvipuntaarenas3212": Variable.get("VTEX_ALVI3212_ACCOUNT_NAME"),
        "alviconcon3211": Variable.get("VTEX_ALVI3211_ACCOUNT_NAME"),
    }

    x_vtex_api_appkey = {
        "alvicl004": Variable.get("X_VTEX_ALVI3074_API_Appkey"),
        #"alvicl003": Variable.get("X_VTEX_ALVI3082_API_Appkey"),
        "alvicl008": Variable.get("X_VTEX_ALVI3089_API_Appkey"),
        "alvicl001": Variable.get("X_VTEX_ALVI3092_API_Appkey"),
        "alvicl010": Variable.get("X_VTEX_ALVI3093_API_Appkey"),
        "alvicl005": Variable.get("X_VTEX_ALVI3098_API_Appkey"),
        "alvicl009": Variable.get("X_VTEX_ALVI3172_API_Appkey"),
        "alvicl002": Variable.get("X_VTEX_ALVI3180_API_Appkey"),
        "alvicl006": Variable.get("X_VTEX_ALVI3181_API_Appkey"),
        "alvicl007": Variable.get("X_VTEX_ALVI3187_API_Appkey"),
        "alvicl011": Variable.get("X_VTEX_ALVI3188_API_Appkey"),
        "alvitobalaba3193": Variable.get("X_VTEX_ALVI3193_API_Appkey"),
        "alvicl012": Variable.get("X_VTEX_ALVI3088_API_Appkey"),
        "alvicl013": Variable.get("X_VTEX_ALVI3094_API_Appkey"),
        "alvicl014": Variable.get("X_VTEX_ALVI3086_API_Appkey"),
        "alvichillan3091": Variable.get("X_VTEX_ALVI3091_API_Appkey"),
        "alvilosandes3206": Variable.get("X_VTEX_ALVI3206_API_Appkey"),
        "alvibelloto3085": Variable.get("X_VTEX_ALVI3085_API_Appkey"),
        "alvipuntaarenas3212": Variable.get("X_VTEX_ALVI3212_API_Appkey"),
        "alviconcon3211": Variable.get("X_VTEX_ALVI3211_API_Appkey"),
    }

    x_vtex_api_apptoken = {
        "alvicl004": Variable.get("X_VTEX_ALVI3074_API_Apptoken"),
        #"alvicl003": Variable.get("X_VTEX_ALVI3082_API_Apptoken"),
        "alvicl008": Variable.get("X_VTEX_ALVI3089_API_Apptoken"),
        "alvicl001": Variable.get("X_VTEX_ALVI3092_API_Apptoken"),
        "alvicl010": Variable.get("X_VTEX_ALVI3093_API_Apptoken"),
        "alvicl005": Variable.get("X_VTEX_ALVI3098_API_Apptoken"),
        "alvicl009": Variable.get("X_VTEX_ALVI3172_API_Apptoken"),
        "alvicl002": Variable.get("X_VTEX_ALVI3180_API_Apptoken"),
        "alvicl006": Variable.get("X_VTEX_ALVI3181_API_Apptoken"),
        "alvicl007": Variable.get("X_VTEX_ALVI3187_API_Apptoken"),
        "alvicl011": Variable.get("X_VTEX_ALVI3188_API_Apptoken"),
        "alvitobalaba3193": Variable.get("X_VTEX_ALVI3193_API_Apptoken"),
        "alvicl012": Variable.get("X_VTEX_ALVI3088_API_Apptoken"),
        "alvicl013": Variable.get("X_VTEX_ALVI3094_API_Apptoken"),
        "alvicl014": Variable.get("X_VTEX_ALVI3086_API_Apptoken"),
        "alvichillan3091": Variable.get("X_VTEX_ALVI3091_API_Apptoken"),
        "alvilosandes3206": Variable.get("X_VTEX_ALVI3206_API_Apptoken"),
        "alvibelloto3085": Variable.get("X_VTEX_ALVI3085_API_Apptoken"),
        "alvipuntaarenas3212": Variable.get("X_VTEX_ALVI3212_API_Apptoken"),
        "alviconcon3211": Variable.get("X_VTEX_ALVI3211_API_Apptoken"),
    }
    all_final_responses = []
    all_exception_cases = []

    for name in vtex_account_name:
        print(name)
        url_list = []
        for sku in sku_list:
            url = "https://"+name+".vtexcommercestable.com.br/api/logistics/pvt/inventory/skus/"+str(sku)
            url_list.append(url)

        session = requests.session()
        thread_num = 40
        task_num = len(url_list)//thread_num # division entera
        adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=thread_num)
        session.mount('https://', adapter)
        thread_tasks = []
        count = 0
        responses = []
        exception_cases = []

        X_VTEX_API_AppKey = x_vtex_api_appkey[name]
        X_VTEX_API_AppToken = x_vtex_api_apptoken[name]
        
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
        #print(responses)
        final_responses = []
        for response in responses:
            response_aux = response['json']
            try:
                for i in response_aux['balance']:
                    try:
                        aux = (response_aux['skuId'],i['warehouseId'],i['totalQuantity'],i['reservedQuantity'],i['hasUnlimitedQuantity'])
                        final_responses.append(aux)
                    except KeyError as e:
                        print(e)
                        print(response)
                        exception_cases.append(response['url'])
            except KeyError as e:
                    print(e)
                    print(response)
                    exception_cases.append(response['url'])
        
        all_final_responses.extend(final_responses)
        all_exception_cases.extend(exception_cases)

    # Cargo todos los resultados juntos
    if all_final_responses:
        _load_final_responses_to_postgres(all_final_responses, ts, 'stock_vtex')

    # Manejo de reintentos en S3 al final de todas las tiendas
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    date_path = ts[:10].replace("-","/")
    s3_path = f"vtex/api/get_stock_url_retries/{date_path}/"
    retries_path = s3_path+"retries"

    s3_hook.load_string(str(all_exception_cases), retries_path, bucket_name=s3_bucket, replace=True)
    ti.xcom_push(key = 'vtex_retries', value = retries_path)

    return


def _vtex_get_stock_retries(ti, ts):
    import requests
    from threading import Thread
    import pandas as pd

    retries_file = ti.xcom_pull(key="vtex_retries", task_ids=["save_vtex_stock_in_ecommdata"])[0]
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+retries_file)
    if not s3_hook.check_for_key(retries_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % retries_file)

    retries_object = s3_hook.get_key(retries_file, bucket_name=s3_bucket)
    retries_string = retries_object.get()["Body"].read().decode('utf-8')[1:-1]
    retries_string = retries_string.replace("'","")
    retries = retries_string.split(",") if retries_string != "" else []

    session = requests.session()
    responses = []
    exception_cases = []
    
    if len(retries) == 0:
        return
    print(retries)
    vtex_account_name = {
        "alvicl004": Variable.get("VTEX_ALVI3074_ACCOUNT_NAME"),
        #"alvicl003": Variable.get("VTEX_ALVI3082_ACCOUNT_NAME"),
        "alvicl008": Variable.get("VTEX_ALVI3089_ACCOUNT_NAME"),
        "alvicl001": Variable.get("VTEX_ALVI3092_ACCOUNT_NAME"),
        "alvicl010": Variable.get("VTEX_ALVI3093_ACCOUNT_NAME"),
        "alvicl005": Variable.get("VTEX_ALVI3098_ACCOUNT_NAME"),
        "alvicl009": Variable.get("VTEX_ALVI3172_ACCOUNT_NAME"),
        "alvicl002": Variable.get("VTEX_ALVI3180_ACCOUNT_NAME"),
        "alvicl006": Variable.get("VTEX_ALVI3181_ACCOUNT_NAME"),
        "alvicl007": Variable.get("VTEX_ALVI3187_ACCOUNT_NAME"),
        "alvicl011": Variable.get("VTEX_ALVI3188_ACCOUNT_NAME"),
        "alvitobalaba3193": Variable.get("VTEX_ALVI3193_ACCOUNT_NAME"),
        "alvicl012": Variable.get("VTEX_ALVI3088_ACCOUNT_NAME"),
        "alvicl013": Variable.get("VTEX_ALVI3094_ACCOUNT_NAME"),
        "alvicl014": Variable.get("VTEX_ALVI3086_ACCOUNT_NAME"),
        "alvichillan3091": Variable.get("VTEX_ALVI3091_ACCOUNT_NAME"),
        "alvilosandes3206": Variable.get("VTEX_ALVI3206_ACCOUNT_NAME"),
        "alvibelloto3085": Variable.get("VTEX_ALVI3085_ACCOUNT_NAME"),
        "alvipuntaarenas3212": Variable.get("VTEX_ALVI3212_ACCOUNT_NAME"),
        "alviconcon3211": Variable.get("VTEX_ALVI3211_ACCOUNT_NAME"),
    }

    x_vtex_api_appkey = {
        "alvicl004": Variable.get("X_VTEX_ALVI3074_API_Appkey"),
        #"alvicl003": Variable.get("X_VTEX_ALVI3082_API_Appkey"),
        "alvicl008": Variable.get("X_VTEX_ALVI3089_API_Appkey"),
        "alvicl001": Variable.get("X_VTEX_ALVI3092_API_Appkey"),
        "alvicl010": Variable.get("X_VTEX_ALVI3093_API_Appkey"),
        "alvicl005": Variable.get("X_VTEX_ALVI3098_API_Appkey"),
        "alvicl009": Variable.get("X_VTEX_ALVI3172_API_Appkey"),
        "alvicl002": Variable.get("X_VTEX_ALVI3180_API_Appkey"),
        "alvicl006": Variable.get("X_VTEX_ALVI3181_API_Appkey"),
        "alvicl007": Variable.get("X_VTEX_ALVI3187_API_Appkey"),
        "alvicl011": Variable.get("X_VTEX_ALVI3188_API_Appkey"),
        "alvitobalaba3193": Variable.get("X_VTEX_ALVI3193_API_Appkey"),
        "alvicl012": Variable.get("X_VTEX_ALVI3088_API_Appkey"),
        "alvicl013": Variable.get("X_VTEX_ALVI3094_API_Appkey"),
        "alvicl014": Variable.get("X_VTEX_ALVI3086_API_Appkey"),
        "alvichillan3091": Variable.get("X_VTEX_ALVI3091_API_Appkey"),
        "alvilosandes3206": Variable.get("X_VTEX_ALVI3206_API_Appkey"),
        "alvibelloto3085": Variable.get("X_VTEX_ALVI3085_API_Appkey"),
        "alvipuntaarenas3212": Variable.get("X_VTEX_ALVI3212_API_Appkey"),
        "alviconcon3211": Variable.get("X_VTEX_ALVI3211_API_Appkey"),
    }

    x_vtex_api_apptoken = {
        "alvicl004": Variable.get("X_VTEX_ALVI3074_API_Apptoken"),
        #"alvicl003": Variable.get("X_VTEX_ALVI3082_API_Apptoken"),
        "alvicl008": Variable.get("X_VTEX_ALVI3089_API_Apptoken"),
        "alvicl001": Variable.get("X_VTEX_ALVI3092_API_Apptoken"),
        "alvicl010": Variable.get("X_VTEX_ALVI3093_API_Apptoken"),
        "alvicl005": Variable.get("X_VTEX_ALVI3098_API_Apptoken"),
        "alvicl009": Variable.get("X_VTEX_ALVI3172_API_Apptoken"),
        "alvicl002": Variable.get("X_VTEX_ALVI3180_API_Apptoken"),
        "alvicl006": Variable.get("X_VTEX_ALVI3181_API_Apptoken"),
        "alvicl007": Variable.get("X_VTEX_ALVI3187_API_Apptoken"),
        "alvicl011": Variable.get("X_VTEX_ALVI3188_API_Apptoken"),
        "alvitobalaba3193": Variable.get("X_VTEX_ALVI3193_API_Apptoken"),
        "alvicl012": Variable.get("X_VTEX_ALVI3088_API_Apptoken"),
        "alvicl013": Variable.get("X_VTEX_ALVI3094_API_Apptoken"),
        "alvicl014": Variable.get("X_VTEX_ALVI3086_API_Apptoken"),
        "alvichillan3091": Variable.get("X_VTEX_ALVI3091_API_Apptoken"),
        "alvilosandes3206": Variable.get("X_VTEX_ALVI3206_API_Apptoken"),
        "alvibelloto3085": Variable.get("X_VTEX_ALVI3085_API_Apptoken"),
        "alvipuntaarenas3212": Variable.get("X_VTEX_ALVI3212_API_Apptoken"),
        "alviconcon3211": Variable.get("X_VTEX_ALVI3211_API_Apptoken"),
    }
    
    all_final_responses = []
    all_exception_cases = []

    for name in vtex_account_name:
        url_list = retries  #retries      
        session = requests.session()
        thread_num = 40
        task_num = len(url_list)//thread_num # division entera
        adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=thread_num)
        session.mount('https://', adapter)
        thread_tasks = []
        count = 0
        responses = []
        exception_cases = []

        X_VTEX_API_AppKey = x_vtex_api_appkey[name]
        X_VTEX_API_AppToken = x_vtex_api_apptoken[name]
        
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
            response_aux = response['json']
            try:
                for i in response_aux['balance']:
                    try:
                        aux = (response_aux['skuId'],i['warehouseId'],i['totalQuantity'],i['reservedQuantity'],i['hasUnlimitedQuantity'])
                        final_responses.append(aux)
                    except KeyError as e:
                        print(e)
                        print(response)
                        exception_cases.append(response['url'])
            except KeyError as e:
                    print(e)
                    print(response)
                    exception_cases.append(response['url'])
        
        all_final_responses.extend(final_responses)
        all_exception_cases.extend(exception_cases)

    if all_final_responses:
        _load_final_responses_to_postgres(all_final_responses, ts, 'retries_stock_vtex')

    if len(all_exception_cases) > 0:
        print(f"Exception cases found during retry: {len(all_exception_cases)}")
        # Note: We don't raise here to allow the DAG to continue to final stock load
    
    return


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'etl_stock_alvi_incremental_load',
    default_args=default_args,
    description="Extracción y carga de tabla stock desde Vtex y Janis.",
    schedule_interval="0 1/4 * * *",
    start_date=pendulum.datetime(2023, 8, 2, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["DATA", "vtex", "janis", "staging", "alvi", "vtex_stock", "janis_stock", "stock", "PATRICIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla stock desde Vtex y Janis de ALVI.
    """ 

    t0 = PostgresOperator(
        task_id = "truncate_janis_staging_table",
        postgres_conn_id="postgresql_conn",
        sql="""
        TRUNCATE staging.stock_janis_alvi
        """,
    )

    t1 = PostgresOperator(
        task_id = "truncate_vtex_staging_table",
        postgres_conn_id="postgresql_conn",
        sql="""
        TRUNCATE staging.stock_vtex_alvi
        """,
    )

    t2 = PythonOperator(
        task_id = "load_full_table_to_s3",
        python_callable = load_full_table_to_s3,
        op_kwargs = {"table_name": "stock"}
    )

    t3 = PythonOperator(
        task_id = "_save_table_stock_janis",
        python_callable = _save_table_stock_janis,
    )

    t4 = PythonOperator(
        task_id = "save_vtex_stock_in_ecommdata",
        python_callable = _save_vtex_stock_in_ecommdata
    )

    t5 = PythonOperator(
        task_id = "vtex_get_stock_retries",
        python_callable = _vtex_get_stock_retries
    )

    t6 = PostgresOperator(
        task_id = "save_stock_final",
        postgres_conn_id = "postgresql_conn",
        sql = "sql/stock_final_alvi.sql"
    )
    
    t7 = PostgresOperator(
        task_id = "delete_old_stock",
        postgres_conn_id = "postgresql_conn",
        sql = """DELETE
            FROM ecommdata_alvi.stock
            WHERE fecha = '{{ds}}'::date - interval '21 days' """
    )


t0 >> t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7