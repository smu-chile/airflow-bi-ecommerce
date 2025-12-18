from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.slack_utils import dag_failure_slack, dag_success_slack

import pendulum

def get(url, responses, session, exception_cases, X_VTEX_API_AppKey, X_VTEX_API_AppToken, sku_id):
    r = session.get(url, headers = {"X-VTEX-API-AppKey" : X_VTEX_API_AppKey, "X-VTEX-API-AppToken" : X_VTEX_API_AppToken})
    try:
        responses.append({'json': r.json(), 'url': url, 'sku_id': sku_id})
    except Exception as e:
        exception_cases.append(url)


def bulk_get(url_sublist, responses, session, exception_cases, X_VTEX_API_AppKey, X_VTEX_API_AppToken, sku_ids):
    for url, sku_id in zip(url_sublist, sku_ids):
        get(url, responses, session, exception_cases, X_VTEX_API_AppKey, X_VTEX_API_AppToken, sku_id)
    return

def get_fixed_prices(ti,ds):
    import pandas as pd
    import requests
    import json
    import numpy as np
    from threading import Thread

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    print("Getting vtex_ids of products in WP with Fixed Price")
    query = f"""select distinct s.vtex_id
        from ecommdata_alvi.skus s
        where s.vtex_id IS NOT NULL;"""
    cursor.execute(query)
    results = cursor.fetchall()
    list_skus = [result[0] for result in results]
    cursor.close()
    pg_connection.close()
    print(f"Se obtuvieron {len(list_skus)} skus")

    if(len(list_skus) == 0):
        print("no hay SKUS activos en VTEX")
        return

    X_VTEX_API_AppKey = Variable.get("X_VTEX_ALVI_API_Appkey")
    X_VTEX_API_AppToken = Variable.get("X_VTEX_ALVI_API_Apptoken")
    accountName = Variable.get("VTEX_ALVI_ACCOUNT_NAME")

    url_list = [f"https://api.vtex.com.br/{accountName}/pricing/prices/{i}/fixed" for i in list_skus]

    session = requests.session()
    thread_num = 40
    task_num = len(url_list)//thread_num # division entera
    adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=thread_num)
    session.mount('https://', adapter)
    thread_tasks = []
    count = 0
    responses = []
    exception_cases = []

    for thr in range(thread_num):
        new_task = Thread(target=bulk_get, args=[url_list[task_num * count:task_num * (count + 1)],
                                                 responses, session, exception_cases, X_VTEX_API_AppKey,
                                                 X_VTEX_API_AppToken, list_skus[task_num * count:task_num * (count + 1)]],
                          daemon=True)
        new_task.start()
        thread_tasks.append(new_task)
        count = count + 1
    # tareas resagadas:
    if task_num * thread_num != len(url_list):
        new_task = Thread(
            target=bulk_get,
            args=[url_list[task_num * thread_num:], responses, session, exception_cases, X_VTEX_API_AppKey,
                  X_VTEX_API_AppToken, list_skus[task_num * thread_num:]],
            daemon=True)
        new_task.start()
        thread_tasks.append(new_task)
    for task in thread_tasks:
        task.join()
        thread_tasks = []
    
    final_responses = []

    for response in responses:
        try:
            for item in response['json']:
                record = {
                    'vtex_id': response['sku_id'],
                    'tradePolicyId': item['tradePolicyId'],
                    'value': item['value'],
                    'listPrice': item['listPrice'],
                    'minQuantity': item['minQuantity'],
                    'Date From': item['dateRange']['from'],
                    'Date To': item['dateRange']['to']
                    
                }
                final_responses.append(record)
        except KeyError as e:
            print(e)
            print(response)
            exception_cases.append(response['url'])
    df = pd.DataFrame(final_responses)

    aux_list = []

    headers = {
        'Accept': "application/json",
        'Content-Type': "application/json",
        "X-VTEX-API-AppKey": X_VTEX_API_AppKey,
        "X-VTEX-API-AppToken":  X_VTEX_API_AppToken,
        "Connection": "keep-alive"
    }
    
    for index, row in df.iterrows():
        if (df.at[index, 'Date To'] < ds):
            print(f"Promoción caducada {df.at[index, 'tradePolicyId']}")
            price_table_id = df.at[index, 'tradePolicyId']
            itemId = df.at[index, 'vtex_id']
            url = f"https://api.vtex.com/{accountName}/pricing/prices/{str(itemId)}/fixed/{price_table_id}"
            print(url)
            if price_table_id not in aux_list:
                aux_list.append(price_table_id)
            df.drop(index, inplace=True)
            response = requests.delete(url, headers =headers)
            if response.status_code == 200:
                print("DELETE request successful")
            else:
                print(f"DELETE request failed with status code: {response.status_code}")
        else:
            print(f"Promoción activa {df.at[index, 'tradePolicyId']}")

    print("Listas Vacias:")
    print(aux_list)        

    df_final = df.reindex(columns=['vtex_id', 'tradePolicyId', 'value', 'listPrice', 'minQuantity', 'Date From', 'Date To'])

    df_final = df_final.rename(columns={'vtex_id': 'SKU ID', 'tradePolicyId': 'Trade Policy',
                                        'value': 'Price', 'listPrice': 'List Price',
                                        'minQuantity': 'Min Quantity'})

    df_final = df_final.astype({'SKU ID': 'int', 'Price': 'int64', 'Min Quantity': 'int64'})
    return df_final.to_json(orient='records')    

def upload_fixed_prices(ti):
    import pandas as pd
    import sqlalchemy

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")

    conn_url = "postgresql+psycopg2://"+username + \
        ":"+password+"@"+host+":5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    json_data = ti.xcom_pull(task_ids=["get_fixed_prices"])[0]
    df_data = pd.read_json(json_data, orient='records')

    df_data.to_sql(name="listas_precios_vtex",
                   con=engine,
                   schema="ecommdata_alvi",
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
    'etl_listas_precios_vtex_alvi',
    default_args=default_args,
    description="Extracción y carga de la tabla listas_precios_vtex desde API.",
    schedule_interval="0 4 * * *",
    start_date=pendulum.datetime(2023, 6, 6, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["vtex", "promociones", "listas_precios", "workflow_promociones", "SERGIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Extracción y carga de la tabla listas_precios_vtex desde API.
    """

    t0 = PythonOperator(
        task_id="get_fixed_prices",
        python_callable=get_fixed_prices
    )

    t1 = PostgresOperator(
        task_id="truncate_listas_precios",
        postgres_conn_id="postgresql_conn",
        sql="TRUNCATE catalogo.listas_precios_vtex",
    )

    t2 = PythonOperator(
        task_id="upload_fixed_prices",
        python_callable=upload_fixed_prices
    )

    t0 >> t1 >> t2
