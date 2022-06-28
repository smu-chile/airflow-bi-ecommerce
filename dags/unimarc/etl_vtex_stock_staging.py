from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor

from queue import Queue
import requests
from threading import Thread
import time
import pandas as pd

from datetime import datetime

def get(url, responses, session):
    print(url)
    X_VTEX_API_AppKey = Variable.get("X_VTEX_API_AppKey")
    X_VTEX_API_AppToken = Variable.get("X_VTEX_API_AppToken")
    r = session.get(url, headers = {"X-VTEX-API-AppKey" : X_VTEX_API_AppKey, "X-VTEX-API-AppToken" : X_VTEX_API_AppToken})
    responses.append(r.json())

def bulk_get(url_sublist, responses, session):
    for url in url_sublist:
        get(url, responses, session)
    return

def _load_vtex_id_list():
    query = """
        select s.vtex_id
        from ( select CONCAT(l.material, '-', l.umv) as ref_id, l.material, l.umv
            from ecommdata.lista8 l
            where l.fecha = (select max(l.fecha)
            from ecommdata.lista8 l)
            group by CONCAT(l.material, '-', l.umv), l.material, l.umv) _t
        inner join ecommdata.skus s on _t.ref_id = s.ref_id
        left join catalogo.productos_excluidos pe on _t.material = pe.material and _t.umv = pe.umv
        where pe.material is null and s.vtex_id is not null
        UNION
        select distinct s.vtex_id  
        from staging.stock_unimarc sa
        inner join ecommdata.skus s on s.id = sa.item_id
        where sa.stock > 0;
        """
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    pg_connection.close()
    return results

def _save_vtex_stock_in_ecommdata(ti):
    import requests
    from threading import Thread
    import pandas as pd
    import sqlalchemy
    
    l_vtex_id = ti.xcom_pull(key="return_value", task_ids=["load_vtex_id_list"])[0]

    if len(l_vtex_id) == 0:
        print('the list of vtex id was empty')
        return

    accountName = Variable.get("VTEX_ACCOUNT_NAME")
    env = Variable.get("VTEX_ENV")
    url_list = [f"https://{accountName}.{env}.com.br/api/logistics/pvt/inventory/skus/{i[0]}" for i in l_vtex_id]
    
    session = requests.session()
    thread_num = 40
    task_num = len(url_list)//thread_num # division entera
    thread_tasks = []
    count = 0
    responses = []

    for thr in range(thread_num):
        new_task = Thread(target=bulk_get, args=[url_list[task_num*count:task_num*(count+1)], responses, session], daemon=True)
        new_task.start()
        thread_tasks.append(new_task)
        count = count + 1
    # tareas resagadas:
    if task_num*thread_num != len(url_list):
        new_task = new_task = Thread(target=bulk_get, args=[url_list[task_num*thread_num:], responses, session], daemon=True)
        new_task.start()
        thread_tasks.append(new_task)
    for task in thread_tasks:
        task.join()
        thread_tasks = []
    
    print(responses)
    final_responses = []
    
    for i in range(len(responses)):
        for j in range(len(responses[i]['balance'])):
            aux = responses[i]['balance'][j]
            aux['skuId'] = responses[i]['skuId']
            final_responses.append(aux)
    
    df = pd.DataFrame(final_responses)
    
    df = df[[
        "skuId",
        "warehouseId",
        "totalQuantity",
        "reservedQuantity",
        "hasUnlimitedQuantity",
    ]]

    df["skuId"] = df["skuId"].astype("str")
    df["warehouseId"] = df["warehouseId"].astype("str")
    df["totalQuantity"] = df["totalQuantity"].astype("int")
    df["reservedQuantity"] = df["reservedQuantity"].astype("int")
    df["hasUnlimitedQuantity"] = df["hasUnlimitedQuantity"].astype("bool")

    columns_rename = {
        "skuId": "vtex_id",
        "warehouseId": "id_tienda",
        "totalQuantity": "cantidad_total",
        "reservedQuantity": "cantidad_reservada",
        "hasUnlimitedQuantity": "cantidad_ilimitada"
    }

    df = df.rename(columns=columns_rename)

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = "postgresql+psycopg2://"+username+":"+password+"@"+host+":5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    df.to_sql(name="stock_vtex_unimarc",
                con=engine,         
                schema="staging",         
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
    'etl_vtex_stock_staging',
    default_args=default_args,
    description="Extracción y carga de tabla vtex_stock desde Vtex hasta Workspace.",
    schedule_interval="0 */4 * * *",
    start_date=datetime(2022, 6, 23),
    catchup=False,
    max_active_runs = 1,
    tags=["DATA", "vtex", "staging", "unimarc", "vtex_stock"],
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla vtex_stock desde Vtex hasta Workspace.
    """ 

    t0 = ExternalTaskSensor(
        task_id="wait_for_stock_staging",
        external_dag_id='etl_stock_staging',
        external_task_id=None,
        allowed_states=['success'],
        failed_states=['failed']
    )

    t1 = PythonOperator(
        task_id = "load_vtex_id_list",
        python_callable = _load_vtex_id_list
    )

    t2 = PostgresOperator(
        task_id = "truncate_staging_table",
        postgres_conn_id="postgresql_conn",
        sql="""
        TRUNCATE staging.stock_vtex_unimarc
        """,
    )

    t3 = PythonOperator(
        task_id = "save_vtex_stock_in_ecommdata",
        python_callable = _save_vtex_stock_in_ecommdata
    )

t0 >> t1 >> t2 >> t3
