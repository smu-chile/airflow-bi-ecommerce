from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor

from utils.janis_utils import load_full_table_to_s3

from datetime import datetime

def _get_table_stock_janis_from_S3(ts, ti):
    import pandas as pd

    curr_datetime = ts[:16].replace("-", "/").replace("T", "/").replace(":", "")
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

    df = _get_table_stock_janis_from_S3(ts, ti)
    df_array = np.array_split(df,5)

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = "postgresql+psycopg2://"+username+":"+password+"@"+host+":5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    for i in df_array:

        i.to_sql(name="stock_unimarc",
                    con=engine,         
                    schema="staging",         
                    if_exists='append',         
                    index=False,         
                    chunksize=20000,         
                    method='multi')
    
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

def get(url, responses, session):
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

def _save_vtex_stock_in_ecommdata(ti, ts):
    import requests
    from threading import Thread
    import pandas as pd
    import sqlalchemy
    
    l_vtex_id = _load_vtex_id_list()

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

    load_full_table_from_staging_to_s3('stock_vtex', df, ts)

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

    t0 = PythonOperator(
        task_id = "load_full_table_to_s3",
        python_callable = load_full_table_to_s3,
        op_kwargs = {"table_name": "stock", "where": "stock > 0"}
    )

    t1 = PostgresOperator(
        task_id = "truncate_janis_staging_table",
        postgres_conn_id="postgresql_conn",
        sql="""
        TRUNCATE staging.stock_unimarc
        """,
    )

    t2 = PythonOperator(
        task_id = "save_table_stock",
        python_callable = _save_table_stock_janis,
    )

    t3 = PostgresOperator(
        task_id = "truncate_vtex_staging_table",
        postgres_conn_id="postgresql_conn",
        sql="""
        TRUNCATE staging.stock_vtex_unimarc
        """,
    )

    t4 = PythonOperator(
        task_id = "save_vtex_stock_in_ecommdata",
        python_callable = _save_vtex_stock_in_ecommdata
    )

t0 >> t1 >> t2 >> t3 >> t4
