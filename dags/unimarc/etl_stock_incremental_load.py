from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from utils.janis_utils import load_full_table_to_s3
from utils.slack_utils import dag_success_slack, dag_failure_slack

from datetime import datetime

import pendulum

def _get_table_stock_janis_from_S3(ts, ti):
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
    from io import StringIO
    import csv

    df = _get_table_stock_janis_from_S3(ts, ti)
    df = df[['id', 'item_id', 'store_id','warehouse_id', 'stock', 'min_stock', 'infinite_stock', 'date_published', 'date_modified', 'operation_type']]
    df = df.loc[df['stock'] >= 0]
    df["date_published"] = pd.to_datetime(df["date_published"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["date_modified"] = pd.to_datetime(df["date_modified"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, quoting=csv.QUOTE_MINIMAL)
    buffer.seek(0)

    conn = engine.raw_connection()
    try:
        cursor = conn.cursor()
        cursor.copy_expert("COPY staging.stock_unimarc (id, item_id, store_id, warehouse_id, stock, min_stock, infinite_stock, date_published, date_modified, operation_type) FROM STDIN WITH CSV", buffer)
        conn.commit()
        cursor.close()
    except Exception as e:
        print(f"Error executing COPY for stock_unimarc: {e}")
        conn.rollback()
        raise e
    finally:
        conn.close()
    
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

def fetch_vtex_stock(url, session, X_VTEX_API_AppKey, X_VTEX_API_AppToken, max_retries=4):
    import time
    import random
    headers = {
        "X-VTEX-API-AppKey": X_VTEX_API_AppKey,
        "X-VTEX-API-AppToken": X_VTEX_API_AppToken,
        "Accept": "application/json"
    }
    for attempt in range(max_retries):
        try:
            r = session.get(url, headers=headers, timeout=10)
            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")
                sleep_time = float(retry_after) if retry_after else (0.5 * (attempt + 1) + random.uniform(0.1, 0.3))
                time.sleep(sleep_time)
                continue
            r.raise_for_status()
            return {"status": "ok", "json": r.json(), "url": url}
        except Exception as e:
            if 'r' in locals() and r is not None and r.status_code == 429:
                sleep_time = 0.5 * (attempt + 1) + random.uniform(0.1, 0.3)
                time.sleep(sleep_time)
                continue
            time.sleep(0.3)
    return {"status": "error", "url": url}

def _load_vtex_id_list():
    query = """
        select s.vtex_id
        from ( select CONCAT(l.material, '-', l.umv) as ref_id, l.material, l.umv
            from ecommdata.lista8 l) _t
        inner join ecommdata.skus s on _t.ref_id = s.ref_id
        where s.vtex_id is not null
        UNION
        select distinct s.vtex_id
        from staging.stock_unimarc sa
        inner join ecommdata.skus s on s.id = sa.item_id
        where sa.stock > 0 and s.vtex_id is not null;
        """
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    pg_connection.close()
    return results

def _load_final_responses_to_postgres(final_responses, ts, file_name):
    import pandas as pd
    import sqlalchemy

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
        "warehouseId": "id_warehouse",
        "totalQuantity": "cantidad_total",
        "reservedQuantity": "cantidad_reservada",
        "hasUnlimitedQuantity": "cantidad_ilimitada"
    }

    df = df.rename(columns=columns_rename)


    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    from io import StringIO
    import csv

    # Escribir el DataFrame en memoria como CSV
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, quoting=csv.QUOTE_MINIMAL)
    buffer.seek(0)

    # Inyectar a Postgres usando COPY para máximo rendimiento
    conn = engine.raw_connection()
    try:
        cursor = conn.cursor()
        cursor.copy_expert("COPY staging.stock_vtex_unimarc (vtex_id, id_warehouse, cantidad_total, cantidad_reservada, cantidad_ilimitada) FROM STDIN WITH CSV", buffer)
        conn.commit()
        cursor.close()
    except Exception as e:
        print(f"Error executing COPY: {e}")
        conn.rollback()
        raise e
    finally:
        conn.close()

    load_full_table_from_staging_to_s3(file_name, df, ts)

    return

def _save_vtex_stock_in_ecommdata(ti, ts):
    import requests
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import pandas as pd
    import sqlalchemy
    
    l_vtex_id = _load_vtex_id_list()

    if len(l_vtex_id) == 0:
        print('the list of vtex id was empty')
        return

    accountName = Variable.get("VTEX_ACCOUNT_NAME")
    env = Variable.get("VTEX_ENV")
    url_list = [f"https://{accountName}.{env}.com.br/api/logistics/pvt/inventory/skus/{i[0]}" for i in l_vtex_id]
    
    max_workers = 20
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=max_workers, pool_maxsize=max_workers)
    session.mount('https://', adapter)

    responses = []
    exception_cases = []

    X_VTEX_API_AppKey = Variable.get("X_VTEX_API_AppKey")
    X_VTEX_API_AppToken = Variable.get("X_VTEX_API_AppToken")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {
            executor.submit(fetch_vtex_stock, url, session, X_VTEX_API_AppKey, X_VTEX_API_AppToken): url
            for url in url_list
        }
        for future in as_completed(future_to_url):
            res = future.result()
            if res["status"] == "ok":
                responses.append({'json': res['json'], 'url': res['url']})
            else:
                exception_cases.append(res['url'])
    
    session.close()

    final_responses = []

    for response in responses:
        try:
            for balance in response['json']['balance']:
                aux = balance.copy()
                aux['skuId'] = response['json']['skuId']
                final_responses.append(aux)
        except KeyError as e:
            print(e)
            print(response)
            exception_cases.append(response['url'])
    
    _load_final_responses_to_postgres(final_responses, ts, 'stock_vtex')

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    date_path = ts[:10].replace("-","/")
    s3_path = f"vtex/api/get_stock_url_retries/{date_path}/"
    retries = s3_path+"retries"

    s3_hook.load_string(str(exception_cases),retries,bucket_name=s3_bucket,replace=True)
    ti.xcom_push(key = 'vtex_retries', value = retries)

    return

def _vtex_get_stock_retries(ti, ts):
    import requests
    from concurrent.futures import ThreadPoolExecutor, as_completed

    retries_file = ti.xcom_pull(key="vtex_retries", task_ids=["save_vtex_stock_in_ecommdata"])[0]
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+retries_file)
    if not s3_hook.check_for_key(retries_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % retries_file)

    retries_object = s3_hook.get_key(retries_file, bucket_name=s3_bucket)
    retries_string = retries_object.get()["Body"].read().decode('utf-8')[1:-1]
    retries_string = retries_string.replace("'","").strip()
    retries = [x.strip() for x in retries_string.split(",") if x.strip() != ""]

    if len(retries) == 0:
        print("No retries to process.")
        return
    
    print(f"Retrying {len(retries)} URLs in parallel...")
    max_workers = min(10, len(retries))
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=max_workers, pool_maxsize=max_workers)
    session.mount('https://', adapter)

    responses = []
    exception_cases = []

    X_VTEX_API_AppKey = Variable.get("X_VTEX_API_AppKey")
    X_VTEX_API_AppToken = Variable.get("X_VTEX_API_AppToken")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {
            executor.submit(fetch_vtex_stock, url, session, X_VTEX_API_AppKey, X_VTEX_API_AppToken, 5): url
            for url in retries
        }
        for future in as_completed(future_to_url):
            res = future.result()
            if res["status"] == "ok":
                responses.append({'json': res['json'], 'url': res['url']})
            else:
                exception_cases.append(res['url'])

    session.close()

    final_responses = []

    for response in responses:
        try:
            for balance in response['json']['balance']:
                aux = balance.copy()
                aux['skuId'] = response['json']['skuId']
                final_responses.append(aux)
        except KeyError as e:
            print(e)
            print(response)
            exception_cases.append(response['url'])

    if len(exception_cases) > 0:
        raise Exception(f'{len(exception_cases)} exception cases found during retry.')
    
    _load_final_responses_to_postgres(final_responses, ts, 'retries_stock_vtex')


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'etl_stock_incremental_load',
    default_args=default_args,
    description="Extracción y carga de tabla stock desde Vtex y Janis.",
    schedule_interval="0 1,4/4 * * *",
    start_date=pendulum.datetime(2022, 7, 11, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["DATA", "vtex", "janis", "staging", "unimarc", "vtex_stock", "janis_stock", "stock", "MATIAS"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla stock desde Vtex y Janis.
    """ 

    t0 = PostgresOperator(
        task_id = "truncate_janis_staging_table",
        postgres_conn_id="postgresql_conn",
        sql="""
        TRUNCATE staging.stock_unimarc
        """,
    )

    t1 = PostgresOperator(
        task_id = "truncate_vtex_staging_table",
        postgres_conn_id="postgresql_conn",
        sql="""
        TRUNCATE staging.stock_vtex_unimarc
        """,
    )

    t2 = PythonOperator(
        task_id = "load_full_table_to_s3",
        python_callable = load_full_table_to_s3,
        op_kwargs = {"table_name": "stock"}
    )

    t3 = PythonOperator(
        task_id = "save_table_stock",
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
        sql = "sql/stock_final.sql"
    )

    t7 = PostgresOperator(
        task_id = "delete_old_stock",
        postgres_conn_id = "postgresql_conn",
        sql = """DELETE
            FROM ecommdata.stock
            WHERE fecha <= '{{ds}}'::date - interval '21 days' """
    )


[t0, t2] >> t3
[t1, t3] >> t4 >> t5 >> t6 >> t7
