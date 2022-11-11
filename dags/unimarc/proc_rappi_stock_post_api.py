from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator, BranchPythonOperator

from datetime import datetime, timedelta
import pendulum

def _check_time(ts):
    import pytz

    exec_datetime = datetime.strptime(ts[:19], '%Y-%m-%dT%H:%M:%S')
    exec_datetime_utc = pendulum.timezone("utc").convert(exec_datetime)
    local_tz = pendulum.timezone("America/Santiago")
    exec_datetime_local = local_tz.convert(exec_datetime_utc)
    time_str = exec_datetime_local.strftime('%H%M')
    print(f"Local execution time: {exec_datetime_local.strftime('%Y/%m/%d %H:%M:%S')}")
    if int(time_str[:2]) > 23 or int(time_str[:2]) < 8:
        print("Outside execution hours. Skipping tasks.")
        return "skip_dag_run"
    else:
        print("Expected time range. Executing tasks.")
        return "check_if_dag_ran_today"

def _check_if_dag_ran_today(ds):
    exec_date_string = ds.replace("-", "/")
    response_files_path = f"rappi/api/stock/post/full/responses/{exec_date_string}/"

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching prefix: "+response_files_path)
    if not s3_hook.check_for_prefix(bucket_name=s3_bucket, prefix=response_files_path, delimiter="/"):
        print("Response prefix not found.\nExecuting a FULL LOAD...")
        return "calculate_full_request_body"
    else:
        print("Response prefix found.\nExecuting a DELTA LOAD...")
        return "calculate_delta_request_body"


def _calculate_request_body(ds, ts, type):
    import json
    import os
    import pandas as pd

    curr_working_directory = os.getcwd()
    print(os.getcwd())
    with open(curr_working_directory+f"/dags/unimarc/sql/rappi_stock_{type}_load.sql", "r") as query_file:
        rappi_stock_query = query_file.read()
    
    exec_datetime_string = ts[:16].replace("-", "/").replace("T", "/").replace(":", "")
    body_file_path = f"rappi/api/stock/post/{type}/requests/{exec_datetime_string}_"
    if type == 'full':
        rappi_stock_query = rappi_stock_query.replace("{ds}", ds)
    else:
        rappi_stock_query = rappi_stock_query.replace("{ds}", ds)

    print(rappi_stock_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    df = pd.read_sql_query(rappi_stock_query, pg_connection)
    print(f"Number of records found: {len(df.index)}")

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    active_stores = df["store_id"].unique().tolist()
    store_body_file_paths = []
    for store_id in active_stores:
        df = df[df["store_id"] == store_id].head(10)
        dict_body = df.to_dict(orient="records")
        json_body = json.dumps(dict_body, ensure_ascii=False)

        store_body_file_path = body_file_path + store_id + ".json"

        s3_hook.load_string(json_body,
                    key=store_body_file_path,
                    bucket_name=s3_bucket,
                    replace=True,
                    encrypt=False)
        
        store_body_file_paths.append(store_body_file_path)

    return store_body_file_paths

def _stock_and_prices_full_post_request(ds):
    print("FULL LOAD")
    
    return

def _stock_and_prices_delta_post_request():
    print("DELTA LOAD")
    return

default_args = {
    "owner": "capacity_and_planning",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    "proc_rappi_post_stock_precio",
    default_args=default_args,
    description="Carga de stock y precios regulares a través de la API de Rappi.",
    schedule_interval=None, 
    start_date=pendulum.datetime(2022, 11, 2, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    concurrency=2,
    tags=["OPS", "Rappi", "API", "POST", "stock", "precios"],
) as dag:

    dag.doc_md = """
    Envía stock y precios regulares para cada SKU perteneciente a las tiendas presentes en Rappi \n
    Por cada tienda presente en Rappi (esto es, que tengan id_rappi no nulo en ecommdata.tiendas), se 
    obtiene el stock y precios regulares desde ecommdata.stock y ecommdata.precios y se envían a un
    endpoint de Rappi mediante una POST request.\n
    Este proceso depende del DAG *etl_stock_incremental_load*.\n
    El proceso cuenta con dos reglas de ejecución:\n
    - Full load: la primera carga del día debe ser una carga completa por cada tienda activa.\n
    - Delta load: las cargas siguentes del día deben representar la variación de stock.
    """ 

    t0 = BranchPythonOperator(
        task_id = "check_time",
        python_callable = _check_time
    )

    t1 = BranchPythonOperator(
        task_id = "check_if_dag_ran_today",
        python_callable = _check_if_dag_ran_today,
        op_kwargs = {
            "dag_latest_run": "{{dag.get_latest_execution_date()}}"
        }
    )

    t2 = PythonOperator(
        task_id = "calculate_full_request_body",
        python_callable = _calculate_request_body,
        op_kwargs = {
            "type": "full"
        }
    )

    t3 = PythonOperator(
        task_id = "calculate_delta_request_body",
        python_callable = _calculate_request_body,
        op_kwargs = {
            "type": "delta"
        }
    )

    td = DummyOperator(
        task_id = "skip_dag_run"
    )

    t0 >> [t1, td] 
    t1 >> [t2, t3]

