from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor

from queue import Queue
import requests
from threading import Thread
import time
import pandas as pd

from datetime import datetime

session = requests.session()
results = {}

class Worker(Thread):
    """ Thread executing tasks from a given tasks queue """

    def __init__(self, tasks):
        Thread.__init__(self)
        self.tasks = tasks
        self.daemon = True
        self.start()

    def run(self):
        while True:
            func, args, kargs = self.tasks.get()
            try:
                func(*args, **kargs)
            except Exception as e:
                # An exception happened in this thread
                print(e)
            finally:
                # Mark this task as done, whether an exception happened or not
                self.tasks.task_done()


class ThreadPool:
    """ Pool of threads consuming tasks from a queue """

    def __init__(self, num_threads):
        self.tasks = Queue(num_threads)
        for _ in range(num_threads):
            Worker(self.tasks)

    def add_task(self, func, *args, **kargs):
        """ Add a task to the queue """
        self.tasks.put((func, args, kargs))

    def map(self, func, args_list):
        """ Add a list of tasks to the queue """
        for args in args_list:
            self.add_task(func, args)

    def wait_completion(self):
        """ Wait for completion of all the tasks in the queue """
        self.tasks.join()

def get(url):
    print(url)
    i = url.split('/')[-1]
    X_VTEX_API_AppKey = Variable.get("X_VTEX_API_AppKey")
    X_VTEX_API_AppToken = Variable.get("X_VTEX_API_AppToken")
    r = session.get(url, headers = {"X-VTEX-API-AppKey" : X_VTEX_API_AppKey, "X-VTEX-API-AppToken" : X_VTEX_API_AppToken})
    results[i] = r.json()

def _load_vtex_id_list():
    query = "SELECT id FROM staging.stock_unimarc"
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    pg_connection.close()
    return results

def _get_stock_from_vtex(ti):
    l_vtex_id = ti.xcom_pull(key="return_value", task_ids=["load_vtex_id_list"])[0]
    l_vtex_id = l_vtex_id[0]
    accountName = Variable.get("VTEX_ACCOUNT_NAME")
    env = Variable.get("VTEX_ENV")
    urls = [f"https://{accountName}.{env}.com.br/api/logistics/pvt/inventory/skus/{i}" for i in l_vtex_id]
    
    pool = ThreadPool(40)
    now = time.time()
    pool.map(get, urls)
    pool.wait_completion()
    time_taken = time.time() - now

    df = pd.DataFrame()

    print("Time taken to get data from vtex:")
    print(time_taken)
    for i in results:
        for j in results[i]['balance']:
            aux = j
            aux['skuId'] = i
            print(aux)
            aux = {k:[v] for k,v in aux.items()}
            temp = pd.DataFrame.from_dict(aux)
            df = pd.concat([df, temp])
    return df

def _save_vtex_stock_in_ecommdata(ti):
    df = ti.xcom_pull(key="return_value", task_ids=["get_stock_from_vtex"])[0]

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

    columns = ["id_tienda", "cantidad_total", "cantidad_reservada", "cantidad_ilimitada",]

    columns_query = ",".join(columns)
    excluded_query = ",".join(["EXCLUDED."+column for column in columns])
    values_query = "%s,"+",".join(["%s" for column in columns])
    df = df.fillna("NULL")
    records = list(df.to_records(index=False))

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
        INSERT INTO ecommdata.vtex_stock (vtex_id,"""+columns_query+""") 
        VALUES ("""+values_query+""")
        ON CONFLICT ON CONSTRAINT vtex_stock_pk
        DO UPDATE SET ("""+columns_query+""") = ("""+excluded_query+""") 
    """

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
    'etl_vtex_stock_incremental_load',
    default_args=default_args,
    description="Extracción y carga de tabla vtex_stock desde Vtex hasta Workspace.",
    schedule_interval="0 */4 * * *",
    start_date=datetime(2022, 6, 23),
    catchup=False,
    max_active_runs = 1,
    tags=["DATA", "vtex", "ecommdata", "unimarc", "stock"],
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla vtex_stock desde Vtex hasta Workspace.
    """ 

#    t0 = ExternalTaskSensor(
#        task_id="wait_for_stock_staging",
#        external_dag_id='etl_stock_staging',
#        external_task_id=None,
#        allowed_states=['success'],
#        failed_states=['failed']
#    )

    t1 = PythonOperator(
        task_id = "load_vtex_id_list",
        python_callable = _load_vtex_id_list
    )

    t2 = PythonOperator(
        task_id = "get_stock_from_vtex",
        python_callable = _get_stock_from_vtex
    )

    t3 = PythonOperator(
        task_id = "save_vtex_stock_in_ecommdata",
        python_callable = _save_vtex_stock_in_ecommdata
    )

t1 >> t2 >> t3
#t0 >> t1 >> t2 >> t3
