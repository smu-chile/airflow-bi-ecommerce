from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator

from datetime import datetime, timedelta
import pendulum

def _check_if_dag_ran_today(ds, dag_latest_run):
    print(dag_latest_run)
    print(ds == dag_latest_run)

    if ds != dag_latest_run:
        print("stock_and_prices_full_post_request")
        return "stock_and_prices_full_post_request"
    else:
        print("stock_and_prices_delta_post_request")
        return "stock_and_prices_delta_post_request"

def _stock_and_prices_full_post_request():
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
        task_id = "check_if_dag_ran_today",
        python_callable = _check_if_dag_ran_today,
        op_kwargs = {
            "dag_latest_run": "{{dag.get_latest_execution_date()}}"
        }
    )

    t1 = PythonOperator(
        task_id = "stock_and_prices_full_post_request",
        python_callable = _stock_and_prices_full_post_request
    )

    t2 = PythonOperator(
        task_id = "stock_and_prices_delta_post_request",
        python_callable = _stock_and_prices_delta_post_request
    )



