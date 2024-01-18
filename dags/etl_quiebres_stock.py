from airflow import DAG
from airflow import macros
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator


from datetime import datetime, timedelta
import pendulum


def funcion():
    print("todo bien acá")
    return



default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_quiebre_stock',
    default_args=default_args,
    description="Carga de datos de quiebres stock 60 dias S3.",
    schedule_interval="0 5 1,15 * *",
    start_date=pendulum.datetime(2022, 8, 25, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["DATA", "ecommdata", "S3"],
) as dag:

    dag.doc_md = """
        Quiebres de stock a 60 dias desde el historico de publicacion catálogo
    """ 

    t1 = PythonOperator(
        task_id = "funcion1",
        python_callable = funcion
    )
    t1 
