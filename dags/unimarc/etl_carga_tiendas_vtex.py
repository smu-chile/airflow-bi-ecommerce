from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

import pendulum

def func():
    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_carga_tiendas_vtex',
    default_args=default_args,
    description="Carga y elimina tradePolicy de tiendas a los productos en vte",
    schedule_interval="0 10 * * *",
    start_date=pendulum.datetime(2024, 7, 30, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "tiendas", "Productos", "ecommdata", "VTEX", "unimarc", "PATRICIO"],
) as dag:
    

    dag.doc_md = """
    Carga y elimina tradePolicy de tiendas a los productos en vtex\n
    guardar en S3.
    """ 

    t0 = PythonOperator(
        task_id = 'func',
        python_callable=func,
    )

    t0