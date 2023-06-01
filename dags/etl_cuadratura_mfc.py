from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

import pendulum

def funcion_crear_data():

    return

def funcion_subir_s3():

    return

def funcion_subir_postgres():

    return


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_cuadratura_mfc',
    default_args=default_args,
    description="crear y cargar cuadratura del dia para MFC",
    schedule_interval=None,    #preguntar a mati k va por acá
    start_date=pendulum.datetime(2023, 6, 1, tz="America/Santiago"),
    catchup=False,
    tags=["catalogo", "cuadratura", "MFC", "unimarc"],
) as dag:
    
    dag.doc_md = """
    construir y cargar cuadratura mfc. \n
    Delete and INSERT en tabla catalogo.cuadratura_mfc.
    """ 

    t0 = PythonOperator(
        task_id = "funcion_crear_data",
        python_callable = funcion_crear_data,
    )

    t1 = PythonOperator(
        task_id = "funcion_subir_s3",
        python_callable = funcion_subir_s3,
    )

    t2 = PythonOperator(
        task_id = "funcion_subir_postgres",
        python_callable = funcion_subir_postgres
    )

    t0 >> t1 >> t2