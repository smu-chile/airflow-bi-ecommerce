from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.dummy import DummyOperator

from datetime import datetime, timedelta

import pendulum

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_resumen_diario',
    default_args=default_args,
    description="Carga de tabla resumen diario",
    schedule_interval="30 8 * * *",
    start_date=pendulum.datetime(2023, 10, 23, tz="America/Santiago"),
    catchup=True,
    max_active_runs=1,
    tags=["DATA", "resumen_diario", "ecommdata", "unimarc", "MATIAS"],
) as dag:

    dag.doc_md = """
    Carga de tabla publicacion_catalogo. El resultado final queda en ecommdata.
    """ 

    t0 = PostgresOperator(
        task_id = "load_table_resumen_diario",
        postgres_conn_id="postgresql_conn",
        sql="sql/resumen_diario.sql",
    )

    
    t0 
