from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator as PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.standard.operators.empty import EmptyOperator

from utils.slack_utils import dag_success_slack, dag_failure_slack

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
    'etl_resumen_catalogo',
    default_args=default_args,
    description="Carga de tabla resumen catalogo",
    schedule="30 8 * * *",
    start_date=pendulum.datetime(2023, 10, 23, tz="America/Santiago"),
    catchup=True,
    max_active_runs=1,
    tags=["DATA", "resumen_catalogo", "ecommdata", "unimarc", "MATIAS"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Carga de tabla resumen catalogo. El resultado final queda en ecommdata.
    """ 

    t0 = PostgresOperator(
        task_id = "load_table_resumen_diario",
        conn_id="postgresql_conn",
        sql="sql/resumen_catalogo.sql",
    )

    
    t0 
