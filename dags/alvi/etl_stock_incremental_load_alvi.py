from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from utils.janis_alvi_utils import load_full_table_to_s3

from datetime import datetime

import pendulum




default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'etl_stock_alvi_incremental_load',
    default_args=default_args,
    description="Extracción y carga de tabla stock desde Vtex y Janis de alvi.",
    schedule_interval="0 0/4 * * *",
    start_date=pendulum.datetime(2023, 6, 19, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["DATA", "vtex", "janis", "staging", "alvi", "vtex_stock", "janis_stock", "stock"],
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla stock desde Vtex y Janis de alvi.
    """ 

    t0 = PostgresOperator(
        task_id = "truncate_janis_staging_table",
        postgres_conn_id="postgresql_conn",
        sql="""
        TRUNCATE staging.stock_alvi
        """,
    )

    #hay que construirlo
    t1 = PostgresOperator(
        task_id = "truncate_vtex_staging_table",
        postgres_conn_id="postgresql_conn",
        sql="""
        TRUNCATE staging.stock_vtex_alvi
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
    
    #construir tabla en base al sql
    t6 = PostgresOperator(
        task_id = "save_stock_final",
        postgres_conn_id = "postgresql_conn",
        sql = "sql/stock_final_alvi.sql"
    )

    t7 = PostgresOperator(
        task_id = "delete_old_stock",
        postgres_conn_id = "postgresql_conn",
        sql = """DELETE
            FROM ecommdata.stock_alvi
            WHERE fecha = '{{ds}}'::date - interval '21 days' """
    )


t0 >> t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7
