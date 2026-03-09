from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator as PostgresOperator
import pendulum

from utils.slack_utils import dag_success_slack, dag_failure_slack

from datetime import datetime, timedelta

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_carga_precios_pajaritos',
    default_args=default_args,
    description="Carga de tabla de precios para Alvi desde Pajaritos.",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2022, 12, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["precios", "modales", "alvi","metabase", "KEVIN"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Carga de tabla de precios para Alvi desde Pajaritos.
    """
    t0 = PostgresOperator(
        task_id = "truncate_table",
        conn_id="postgresql_conn",
        sql="""
        truncate ecommdata_alvi.precios_pajaritos
        """,
    )

    t1 = PostgresOperator(
        task_id = "load_table_precios_pajaritos",
        conn_id="postgresql_conn",
        sql="sql/precios_pajaritos.sql",
    )
    
    t0 >> t1