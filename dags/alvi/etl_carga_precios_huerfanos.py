from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pendulum
from datetime import datetime, timedelta

from utils.slack_utils import dag_success_slack, dag_failure_slack

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_carga_precios_huerfanos',
    default_args=default_args,
    description="Carga de tabla de precios para productos huérfanos que no están en Pajaritos (Alvi).",
    schedule_interval="30 7 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["precios", "alvi", "huerfanos"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Carga de tabla de precios para productos huérfanos en Alvi.
    Detecta productos que no están en la lista8 de Pajaritos (SAP ID 3092), elige la tienda ganadora según reglas comerciales e inyecta esos precios en todas las tiendas (incluyendo Pajaritos) en la tabla `ecommdata_alvi.precios_huerfanos`.
    """
    
    t0 = PostgresOperator(
        task_id = "truncate_table_precios_huerfanos",
        postgres_conn_id="postgresql_conn",
        sql="""
        TRUNCATE ecommdata_alvi.precios_huerfanos;
        """,
    )

    t1 = PostgresOperator(
        task_id = "load_table_precios_huerfanos",
        postgres_conn_id="postgresql_conn",
        sql="sql/precios_huerfanos.sql",
    )
    
    t0 >> t1
