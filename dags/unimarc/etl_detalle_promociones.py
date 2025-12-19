from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
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
    'etl_detalle_promociones',
    default_args=default_args,
    description="Carga de tabla detalle promociones",
    schedule_interval="0 7 * * *",
    start_date=pendulum.datetime(2022, 12, 1, tz="America/Santiago"),
    catchup=True,
    max_active_runs=1,
    tags=["detalle_promociones", "venta", "unimarc", "MATIAS"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Carga de tabla detalle promociones. Realizada a través de query de datos internos.
    """ 
    t0 = PostgresOperator(
        task_id = "load_table_detalle_promociones",
        postgres_conn_id="postgresql_conn",
        sql="sql/detalle_promociones.sql",
    )

    t1 = PostgresOperator(
        task_id = "delete_old_data",
        postgres_conn_id="postgresql_conn",
        sql="""
            DELETE FROM ventas_unimarc.detalle_promociones
            WHERE fecha_creacion < '{{ds}}'::date - interval '730 days'
            """,
    )

    t0 >> t1
