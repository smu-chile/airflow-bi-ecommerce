from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import timedelta
import pendulum

from utils.slack_utils import dag_success_slack, dag_failure_slack

defaul_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="etl_costos_logisticos_unimarc",
    description="Carga diaria de pedidos prefactura, armado y asegurado de Unimarc",
    default_args=defaul_args,
    schedule_interval="0 7 * * *",
    start_date=pendulum.datetime(2025, 2, 26, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["logistica", "costos", "unimarc", "KEVIN"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    t0 = PostgresOperator(
        task_id="pedidos_prefactura_unimarc",
        postgres_conn_id="postgresql_conn",
        sql="sql/pedidos_prefactura_unimarc.sql",
    )

    t1 = PostgresOperator(
        task_id="estimacion_costo_armado",
        postgres_conn_id="postgresql_conn",
        sql="sql/estimacion_costo_armado.sql",
    )

    t2 = PostgresOperator(
        task_id="estimacion_costo_asegurado",
        postgres_conn_id="postgresql_conn",
        sql="sql/estimacion_costo_asegurado.sql",
    )

    t0 >> t1 >> t2
