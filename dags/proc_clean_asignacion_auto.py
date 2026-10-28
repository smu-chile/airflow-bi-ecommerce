from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pendulum

from utils.slack_utils import dag_success_slack, dag_failure_slack

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    "proc_clean_asignacion_auto",
    default_args=default_args,
    description="Trunca diariamente la tabla picker_turnos_asignaciones",
    schedule_interval="0 21 * * *",
    start_date=pendulum.datetime(2026, 10, 26, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["picker", "asignaciones", "unimarc", "limpieza","RODRIGO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Limpieza diaria (21:00 hora Chile) de la tabla
    `ecommdata.picker_turnos_asignaciones` mediante TRUNCATE.
    """

    truncate_picker_turnos_asignaciones = PostgresOperator(
        task_id="truncate_picker_turnos_asignaciones",
        postgres_conn_id="postgresql_conn",
        sql="TRUNCATE TABLE ecommdata.picker_turnos_asignaciones;",
    )
