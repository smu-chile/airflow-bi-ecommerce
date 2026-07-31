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
    'etl_stock_trapenses_v2_comparison',
    default_args=default_args,
    description="Monitoreo y cálculo horario de cambios de stock para Los Trapenses v2 comparando Janis v1",
    schedule_interval="0 * * * *",
    start_date=pendulum.datetime(2026, 7, 31, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["DATA", "janis", "stock", "trapenses", "monitoreo"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Cálculo de delta de stock por hora para tienda Los Trapenses.
    Compara el stock actual en Janis v1 (bodega 3968) con la tabla de imagen ecommdata.stock_trapenses_v2.
    Registra el conteo de cambios en ecommdata.stock_trapenses_v2_changes y actualiza la tabla de imagen.
    """

    calculate_changes = PostgresOperator(
        task_id="calculate_and_apply_changes",
        postgres_conn_id="postgresql_conn",
        sql="sql/stock_trapenses_v2_comparison.sql"
    )

    calculate_changes
