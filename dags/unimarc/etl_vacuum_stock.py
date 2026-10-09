from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from utils.slack_utils import dag_failure_slack
import pendulum

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

with DAG(
    "etl_vacuum_stock",
    default_args=default_args,
    description="Mantenimiento asincrono y optimizacion de ecommdata.stock tras carga incremental.",
    schedule_interval=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["DATA", "mantenimiento", "vacuum", "unimarc", "stock"],
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    ### Mantenimiento Asíncrono de Stock (VACUUM ANALYZE)
    Este DAG es disparado automáticamente por `etl_stock_incremental_load` al terminar de procesar todas las tiendas.
    
    **Propósito:**
    1. Limpiar tuplas muertas generadas durante los INSERT/DELETE de las 150 tiendas y depuración histórica (>21 días).
    2. Actualizar las estadísticas de la tabla en Postgres (`ANALYZE`) para optimizar los planes de ejecución de consultas downstream.
    3. Correr de manera asíncrona (`wait_for_completion=False`) sin demorar ni bloquear los pipelines principales de negocio.
    """

    vacuum_stock = PostgresOperator(
        task_id="vacuum_analyze_stock",
        postgres_conn_id="postgresql_conn",
        sql="VACUUM ANALYZE ecommdata.stock;",
        autocommit=True,
    )

    vacuum_stock
