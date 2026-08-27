from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import pendulum

# from utils.slack_utils import dag_success_slack, dag_failure_slack

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'etl_ranking_top_100',
    default_args=default_args,
    description="Carga del ranking top 100 de productos con promociones vigentes en ecommdata.ranking_top_100",
    schedule_interval="0 7 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["DATA", "ecommdata", "ranking", "promociones", "Unimarc"],
    # on_success_callback=dag_success_slack,
    # on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Carga de ranking top 100 productos en promociones vigentes en la tabla ecommdata.ranking_top_100.
    """

    t0 = PostgresOperator(
        task_id="load_ranking_top_100",
        postgres_conn_id="postgresql_conn",
        sql="sql/ranking_top_100.sql",
    )

    t0
