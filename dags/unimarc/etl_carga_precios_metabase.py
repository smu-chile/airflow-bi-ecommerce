from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pendulum


from datetime import datetime, timedelta

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_carga_precios_metabase',
    default_args=default_args,
    description="Carga de tabla de precios de tienda san felipe para Metabase",
    schedule_interval="0 7 * * *",
    start_date=pendulum.datetime(2022, 12, 1, tz="America/Santiago"),
    catchup=True,
    max_active_runs=1,
    tags=["precios", "modales", "unimarc","metabase", "SERGIO"],
) as dag:

    dag.doc_md = """
    Carga de tabla de precios de tienda san felipe para Metabase.
    """
    t0 = PostgresOperator(
        task_id = "truncate_table",
        postgres_conn_id="postgresql_conn",
        sql="""
        truncate ecommdata.precios_san_felipe
        """,
    )

    t1 = PostgresOperator(
        task_id = "load_table_precios_san_felipe",
        postgres_conn_id="postgresql_conn",
        sql="sql/precios_san_felipe.sql",
    )
    
    t0 >> t1