from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

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
    'etl_ventas_unimarc_incremental_load',
    default_args=default_args,
    description="Carga de tabla found_rate_productos",
    schedule_interval="0 7 * * *",
    start_date=pendulum.datetime(2021, 10, 1, tz="America/Santiago"),
    catchup=True,
    max_active_runs = 1,
    tags=["DATA", "ventas", "ventas_unimarc", "unimarc", "MATIAS"],
) as dag:

    dag.doc_md = """
    Carga de tabla ventas. El resultado final queda en datamart ventas_unimarc.
    """ 
    t0 = PostgresOperator(
        task_id = "load_table_ventas_staging",
        postgres_conn_id="postgresql_conn",
        sql="sql/ventas_staging.sql",
    )

    t1 = PostgresOperator(
        task_id = "load_table_ventas_contr2_staging",
        postgres_conn_id="postgresql_conn",
        sql="sql/ventas_contr2_staging.sql",
    )

    t2 = PostgresOperator(
        task_id = "load_table_ventas",
        postgres_conn_id="postgresql_conn",
        sql="sql/ventas.sql",
    )

    t3 = PostgresOperator(
        task_id = "delete_ventas_staging_data",
        postgres_conn_id="postgresql_conn",
        sql="truncate staging.ventas_unimarc;",
    )

    t4 = PostgresOperator(
        task_id = "delete_ventas_contr2_staging_data",
        postgres_conn_id="postgresql_conn",
        sql="truncate staging.ventas_unimarc_contr2;",
    )

    t3 >> t0
    t4 >> t0
    t0 >> t1 >> t2
