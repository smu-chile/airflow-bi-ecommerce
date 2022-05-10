from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime



default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'foundrate',
    default_args=default_args,
    description="Carga de tabla foundrate",
    schedule_interval="30 * * * *",
    start_date=datetime(2022, 5, 9),
    catchup=True,
    tags=["DATA", "foundrate", "operaciones_unimarc"],
) as dag:

    dag.doc_md = """
    Carga de tabla foundrate. El resultado final queda en datamart operaciones_unimarc.
    """ 
    t0 = PostgresOperator(
        task_id = "load_table_foundrate",
        postgres_conn_id="postgresql_conn",
        sql="sql/foundrate.sql",
    )

    t1 = PostgresOperator(
        task_id = "delete_old_data",
        postgres_conn_id="postgresql_conn",
        sql="""
        DELETE from operaciones_unimarc.found_rate
        WHERE fecha_facturacion <= to_date('{{execution_date.strftime('%Y-%m-%d')}}', '%YYYY-%mm-%dd') - interval '5 days'
        """,
    )

    t0 >> t1
