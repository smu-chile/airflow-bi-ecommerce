from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor

from datetime import datetime



default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_found_rate_productos_alvi',
    default_args=default_args,
    description="Carga de tabla found_rate_productos de Alvi",
    schedule_interval="30 * * * *",
    start_date=datetime(2022, 5, 1),
    catchup=True,
    tags=["DATA", "found_rate_productos", "operaciones_alvi", "Alvi"],
) as dag:

    dag.doc_md = """
    Carga de tabla found_rate_productos de Alvi. El resultado final queda en datamart operaciones_alvi.
    """ 
    t0 = ExternalTaskSensor(
        task_id="wait_for_ordenes_janis",
        external_dag_id='etl_ordenes_janis_alvi_incremental_load',
        external_task_id=None,
        allowed_states=['success'],
        failed_states=['failed', 'skipped']
    )

    t1 = ExternalTaskSensor(
        task_id="wait_for_orden_productos",
        external_dag_id='etl_orden_productos_alvi_incremental_load',
        external_task_id=None,
        allowed_states=['success'],
        failed_states=['failed', 'skipped']
    )

    t2 = ExternalTaskSensor(
        task_id="wait_for_orden_producto_pesables",
        external_dag_id='etl_orden_producto_pesables_alvi_incremental_load',
        external_task_id=None,
        allowed_states=['success'],
        failed_states=['failed']
    )
    
    t3 = PostgresOperator(
        task_id = "load_table_foundrate",
        postgres_conn_id="postgresql_conn",
        sql="sql/found_rate_productos_alvi.sql",
    )

    t4 = PostgresOperator(
        task_id = "delete_old_data",
        postgres_conn_id="postgresql_conn",
        sql="""
        DELETE from operaciones_alvi.found_rate_productos
        WHERE fecha_facturacion <= to_date('{{execution_date.strftime('%Y-%m-%d')}}', '%YYYY-%mm-%dd') - interval '24 months'
        """,
    )

    [t0, t1, t2] >> t3 >> t4
