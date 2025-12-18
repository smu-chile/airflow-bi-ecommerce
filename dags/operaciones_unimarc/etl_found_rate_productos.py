from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor

from utils.slack_utils import dag_success_slack, dag_failure_slack

from datetime import datetime

import pendulum

def _get_query_order_ids_from_s3(ts):
    import pandas as pd

    curr_datetime = ts[:16].replace("-", "/").replace("T", "/").replace(":", "")
    orders_file = f"janis/replica/wms_orders/{curr_datetime}_wms_orders.csv"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+orders_file)
    if not s3_hook.check_for_key(orders_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % orders_file)

    orders_object = s3_hook.get_key(orders_file, bucket_name=s3_bucket)

    df = pd.read_csv(orders_object.get()["Body"])
    print(f"Number of records found: {len(df.index)}")
    order_ids = df["seq_id"].tolist()
    if len(order_ids) == 0:
        s3_object_name = "(0)"
        return s3_object_name
    query_order_ids = "(" + ",".join([str(order_id) for order_id in order_ids]) + ")"
    return query_order_ids

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_found_rate_productos_unimarc',
    default_args=default_args,
    description="Carga de tabla found_rate_productos",
    schedule_interval="*/30 * * * *",
    start_date=pendulum.datetime(2021, 9, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["DATA", "found_rate_productos", "operaciones_unimarc", "unimarc", "cyber", "MATIAS"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Carga de tabla found_rate_productos. El resultado final queda en datamart operaciones_unimarc.
    """ 
    t0 = ExternalTaskSensor(
        task_id="wait_for_modelo_ordenes",
        external_dag_id='etl_modelo_incremental_ordenes_unimarc',
        external_task_id=None,
        allowed_states=['success'],
        failed_states=['failed']
    )

    t2 = ExternalTaskSensor(
        task_id="wait_for_orden_producto_pesables",
        external_dag_id='etl_orden_producto_pesables_incremental_load',
        external_task_id=None,
        allowed_states=['success'],
        failed_states=['failed']
    )
    
    t3 = PythonOperator(
        task_id = "get_query_order_ids_from_s3",
        python_callable = _get_query_order_ids_from_s3
    )
    
    t4 = PostgresOperator(
        task_id = "load_table_foundrate",
        postgres_conn_id="postgresql_conn",
        sql="sql/found_rate_productos.sql",
    )

    t5 = PostgresOperator(
        task_id = "delete_old_data",
        postgres_conn_id="postgresql_conn",
        sql="""
        DELETE from operaciones_unimarc.found_rate_productos
        WHERE fecha_facturacion <= to_date('{{execution_date.strftime('%Y-%m-%d')}}', '%YYYY-%mm-%dd') - interval '24 months'
        """,
    )

    [t0, t2] >> t3 >> t4 >> t5
