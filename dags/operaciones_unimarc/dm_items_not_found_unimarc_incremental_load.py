from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime

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
    order_ids = df["id"].tolist()
    if len(order_ids) == 0:
        s3_object_name = "(0)"
        return s3_object_name
    query_order_ids = "(" + ",".join([str(order_id) for order_id in order_ids]) + ")"
    return query_order_ids

def _select_table_from_ecommdata(ts):
    query = """
    select frp.ref_id
    , s.ean_primario as ean
    , frp.id_tienda
    , frp.fecha_picking
    from operaciones_unimarc.found_rate_productos frp
    left join ecommdata.skus s on frp.ref_id = s.ref_id 
    where frp.estado_foundrate = 1 and frp.orden in {{ti.xcom_pull(key="return_value", task_ids=['get_query_order_ids_from_s3'])[0]}};
    """
    pg_hook = PostgresHook("postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    print(results)
    cursor.close()
    pg_connection.close()
    return results

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0
}
with DAG(
    'dm_productos_no_encontrados',
    default_args=default_args,
    description="Carga de tabla de productos no encontrados",
    schedule_interval="30 * * * *",
    start_date=datetime(2022, 6, 2),
    catchup=False,
    tags=["data", "datamind", "not_found", "unimarc"],
) as dag:

    dag.doc_md = """
    Carga de tabla de productos no encontrados en base a datos de found rate unimarc.
    """ 
    t0 = ExternalTaskSensor(
        task_id="wait_for_found_rate_productos",
        external_dag_id='etl_found_rate_productos_unimarc',
        external_task_id=None,
        allowed_states=['success'],
        failed_states=['failed']
    )

    t1 = PythonOperator(
        task_id = "get_query_order_ids_from_s3",
        python_callable = _get_query_order_ids_from_s3
    )

    t2 = PythonOperator(
        task_id = "select_table_from_ecommdata",
        python_callable = _select_table_from_ecommdata
    )

    t0 >> t1 >> t2
