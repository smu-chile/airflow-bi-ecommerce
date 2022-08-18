from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor

from datetime import datetime, timedelta

def _store_periodic_data(ts):
    from io import StringIO
    import boto3
    import pandas as pd

    dt_string = ts[:16]
    curr_datetime = dt_string.replace('T',' ')
    curr_dt_object = datetime.strptime(curr_datetime, "%Y-%m-%d %H:%M")
    past_dt_object = curr_dt_object - timedelta(weeks = 2)
    past_datetime = past_dt_object.strftime("%Y/%d/%m/%H%M")
    prefix = "ecommdata/publicacion_catalogo/"+past_datetime
    file_name = prefix+"publicacion_catalogo_periodico.csv"

    select_query = f"""
        select *
        from ecommdata.publicacion_catalogo pc
        where pc.fecha_hora < '{ts}'::timestamp - interval '14 days' and pc.fecha_hora::time <> '12:00:00'
    """
    print(select_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    df = pd.read_sql_query(select_query, pg_connection)

    buffer = StringIO()
    df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    buffer.seek(0)

    access_key = Variable.get("AWS_ACCESS_KEY")
    secret_key = Variable.get("AWS_SECRET_KEY")
    bucket_name = Variable.get("AWS_S3_BUCKET_NAME")
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name = "us-east-1"
    )
    response = s3_client.put_object(
        Bucket=bucket_name, Key=file_name, Body=buffer.getvalue()
    )

    return

def _delete_periodic_data(ts):

    delete_query = f"""
        delete
        from ecommdata.publicacion_catalogo pc
        where pc.fecha_hora < '{ts}'::timestamp - interval '14 days' and pc.fecha_hora::time <> '12:00:00'
    """

    print(delete_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(delete_query)
    pg_connection.commit()
    cursor.close()

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_publicacion_catalogo',
    default_args=default_args,
    description="Carga de tabla publicacion catalogo",
    schedule_interval="0 */4 * * *",
    start_date=datetime(2022, 8, 18),
    catchup=False,
    max_active_runs=1,
    tags=["DATA", "publicacion_catalogo", "ecommdata", "unimarc"],
) as dag:

    dag.doc_md = """
    Carga de tabla publicacion_catalogo. El resultado final queda en ecommdata.
    """ 
    t0 = ExternalTaskSensor(
        task_id="wait_for_stock",
        external_dag_id='etl_stock_incremental_load',
        external_task_id=None,
        allowed_states=['success'],
        failed_states=['failed']
    )
    
    t1 = PostgresOperator(
        task_id = "load_table_publicacion_catalogo",
        postgres_conn_id="postgresql_conn",
        sql="sql/publicacion_catalogo.sql",
    )

    t2 = PythonOperator(
        task_id = "store_periodic_data",
        python_callable = _store_periodic_data
    )

    t3 = PythonOperator(
        task_id = "delete_periodic_data",
        python_callable = _delete_periodic_data
    )

    t0 >> t1 >> t2 >> t3
