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
    order_ids = df["seq_id"].tolist()
    if len(order_ids) == 0:
        s3_object_name = "(0)"
        return s3_object_name
    query_order_ids = "(" + ",".join([str(order_id) for order_id in order_ids]) + ")"
    return query_order_ids

def _insert_table_from_ecommdata_into_DM(ti):
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text
    query = f"""
    select frp.ref_id
    , s.ean_primario as ean
    , frp.id_tienda
    , frp.fecha_picking
    , m.nombre as marca
    from operaciones_unimarc.found_rate_productos frp
    left join ecommdata.skus s on frp.ref_id = s.ref_id
    left join ecommdata.productos p on frp.ref_id = p.ref_id
    left join ecommdata.marcas m on p.id_marca = m.id
    where frp.estado_foundrate = 1 and frp.orden in {ti.xcom_pull(key="return_value", task_ids=['get_query_order_ids_from_s3'])[0]} and m.nombre = 'SOPROLE';
    """
    pg_hook = PostgresHook("postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    pg_connection.close()
    
    df = pd.DataFrame(
        data = results,
        columns = ['ref_id', 'ean', 'id_tienda', 'fecha_picking', 'marca']
    )

    print("Number of records to be loaded: "+str(len(df.index)))

    host = Variable.get("DM_HOST")
    database = Variable.get("DM_DB")
    username = Variable.get("DM_USER")
    password = Variable.get("DM_PASSWORD")
    
    conn_url = "postgresql+psycopg2://"+username+":"+password+"@"+host+":5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    connection = engine.connect()
    truncate_query = "TRUNCATE TABLE datamind.alerta_found_rate"
    connection.execute(text(truncate_query))
    connection.close()

    # Save to PostgreSQL:
    df.to_sql(name="alerta_found_rate",
                con=engine,         
                schema="datamind",         
                if_exists='append',         
                index=False,         
                chunksize=20000,         
                method='multi')

    print("Data saved to PostgreSQL. Table: datamind.alerta_found_rate")

    

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
        task_id = "insert_table_from_ecommdata_into_DM",
        python_callable = _insert_table_from_ecommdata_into_DM
    )
    

    t0 >> t1 >> t2
