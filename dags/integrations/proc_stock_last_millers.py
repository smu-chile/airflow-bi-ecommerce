from airflow import DAG
from airflow import macros
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable

import pendulum
from datetime import timedelta

def _get_last_millers_stores():
    last_millers_stores_query = """
        SELECT id
        FROM integraciones.tiendas_last_millers;
    """
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(last_millers_stores_query)
    results = cursor.fetchall()
    cursor.close()
    pg_connection.close()
    return results

def _get_stock_and_promo_prices_from_datawarehouse(ds, ti):
    import io
    import jaydebeapi
    import os
    import pandas as pd

    ids_tiendas = ti.xcom_pull(key="return_value", task_ids=["get_last_millers_stores"])[0]
    
    curr_working_directory = os.getcwd()
    print(os.getcwd())
    with open(curr_working_directory+"/dags/integrations/sql/stock_precio_promo.sql", "r") as query_file:
        base_query = query_file.read()

    exec_date = macros.ds_add(ds, 1)
    exec_date = exec_date.replace("-", "/")

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    dsn_database = Variable.get("DW_SECRET_DATABASE") 
    dsn_hostname = Variable.get("DW_SECRET_HOSTNAME")
    dsn_port = "5480" 
    dsn_uid = Variable.get("DW_SECRET_USER")
    dsn_pwd = Variable.get("DW_PASSWORD")
    jdbc_driver_name = "org.netezza.Driver" 
    jdbc_driver_loc = os.path.join('/opt/airflow/include/jdbcdriver/nzjdbc.jar')

    connection_string = 'jdbc:netezza://' + dsn_hostname + ':' + dsn_port + '/' + dsn_database
    conn = jaydebeapi.connect(jdbc_driver_name, connection_string, {'user': dsn_uid, 'password': dsn_pwd},jars=jdbc_driver_loc)
    cur = conn.cursor()

    for tienda in ids_tiendas:
        id_tienda = tienda[0]
        tienda_query = base_query.replace("{store_id}", id_tienda)
        
        file_name = f"integraciones/last_millers/stock/datawarehouse/{exec_date}/{id_tienda}.csv"

        # Check if file is already loaded
        if s3_hook.check_for_key(file_name, bucket_name=s3_bucket):
            print(f"File {file_name} already exists on S3 bucket. Skipping...")
            continue

        print("Ejecutando tienda:" + id_tienda)

        cur.execute(tienda_query)
        results = cur.fetchall()
        columns = [i[0] for i in cur.description]
        df = pd.DataFrame(results, columns=columns)
        print(f"Records found: {len(df.index)}")

        buffer = io.StringIO()
        df.to_csv(buffer, header=True, index=False, encoding="utf-8")
        buffer.seek(0)

        s3_hook.load_string(buffer.getvalue(),
                    key=file_name,
                    bucket_name=s3_bucket,
                    replace=True,
                    encrypt=False)

    return

def _get_base_product_prices(ds):
    import io
    import os
    import pandas as pd

    curr_working_directory = os.getcwd()
    print(os.getcwd())
    with open(curr_working_directory+"/dags/integrations/sql/precios_modales.sql", "r") as query_file:
        prices_query = query_file.read()

    exec_date = macros.ds_add(ds, 1)
    exec_date = exec_date.replace("-", "/")

    file_name = f"integraciones/last_millers/stock/ecommdata/{exec_date}/precios_modales.csv"

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    # Check if file is already loaded
    if s3_hook.check_for_key(file_name, bucket_name=s3_bucket):
        print(f"File {file_name} already exists on S3 bucket. Skipping...")
        return

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()

    cursor = pg_connection.cursor()
    cursor.execute(prices_query)
    results = cursor.fetchall()
    cursor.close()

    columns = [
        "id_tienda",
        "ref_id",
        "material",
        "umv",
        "precio",
    ]
    df = pd.DataFrame(results, columns=columns)
    print(f"Records found: {len(df.index)}")

    buffer = io.StringIO()
    df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    buffer.seek(0)

    s3_hook.load_string(buffer.getvalue(),
                key=file_name,
                bucket_name=s3_bucket,
                replace=True,
                encrypt=False)    

    return

default_args = {
    "owner": "ecommerce_ops",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    "proc_stock_precios_last_millers",
    default_args=default_args,
    description="Extracción de stock, precios y precios promocionales simples para integraciones Last Millers.",
    schedule_interval="30 8 * * *", 
    start_date=pendulum.datetime(2023, 2, 21, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    concurrency=2,
    tags=["OPS", "last_millers", "dw", "stock", "precios"],
) as dag:

    dag.doc_md = """
    Extracción de stock, precios y precios promocionales simples para integraciones Last Millers. \n
    Se obtiene listado de tiendas activas en integraciones con Last Millers desde la tabla **integraciones.tiendas_last_millers**. \n
    Por cada tienda activa, se realiza una consulta al **data warehouse** para calcular stock y precios promocionales, mientras que 
    en paralelo se calcula listado de precios modales a partir de stock de la tienda Mirador filtrado por el contenido de lista8. \n
    Todos los resultados son almacenados en S3 para posterior uso. \n
    Al finalizar, este DAG gatilla otros dos DAGs: [ **proc_integracion_stock_peya** , **proc_integracion_stock_rappi** ].
    """ 

    t0 = PythonOperator(
        task_id = "get_last_millers_stores",
        python_callable = _get_last_millers_stores
    )

    t1 = PythonOperator(
        task_id = "get_stock_and_promo_prices_from_datawarehouse",
        python_callable = _get_stock_and_promo_prices_from_datawarehouse,
        execution_timeout = timedelta(minutes=30),
        retries = 3,
        retry_delay = timedelta(minutes=5)
    )

    t2 = PythonOperator(
        task_id = "get_base_product_prices",
        python_callable = _get_base_product_prices
    )

    t3 = TriggerDagRunOperator(
        task_id="trigger_peya_stock_integration",
        trigger_dag_id="proc_peya_stock_integration",
        wait_for_completion=False
    )

    t4 = TriggerDagRunOperator(
        task_id="trigger_proc_rappi_stock_integration",
        trigger_dag_id="proc_rappi_stock_integration",
        wait_for_completion=False
    )

    td = DummyOperator(
        task_id = "dummy_task"
    )

    t0 >> t1 >> td
    t2 >> td
    td >> [t3, t4]
