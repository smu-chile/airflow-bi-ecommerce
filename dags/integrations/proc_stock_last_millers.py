from airflow import DAG
from airflow import macros
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
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

def _get_stock_from_datawarehouse(ti, ds):
    import io
    import jaydebeapi
    import os
    import pandas as pd

    ids_tiendas = ti.xcom_pull(key="return_value", task_ids=["get_last_millers_stores"])[0]
    ids_tiendas = [id[0] for id in ids_tiendas]
    
    curr_working_directory = os.getcwd()
    print(os.getcwd())
    with open(curr_working_directory+"/dags/integrations/sql/stock_datawarehouse.sql", "r") as query_file:
        base_query = query_file.read()

    exec_date = macros.ds_add(ds, 0)
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

    ids_tiendas_str = str(tuple(ids_tiendas))
    stock_query = base_query.replace("{store_ids}", ids_tiendas_str).replace("{exec_date}", exec_date.replace("/", "-"))
    print(stock_query)
    
    file_name = f"integraciones/last_millers/stock/datawarehouse/{exec_date}/stock_datawarehouse.csv"

    if s3_hook.check_for_key(file_name, bucket_name=s3_bucket):
        print(f"File {file_name} already exists on S3 bucket. Skipping...")
        return file_name

    cur.execute(stock_query)
    results = cur.fetchall()
    columns = [i[0] for i in cur.description]
    df = pd.DataFrame(results, columns=columns)
    print(f"Records found: {len(df.index)}")

    if len(df.index) == 0:
        raise Exception("ERROR: No records found.")

    buffer = io.StringIO()
    df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    buffer.seek(0)

    s3_hook.load_string(buffer.getvalue(),
                key=file_name,
                bucket_name=s3_bucket,
                replace=True,
                encrypt=False)

    return file_name

def _load_stock_to_postgres(ti):
    import pandas as pd
    from sqlalchemy import create_engine

    file_name = ti.xcom_pull(key="return_value", task_ids="get_stock_from_datawarehouse")

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    if not s3_hook.check_for_key(file_name, bucket_name=s3_bucket):
        print(f"ERROR: File {file_name} not found on S# bucket: {s3_bucket}")
        return
    
    stock_object = s3_hook.get_key(file_name, bucket_name=s3_bucket)
    df = pd.read_csv(stock_object.get()["Body"], dtype="object")
    df.columns = map(str.lower, df.columns)
    print(f"Number of records found: {len(df.index)}")

    print(df.isna().sum())

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = create_engine(conn_url)

    # Save to PostgreSQL:
    df.to_sql(name="stock",
                con=engine,         
                schema="integraciones",
                if_exists='append',         
                index=False,         
                chunksize=20000,         
                method='multi')
    return

def _get_products_from_datawarehouse(ds):
    import io
    import jaydebeapi
    import os
    import pandas as pd

    curr_working_directory = os.getcwd()
    print(os.getcwd())
    with open(curr_working_directory+"/dags/integrations/sql/productos.sql", "r") as query_file:
        products_query = query_file.read()

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

    file_name = f"integraciones/last_millers/stock/datawarehouse/{exec_date}/productos.csv"

    if s3_hook.check_for_key(file_name, bucket_name=s3_bucket):
        print(f"File {file_name} already exists on S3 bucket. Skipping...")
        return file_name

    cur.execute(products_query)
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

    return file_name

def _load_products_to_postgres(ti):
    import pandas as pd
    from sqlalchemy import create_engine
    import csv

    file_name = ti.xcom_pull(key="return_value", task_ids="get_products_from_datawarehouse")
    print(file_name)

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    if not s3_hook.check_for_key(file_name, bucket_name=s3_bucket):
        print(f"ERROR: File {file_name} not found on S3 bucket: {s3_bucket}")
        return
    
    products_object = s3_hook.get_key(file_name, bucket_name=s3_bucket)
    
    # Read CSV with custom quoting to handle double quotes within fields
    df = pd.read_csv(products_object.get()["Body"], dtype="object", quoting=csv.QUOTE_MINIMAL)
    df.columns = map(str.lower, df.columns)
    print(df.info())
    print(f"Number of records found: {len(df.index)}")
    df = df.dropna(subset=df.columns[:3])
    print(df.info())


    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = create_engine(conn_url)

    df.to_sql(name="productos",
                con=engine,         
                schema="integraciones",         
                if_exists='append',         
                index=False,         
                chunksize=20000,         
                method='multi')

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
    schedule_interval="0 9 * * *", 
    start_date=pendulum.datetime(2023, 2, 21, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    concurrency=2,
    tags=["OPS", "last_millers", "dw", "stock", "precios", "NICOLAS"],
) as dag:

    dag.doc_md = """
    Extracción de stock, precios y precios promocionales simples para integraciones Last Millers. \n
    Se obtiene listado de tiendas activas en integraciones con Last Millers desde la tabla **integraciones.tiendas_last_millers**. \n
    Por cada tienda activa, se realiza una consulta al **data warehouse** para calcular stock y precios promocionales, mientras que 
    en paralelo se calcula listado de precios modales. \n
    **Cálculo de precios modales**: Para tiendas ecommerce se toman sus precios de la tabla **ecommdata.precios**, mientras que para el
    resto de las tiendas (no-ecommerce) se usa el mayor valor de precio entre todas las tiendas ecommerce por cada producto. \n
    Todos los resultados son almacenados en S3 para posterior uso. \n
    Al finalizar, este DAG gatilla otros dos DAGs: [ **proc_integracion_stock_peya** , **proc_integracion_stock_rappi** ].
    """ 

    # Last millers stores
    t0 = PythonOperator(
        task_id = "get_last_millers_stores",
        python_callable = _get_last_millers_stores
    )

    # Productos
    t1 = PythonOperator(
        task_id = "get_products_from_datawarehouse",
        python_callable = _get_products_from_datawarehouse
    )

    t2 = PostgresOperator(
        task_id = "truncate_integraciones_productos_table",
        postgres_conn_id="postgresql_conn",
        sql="""
        TRUNCATE integraciones.productos;
        """,
    )

    t3 = PythonOperator(
        task_id = "load_products_to_postgres",
        python_callable = _load_products_to_postgres
    )

    # Stock
    t4 = PythonOperator(
        task_id = "get_stock_from_datawarehouse",
        python_callable = _get_stock_from_datawarehouse
    )

    t5 = DummyOperator(
        task_id = "datawarehouse_error_side_path",
        trigger_rule = "one_failed"
    )

    t6 = PostgresOperator(
        task_id = "truncate_integraciones_stock_table",
        postgres_conn_id="postgresql_conn",
        sql="""
        TRUNCATE integraciones.stock;
        """
    )

    t7 = PythonOperator(
        task_id = "load_stock_to_postgres",
        python_callable = _load_stock_to_postgres
    )

    t8 = DummyOperator(
        task_id = "join_paths",
        trigger_rule = "one_success"
    )

    # Calculate joined table
    t9 = PostgresOperator(
        task_id = "truncate_stock_prices_promos",
        postgres_conn_id = "postgresql_conn",
        sql="""
        TRUNCATE integraciones.lm_stock_precio_promo;
        """
    )

    t10 = PostgresOperator(
        task_id = "calculate_stock_prices_promos",
        postgres_conn_id = "postgresql_conn",
        sql = "sql/insert_stock_prices_promos.sql"
    )

    t11 = PostgresOperator(
        task_id = "calculate_stock_prices_promos_ph",
        postgres_conn_id = "postgresql_conn",
        sql = "sql/insert_stock_prices_promos_padre_hijo.sql"
    )

    t12 = PostgresOperator(
        task_id = "calculate_stock_prices_promos_no_ecommerce",
        postgres_conn_id = "postgresql_conn",
        sql = "sql/insert_stock_prices_promos_no_ecommerce.sql"
    )

    t13 = PostgresOperator(
        task_id = "calculate_stock_prices_promos_no_ecommerce_ph",
        postgres_conn_id = "postgresql_conn",
        sql = "sql/insert_stock_prices_promos_no_ecommmerce_padre_hijo.sql"
    )

    td = DummyOperator(
        task_id = "dummy_task"
    )

    # Trigger last miller's DAGs
    t14 = TriggerDagRunOperator(
        task_id="trigger_proc_rappi_stock_integration",
        trigger_dag_id="proc_rappi_stock_integration",
        wait_for_completion=False
    )

    t15 = TriggerDagRunOperator(
        task_id="trigger_proc_uber_promotions_integration",
        trigger_dag_id="proc_uber_promotions_integration",
        wait_for_completion=False
    )

    t0 >> [t1, t4]
    t1 >> t2 >> t3
    t4 >> [t5, t6] 
    t6 >> t7 >> t8
    t5 >> t8
    t3 >> td
    t8 >> td
    td >> t9 >> t10 >> t11 >> t12 >> t13
    t13 >> [t14, t15]