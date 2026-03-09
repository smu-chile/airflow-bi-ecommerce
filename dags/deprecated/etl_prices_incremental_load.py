from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from utils.janis_utils import load_full_table_to_s3
from datetime import datetime, timedelta

def _prices_table_full_load(ts):
    exec_date = ts[:10].replace("-","/")
    exec_date = datetime.strptime(exec_date, "%Y/%m/%d") + timedelta(days=1)
    exec_date = exec_date.strftime("%Y/%m/%d")
    prefix = f"janis/replica/price/{exec_date}/"
    print(f"Searching prefix: {prefix}")
    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    s3_file_list = s3_hook.list_keys(s3_bucket, prefix=prefix)
    print(f"Number of files found: {len(s3_file_list)}")

    if len(s3_file_list) == 0:
        return load_full_table_to_s3("price")
    else:
        print(s3_file_list[0])
        return s3_file_list[0]
    

def _incremental_load_prices_table(ti, ts):
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text
    
    exec_date = ts[:10].replace("-","/")
    
    prices_file = ti.xcom_pull(key="return_value", task_ids=["load_full_table_to_s3"])[0]
    print(f"Searching file: {prices_file}")

    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+prices_file)
    if not s3_hook.check_for_key(prices_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % prices_file)

    prices_object = s3_hook.get_key(prices_file, bucket_name=s3_bucket)

    df = pd.read_csv(prices_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")

    # Select only relevant columns:
    df = df[[
        "id",
        "item_id", # cruzar para quedar con item_id, ref_id y descr
        "store_id", # id_tienda_janis
        "price",
        "list_price",
        "cost_price",
        "valid_from",
        "valid_to",
        "publish_attempts",
        "publish_last_attempt",
        "publish_next_attempt",
        "blocked_by_audit",
        "status",
        "user_published",
        "user_modified",
        "user_created",
        "date_modified",
        "date_created",
        "date_published"
    ]]

    # Rename columns to match workspace schema:
    columns_rename = {
        "item_id": "id_sku_janis", # cruzar para quedar con item_id, ref_id y descr
        "store_id": "id_tienda_janis", # id_tienda_janis
        "price": "precio",
        "list_price": "precio_lista",
        "cost_price": "costo",
        "valid_from": "valido_desde",
        "valid_to": "valido_hasta",
        "publish_attempts": "intentos_publicacion",
        "publish_last_attempt": "ultimo_intento_publicacion",
        "publish_next_attempt": "proximo_intento_publicacion",
        "blocked_by_audit": "bloqueado_por_auditoria",
        "status": "estado",
        "user_published": "publicado_por",
        "user_modified": "modificado_por",
        "user_created": "creado_por",
        "date_modified": "fecha_modificacion",
        "date_created": "fecha_creacion",
        "date_published": "fecha_publicacion"
    }
    df = df.rename(columns=columns_rename)

    # Calculate extra columns:
    df["valido_desde"] = pd.to_datetime(df["valido_desde"], unit="s")
    df["valido_hasta"] = pd.to_datetime(df["valido_hasta"], unit="s", errors="coerce")
    df["ultimo_intento_publicacion"] = pd.to_datetime(df["ultimo_intento_publicacion"], unit="s")
    df["proximo_intento_publicacion"] = pd.to_datetime(df["proximo_intento_publicacion"], unit="s")
    df["fecha_modificacion"] = pd.to_datetime(df["fecha_modificacion"], unit="s")
    df["fecha_creacion"] = pd.to_datetime(df["fecha_creacion"], unit="s")
    df["fecha_publicacion"] = pd.to_datetime(df["fecha_publicacion"], unit="s")
    df["fecha_carga"] = exec_date
    df["valido_hasta"] = df["valido_hasta"].fillna("9999-12-31")

    df = df.astype({
        "id": "int",
        "id_sku_janis": "int",
        "id_tienda_janis": "int",
        "precio": "int",
        "precio_lista": "int",
        "costo": "int",
        "valido_desde": "string",
        "valido_hasta": "string",
        "intentos_publicacion": "int",
        "ultimo_intento_publicacion": "string",
        "proximo_intento_publicacion": "string",
        "bloqueado_por_auditoria": "bool",
        "estado": "int",
        "publicado_por": "int",
        "modificado_por": "int",
        "creado_por": "int",
        "fecha_modificacion": "string",
        "fecha_creacion": "string",
        "fecha_publicacion": "string",
        "fecha_carga": "string"
    }, errors="ignore")

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    query_skus = """
        SELECT id, ref_id, nombre_sku
        FROM ecommdata.skus;
    """
    df_skus = pd.read_sql(query_skus, engine)
    df_skus = df_skus.rename(columns={"id": "id_sku_temp"})

    print(f"Num records prices: {len(df.index)}")
    print(f"Num records skus: {len(df_skus.index)}")

    df = df.merge(df_skus, how="left", left_on="id_sku_janis", right_on="id_sku_temp")
    df = df.drop(columns=["id_sku_temp"])
    print(len(df.index))
    print(df.columns)

    print("Delete exec_date data from ecommdata.precios to avoid duplicates...")
    connection = engine.connect()
    truncate_query = f"DELETE FROM ecommdata.precios WHERE fecha_carga = '{exec_date}'::date;"
    connection.execute(text(truncate_query))
    connection.close()
    print("Data deleted.")

    print("Writing data into PostgreSQL...")
    # Save to PostgreSQL:
    df.to_sql(name="precios",
                con=engine,         
                schema="ecommdata",         
                if_exists='append',         
                index=False,         
                chunksize=20000,         
                method='multi')

    print("Data saved to PostgreSQL.")

    return

def _delete_old_data(ts):
    import sqlalchemy
    from sqlalchemy import text

    exec_date = ts[:10]
    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    print("Delete 30 days old data from ecommdata.precios...")
    connection = engine.connect()
    truncate_query = f"DELETE FROM ecommdata.precios WHERE fecha_carga <= '{exec_date}'::date - interval '30 days';"
    connection.execute(text(truncate_query))
    connection.close()
    print("Data deleted.")

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_precios_incremental_load',
    default_args=default_args,
    description="Extracción y carga de tabla price desde Janis Replica hasta el Workspace en Postgresql.",
    schedule="0 7 * * *",
    start_date=datetime(2022, 3, 1),
    catchup=True,
    max_active_runs = 1,
    tags=["DATA", "Janis", "ecommdata", "precios"],
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla de price de Janis en la tabla ecommdata.precios. \n
    Carga diaria de la tabla completa con un identificador adicional basado en la fecha de ejecución con el fin
    de conservar historial de cambios. \n
    Incluye una tarea final de limpieza de la tabla, borrando cualquier registro que tenga más de 30 días de antiguedad.
    """ 
    t0 = PythonOperator(
        task_id = "load_full_table_to_s3",
        python_callable = _prices_table_full_load
    )

    t1 = PythonOperator(
        task_id = "incremental_load_prices_table",
        python_callable = _incremental_load_prices_table
    )

    t2 = PythonOperator(
        task_id = "delete_old_data",
        python_callable = _delete_old_data
    )

    t0 >> t1 >> t2
