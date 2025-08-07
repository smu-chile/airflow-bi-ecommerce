from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime

import pendulum

def from_s3_to_postgress(ti):
    from airflow.models import Variable
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from datetime import datetime, timedelta
    import pandas as pd
    from io import StringIO
    import sqlalchemy
    from sqlalchemy import create_engine    

    # Fecha de ayer
    fecha_ayer = (datetime.today() - timedelta(days=1)).strftime('%Y%m%d')
    
    # Nombre del archivo
    filename = f"membresia_diamante_venta_tienda_fisica/discount_data/discount_data_{fecha_ayer}"
    
    # Variables y conexión S3
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: " + filename)
    if not s3_hook.check_for_key(filename, bucket_name=s3_bucket):
        raise Exception(f"Key {filename} does not exist in bucket {s3_bucket}.")

    # Leemos el contenido del archivo como texto
    file_obj = s3_hook.get_key(filename, bucket_name=s3_bucket)
    file_content = file_obj.get()["Body"].read().decode("utf-8")

    # Convertimos a DataFrame con los nombres correctos
    columnas = [
        "rut_hash",
        "fecha",
        "id_transaccion",
        "tienda",
        "venta",
        "ahorro"
    ]
    df = pd.read_csv(StringIO(file_content), sep=",", header=None, names=columnas)

    # Nos aseguramos de que los tipos estén como corresponde
    df["fecha"] = pd.to_datetime(df["fecha"]).dt.date
    df["venta"] = df["venta"].astype(float)
    df["ahorro"] = df["ahorro"].astype(float)

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    df.to_sql(
        name="membresia_diamante_venta_tienda_fisica",
        con=engine,
        schema="ecommdata",
        if_exists="append",
        index=False
    )

    print("Carga exitosa a PostgreSQL")

    return 

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

# Definir el DAG

with DAG(
    'elt_carga_membresia_diamante_venta_tienda_fisica',
    default_args=default_args,
    description='guarda los datos de venta tienda fisica membresia',
    schedule_interval='0 9 * * *',
    start_date=pendulum.datetime(2024, 5, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["DATA", "postgres", "ecommdata", "Membresia", "S3", "NICOLAS"]
) as dag:

    dag.doc_md = """
        Una funcion que carga una tabla de s3 a postgress.
        """ 
    # Definir las tareas

    t0 = PythonOperator(
        task_id='from_s3_to_postgress',
        python_callable=from_s3_to_postgress
    )

    t0