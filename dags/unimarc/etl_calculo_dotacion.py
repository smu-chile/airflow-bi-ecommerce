from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

import pendulum

def get_matriz_dotacion_from_postgres(ds):
    import pandas as pd
    import io
    from io import StringIO
    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"dotacion/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
    matriz_query = """SELECT *
        from ecommdata.matriz_dotacion;"""
    print(matriz_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(matriz_query)
    results = cursor.fetchall()
    matriz=pd.DataFrame(results)
    print(matriz)
    matriz.columns = ["turno","lunes","martes","miercoles","jueves","viernes","sabado","domingo","horas"]
    cursor.close()
    pg_connection.close()
    
    buffer = io.StringIO()
    matriz.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"dotacion/{exec_date}/matriz_{date_aux}.csv"
    buffer.seek(0)
    print("se logro transformar el dataframe a un archivo .csv")
    print(f"con fecha {ds} y nombre de filename como {filename}")
    s3_hook.load_string(buffer.getvalue(),
                key=filename,
                bucket_name=s3_bucket,
                replace=True,
                encrypt=False)
    
    print(f"File load on S3: {prefix}")

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_dotacion_mfc',
    default_args=default_args,
    description="calculo de dotacion con matriz de pesos de turnos para MFC",
    schedule_interval="0 12 1 1-12 *",
    start_date=pendulum.datetime(2023, 6, 1, tz="America/Santiago"),
    catchup=False,
    tags=["catalogo", "cuadratura", "MFC", "unimarc", "PATRICIO"],
) as dag:
    
    dag.doc_md = """
    construir y cargar cuadratura mfc. \n
    Upsert en tabla catalogo.cuadratura_mfc.
    """ 

    t0 = PythonOperator(
        task_id = "get_matriz_dotacion_from_postgres",
        python_callable = get_matriz_dotacion_from_postgres,
    )
    
    t0
    