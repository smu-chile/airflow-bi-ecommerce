from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime

import pendulum

def load_cantidad_promociones_to_s3(ds):
    import pandas as pd
    import numpy as np
    import io
    from io import StringIO

    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"cantidad_promociones/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()

    cantidad_promociones_query = f"""SELECT TO_DATE('{ds}', 'YYYY-MM-DD') AS dia,
           COUNT(material) AS cantidad_promociones_activas,
           id_mecanica,
           descripcion_mecanica
    FROM ecommdata.workflow_promociones wp
    WHERE wp.fecha_inicio_de_promocion <= '{ds}'
      AND wp.fecha_fin_de_promocion >= '{ds}'
      AND wp.id_mecanica NOT IN (36, 99, 84, 12, 37, 51, 93, 53, 96, 77, 59)
    GROUP BY id_mecanica, descripcion_mecanica;"""
    print(cantidad_promociones_query)

    cursor.execute(cantidad_promociones_query)
    results = cursor.fetchall()
    columns_name = [i[0] for i in cursor.description]
    df = pd.DataFrame(results, columns=columns_name)
    cursor.close()
    pg_connection.close()

    buffer = io.StringIO()
    df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"cantidad_promociones/{exec_date}/cantidad_promociones_{date_aux}.csv"
    buffer.seek(0)
    print("se logro transformar el dataframe a un archivo .csv")
    print(f"con fecha {ds} y nombre de filename como {filename}")
    s3_hook.load_string(buffer.getvalue(),
                key=filename,
                bucket_name=s3_bucket,
                replace=True,
                encrypt=False)
    
    print(f"File load on S3: {prefix}")

    return filename

def load_cantidad_promociones_to_postgres(ti):
    import pandas as pd
    import sqlalchemy

    cantidad_promociones_file = ti.xcom_pull(key="return_value", task_ids=["load_cantidad_promociones_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+cantidad_promociones_file)
    if not s3_hook.check_for_key(cantidad_promociones_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % cantidad_promociones_file)

    limit_object = s3_hook.get_key(cantidad_promociones_file, bucket_name=s3_bucket)

    df_cantidad_promociones = pd.read_csv(limit_object.get()["Body"])

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")

    conn_url = "postgresql+psycopg2://"+username + \
        ":"+password+"@"+host+":5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    df_cantidad_promociones.to_sql(name="cantidad_promociones_diarias",
                   con=engine,
                   schema="ecommdata",
                   if_exists='append',
                   index=False,
                   chunksize=20000,
                   method='multi')
    return


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_cantidad_promociones',
    default_args=default_args,
    description="Extracción de datos de tabla workflow_promociones y posterior carga de cantidad de promociones diarias segmentadas por mecanica",
    schedule_interval="0 7 * * *",
    start_date=pendulum.datetime(2022, 8, 11, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "ecommdata", "cantidad_promociones", "Unimarc", "workflow_promociones"],
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla de productos_excluidos_por_tienda de Janis Unimarc a Workspace. \n
    """ 
    t0 = PythonOperator(
        task_id = "load_cantidad_promociones_to_s3",
        python_callable = load_cantidad_promociones_to_s3,
    )

    t1 = PythonOperator(
        task_id = "load_cantidad_promociones_to_postgres",
        python_callable = load_cantidad_promociones_to_postgres
    )

    t0 >> t1