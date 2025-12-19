from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

from utils.slack_utils import dag_success_slack, dag_failure_slack

import pendulum

from datetime import datetime, timedelta

def render_netezza_view():
    from io import StringIO
    import os
    import jaydebeapi
    import pandas as pd

    sql_str= f""" 
    SELECT 
        sku_key, 
        altura, ancho, 
        envase, longitud, 
        peso_bruto,
        peso_neto ,
        SKU_PRODUCT ,
        volumen, 
        unidad_de_volumen, 
        unidad_peso, 
        unidad_laa, 
        unidad, 
        nm
    FROM DWC_SMU.SMU.VW_DIM_SKU_ATTR
    """

    print(sql_str)

    dsn_database = Variable.get("DW_SECRET_DATABASE") 
    dsn_hostname = Variable.get("DW_SECRET_HOSTNAME")
    dsn_port = "5480" 
    dsn_uid = Variable.get("DW_SECRET_USER")
    dsn_pwd = Variable.get("DW_PASSWORD")
    jdbc_driver_name = "org.netezza.Driver" 
    jdbc_driver_loc = os.path.join('/opt/airflow/include/jdbcdriver/nzjdbc.jar')

    connection_string='jdbc:netezza://'+dsn_hostname+':'+dsn_port+'/'+dsn_database
    
    conn = jaydebeapi.connect(jdbc_driver_name, 
                                connection_string, {'user': dsn_uid, 'password': dsn_pwd},
                                jars=jdbc_driver_loc)

    cur = conn.cursor()
    cur.execute(sql_str)
    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=columns)
    df = df[['SKU_KEY','ALTURA','ANCHO','ENVASE','LONGITUD','PESO_BRUTO','PESO_NETO','SKU_PRODUCT',
             'VOLUMEN','UNIDAD_DE_VOLUMEN','UNIDAD_PESO','UNIDAD_LAA','UNIDAD','NM']]
    print(df)
    cur.close()
    conn.close()

    return df

def data_out_to_s3(ds):
    import pandas as pd
    import numpy as np
    import io
    from io import StringIO

    print("Se comienza a ejecutar el S3")
    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"promociones_comparadas/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    df = render_netezza_view()

    print("Correcta extracion de datos de Neeteezaa")

    df.columns = df.columns.str.lower()

    df = df[['sku_key',
             'altura',
             'ancho',
             'envase',
             'longitud',
             'peso_bruto',
             'peso_neto',
             'sku_product',
             'volumen',
             'unidad_de_volumen',
             'unidad_peso',
             'unidad_laa',
             'unidad',
             'nm']]
    
    print("\nHasta acá todo bien al filtrar las columnas :D\n")

    df.columns = ['sku_key',
             'altura',
             'ancho',
             'envase',
             'longitud',
             'peso_bruto',
             'peso_neto',
             'sku_product',
             'volumen',
             'unidad_de_volumen',
             'unidad_peso',
             'unidad_laa',
             'unidad',
             'nm']

    print(df.info())

    buffer = io.StringIO()
    df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"Peso_volumen_alvi/{exec_date}/peso_volumen_alvi{date_aux}.csv"
    buffer.seek(0)
    print("se transformo el dataframe a un archivo .csv")
    print(f"con fecha {ds} y nombre de filename como {filename}")
    s3_hook.load_string(buffer.getvalue(),
                key=filename,
                bucket_name=s3_bucket,
                replace=True,
                encrypt=False)
    
    print(f"File load on S3: {prefix}")

    return filename

def data_to_postgresql(ti):
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text

    filename = ti.xcom_pull(key="return_value", task_ids=["data_out_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+filename)
    if not s3_hook.check_for_key(filename, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % filename)

    s_stock_object = s3_hook.get_key(filename, bucket_name=s3_bucket)

    df = pd.read_csv(s_stock_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")
    print(df.info())

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    connection = engine.connect()
    truncate_query = "TRUNCATE TABLE ecommdata_alvi.peso_volumen_alvi"
    connection.execute(text(truncate_query))
    connection.close()

    with engine.begin() as conn:
        df.to_sql(name="peso_volumen_alvi",
                    con=conn,         
                    schema="ecommdata_alvi",         
                    if_exists='append',         
                    index=False,         
                    chunksize=20000,         
                    method='multi')

    print("Data saved to PostgreSQL.")

    return

def truncate_table():
    
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")

    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    connection = engine.connect()
    truncate_query = "TRUNCATE TABLE ecommdata_alvi.peso_volumen_alvi"
    connection.execute(text(truncate_query))
    connection.close()

    print("Tabla borrada con exito")

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'cargar_Peso_volumen_alvi',
    default_args=default_args,
    description='Guarda datos de peso y volumen de alvi en S3 y las carga en la base de datos',
    schedule_interval='15 9 * * *',
    start_date=pendulum.datetime(2024, 5, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["DATA", "postgres", "ecommdata_alvi", "Promociones_comparadas", "S3", "NICOLAS" ,"Capacity"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
        carga de datos peso y volumen alvi , solicitado por el equipo de capacity
        """ 

    t0 = PythonOperator(
        task_id='truncate_table',
        python_callable=truncate_table
    )
    t1 = PythonOperator(
        task_id='data_out_to_s3',
        python_callable=data_out_to_s3
    )
    t2 = PythonOperator(
        task_id='data_to_postgresql',
        python_callable=data_to_postgresql
    )

    t0 >> t1 >> t2