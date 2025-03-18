from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable


import pendulum

from datetime import datetime, timedelta

def render_netezza_view():
    from io import StringIO
    import os
    import jaydebeapi
    import pandas as pd


    sql_str= f"""WITH DatosConRank AS (
            SELECT 
                MATERIAL,
                N_PROMOCION,
                NOMBRE_PROMOCION,
                ID_EVENTO,
                DESCRIPCION_EVENTO_PROMOCIONAL,
                ID_MECANICA,
                DESCRIPCION_MECANICA,
                DESC_MATERIAL,
                UN_MEDIDA_VENTA,
                EAN,
                PRECIO_MODAL,
                PRECIO_MODAL_TOTAL,
                PRECIO_PROMOCIONAL,
                PRECIO_TOTAL_PROMOCIONAL,
                CANAL_DISTRIBUCION,
                fecha_inicio_de_promocion,
                fecha_fin_de_promocion,
                ultima_carga,
                ROW_NUMBER() OVER (PARTITION BY MATERIAL ORDER BY fecha_inicio_de_promocion DESC) AS rn
            FROM DWC_SMU.SMU.VW_FACT_WORKFLOW
        )
        SELECT 
            actual.N_PROMOCION,
            actual.NOMBRE_PROMOCION,
            actual.CANAL_DISTRIBUCION,
            actual.ID_EVENTO,
            actual.DESCRIPCION_EVENTO_PROMOCIONAL,
            actual.ID_MECANICA,
            actual.DESCRIPCION_MECANICA,
            actual.MATERIAL,
            actual.DESC_MATERIAL,
            actual.UN_MEDIDA_VENTA,
            actual.EAN,
            actual.PRECIO_MODAL,
            actual.PRECIO_MODAL_TOTAL,
            actual.PRECIO_PROMOCIONAL,
            actual.PRECIO_TOTAL_PROMOCIONAL,
            actual.fecha_inicio_de_promocion ,
            actual.fecha_fin_de_promocion, 
            anterior.fecha_inicio_de_promocion AS fecha_inicio_anterior, 
            anterior.fecha_fin_de_promocion AS fecha_fin_anterior
        FROM DatosConRank actual
        LEFT JOIN DatosConRank anterior 
            ON actual.MATERIAL = anterior.MATERIAL 
            AND actual.rn = 1 
            AND anterior.rn = 2
        WHERE actual.ultima_carga = 'X'  
        AND (actual.fecha_inicio_de_promocion <> anterior.fecha_inicio_de_promocion 
            OR actual.fecha_fin_de_promocion <> anterior.fecha_fin_de_promocion);"""
    
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
    df = df[['N_PROMOCION','NOMBRE_PROMOCION','CANAL_DISTRIBUCION','ID_EVENTO',
             'DESCRIPCION_EVENTO_PROMOCIONAL','ID_MECANICA','DESCRIPCION_MECANICA',
             'MATERIAL','UN_MEDIDA_VENTA','EAN','PRECIO_MODAL','PRECIO_MODAL_TOTAL',
             'PRECIO_PROMOCIONAL','PRECIO_TOTAL_PROMOCIONAL','FECHA_INICIO_DE_PROMOCION',
             'FECHA_FIN_DE_PROMOCION','FECHA_INICIO_ANTERIOR','FECHA_FIN_ANTERIOR']]
    print(df)
    cur.close()
    conn.close()

    return df

def promos_out_to_s3(ds):
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

    df = render_netezza_view(ds)

    print("Correcta extracion de datos de Neeteezaa")

    # Cambiando columnas a minusculas
    
    df = df[['N_PROMOCION',
             'NOMBRE_PROMOCION',
             'CANAL_DISTRIBUCION',
             'ID_EVENTO',
             'DESCRIPCION_EVENTO_PROMOCIONAL',
             'ID_MECANICA',
             'DESCRIPCION_MECANICA',
             'MATERIAL',
             'DESC_MATERIAL',
             'UN_MEDIDA_VENTA',
             'EAN',
             'PRECIO_MODAL',
             'PRECIO_PROMOCIONAL',
             'PRECIO_TOTAL_PROMOCIONAL',
             'fecha_inicio_de_promocion',
             'fecha_fin_de_promocion',
             'fecha_inicio_anterior',
             'fecha_fin_anterior']]
    
    print("\nHasta acá todo bien al filtrar las columnas :D\n")
    
    df.columns = ['N_PROMOCION',
             'NOMBRE_PROMOCION',
             'CANAL_DISTRIBUCION',
             'ID_EVENTO',
             'DESCRIPCION_EVENTO_PROMOCIONAL',
             'ID_MECANICA',
             'DESCRIPCION_MECANICA',
             'MATERIAL',
             'DESC_MATERIAL',
             'UN_MEDIDA_VENTA',
             'EAN',
             'PRECIO_MODAL',
             'PRECIO_PROMOCIONAL',
             'PRECIO_TOTAL_PROMOCIONAL',
             'fecha_inicio_de_promocion',
             'fecha_fin_de_promocion',
             'fecha_inicio_anterior',
             'fecha_fin_anterior']

    print(df.info())

    buffer = io.StringIO()
    df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"Promociones_comparadas/{exec_date}/promociones_comparadas{date_aux}.csv"
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

def promociones_comparadas_to_postgresql(ti):
    print("todo bien por acá")
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text

    filename = ti.xcom_pull(key="return_value", task_ids=["promos_out_to_s3"])[0]

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

    with engine.begin() as conn:
        df.to_sql(name="promociones_comparadas",
                    con=conn,         
                    schema="ecommdata",         
                    if_exists='append',         
                    index=False,         
                    chunksize=20000,         
                    method='multi')

    print("Data saved to PostgreSQL.")

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
    'cargar_promociones_comparadas',
    default_args=default_args,
    description='Guarda promociones comparadas en S3 y las carga en la base de datos',
    schedule_interval='0 9 * * *',
    start_date=pendulum.datetime(2024, 5, 1, tz="America/Santiago"),
    catchup=True,
    max_active_runs=1,
    tags=["DATA", "postgres", "ecommdata", "Promociones_comparadas", "S3", "NICOLAS"]
) as dag:

    dag.doc_md = """
        Carga y actualiza data de API driv.in, Rutas, Escenarios, Vehiculos, Ordene y direcciones\n
        guardar en S3 y Upsert en postgres.
        """ 
    # Definir las tareas

    t0 = PythonOperator(
        task_id='promos_out_to_s3',
        python_callable=promos_out_to_s3,
        dag=dag,
    )
    t1 = PythonOperator(
        task_id='Promociones_comparadas_to_postgresql',
        python_callable=promociones_comparadas_to_postgresql,
        dag=dag,
    )

    t0 >> t1 
