from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.providers.standard.operators.empty import EmptyOperator

from utils.slack_utils import dag_success_slack, dag_failure_slack

import pendulum

def venta_mfc_semana():
    import pandas as pd
    ventas_query = """SELECT 
                    _t.material,
                    SUM(CASE WHEN _t.dia = 0 THEN _t.venta_prom_dia ELSE 0 END) AS domingo,
                    SUM(CASE WHEN _t.dia = 1 THEN _t.venta_prom_dia ELSE 0 END) AS lunes,
                    SUM(CASE WHEN _t.dia = 2 THEN _t.venta_prom_dia ELSE 0 END) AS martes,
                    SUM(CASE WHEN _t.dia = 3 THEN _t.venta_prom_dia ELSE 0 END) AS miercoles,
                    SUM(CASE WHEN _t.dia = 4 THEN _t.venta_prom_dia ELSE 0 END) AS jueves,
                    SUM(CASE WHEN _t.dia = 5 THEN _t.venta_prom_dia ELSE 0 END) AS viernes,
                    SUM(CASE WHEN _t.dia = 6 THEN _t.venta_prom_dia ELSE 0 END) AS sabado
                    FROM (
                    SELECT 
                        date_part('dow', fecha) AS dia, 
                        lpad(material, 18, '0') AS material,
                        sum(venta_umv)/10 AS venta_prom_dia
                    FROM ecommdata.venta_sku_tienda vst 
                    WHERE id_tienda = '1917'
                    GROUP BY fecha, date_part('dow', fecha), lpad(material, 18, '0')
                    ) AS _t
                    GROUP BY _t.material
                    ORDER BY _t.material;"""
    print(ventas_query)
    pg_hook = PostgresHook(conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(ventas_query)
    results = cursor.fetchall()
    results=pd.DataFrame(results)
    results.columns = ["material","domingo","lunes","martes","miercoles","jueves","viernes","sabado"]
    cursor.close()
    pg_connection.close()

    return results

def ventas_mfc_to_s3(ts,ds):
    import pandas as pd
    import numpy as np
    import io

    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"venta_mfc/{exec_date}/"
    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    df = venta_mfc_semana()

    buffer = io.StringIO()
    df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"venta_mfc/{exec_date}/venta_mfc_{date_aux}.csv"
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

def ventas_mfc_to_postgres(ti):
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text

    filename = ti.xcom_pull(key="return_value", task_ids=["ventas_mfc_to_s3"])[0]

    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
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
    df['material'] = df['material'].apply(lambda x: str(x).zfill(18))
    df['domingo'] = df['domingo'].astype(float)
    df['lunes'] = df['lunes'].astype(float)
    df['martes'] = df['martes'].astype(float)
    df['miercoles'] = df['miercoles'].astype(float)
    df['jueves'] = df['jueves'].astype(float)
    df['viernes'] = df['viernes'].astype(float)
    df['sabado'] = df['sabado'].astype(float)
    df.info()

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    with engine.begin() as conn:
        conn.execute("TRUNCATE ecommdata.venta_prom_semanal_mfc") 
        df.to_sql(name="venta_prom_semanal_mfc",
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
with DAG(
    'etl_venta_mfc',
    default_args=default_args,
    description="cargar venta en formato semana del MFC",
    schedule="0 20 * * *",
    start_date=pendulum.datetime(2023, 7, 11, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "MFC", "ventas", "unimarc", "PATRICIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:
    

    dag.doc_md = """
    Carga venta a postgres del MFC \n
    guardar en S3.
    """ 
    
    t0 = PythonOperator(
        task_id = "ventas_mfc_to_s3",
        python_callable = ventas_mfc_to_s3,
    )
    t1 = PythonOperator(
        task_id = "ventas_mfc_to_postgres",
        python_callable = ventas_mfc_to_postgres,
    )
    t0 >> t1