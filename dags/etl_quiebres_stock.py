from airflow import DAG
from airflow import macros
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator


from datetime import datetime, timedelta
import pendulum

def publicacion_catalogo(ds):
    publicacion_query = f"""SELECT *
                        from ecommdata.publicacion_catalogo pc
                        where fecha_hora > '{ds}'::date-15
                        and c1 = 'Frutas y Verduras'
                        and stock_janis < 1
                        and EXTRACT(HOUR FROM fecha_hora) = 12"""
    print(publicacion_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(publicacion_query)
    results = cursor.fetchall()
    cursor.close()
    pg_connection.close()
    return results

def extraccion_s3_publicacion_catalogo(ds):
    from datetime import datetime, timedelta
    import pandas as pd
    import io
    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"quiebres_inventario/{exec_date}/"

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    hoy = datetime.now().date() - timedelta(days=15)
    dias_a_restar = 5 #cambiar por 45
    fecha_inicial = hoy - timedelta(days=dias_a_restar)

    lista = []
    for i in range((hoy - fecha_inicial).days + 1):
        fecha_actual = fecha_inicial + timedelta(days=i)
        fecha_formateada = fecha_actual.strftime("%Y/%d/%m")
        aux = f"{fecha_formateada}/1100publicacion_catalogo_periodico.csv"
        lista.append(aux)

    print(lista)

    dataframes_list = []

    for aux in lista:
        s3_filename = f"ecommdata/publicacion_catalogo/{aux}"

        print("Loading file:", s3_filename)
        if not s3_hook.check_for_key(s3_filename, bucket_name=s3_bucket):
            print(f"WARNING: File {s3_filename} not found.")
            continue

        s3_object = s3_hook.get_key(s3_filename, bucket_name=s3_bucket)
        df = pd.read_csv(s3_object.get()["Body"])
        df = df[df["c1"] == "Frutas y Verduras"]
        df = df[df["stock_janis"]<1]
        dataframes_list.append(df)

    result_df = pd.concat(dataframes_list, ignore_index=True)
    df_catalogo_15d = publicacion_catalogo(ds)
    df_catalogo_15d.columns = result_df.columns
    result_df = pd.concat(result_df,df_catalogo_15d)

    print("Final DataFrame:")
    print(result_df.info())

    buffer = io.StringIO()
    result_df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"quiebres_inventario/{exec_date}/quiebres_inventario_{date_aux}.csv"
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

def quiebres_to_postgres(ti):
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text

    filename = ti.xcom_pull(key="return_value", task_ids=["extraccion_s3_publicacion_catalogo"])[0]

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
        conn.execute("TRUNCATE ecommdata.quiebres_inventario")
        df.to_sql(name="quiebres_inventario",
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
    'etl_quiebre_stock',
    default_args=default_args,
    description="Carga de datos de quiebres stock 60 dias S3.",
    schedule_interval="0 5 1/15 * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["DATA", "ecommdata", "S3"],
) as dag:

    dag.doc_md = """
        Quiebres de stock a 60 dias desde el historico de publicacion catálogo
    """ 

    t1 = PythonOperator(
        task_id = "extraccion_s3_publicacion_catalogo",
        python_callable = extraccion_s3_publicacion_catalogo
    )
    t1 
