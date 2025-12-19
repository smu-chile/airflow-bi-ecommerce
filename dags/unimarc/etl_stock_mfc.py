from airflow import DAG
from airflow import macros
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

from utils.slack_utils import dag_success_slack, dag_failure_slack

from datetime import datetime, timedelta
import pendulum

def _check_for_s3_file_with_date(ds):
    exec_date = datetime.strptime(ds, "%Y-%m-%d") + timedelta(days=1)
    exec_date = f"{exec_date.year}/{exec_date.month}/{exec_date.day}"
    prefix = f"datastage/stock_mfc/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
    flag_key = prefix + "stock.trg"

    print(f"Checking for key: {flag_key}")
    if not s3_hook.check_for_key(flag_key, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % flag_key)
    
    print("Flag file found.")
    return

def _load_stock_mfc(ds):
    import pandas as pd
    import sqlalchemy

    exec_date = datetime.strptime(ds, "%Y-%m-%d") + timedelta(days=1)
    exec_date = f"{exec_date.year}/{exec_date.month}/{exec_date.day}"
    prefix = f"datastage/stock_mfc/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    s3_file_list = s3_hook.list_keys(s3_bucket, prefix=prefix)
    print(f"Files detected: {s3_file_list}")
    column_types = {
        "CENTRO": "string",
        "MATERIAL": "string",
        "DESCRIPCION": "string",
        "UN_VTA": "string",
        "NEGOCIO": "string",
        "SECCION": "string",
        "LINEA": "string",
        "CATEGORIA": "string",
        "GRUPO_ARTICULO": "string",
        "PRECIO_REGULAR": "int",
        "PRECIO_PROMOCIONAL": "int",
        "STOCK_X_UM": "int"
    }

    column_names = {
        "CENTRO": "id_tienda",
        "MATERIAL": "material",
        "DESCRIPCION": "descripcion",
        "UN_VTA": "unidad_venta",
        "NEGOCIO": "negocio",
        "SECCION": "seccion",
        "LINEA": "linea",
        "CATEGORIA": "categoria",
        "GRUPO_ARTICULO": "grupo",
        "PRECIO_REGULAR": "precio_regular",
        "PRECIO_PROMOCIONAL": "precio_promocional",
        "STOCK_X_UM": "stock"
    }

    dataframe_list = []
    for s3_file in s3_file_list:
        if not s3_file.endswith((".csv", ".CSV")):
            # Skip empty any non-csv file
            continue
        print(f"Loading file: {s3_file}")
        stock_mfc_object = s3_hook.get_key(s3_file, bucket_name=s3_bucket)
        df = pd.read_csv(stock_mfc_object.get()["Body"], sep=";")
        df = df.astype(column_types)
        dataframe_list.append(df)
    df_full = pd.concat(dataframe_list, ignore_index=True)
    df_full = df_full.rename(columns=column_names)

    df_full["fecha_carga"] = macros.ds_add(ds, 1)
    print("Number of records to be loaded: "+str(len(df_full.index)))

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    # Save to PostgreSQL:

    df_full.to_sql(name="stock_mfc",
                con=engine,         
                schema="ecommdata",         
                if_exists='append',         
                index=False,         
                chunksize=20000,         
                method='multi')

    print("Data saved to PostgreSQL. Table: ecommdata.stock_mfc")

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_stock_mfc',
    default_args=default_args,
    description="Carga de datos de Stock MFC desde bucket de S3 al workspace de Postgresql.",
    schedule_interval="0 4 * * *",
    start_date=pendulum.datetime(2022, 8, 25, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["DATA", "SAP", "ecommdata", "stock_mfc", "MATIAS"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Extracción de archivos csv de Stock MFC desde bucket de S3, transformación y carga de datos en tabla ecommdata.stock_mfc. \n
    Un sensor espera por 30 minutos la presencia de un archivo bandera (.TRG) que indique que la carga del csv de datos está completa.
    """ 
    t0 = PythonOperator(
        task_id = "check_for_stock_mfc_flag_file",
        python_callable = _check_for_s3_file_with_date,
        retries = 6,
        retry_delay = timedelta(minutes=5),
    )

    t1 = PythonOperator(
        task_id = "load_stock_mfc",
        python_callable = _load_stock_mfc
    )

    t2 = PostgresOperator(
        task_id = "delete_old_data",
        postgres_conn_id="postgresql_conn",
        sql="""
        DELETE FROM ecommdata.stock_mfc
        WHERE fecha_carga < '{{ds}}'::date - interval '30 days';
        """
    )

    t0 >> t1 >> t2
