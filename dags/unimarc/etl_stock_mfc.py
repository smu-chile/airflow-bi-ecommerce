from airflow import DAG
from airflow import macros
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator


from datetime import datetime, timedelta
import pendulum

def _load_stock_mfc(ds):
    import pandas as pd
    import sqlalchemy

    exec_date = macros.ds_add(ds, 1).replace("-", "/")
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

    print("Number of records to be loaded: "+str(len(df_full.index)))

    # host = Variable.get("POSTGRESQL_HOST")
    # database = Variable.get("POSTGRESQL_DB")
    # username = Variable.get("POSTGRESQL_USER")
    # password = Variable.get("POSTGRESQL_PASSWORD")
    
    # conn_url = "postgresql+psycopg2://"+username+":"+password+"@"+host+":5432/"+database
    # engine = sqlalchemy.create_engine(conn_url)

    # Save to PostgreSQL:

    # with engine.begin() as conn:
    #     df_full.to_sql(name="stock_mfc",
    #                 con=conn,         
    #                 schema="ecommdata",         
    #                 if_exists='append',         
    #                 index=False,         
    #                 chunksize=20000,         
    #                 method='multi')

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
    tags=["DATA", "SAP", "ecommdata", "stock_mfc"],
) as dag:

    dag.doc_md = """
    Extracción de archivos csv de Stock MFC desde bucket de S3, transformación y carga de datos en tabla ecommdata.stock_mfc. \n
    Un sensor espera por 30 minutos la presencia de un archivo bandera (.TRG) que indique que la carga del csv de datos está completa.
    """ 
    t0 = S3KeySensor(
        task_id = "wait_for_stock_mfc_flag_file",
        bucket_key = "datastage/stock_mfc/{{macros.ds_add(ds, 1).strftime('%Y/%m/%d')}}/stock.trg",
        bucket_name = Variable.get("AWS_S3_BUCKET_NAME"),
        aws_conn_id = "aws_s3_connection",
        timeout = 60*10,
        retries = 3,
        retry_delay = timedelta(minutes=1),
    )

    t1 = PythonOperator(
        task_id = "load_stock_mfc",
        python_callable = _load_stock_mfc
    )
