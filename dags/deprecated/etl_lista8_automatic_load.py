from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator


from datetime import datetime, timedelta

def _load_lista8(ts):
    import pandas as pd
    import sqlalchemy

    exec_date = datetime.strptime(ts[:10], "%Y-%m-%d") + timedelta(days=1)
    exec_date = exec_date.strftime("%Y/%m/%d")
    prefix = f"sap/lista8/{exec_date}/"
    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    s3_file_list = s3_hook.list_keys(s3_bucket, prefix=prefix)
    print(f"Files detected: {s3_file_list}")
    column_types = {
        "CENTRO": "str",
        "MATERIAL":	"str",
        "UM VTA":	"str",
        "NEGOCIO": "str",
        "SECCION": "str",
        "LINEA": "str",
        "CATEGORIA": "str",
        "GRUPO ARTICULO": "str",
        "PRECIO REGULAR": "int",
        "PRECIO PROMOCIONAL": "int",
        "DESCRIPCION": "str",
    }

    column_names = {
        "CENTRO": "id_tienda",
        "MATERIAL":	"material",
        "UM VTA":	"umv",
        "NEGOCIO": "negocio",
        "SECCION": "seccion",
        "LINEA": "linea",
        "CATEGORIA": "categoria",
        "GRUPO ARTICULO": "grupo_articulo",
        "PRECIO REGULAR": "precio_regular",
        "PRECIO PROMOCIONAL": "precio_promocional",
        "DESCRIPCION": "descripcion",
    }

    dataframe_list = []
    for s3_file in s3_file_list:
        if s3_file in [prefix, prefix+"flag.txt"]:
            # Skip empty 'folder' file and flag flie
            continue
        print(f"Loading file: {s3_file}")
        lista8_object = s3_hook.get_key(s3_file, bucket_name=s3_bucket)
        df = pd.read_csv(lista8_object.get()["Body"], sep=";")
        df = df.astype(column_types)
        dataframe_list.append(df)
    df_full = pd.concat(dataframe_list, ignore_index=True)
    df_full = df_full.rename(columns=column_names)
    df_full["fecha"] = exec_date
    df_full["id_tienda"] = df_full["id_tienda"].str.zfill(4)
    df_full["material"] = df_full["material"].str.zfill(18)

    # Drop duplicates
    df_full = df_full.drop_duplicates()

    print("Number of records to be loaded: "+str(len(df_full.index)))

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    # Save to PostgreSQL:
    df_full.to_sql(name="lista8",
                con=engine,         
                schema="ecommdata",         
                if_exists='append',         
                index=False,         
                chunksize=20000,         
                method='multi')

    print("Data saved to PostgreSQL. Table: ecommdata.lista8")

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_lista8_incremental_load',
    default_args=default_args,
    description="Carga de datos de lista8 desde bucket de S3 al workspace de Postgresql.",
    schedule="0 12 * * *",
    start_date=datetime(2022, 2, 20),
    catchup=True,
    max_active_runs = 1,
    tags=["DATA", "SAP", "ecommdata", "lista8"],
) as dag:

    dag.doc_md = """
    Extracción de archivos csv de lista8 desde bucket de S3, transformación y carga de datos en tabla ecommdata.lista8. \n
    Un sensor espera por 3 horas la presencia de un archivo bandera que indique que la carga de los csv de datos está completa.
    """ 
    t0 = S3KeySensor(
        task_id = "wait_for_lista8_flag_file",
        bucket_key = "sap/lista8/{{(execution_date + macros.timedelta(days=1)).strftime('%Y/%m/%d')}}/flag.txt",
        bucket_name = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket'),
        aws_conn_id = "aws_s3_connection",
        timeout = 60*60*3
    )

    t1 = PythonOperator(
        task_id = "load_lista8",
        python_callable = _load_lista8
    )

    t0 >> t1 
