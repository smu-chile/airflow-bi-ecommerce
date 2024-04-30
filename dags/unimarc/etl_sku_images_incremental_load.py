from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from utils.janis_utils import load_full_table_to_s3


from datetime import datetime

import pendulum

def _truncate_and_load_sku_images_table(ti):
    import pandas as pd
    import sqlalchemy
    
    sku_images_file = ti.xcom_pull(key="return_value", task_ids=["load_full_table_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+sku_images_file)
    if not s3_hook.check_for_key(sku_images_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % sku_images_file)

    sku_images_object = s3_hook.get_key(sku_images_file, bucket_name=s3_bucket)

    df = pd.read_csv(sku_images_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")

    # Select only relevant columns:
    df = df[["id",
            "sku",
            "image",
            "label",
            "ord",
            "date_scheduled",
            "user_created",
            "user_modified",
            "date_created",
            "date_modified",
            ]]

    # Rename columns to match workspace schema:
    columns_rename = {
        "id": "id",
        "sku": "id_sku_janis",
        "image": "imagen",
        "label": "etiqueta",
        "ord": "orden",
        "date_scheduled": "fecha_programada",
        "user_created": "creacion_usuario",
        "user_modified": "modificacion_usuario",
        "date_created": "fecha_creacion",
        "date_modified": "fecha_modificacion"
    }
    df = df.rename(columns=columns_rename)

    # Calculate extra columns:
    df["ref_id"] = ""
    df["nombre_producto"] = ""
    df["fecha_programada"] = pd.to_datetime(df["fecha_programada"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["fecha_creacion"] = pd.to_datetime(df["fecha_creacion"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["fecha_modificacion_unixtime"] = df["fecha_modificacion"]
    df["fecha_modificacion"] = pd.to_datetime(df["fecha_modificacion"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")

    df = df.astype({
        "id": "int",
        "id_sku_janis": "int",
        "imagen": "string",
        "etiqueta": "string",
        "orden": "int",
        "fecha_creacion": "string",
        "fecha_modificacion": "string",
        "fecha_programada": "string",
        "creacion_usuario": "bool",
        "modificacion_usuario": "bool"
    }, errors="ignore")

    df = df[["id",
        "ref_id",
        "nombre_producto",
        "id_sku_janis",
        "imagen",
        "etiqueta",
        "orden",
        "fecha_programada",
        "creacion_usuario",
        "modificacion_usuario",
        "fecha_creacion",
        "fecha_modificacion",
        "fecha_modificacion_unixtime"
    ]]

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    with engine.begin() as conn:
        conn.execute("TRUNCATE ecommdata.imagenes_sku") 
        df.to_sql(name="imagenes_sku",
                    con=conn,         
                    schema="ecommdata",         
                    if_exists='append',         
                    index=False,         
                    chunksize=20000,         
                    method='multi')
        conn.execute("""
                    update ecommdata.imagenes_sku sk
                    set ref_id = s.ref_id, nombre_producto = s.nombre_sku
                    from ecommdata.skus s 
                    where s.id = sk.id_sku_janis;
                    """)

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_imagenes_sku_incremental_load',
    default_args=default_args,
    description="Extracción y carga de tabla imagenes_sku desde Janis Unimarc Replica hasta Workspace.",
    schedule_interval="0 6 * * *",
    start_date=pendulum.datetime(2022, 7, 1, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "Janis", "ecommdata", "imagenes_sku", "Unimarc", "MATIAS"],
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla de imagenes sku de Janis Unimarc a Workspace. \n
    UPSERT incremental basado en fecha_modificacion_unixtime.
    """ 
    t0 = PythonOperator(
        task_id = "load_full_table_to_s3",
        python_callable = load_full_table_to_s3,
        op_kwargs = {
            "table_name": "sku_images",
        }
    )

    t1 = PythonOperator(
        task_id = "truncate_and_load_sku_images_table",
        python_callable = _truncate_and_load_sku_images_table
    )

    t0 >> t1
