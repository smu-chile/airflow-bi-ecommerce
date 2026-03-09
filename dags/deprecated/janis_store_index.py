from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from datetime import datetime
from io import StringIO

import boto3
import botocore
import mysql.connector
import pandas as pd
import psycopg2
import sqlalchemy

def check_process_run():
    curr_datetime = datetime.utcnow()
    prefix = "janis/replica/wms_stores/"+curr_datetime.strftime("%Y/%m/%d/")
    file_name = prefix+"wms_stores.csv"

    access_key = Variable.get("AWS_ACCESS_KEY")
    secret_key = Variable.get("AWS_SECRET_KEY")
    bucket_name = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_resource = boto3.resource("s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name="us-east-1")
    bucket = s3_resource.Bucket(bucket_name)
    try:
        bucket.Object(file_name).get()
    except botocore.exceptions.ClientError as e:
        print("File not found: "+file_name)
        print("Starting process...")
        return True
    print("File already exists: "+file_name)
    print("Stoping process...")
    return False


def get_janis_store_list():
    curr_datetime = datetime.utcnow()
    prefix = "janis/replica/wms_stores/"+curr_datetime.strftime("%Y/%m/%d/")
    file_name = prefix+"wms_stores.csv"

    try:
        conn = mysql.connector.connect(
            user=Variable.get("JANIS_MARIADB_USER"),
            password=Variable.get("JANIS_MARIADB_PASSWORD"),
            host=Variable.get("JANIS_MARIADB_HOST"),
            port=3306,
            database=Variable.get("JANIS_MARIADB_DATABASE")
        )
    except mysql.connector.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        return

    # Get Cursor
    cur = conn.cursor()

    query = """
    select id
        , title as nombre_tienda_janis
        , '' as nombre_tienda_DW
        , ref_id as id_sap
        , sales_channel as canal_venta_vtex
        , lat as latitud
        , lng as longitud
        , street_name as calle
        , street_number as numero
        , city as ciudad
        , state as region
        , neighborhood as comuna
        , '' as gerente_zona_DW
        , '' as m2_sala_DW
        , status
        , ws.date_modified
        , ws.date_created
    from wms_stores ws
    """

    cur.execute(query)
    results = cur.fetchall()
    print(results)
    cur.close()
    conn.close()

    columns = ["id",
                "nombre_tienda_janis",
                "nombre_tienda_DW",
                "id_sap",
                "canal_venta_vtex",
                "latitud",
                "longitud",
                "calle",
                "numero",
                "ciudad",
                "region",
                "comuna",
                "gerente_zona_DW",
                "m2_sala_DW",
                "status",
                "date_modified",
                "date_created"]
    df = pd.DataFrame(results, columns=columns)
    buffer = StringIO()

    print(df)
    df["date_modified"] = pd.to_datetime(df["date_modified"], unit="s")
    df["date_created"] = pd.to_datetime(df["date_created"], unit="s")
    print(df)
    df.to_csv(buffer, header=True, index=False)
    buffer.seek(0)

    access_key = Variable.get("AWS_ACCESS_KEY")
    secret_key = Variable.get("AWS_SECRET_KEY")
    bucket_name = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name = "us-east-1"
    )
    response = s3_client.put_object(
        Bucket=bucket_name, Key=file_name, Body=buffer.getvalue()
    )

    return file_name

def load_janis_store_list(ti):
    file_name = ti.xcom_pull(key="return_value", task_ids=["get_janis_store_list"])[0]

    access_key = Variable.get("AWS_ACCESS_KEY")
    secret_key = Variable.get("AWS_SECRET_KEY")
    bucket_name = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_resource = boto3.resource("s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name="us-east-1")
    bucket = s3_resource.Bucket(bucket_name)
    csv_file = bucket.Object(file_name)

    df = pd.read_csv(csv_file.get()["Body"])
    print(df)

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    # Save to PostgreSQL:
    df.to_sql(name="tiendas",
                con=engine,         
                schema="public",         
                if_exists='replace',         
                index=False,         
                chunksize=20000,         
                method='multi')

    print("Data saved to PostgreSQL.")

    return

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'janis_wms_stores',
    default_args=default_args,
    description="Extracción y carga de tabla wms_stores desde Janis Replica.",
    schedule="0 10 * * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["DATA", "Janis"],
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla-indice de tiendas de Janis.
    """ 

    t0 = ShortCircuitOperator(
        task_id = "check_process_run",
        python_callable = check_process_run
    )
    
    t1 = PythonOperator(
        task_id = "get_janis_store_list",
        python_callable = get_janis_store_list
    )

    t2 = PythonOperator(
        task_id = "load_janis_store_list",
        python_callable = load_janis_store_list
    )

    t0 >> t1 >> t2
