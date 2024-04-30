from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

import pendulum

from datetime import datetime, timedelta

def bool_a_str(obj):
    if isinstance(obj, bool):
        return str(obj).lower()
    raise TypeError

def _api_google_token():
    import requests
    import json

    print("\nObteniendo Token\n")

    correo = Variable.get("API_GOOGLE_MAIL")
    password = Variable.get("API_GOOGLE_PASSWORD")
    key = Variable.get("API_GOOGLE_KEY")
    url_api = Variable.get("API_GOOGLE_URL")
    url = f"{url_api}?key={key}"

    body = {
        "email": correo,
        "password": password,
        "returnSecureToken": True
    }

    try:
        # Convertir el cuerpo a JSON utilizando el método personalizado para los valores booleanos
        payload_json = json.dumps(body, default=bool_a_str)

        # Hacer la solicitud POST a la API con el cuerpo JSON
        respuesta = requests.post(url, data=payload_json)

        # Verificar si la solicitud fue exitosa (código de estado 200)
        if respuesta.status_code == 200:
            # Convertir la respuesta en JSON a un diccionario Python
            data = respuesta.json()
            #print(json.dumps(data, indent=4))  # Imprime la data de forma legible

            # Extraer el idToken y guardarlo en una variable
            id_token = data.get('idToken')
            if id_token:
                print("idToken obtenido :D")  # Imprimir el idToken
            else:
                print("idToken no encontrado en la respuesta.")
        else:
            print(f"Error: No se pudo obtener la data. Código de estado: {respuesta.status_code}")
    except Exception as e:
        print(f"Ocurrió un error: {e}")

    return id_token

def _api_takeoff(token):
    import requests
    import pandas as pd

    print("\nObteniendo datos de la API de Takeoff\n")

    sites_id = Variable.get("API_TAKEOFF_SITE_ID")
    url_api = Variable.get("API_TAKEOFF_URL")
    url = f"{url_api}/{sites_id}/inventorySnapshot?include-zero-quantity=true"
    headers = {"x-token": token}

    try:
        # Hacer la solicitud a la API
        respuesta = requests.get(url, headers=headers)

        # Verificar si la solicitud fue exitosa (código de estado 200)
        if respuesta.status_code == 200:
            # Cargar la respuesta JSON
            json_respuesta = respuesta.json()

            # Convertir la clave 'data' del JSON en un DataFrame
            df = pd.DataFrame(json_respuesta['data'])

            # Retornar el DataFrame
            return df
        else:
            print(f"Error: No se pudo obtener la data. Código de estado: {respuesta.status_code}")
            return None
    except Exception as e:
        print(f"Ocurrió un error: {e}")
        return None
    return

def stock_mfc_to_s3(ds,ts):
    import io
    from io import StringIO
    import pandas as pd

    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    date_aux_filename = ts.replace("-","_")
    prefix = f"stock_mfc_takeoff/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
    token = _api_google_token()
    print("\nidToken: ", token)
    df = _api_takeoff(token)
    df["fecha"] = ts
    print(df.info())

    buffer = io.StringIO()
    df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"stock_mfc_takeoff/{exec_date}/stock_mfc_takeoff_{date_aux_filename}.csv"
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

def stock_mfc_to_postgresql(ti):
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text

    filename = ti.xcom_pull(key="return_value", task_ids=["stock_mfc_to_s3"])[0]

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
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    with engine.begin() as conn:
        df.to_sql(name="stock_mfc_takeoff",
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
    'etl_stock_mfc_takeoff_2',
    default_args=default_args,
    description="utiliza la API de takeoff para extraer el stock de MFC, lo carga a S3 y lo sube a postgresql",
    schedule_interval= "0 1,4/4 * * *",
    start_date=pendulum.datetime(2023, 9, 27, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "postgres", "MFC", "s3", "stock", "takeoff", "PATRICIO"],
) as dag:

    dag.doc_md = """
    generar dataframe a partir del consumo de la API takeoff, lo carga a S3 y lo sube a postgresql. \n
    cada 4 horas.
    """ 

    t0 = PythonOperator(
        task_id = "stock_mfc_to_s3",
        python_callable = stock_mfc_to_s3,
    )

    t1 = PythonOperator(
       task_id = "stock_mfc_to_postgresql",
        python_callable = stock_mfc_to_postgresql,
    )

    t2 = PostgresOperator(
        task_id = "delete_old_stock_mfc_takeoff",
        postgres_conn_id = "postgresql_conn",
        sql = """DELETE
            FROM ecommdata.stock_mfc_takeoff
            WHERE fecha = '{{ds}}'::date - interval '29 days' """
    )

    t0 >> t1 >> t2
