from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator as PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable

from utils.slack_utils import dag_success_slack, dag_failure_slack

import pendulum

from datetime import datetime, timedelta

def transportadoras():
    import json
    import pandas as pd
    import requests
    import http.client
    import logging

    X_VTEX_API_AppKey = Variable.get("X_VTEX_API_AppKey")
    X_VTEX_API_AppToken = Variable.get("X_VTEX_API_AppToken")
    accountName = Variable.get("VTEX_ACCOUNT_NAME")
    env = Variable.get("VTEX_ENV")

    conn = http.client.HTTPSConnection(f"{accountName}.{env}.com.br")

    headers = {
        'Accept': "application/json",
        'Content-Type': "application/json",
        'X-VTEX-API-AppKey': X_VTEX_API_AppKey,
        'X-VTEX-API-AppToken': X_VTEX_API_AppToken
        }

    conn.request("GET", "/api/logistics/pvt/shipping-policies?page=1&perPage=500", headers=headers)
    res = conn.getresponse()
    
    data = res.read()
    
    # Loguear la respuesta antes de procesarla
    logging.info(f"API Response Status: {res.status}")

    if res.status != 200:
        raise Exception(f"API request failed with status {res.status}: {res.reason}")

    if not data:
        raise Exception("API response is empty")

    try:
        response_data = json.loads(data)
    except json.JSONDecodeError as e:
        raise Exception(f"Failed to decode JSON: {str(e)}")

    if "items" not in response_data:
        raise Exception("No 'items' key found in the response data")

    items = response_data["items"]
    parsed_data = [{"id": item.get("id"),
                    "name": item.get("name"),
                    "shippingMethod": item.get("shippingMethod"),
                    "isActive": item.get("isActive"),
                    "deliveryChannel": item.get("deliveryChannel")} for item in items]

    df = pd.DataFrame(parsed_data)
    
    return df


def poligonos(transportadora):
    import json
    import pandas as pd
    import requests
    import http.client
    import logging

    X_VTEX_API_AppKey = Variable.get("X_VTEX_API_AppKey")
    X_VTEX_API_AppToken = Variable.get("X_VTEX_API_AppToken")
    accountName = Variable.get("VTEX_ACCOUNT_NAME")
    env = Variable.get("VTEX_ENV")

    conn = http.client.HTTPSConnection(f"{accountName}.{env}.com.br")

    headers = {
        'Accept': "application/json",
        'Content-Type': "application/json",
        'X-VTEX-API-AppKey': X_VTEX_API_AppKey,
        'X-VTEX-API-AppToken': X_VTEX_API_AppToken
        }

    conn.request("GET", f"/api/logistics/pvt/configuration/freights/{transportadora}/00000000/values", headers=headers)
    res = conn.getresponse()
    data = res.read()

    # Loguear la respuesta antes de procesarla
    logging.info(f"API Response Status: {res.status}")
    logging.info(f"API Response Data: {data.decode('utf-8')}")

    if res.status != 200:
        raise Exception(f"API request failed with status {res.status}: {res.reason}")

    if not data:
        raise Exception("API response is empty")

    response_data = json.loads(data)

    if "error" in response_data:
        error_message = response_data["error"].get("message", "Unknown error")
        logging.warning(f"Skipping transportadora {transportadora} due to error: {error_message}")
        return pd.DataFrame()  # Devolver un DataFrame vacío si hay un error
    
    items = response_data
    parsed_data = [{"id":transportadora, 
                    "polygon": item["polygon"] } for item in items]
    
    df = pd.DataFrame(parsed_data)
    
    return df

def coordenadas_poligono(poligono):
    import json
    import pandas as pd
    import requests
    import http.client
    import urllib.parse

    try: 
        X_VTEX_API_AppKey = Variable.get("X_VTEX_API_AppKey")
        X_VTEX_API_AppToken = Variable.get("X_VTEX_API_AppToken")
        accountName = Variable.get("VTEX_ACCOUNT_NAME")
        env = Variable.get("VTEX_ENV")

        conn = http.client.HTTPSConnection(f"{accountName}.{env}.com.br")

        headers = {
            'Accept': "application/json",
            'Content-Type': "application/json",
            'X-VTEX-API-AppKey': X_VTEX_API_AppKey,
            'X-VTEX-API-AppToken': X_VTEX_API_AppToken
            }
        poligono = urllib.parse.quote(poligono, safe='')
        conn.request("GET", f"/api/logistics/pvt/configuration/geoshape/{poligono}", headers=headers)
        res = conn.getresponse()
        if res.status == 200:
            data = res.read()
            response_data = json.loads(data)
            items = response_data["geoShape"]["coordinates"]

            print(f"Se esta imprimiendo este poligono: {poligono}")

            flattened_coordinates = [coordinate for coordinates_list in items for coordinate in coordinates_list]
            parsed_data = {"poligono": poligono, "coordenadas": flattened_coordinates}
            print(parsed_data)
            # filtramos duplicados manteniendo orden
            seen = set()
            unique_coords = []
            for coord in flattened_coordinates:
                tup = tuple(coord)
                if tup not in seen:
                    seen.add(tup)
                    unique_coords.append(coord)
            if unique_coords:
                unique_coords.append(unique_coords[0])  # Aseguramos que el polígono se cierre

            # ya no hay ni repeticiones al cerrar el polígono ni por re-procesos
            parsed = {"poligono": poligono, "coordenadas": unique_coords}
            df = pd.DataFrame(parsed)

            return df
        else:
            print(f"Error {res.status} al obtener las coordenas del poligono {poligono}")
            pass
    except Exception as e:
        print(f"Ocurrió un error: {e}")
        pass


def poligonos_to_s3(ds):
    import pandas as pd
    import numpy as np
    import io
    from io import StringIO
    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"poligonos/{exec_date}/"
    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    df = transportadoras()
    lista_id_transportadoras = df["id"].tolist()
    print(lista_id_transportadoras)
    df_poligonos = pd.DataFrame()
    for transportadora in lista_id_transportadoras:
        df_aux = poligonos(transportadora)
        df_poligonos = pd.concat([df_poligonos, df_aux], axis=0)
        
    df_final = df.merge(df_poligonos,how = 'left', on = "id")

    print(df_final)

    lista_poligonos_activos = df_final["polygon"].dropna().unique().tolist()
    df_coordenadas = pd.DataFrame()
    for poligono in lista_poligonos_activos:
        df_aux = coordenadas_poligono(poligono)
        df_coordenadas = pd.concat([df_coordenadas, df_aux], axis=0)

    print(df_coordenadas)

    df_coordenadas["poligono"] = df_coordenadas['poligono'].apply(lambda x: x.replace("%20", " "))
    df_coordenadas["poligono"] = df_coordenadas['poligono'].apply(lambda x: x.replace("%C3%B1", "ñ"))
    df_coordenadas = df_coordenadas.groupby('poligono')['coordenadas'].apply(list).reset_index()
    df_coordenadas.columns = ["polygon","coordenadas"]
    df_final = df_final.merge(df_coordenadas,how = 'left', on = "polygon")
    df_final["coordenadas"] = df_final["coordenadas"].astype(str)
    df_final["coordenadas"] = df_final["coordenadas"].replace("\[", "(", regex=True).replace("\]", ")", regex=True)
    df_final["coordenadas"] = df_final["coordenadas"].replace("nan", np.nan, regex=True)
    df_final.info()

    buffer = io.StringIO()
    df_final.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"poligonos/{exec_date}/poligonos_{date_aux}.csv"
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

def poligonos_to_postgres(ti,ds):
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text

    filename = ti.xcom_pull(key="return_value", task_ids=["poligonos_to_s3"])[0]

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
    df["fecha"] = ds
    df.info()

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    with engine.begin() as conn:
        df.to_sql(name="poligonos",
                    con=conn,         
                    schema="forecast_and_planning",         
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
    'etl_poligonos',
    default_args=default_args,
    description="cargar tabla poligonos tiendas",
    schedule="0 8 * * *",
    start_date=pendulum.datetime(2023, 12, 6, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "Janis", "forcast_and_plannig", "polygons", "unimarc", "PATRICIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:
    dag.doc_md = """
    Carga tabla poligonos\n
    guardar en S3.
    """ 
    t0 = PythonOperator(
        task_id='poligonos_to_s3',
        python_callable=poligonos_to_s3,
    )

    t1 = PythonOperator(
        task_id = "poligonos_to_postgres",
        python_callable = poligonos_to_postgres,
    )

    t2 = PostgresOperator(
        task_id = "delete_old_poligons",
        conn_id="postgresql_conn",
        sql = """DELETE
            FROM forecast_and_planning.poligonos
            WHERE fecha = '{{ds}}'::date - interval '360 days' """
    )
    t0 >> t1 >> t2


