from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

from utils.slack_utils import dag_success_slack, dag_failure_slack

import pendulum

from datetime import datetime, timedelta

def transportadoras(accountName, env, X_VTEX_API_AppKey, X_VTEX_API_AppToken):
    import json
    import pandas as pd
    import requests
    import http.client
    import logging

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


def poligonos(transportadora, accountName, env, X_VTEX_API_AppKey, X_VTEX_API_AppToken):
    import json
    import pandas as pd
    import requests
    import http.client
    import logging

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

def coordenadas_poligono(poligono, accountName, env, X_VTEX_API_AppKey, X_VTEX_API_AppToken):
    import json
    import pandas as pd
    import requests
    import http.client
    import urllib.parse

    try: 
        conn = http.client.HTTPSConnection(f"{accountName}.{env}.com.br")

        headers = {
            'Accept': "application/json",
            'Content-Type': "application/json",
            'X-VTEX-API-AppKey': X_VTEX_API_AppKey,
            'X-VTEX-API-AppToken': X_VTEX_API_AppToken
            }
        poligono_quoted = urllib.parse.quote(poligono, safe='')
        conn.request("GET", f"/api/logistics/pvt/configuration/geoshape/{poligono_quoted}", headers=headers)
        res = conn.getresponse()
        if res.status == 200:
            data = res.read()
            response_data = json.loads(data)
            items = response_data["geoShape"]["coordinates"]

            print(f"Se esta imprimiendo este poligono: {poligono}")

            flattened_coordinates = [coordinate for coordinates_list in items for coordinate in coordinates_list]
            parsed_data = {"poligono": poligono, "coordenadas": flattened_coordinates}
            
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
    
    return pd.DataFrame()


def poligonos_to_s3(ds):
    import pandas as pd
    import numpy as np
    import io
    from io import StringIO
    
    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"poligonos_alvi/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    env = Variable.get("VTEX_ENV")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    vtex_account_name = {
        "alvicl004": Variable.get("VTEX_ALVI3074_ACCOUNT_NAME"),
        "alvicl008": Variable.get("VTEX_ALVI3089_ACCOUNT_NAME"),
        "alvicl001": Variable.get("VTEX_ALVI3092_ACCOUNT_NAME"),
        "alvicl010": Variable.get("VTEX_ALVI3093_ACCOUNT_NAME"),
        "alvicl005": Variable.get("VTEX_ALVI3098_ACCOUNT_NAME"),
        "alvicl009": Variable.get("VTEX_ALVI3172_ACCOUNT_NAME"),
        "alvicl002": Variable.get("VTEX_ALVI3180_ACCOUNT_NAME"),
        "alvicl006": Variable.get("VTEX_ALVI3181_ACCOUNT_NAME"),
        "alvicl007": Variable.get("VTEX_ALVI3187_ACCOUNT_NAME"),
        "alvicl011": Variable.get("VTEX_ALVI3188_ACCOUNT_NAME"),
        "alvitobalaba3193": Variable.get("VTEX_ALVI3193_ACCOUNT_NAME"),
        "alvicl012": Variable.get("VTEX_ALVI3088_ACCOUNT_NAME"),
        "alvicl013": Variable.get("VTEX_ALVI3094_ACCOUNT_NAME"),
        "alvicl014": Variable.get("VTEX_ALVI3086_ACCOUNT_NAME"),
        "alvichillan3091": Variable.get("VTEX_ALVI3091_ACCOUNT_NAME"),
        "alvilosandes3206": Variable.get("VTEX_ALVI3206_ACCOUNT_NAME"),
        "alvibelloto3085": Variable.get("VTEX_ALVI3085_ACCOUNT_NAME"),
        "alvipuntaarenas3212": Variable.get("VTEX_ALVI3212_ACCOUNT_NAME"),
        "alviconcon3211": Variable.get("VTEX_ALVI3211_ACCOUNT_NAME"),
        "alvicurico3223": Variable.get("VTEX_ALVI3223_ACCOUNT_NAME"),
    }

    x_vtex_api_appkey = {
        "alvicl004": Variable.get("X_VTEX_ALVI3074_API_Appkey"),
        "alvicl008": Variable.get("X_VTEX_ALVI3089_API_Appkey"),
        "alvicl001": Variable.get("X_VTEX_ALVI3092_API_Appkey"),
        "alvicl010": Variable.get("X_VTEX_ALVI3093_API_Appkey"),
        "alvicl005": Variable.get("X_VTEX_ALVI3098_API_Appkey"),
        "alvicl009": Variable.get("X_VTEX_ALVI3172_API_Appkey"),
        "alvicl002": Variable.get("X_VTEX_ALVI3180_API_Appkey"),
        "alvicl006": Variable.get("X_VTEX_ALVI3181_API_Appkey"),
        "alvicl007": Variable.get("X_VTEX_ALVI3187_API_Appkey"),
        "alvicl011": Variable.get("X_VTEX_ALVI3188_API_Appkey"),
        "alvitobalaba3193": Variable.get("X_VTEX_ALVI3193_API_Appkey"),
        "alvicl012": Variable.get("X_VTEX_ALVI3088_API_Appkey"),
        "alvicl013": Variable.get("X_VTEX_ALVI3094_API_Appkey"),
        "alvicl014": Variable.get("X_VTEX_ALVI3086_API_Appkey"),
        "alvichillan3091": Variable.get("X_VTEX_ALVI3091_API_Appkey"),
        "alvilosandes3206": Variable.get("X_VTEX_ALVI3206_API_Appkey"),
        "alvibelloto3085": Variable.get("X_VTEX_ALVI3085_API_Appkey"),
        "alvipuntaarenas3212": Variable.get("X_VTEX_ALVI3212_API_Appkey"),
        "alviconcon3211": Variable.get("X_VTEX_ALVI3211_API_Appkey"),
        "alvicurico3223": Variable.get("X_VTEX_ALVI3223_API_Appkey"),
    }

    x_vtex_api_apptoken = {
        "alvicl004": Variable.get("X_VTEX_ALVI3074_API_Apptoken"),
        "alvicl008": Variable.get("X_VTEX_ALVI3089_API_Apptoken"),
        "alvicl001": Variable.get("X_VTEX_ALVI3092_API_Apptoken"),
        "alvicl010": Variable.get("X_VTEX_ALVI3093_API_Apptoken"),
        "alvicl005": Variable.get("X_VTEX_ALVI3098_API_Apptoken"),
        "alvicl009": Variable.get("X_VTEX_ALVI3172_API_Apptoken"),
        "alvicl002": Variable.get("X_VTEX_ALVI3180_API_Apptoken"),
        "alvicl006": Variable.get("X_VTEX_ALVI3181_API_Apptoken"),
        "alvicl007": Variable.get("X_VTEX_ALVI3187_API_Apptoken"),
        "alvicl011": Variable.get("X_VTEX_ALVI3188_API_Apptoken"),
        "alvitobalaba3193": Variable.get("X_VTEX_ALVI3193_API_Apptoken"),
        "alvicl012": Variable.get("X_VTEX_ALVI3088_API_Apptoken"),
        "alvicl013": Variable.get("X_VTEX_ALVI3094_API_Apptoken"),
        "alvicl014": Variable.get("X_VTEX_ALVI3086_API_Apptoken"),
        "alvichillan3091": Variable.get("X_VTEX_ALVI3091_API_Apptoken"),
        "alvilosandes3206": Variable.get("X_VTEX_ALVI3206_API_Apptoken"),
        "alvibelloto3085": Variable.get("X_VTEX_ALVI3085_API_Apptoken"),
        "alvipuntaarenas3212": Variable.get("X_VTEX_ALVI3212_API_Apptoken"),
        "alviconcon3211": Variable.get("X_VTEX_ALVI3211_API_Apptoken"),
        "alvicurico3223": Variable.get("X_VTEX_ALVI3223_API_Apptoken"),
    }

    all_stores_df = pd.DataFrame()

    for seller_key in vtex_account_name:
        accountName = vtex_account_name[seller_key]
        X_VTEX_API_AppKey = x_vtex_api_appkey[seller_key]
        X_VTEX_API_AppToken = x_vtex_api_apptoken[seller_key]
        
        print(f"Obtaining data for seller: {seller_key}, account: {accountName}")
        
        try:
            df = transportadoras(accountName, env, X_VTEX_API_AppKey, X_VTEX_API_AppToken)
            if df.empty:
                continue
                
            lista_id_transportadoras = df["id"].tolist()
            
            df_poligonos = pd.DataFrame()
            for transportadora in lista_id_transportadoras:
                df_aux = poligonos(transportadora, accountName, env, X_VTEX_API_AppKey, X_VTEX_API_AppToken)
                if not df_aux.empty:
                    df_poligonos = pd.concat([df_poligonos, df_aux], axis=0)
                
            if not df_poligonos.empty:
                df_final = df.merge(df_poligonos,how = 'left', on = "id")
            else:
                df_final = df.copy()
                df_final['polygon'] = np.nan

            lista_poligonos_activos = df_final["polygon"].dropna().unique().tolist()
            df_coordenadas = pd.DataFrame()
            for poligono in lista_poligonos_activos:
                df_aux = coordenadas_poligono(poligono, accountName, env, X_VTEX_API_AppKey, X_VTEX_API_AppToken)
                if not df_aux.empty:
                    df_coordenadas = pd.concat([df_coordenadas, df_aux], axis=0)

            if not df_coordenadas.empty:
                df_coordenadas["poligono"] = df_coordenadas['poligono'].apply(lambda x: x.replace("%20", " "))
                df_coordenadas["poligono"] = df_coordenadas['poligono'].apply(lambda x: x.replace("%C3%B1", "ñ"))
                df_coordenadas = df_coordenadas.groupby('poligono')['coordenadas'].apply(list).reset_index()
                df_coordenadas.columns = ["polygon","coordenadas"]
                df_final = df_final.merge(df_coordenadas,how = 'left', on = "polygon")
                df_final["coordenadas"] = df_final["coordenadas"].astype(str)
                df_final["coordenadas"] = df_final["coordenadas"].replace("\[", "(", regex=True).replace("\]", ")", regex=True)
                df_final["coordenadas"] = df_final["coordenadas"].replace("nan", np.nan, regex=True)
            else:
                df_final['coordenadas'] = np.nan
                
            # Add the vtex_account column to identify the seller
            df_final['vtex_account'] = accountName
            
            all_stores_df = pd.concat([all_stores_df, df_final], axis=0)
            
        except Exception as e:
            print(f"Error processing {seller_key}: {e}")
            continue

    if all_stores_df.empty:
        print("No records retrieved from any Alvi seller. Skipping S3 upload.")
        return "empty"

    all_stores_df.info()

    buffer = io.StringIO()
    all_stores_df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"poligonos_alvi/{exec_date}/poligonos_{date_aux}.csv"
    buffer.seek(0)
    print("se logro transformar el dataframe unificado a un archivo .csv")
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

    if filename == "empty":
        print("There are no records from the previous task. Task will exit as successful.")
        return

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+filename)
    if not s3_hook.check_for_key(filename, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % filename)

    s_stock_object = s3_hook.get_key(filename, bucket_name=s3_bucket)

    df = pd.read_csv(s_stock_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successful.")
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
        df.to_sql(name="poligonos_alvi",
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
    'etl_poligonos_alvi',
    default_args=default_args,
    description="cargar tabla poligonos tiendas alvi iterando sellers",
    schedule_interval="0 8 * * *",
    start_date=pendulum.datetime(2023, 12, 6, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "Janis", "forcast_and_plannig", "polygons", "alvi", "seller", "PATRICIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:
    dag.doc_md = """
    Carga tabla poligonos para tiendas alvi (iterando sobre todos los sellers VTEX) \n
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
        task_id = "delete_old_poligons_alvi",
        postgres_conn_id = "postgresql_conn",
        sql = """DELETE
            FROM forecast_and_planning.poligonos_alvi
            WHERE fecha = '{{ds}}'::date - interval '360 days' """
    )
    t0 >> t1 >> t2
