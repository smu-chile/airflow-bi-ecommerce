from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

from utils.slack_utils import dag_success_slack, dag_failure_slack

from datetime import datetime, timedelta
import pendulum

def load_last_zippedi_session(ds):
    import requests
    import pandas as pd
    import numpy as np

    username = Variable.get("ZIPPEDI_API_USERNAME")
    password = Variable.get("ZIPPEDI_API_PASSWORD")

    URL = "https://api.zippedi.com"

    # Get token
    endpoint = '/auth'
    response = requests.post(URL + endpoint, json={"username": username, "password": password})
    token = response.json()["access_token"]

    # Get store info
    endpoint = '/status/store'
    response = requests.get(URL + endpoint, headers={"Authorization": "jwt {0}".format(token)})
    stores_data = response.json()
    
    store_ids = [store["store"] for store in stores_data]
    store_labels = []

    for store in store_ids:
        # Get session info from store
        endpoint = '/status/session'
        date = datetime.strptime(ds, "%Y-%m-%d")
        payload = {"store": store, "date": date.strftime("%Y%m%d")}
        parameters = "&".join(["{0}={1}".format(parameter, value) for parameter, value in payload.items()])

        response = requests.get(URL + endpoint + "?" + parameters, headers={"Authorization": "jwt {0}".format(token)})
        sessions = response.json()

        if sessions and isinstance(sessions, list) and 'session' in sessions[0]:
            # Get product labels from session
            session_value = sessions[0]['session']
            endpoint = '/data/labels'
            payload = {"store": store, "session": session_value}
            parameters = "&".join(["{0}={1}".format(parameter, value) for parameter, value in payload.items()])
            response = requests.get(URL + endpoint + "?" + parameters, headers={"Authorization": "jwt {0}".format(token)})
            labels = response.json()
            
            # Add store value to each label
            for label in labels:
                label['store'] = store
            
            store_labels.extend(labels)
        else:
            print(f"No new sessions found for {store}.")
    
    df = pd.DataFrame(store_labels)

    df = df.rename(columns={
        "item": "material",
        "description": "nombre_sku",
        "aisle_facing": "pasillo",
        "bay": "bahia"
    })

    df['tienda'] = df['store'].str.replace('UNI', '').str.zfill(4)
    df = df.drop(columns=['store'])

    df['material'] = df['material'].str.zfill(18)

    columns = ['tienda'] + [col for col in df.columns if col != 'tienda']
    df = df[columns]

    df = df[[
            "tienda",
            "material",
            "ean",
            "nombre_sku",
            "pasillo",
            "bahia"
            ]]

    column_types = {
        "tienda": "string",
        "material": "string",
        "ean": "string", 
        "nombre_sku": "string",
        "pasillo": "string", 
        "bahia": "string"
    }
    df = df.astype(column_types, errors="ignore")
    
    columns = [
        "ean",
        "nombre_sku",
        "pasillo",
        "bahia"
    ]

    columns_query = ",".join(columns)
    values_query = "%s,%s,"+",".join(["%s" for column in columns])
    excluded_query = ",".join(["EXCLUDED."+column for column in columns])
    df = df.fillna("NULL")
    records = list(df.to_records(index=False))
    
    # Change data types to native python types
    fixed_records = []
    for record in records:
        fixed_record = []
        for value in record:
            if isinstance(value, np.generic):
                fixed_record.append(value.item())
            elif value == "NULL":
                fixed_record.append(None)
            else:
                fixed_record.append(value)
        fixed_records.append(tuple(fixed_record))
    print(f"Number of records to load: {str(len(fixed_records))}")
    incremental_query = """
        INSERT INTO ecommdata.localizacion_zippedi (tienda, material,"""+columns_query+""") 
        VALUES ("""+values_query+""")
        ON CONFLICT (tienda,material)
        DO UPDATE SET ("""+columns_query+""") = ("""+excluded_query+"""); 
    """
    print(incremental_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.executemany(incremental_query, fixed_records)
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    print("Data loaded to Postgres")

    return

default_args = {
    "owner": "ecommerce_ops",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'proc_localizacion_zippedi',
    default_args=default_args,
    description="""Extraction and insert of attributes from Zippedi API to Janis.""",
    schedule_interval="0 10 * * *",
    start_date=pendulum.datetime(2024, 9, 1, tz="America/Santiago"),
    catchup=False,
    tags=["API", "Janis", "zippedi", 'localizacion', "SERGIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Extraction and insert of attributes from Zippedi API to Janis.
    """

    t0 = PythonOperator(
        task_id="load_last_zippedi_session",
        python_callable=load_last_zippedi_session,
    )

t0