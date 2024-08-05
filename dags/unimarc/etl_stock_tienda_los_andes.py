from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime

import pendulum

def divide_chunks(l, n): 
    for i in range(0, len(l), n):  
        yield l[i:i + n] 

def _send_stock_999_to_janis(ds):
    import pandas as pd
    import sqlalchemy
    import requests
    import json

    print(f"date: {ds}")
    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)
    conn = engine.connect()


    query = """
        select distinct material
        from ecommdata.lista8 
    """

    result = conn.execute(query)
    list = result.fetchall()
    print(list)

    separated_list = divide_chunks(list, 400)

    base_url = Variable.get("JANIS_API_URL")

    url = f"{base_url}stock"

    JANIS_API_KEY = Variable.get("JANIS_API_KEY")
    JANIS_API_SECRET = Variable.get("JANIS_API_SECRET")
    JANIS_CLIENT = Variable.get("JANIS_CLIENT")

    headers = {
    "janis-api-key" : JANIS_API_KEY,
    "janis-api-secret" : JANIS_API_SECRET,
    "janis-client" : JANIS_CLIENT,
    "Connection" : "keep-alive"
    }
    for chunk in separated_list:
        payload=[]
        for r in chunk:
            material = str(r[0]).zfill(18)
            id_tienda = "0054"
            warehouse = "0054"
            row = {"IdSku": material, "Quantity": 999, "Store": id_tienda, "Warehouse": warehouse}
            print(row)
            payload.append(row)
        payload = json.dumps(payload)
        response = requests.request("POST", url, headers=headers, data=payload)
        print(response.text)
    return

default_args = {
    "owner": "ecommerce_ops",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_stock_tienda_los_andes',
    default_args=default_args,
    description="Se agrega stock a tienda de prueba Los Andes para emular ordenes en Janis Picking",
    schedule_interval="30 * * * *",
    start_date=pendulum.datetime(2024, 7, 3, tz="America/Santiago"),
    catchup=False,
    tags=["Janis", "ecommdata", "catalogo", "Los Andes", "stock", "SERGIO"],
) as dag:

    dag.doc_md = """
    Borrado de Stock Janis en base a historia de ventas en dw y parametros entregados en tabla catalogo.categoria_tienda_inmovilizada.
    """ 
    
    t0 = PythonOperator(
        task_id = "send_stock_999_to_janis",
        python_callable = _send_stock_999_to_janis
    )

    t0
