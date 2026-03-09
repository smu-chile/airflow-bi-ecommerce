from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator as PostgresOperator

from utils.slack_utils import dag_success_slack, dag_failure_slack

from datetime import datetime

import pendulum

def divide_chunks(l, n): 
    for i in range(0, len(l), n):  
        yield l[i:i + n] 

def _send_stock_999_to_janis_pan(ds):
    import pandas as pd
    import sqlalchemy
    import requests
    import json
    from sqlalchemy.sql import text
    from collections import defaultdict

    print(f"date: {ds}")

    # 🔐 Conexión a BD
    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")

    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)
    conn = engine.connect()

    # 📦 Consulta materiales por tienda
    query = """
        SELECT DISTINCT 
            COALESCE(s.erp_id, l.material) AS material,
            l.id_tienda
        FROM ecommdata.lista8 l
        LEFT JOIN ecommdata.skus s ON concat(l.material,'-',l.umv) = s.ref_id
        LEFT JOIN ecommdata.productos p ON p.ref_id = s.ref_id 
        LEFT JOIN ecommdata.categorias c ON p.id_categoria = c.id 
        WHERE c.n2 = 'Pan'
    """

    result = conn.execute(text(query))
    rows = result.fetchall()
    print(f"Total SKU-tienda encontrados: {len(rows)}")

    # 🧩 Excepciones warehouse
    warehouse_excepciones = {
        '0332': '15f52fc',
        '0469': '0003',
        '0581': '18bced3',
        '0917': '193949d',
        '0956': '956',
    }
    tiendas_sin_warehouse_default = ['0463', '0486', '0576', '0915', '0931', '0979']

    # 🌐 Config API Janis
    base_url = Variable.get("JANIS_API_URL")
    url = f"{base_url}stock"
    headers = {
        "janis-api-key": Variable.get("JANIS_API_KEY"),
        "janis-api-secret": Variable.get("JANIS_API_SECRET"),
        "janis-client": Variable.get("JANIS_CLIENT"),
        "Connection": "keep-alive"
    }

    # 🧩 Agrupar materiales por tienda
    tienda_to_payload = defaultdict(list)
    for material, tienda in rows:
        if tienda in tiendas_sin_warehouse_default:
            continue
        warehouse = warehouse_excepciones.get(tienda, tienda)
        sku = str(material).zfill(18)
        tienda_to_payload[tienda].append({
            "IdSku": sku,
            "Quantity": 999,
            "Store": tienda,
            "Warehouse": warehouse
        })

    # 🔁 Un solo POST por tienda
    for tienda, payload in tienda_to_payload.items():
        print(f"⬆️ Enviando {len(payload)} SKUs a tienda {tienda}")
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        print(f"📦 Store: {tienda} | Status: {response.status_code} | Response: {response.text}")

    return



default_args = {
    "owner": "ecommerce_ops",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_stock_materiales_panaderia',
    default_args=default_args,
    description="Se agrega stock 999 de materiales de panaderia a tiendas que correspondan",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2024, 7, 3, tz="America/Santiago"),
    catchup=False,
    tags=["Janis", "Panaderia", "KEVIN", "ecommdata", "catalogo", "stock"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Carga de stock 999 en todas las tiendas para materiales de panaderia.
    """

    t0 = PythonOperator(
        task_id = "send_stock_999_to_janis_pan",
        python_callable = _send_stock_999_to_janis_pan
    )

    t0
