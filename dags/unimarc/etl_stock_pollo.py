from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime
import pendulum


def _send_stock_to_janis_pollos(ds):
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

    # 📦 Query para obtener stock combinado de materiales de pollos
    query = """
        SELECT 
            s.id_tienda, 
            s.id_bodega, 
            t.id_janis,
            SUM(s.stock_janis::float) FILTER (WHERE s.ref_id IN ('000000000000051712-KGV', '000000000000051813-KGV')) AS stock_051813_051712_KGV,
            SUM(s.stock_janis::float) FILTER (WHERE s.ref_id IN ('000000000000051728-KG', '000000000000051845-KGV')) AS stock_051845_051728,
            SUM(s.stock_janis::float) FILTER (WHERE s.ref_id IN ('000000000000051802-KGV', '000000000000668742-KGV')) AS stock_668742_051802,
            SUM(s.stock_janis::float) FILTER (WHERE s.ref_id IN ('000000000000051806-KGV', '000000000000674766-KGV')) AS stock_051806_674766
        FROM ecommdata.stock s 
        LEFT JOIN ecommdata.tiendas t ON t.id = s.id_tienda 
        WHERE s.ref_id IN (
            '000000000000051712-KGV', '000000000000051813-KGV',
            '000000000000051728-KG', '000000000000051845-KGV',
            '000000000000051802-KGV', '000000000000668742-KGV',
            '000000000000051806-KGV', '000000000000674766-KGV'
        )
        AND s.fecha = current_date
        AND t.status = 1
        AND t.id not in ('0053', '0054', '0398')
        GROUP BY s.id_tienda, s.id_bodega, t.id_janis
    """

    result = conn.execute(text(query))
    rows = result.fetchall()
    print(f"Total tiendas con stock agregado: {len(rows)}")

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

    # Pares de ref_id que comparten stock combinado (materiales de pollo)
    pares_ref_id = [
        ('000000000000051712', '000000000000051813'),
        ('000000000000051728',  '000000000000051845'),
        ('000000000000051802', '000000000000668742'),
        ('000000000000051806', '000000000000674766')
    ]

    # 🧩 Construcción del payload
    tienda_to_payload = defaultdict(list)
    for row in rows:
        tienda_id = str(row[0])

        if tienda_id in tiendas_sin_warehouse_default:
            continue

        warehouse = warehouse_excepciones.get(tienda_id, tienda_id)

        for i, (sku1, sku2) in enumerate(pares_ref_id):
            stock_value = row[i + 3]  # offset de columnas
            if stock_value is None or stock_value <= 0:
                continue

            for sku in (sku1, sku2):
                tienda_to_payload[tienda_id].append({
                    "IdSku": sku,
                    "Quantity": int(stock_value),
                    "Store": tienda_id,
                    "Warehouse": warehouse
                })
                print(f"SKU: {sku} | Cantidad: {stock_value} | Tienda: {tienda_id} | Warehouse: {warehouse}")

    # 🔁 Enviar payload por tienda
    for tienda, payload in tienda_to_payload.items():
        print(f"⬆️ Enviando {len(payload)} SKUs a tienda {tienda}")
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        print(f"📦 Store: {tienda} | Status: {response.status_code} | Response: {response.text}")


# 🎛 Configuración del DAG
default_args = {
    "owner": "ecommerce_ops",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'etl_stock_materiales_pollos',
    default_args=default_args,
    description="Carga de stock 999 de materiales de pollo (pares de ref_id) en tiendas habilitadas",
    schedule_interval= "0 9 * * *",  
    start_date=pendulum.datetime(2025, 6, 4, tz="America/Santiago"),
    catchup=False,
    tags=["Janis", "Pollos", "Stock", "ecommdata", "local","KEVIN"],
) as dag:

    dag.doc_md = """
    ### DAG: etl_stock_materiales_pollos
    Suma el stock de pares de ref_id específicos de productos de pollo y los carga duplicados por SKU en la API de Janis.
    """

    t0 = PythonOperator(
        task_id="send_stock_to_janis_pollos",
        python_callable=_send_stock_to_janis_pollos,
    )

    t0
