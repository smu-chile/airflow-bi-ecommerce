#!/usr/bin/env python3
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator

from utils.slack_utils import dag_success_slack, dag_failure_slack

import pendulum


def _choose_branch(ds, **kwargs):
    # Permitir forzar la rama manualmente
    conf = kwargs.get('dag_run').conf or {}
    if 'force_branch' in conf:
        print(f"➡️ Forzando rama manualmente por config: {conf['force_branch']}")
        return conf['force_branch']

    execution_date = kwargs['logical_date']
    hora_chile = execution_date.in_tz('America/Santiago').hour
    print(f"Hora Chile evaluada: {hora_chile}")
    
    if hora_chile < 16:
        print("➡️ Ejecutando rama: 999 (AM)")
        return "send_stock_999"
    else:
        print("➡️ Ejecutando rama: 0 (PM)")
        return "send_stock_0"


def _send_stock(quantity, infinite_stock, ds, **kwargs):
    import sqlalchemy
    import requests
    import json
    from sqlalchemy.sql import text
    from collections import defaultdict

    print(f"date: {ds}")
    print(f"➡️ Quantity: {quantity} | InfiniteStock: {infinite_stock}")

    # 🔐 Conexión a BD
    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")

    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)
    conn = engine.connect()

    # 📦 Consulta tiendas activas para los dos materiales específicos
    query = """
        SELECT DISTINCT 
            COALESCE(s.erp_id, l.material) AS material,
            l.id_tienda
        FROM ecommdata.lista8 l
        LEFT JOIN ecommdata.skus s ON concat(l.material,'-',l.umv) = s.ref_id
        WHERE l.material IN ('000000000000612745', '000000000000665186')
          AND l.umv = 'UN'
    """

    result = conn.execute(text(query))
    rows = result.fetchall()
    print(f"Total SKU-tienda encontrados: {len(rows)}")

    # 🧩 Excepciones warehouse
    warehouse_excepciones = {
        '0005': 'un05',
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

    # 🧩 Armar payload completo
    full_payload = []
    for material, tienda in rows:
        if tienda in tiendas_sin_warehouse_default:
            continue
        warehouse = warehouse_excepciones.get(tienda, tienda)
        sku = str(material).zfill(18)
        full_payload.append({
            "IdSku": sku,
            "Quantity": quantity,
            "Store": tienda,
            "Warehouse": warehouse,
            "MeasurementUnit": "UN",
            "Type": 1,
            "InfiniteStock": infinite_stock
        })

    # 🔁 Enviar en chunks (ej: hasta 500 SKUs por petición)
    chunk_size = 500
    for i in range(0, len(full_payload), chunk_size):
        chunk = full_payload[i:i + chunk_size]
        print(f"⬆️ Enviando chunk de {len(chunk)} SKUs (desde el {i} al {i + len(chunk) - 1})")
        response = requests.post(url, headers=headers, data=json.dumps(chunk))
        print(f"📦 Status: {response.status_code} | Response: {response.text}")

    conn.close()
    engine.dispose()
    return


default_args = {
    "owner": "ecommerce_ops",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_stock_materiales_elab_propia',
    default_args=default_args,
    description="Stock 999 a las 8AM y stock 0 a las 16PM para materiales de elaboración propia (612745-UN y 665186-UN)",
    schedule_interval="0 8,16 * * *",
    start_date=pendulum.datetime(2026, 8, 13, tz="America/Santiago"),
    catchup=False,
    tags=["Janis", "stock", "elab_propia", "ecommdata", "MAURICIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Actualización programada de stock para materiales específicos:
    - 000000000000612745-UN
    - 000000000000665186-UN
    
    A las 8:00 AM se establece stock 999 (disponible).
    A las 16:00 PM se establece stock 0 (no disponible).
    """

    t_branch = BranchPythonOperator(
        task_id="choose_branch",
        python_callable=_choose_branch
    )

    t_999 = PythonOperator(
        task_id="send_stock_999",
        python_callable=_send_stock,
        op_kwargs={"quantity": 999, "infinite_stock": False}
    )
    
    t_0 = PythonOperator(
        task_id="send_stock_0",
        python_callable=_send_stock,
        op_kwargs={"quantity": 0, "infinite_stock": False}
    )

    t_branch >> [t_999, t_0]
