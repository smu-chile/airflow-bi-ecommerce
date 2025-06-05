from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
import pendulum

def reasign_process():
    import psycopg2
    import pandas as pd
    import requests
    import time

    def actualizar_xConvenio(document_id, xConvenio_value, max_retries=3, delay=10):
        API_URL = "https://unimarc.vtexcommercestable.com.br/api/dataentities/CL/documents"
        API_KEY = Variable.get("X_VTEX_API_AppKey")
        API_TOKEN = Variable.get("X_VTEX_API_AppToken")

        HEADERS = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-VTEX-API-AppKey": API_KEY,
            "X-VTEX-API-AppToken": API_TOKEN
        }

        for attempt in range(max_retries):
            response = requests.patch(API_URL, json={
                "userId": document_id,
                "xConvenio": xConvenio_value
            }, headers=HEADERS)
            if response.status_code in [200, 304]:
                return True
            time.sleep(delay)
        return False

    conn = psycopg2.connect(
        host=Variable.get("POSTGRESQL_HOST"),
        database=Variable.get("POSTGRESQL_DB"),
        user=Variable.get("POSTGRESQL_USER"),
        password=Variable.get("POSTGRESQL_PASSWORD"),
        port="5432"
    )

    df_users = pd.read_sql_query("""
        SELECT user_profile_id, descuento_colaborador, descuento_referido 
        FROM ecommdata.excluidos_colaborador;
    """, conn)

    if df_users.empty:
        print("⚠️ No hay usuarios para reasignar.")
        conn.close()
        return

    for _, row in df_users.iterrows():
        user_id = row["user_profile_id"]
        if row["descuento_colaborador"] < 0:
            xConvenio = "Unimarc"
        elif row["descuento_referido"] < 0:
            xConvenio = "referido2023"
        else:
            continue
        actualizar_xConvenio(user_id, xConvenio)

    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE ecommdata.excluidos_colaborador;")
        conn.commit()
    conn.close()

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    'etl_reasignacion_colaborador',
    default_args=default_args,
    description="Proceso mensual de reasignacion de colaboradores excluidos.",
    schedule_interval="0 0 1 * *",
    start_date=pendulum.datetime(2025, 4, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["VTEX", "xConvenio", "colaborador", "Mensual", "KEVIN"]
) as dag:

    t0 = PythonOperator(
        task_id="reasign_process",
        python_callable=reasign_process
    )
    t0
