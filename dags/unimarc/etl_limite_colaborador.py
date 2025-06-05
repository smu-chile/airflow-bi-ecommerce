from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
import pendulum

# 📌 Función principal: Extrae datos de PostgreSQL y los inserta en la tabla 'excluidos_colaborador'
def get_users_for_limit():
    import pandas as pd
    import os
    import psycopg2
    import requests
    import time

    def actualizar_xConvenio(document_id, xConvenio_value, max_retries=3, delay=10):
        API_URL = "https://unimarc.vtexcommercestable.com.br/api/dataentities/CL/search"
        API_KEY = Variable.get("X_VTEX_API_AppKey")
        API_TOKEN = Variable.get("X_VTEX_API_AppToken")

        HEADERS = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-VTEX-API-AppKey": API_KEY,
            "X-VTEX-API-AppToken": API_TOKEN,
            "Connection": "keep-alive"
        }

        query_params = {
            "userId": document_id,
            "_fields": "xConvenio,userId"
        }

        response_get = requests.get(API_URL, headers=HEADERS, params=query_params)
        if response_get.status_code != 200 or not response_get.json():
            return False

        current_value = response_get.json()[0].get("xConvenio", "").strip().lower()
        if current_value == "baja":
            return False

        update_url = "https://unimarc.vtexcommercestable.com.br/api/dataentities/CL/documents"
        update_payload = {
            "userId": document_id,
            "xConvenio": xConvenio_value
        }

        for attempt in range(max_retries):
            response = requests.patch(update_url, json=update_payload, headers=HEADERS)
            if response.status_code in [200, 304]:
                return True
            time.sleep(delay)
        return False

    query_path = os.path.join(os.getcwd(), "dags/unimarc/sql/limite_colaboradores.sql")
    with open(query_path, "r") as query_file:
        limite_colaboradores_query = query_file.read()

    conn = psycopg2.connect(
        host=Variable.get("POSTGRESQL_HOST"),
        database=Variable.get("POSTGRESQL_DB"),
        user=Variable.get("POSTGRESQL_USER"),
        password=Variable.get("POSTGRESQL_PASSWORD"),
        port="5432"
    )

    df_limite = pd.read_sql_query(limite_colaboradores_query, conn)

    if df_limite.empty:
        print("⚠️ No hay datos.")
        conn.close()
        return

    aux_list = []
    xConvenio_value = "sobre500"
    for _, row in df_limite.iterrows():
        if actualizar_xConvenio(row["user_profile_id"], xConvenio_value):
            aux_list.append(row)

    df_success = pd.DataFrame(aux_list)

    with conn.cursor() as cur:
        insert_query = """
            INSERT INTO ecommdata.excluidos_colaborador 
            (user_profile_id, email, nombre, apellido, descuento_colaborador, descuento_referido) 
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_profile_id) DO NOTHING;
        """
        values = [tuple(row) for row in df_success.itertuples(index=False, name=None)]
        cur.executemany(insert_query, values)

    conn.commit()
    conn.close()
    print(f"✅ {len(df_success)} filas insertadas.")

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    'etl_limite_colaborador',
    default_args=default_args,
    description="Limite de colaboradores y referidos.",
    schedule_interval="0 4 * * *",
    start_date=pendulum.datetime(2025, 3, 31, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["VTEX", "xConvenio", "colaborador", "KEVIN"]
) as dag:

    t0 = PythonOperator(
        task_id="get_users_for_limit",
        python_callable=get_users_for_limit
    )
    t0
