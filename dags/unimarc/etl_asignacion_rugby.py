from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
import pendulum

# 📌 Función auxiliar: Actualizar xConvenio en VTEX con reintentos
def actualizar_xConvenio(document_id, xConvenio_value, max_retries=3, delay=10):
    import requests
    import time

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

    # 1. Obtener valor actual de xConvenio filtrando por userId
    query_params = {
        "document": document_id,
        "_fields": "xConvenio,userId"
    }

    print(f"🔹 Revisando - userId: {document_id}")

    response_get = requests.get(API_URL, headers=HEADERS, params=query_params)

    if response_get.status_code != 200:
        print(f"⚠️ No se pudo obtener xConvenio para {document_id}. Status: {response_get.status_code}")
        return False

    data = response_get.json()

    if not data:
        print(f"⚠️ No se encontró ningún documento con documentId = {document_id}.")
        return False

    current_value = data[0].get("xConvenio")
    current_value = current_value.strip().lower() if current_value else ""

    user_id = data[0].get("userId")

    if current_value == "rugby2024":
        print(f"⛔ Usuario {document_id} tiene xConvenio='Rugby2024'. No se actualiza.")
        return False

    # 2. PATCH si pasó validación
    update_url = "https://unimarc.vtexcommercestable.com.br/api/dataentities/CL/documents"
    update_payload = {
        "userId": user_id,
        "xConvenio": xConvenio_value
    }

    for attempt in range(max_retries):
        response = requests.patch(update_url, json=update_payload, headers=HEADERS)

        if response.status_code == 200:
            print(f"✅ xConvenio actualizado para {document_id} con valor '{xConvenio_value}' (intento {attempt + 1})")
            return True
        elif response.status_code == 304:
            print(f"ℹ️ xConvenio para {document_id} ya estaba con el valor '{xConvenio_value}'.")
            return True
        else:
            print(f"⚠️ Error PATCH en {document_id} intento {attempt + 1}: {response.status_code}")
            time.sleep(delay)

    print(f"❌ Fallo definitivo en la actualización de {document_id} tras {max_retries} intentos.")
    return False

# 📌 Función principal: Extrae datos de PostgreSQL y los inserta en la tabla 'excluidos_colaborador'
def get_users_for_limit():
    import pandas as pd
    import os
    import psycopg2

    """
    Extrae datos desde PostgreSQL y actualiza su xConvenio en VTEX si corresponde.
    """
    query_path = os.path.join(os.getcwd(), "dags/unimarc/sql/colaboradores_rugby.sql")

    with open(query_path, "r") as query_file:
        rugby_query = query_file.read()

    print("Base query:")
    print(rugby_query)

    # 📌 Conectar a PostgreSQL
    conn = psycopg2.connect(
        host=Variable.get("POSTGRESQL_HOST"),
        database=Variable.get("POSTGRESQL_DB"),
        user=Variable.get("POSTGRESQL_USER"),
        password=Variable.get("POSTGRESQL_PASSWORD"),
        port="5432"
    )

    df_limite = pd.read_sql_query(rugby_query, conn)
    conn.close()

    if df_limite.empty:
        print("⚠️ No hay datos para actualizar en 'Rugby 2024'.")
        return

    xConvenio_value = "Rugby2024"
    for _, row in df_limite.iterrows():
        if actualizar_xConvenio(row["user_profile_id"], xConvenio_value):
            print(f"🔹 Usuario actualizado: {row['user_profile_id']}")
    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'etl_asignacion_rugby_semanal',
    default_args=default_args,
    description="Asignar xConvenio a usuarios con convenio Rugby.",
    schedule_interval="0 7 * * 1", # Ejecuta cada lunes a las 07:00 AM
    start_date=pendulum.datetime(2025, 10, 31, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["VTEX", "xConvenio", "Master Data", "Rugby", "FRANCISCO"]
) as dag:
    
    dag.doc_md = f"""
    Proceso semanal para asignar xConvenio='Rugby2024' a usuarios con convenio Rugby.
    """

    t0 = PythonOperator(
        task_id="get_users_for_limit",
        python_callable=get_users_for_limit,
    )

    t0 