from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
import pendulum

# 📌 Función auxiliar: Actualizar xCluster en VTEX con reintentos
def actualizar_xCluster(document_id, xCluster_value, max_retries=3, delay=10):
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

    # 1. Obtener valor actual de xCluster filtrando por userId
    query_params = {
        "userId": document_id,
        "_fields": "xCluster,userId"
    }

    print(f"🔹 Revisando - userId: {document_id}")

    response_get = requests.get(API_URL, headers=HEADERS, params=query_params)

    if response_get.status_code != 200:
        print(f"⚠️ No se pudo obtener xCluster para {document_id}. Status: {response_get.status_code}")
        return False

    data = response_get.json()

    if not data:
        print(f"⚠️ No se encontró ningún documento con userId = {document_id}.")
        return False

    current_value = data[0].get("xCluster")
    current_value = current_value.strip().lower() if current_value else ""

    if current_value == "rugby2024":
        print(f"⛔ Usuario {document_id} tiene xCluster='Rugby2024'. No se actualiza.")
        return False

    # 2. PATCH si pasó validación
    update_url = "https://unimarc.vtexcommercestable.com.br/api/dataentities/CL/documents"
    update_payload = {
        "userId": document_id,
        "xCluster": xCluster_value
    }

    for attempt in range(max_retries):
        response = requests.patch(update_url, json=update_payload, headers=HEADERS)

        if response.status_code == 200:
            print(f"✅ xCluster actualizado para {document_id} con valor '{xCluster_value}' (intento {attempt + 1})")
            return True
        elif response.status_code == 304:
            print(f"ℹ️ xCluster para {document_id} ya estaba con el valor '{xCluster_value}'.")
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
    Extrae datos desde PostgreSQL y actualiza su xCluster en VTEX si corresponde.
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

    xCluster_value = "Rugby2024"
    for _, row in df_limite.iterrows():
        if actualizar_xCluster(row["user_profile_id"], xCluster_value):
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
    description="Asignar xCluster a usuarios con convenio Rugby.",
    schedule_interval="0 7 * * 1", # Ejecuta cada lunes a las 07:00 AM
    start_date=pendulum.datetime(2025, 10, 31, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["VTEX", "xCluster", "Master Data", "Rugby", "FRANCISCO"]
) as dag:
    
    dag.doc_md = f"""
    Proceso semanal para asignar xCluster='Rugby2024' a usuarios con convenio Rugby.
    """

    t0 = PythonOperator(
        task_id="get_users_for_limit",
        python_callable=get_users_for_limit,
    )

    t0 