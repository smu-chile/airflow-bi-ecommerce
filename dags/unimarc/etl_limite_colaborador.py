from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
import pendulum

# 📌 Función principal: Extrae datos de PostgreSQL y los inserta en la tabla 'excluidos_colaborador'
def get_users_for_limit():
    import pandas as pd
    import os
    import psycopg2

    """
    Extrae datos desde PostgreSQL y los inserta en la tabla 'excluidos_colaborador'.
    """
    query_path = os.path.join(os.getcwd(), "dags/unimarc/sql/limite_colaboradores.sql")

    with open(query_path, "r") as query_file:
        limite_colaboradores_query = query_file.read()

    print("Base query:")
    print(limite_colaboradores_query)

    # 📌 Conectar a PostgreSQL
    conn = psycopg2.connect(
        host=Variable.get("POSTGRESQL_HOST"),
        database=Variable.get("POSTGRESQL_DB"),
        user=Variable.get("POSTGRESQL_USER"),
        password=Variable.get("POSTGRESQL_PASSWORD"),
        port="5432"
    )

    df_limite = pd.read_sql_query(limite_colaboradores_query, conn)
    
    if df_limite.empty:
        print("⚠️ No hay datos para insertar en 'excluidos_colaborador'.")
        conn.close()
        return

    aux_list = []  
    xConvenio_value = "sobre500"
    for _, row in df_limite.iterrows():
        if actualizar_xConvenio(row["user_profile_id"], xConvenio_value):
            aux_list.append(row)

    df_success = pd.DataFrame(aux_list)

    print("🔹 Filas exitosas:")
    df_success.info()

    # 📌 Insertar datos en PostgreSQL
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
    print(f"✅ {len(df_success)} filas insertadas en 'excluidos_colaborador'.")
    
    conn.close()

    return


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
        "userId": document_id,
        "_fields": "xConvenio,userId"
    }

    print(f"🔹 Revisando - userId: {document_id}")

    response_get = requests.get(API_URL, headers=HEADERS, params=query_params)

    if response_get.status_code != 200:
        print(f"⚠️ No se pudo obtener xConvenio para {document_id}. Status: {response_get.status_code}")
        return False

    data = response_get.json()

    if not data:
        print(f"⚠️ No se encontró ningún documento con userId = {document_id}.")
        return False

    current_value = data[0].get("xConvenio")
    current_value = current_value.strip().lower() if current_value else ""

    if current_value == "baja":
        print(f"⛔ Usuario {document_id} tiene xConvenio='Baja'. No se actualiza.")
        return False
    if current_value == "":
        print(f"⚠️ xConvenio para {document_id} está vacío. No se actualiza.")
        return False

    # 2. PATCH si pasó validación
    update_url = "https://unimarc.vtexcommercestable.com.br/api/dataentities/CL/documents"
    update_payload = {
        "userId": document_id,
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


#📌 Función Mensual: Reasigna xConvenio
def reasign_process():
    import psycopg2
    import pandas as pd
    
    """
    Proceso mensual que recupera los datos de 'excluidos_colaborador' y reasigna xConvenio en VTEX.
    """
    print("🔹 Iniciando proceso de reasignación de xConvenio...")

    conn = psycopg2.connect(
        host=Variable.get("POSTGRESQL_HOST"),
        database=Variable.get("POSTGRESQL_DB"),
        user=Variable.get("POSTGRESQL_USER"),
        password=Variable.get("POSTGRESQL_PASSWORD"),
        port="5432"
    )

    query = """
        SELECT user_profile_id, descuento_colaborador, descuento_referido 
        FROM ecommdata.excluidos_colaborador;
    """

    df_users = pd.read_sql_query(query, conn)

    print(df_users.info())

    conn.close()

    if df_users.empty:
        print("⚠️ No hay usuarios para reasignar xConvenio este mes.")
        return

    print(f"🔹 Se encontraron {len(df_users)} usuarios para actualizar.")
    for _, row in df_users.iterrows():
        user_id = row["user_profile_id"]
        
        # Asigna el valor de xConvenio según las condiciones
        print(f"Procesando usuario {user_id} - descuento_colaborador: {row['descuento_colaborador']}, descuento_referido: {row['descuento_referido']}")
        if row["descuento_colaborador"] < 0:
            xConvenio_value = "Unimarc"
        elif row["descuento_referido"] < 0:
            xConvenio_value = "referido2023"
        else:
            continue  # Si no cumple ninguna condición, no se actualiza
        actualizar_xConvenio(user_id, xConvenio_value)
    
    # 📌 Truncar la tabla 'excluidos_colaborador' después de reasignar xConvenio
    conn = psycopg2.connect(
        host=Variable.get("POSTGRESQL_HOST"),
        database=Variable.get("POSTGRESQL_DB"),
        user=Variable.get("POSTGRESQL_USER"),
        password=Variable.get("POSTGRESQL_PASSWORD"),
        port="5432"
    )

    # Ejecutar el comando TRUNCATE para vaciar la tabla
    with conn.cursor() as cur:
        truncate_query = "TRUNCATE TABLE ecommdata.excluidos_colaborador;"
        cur.execute(truncate_query)
        conn.commit()
        print("✅ Tabla 'excluidos_colaborador' truncada exitosamente.")

    conn.close()
    print("✅ Proceso de reasignación de xConvenio finalizado.")
    return

# 📌 Configuración base
default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "retries": 0,
}


# 📌 DAG Diario
with DAG(
    'etl_limite_colaborador',
    default_args=default_args,
    description="Limite de colaboradores y referidos.",
    schedule_interval="0 4 * * *",  # 🔹 Se ejecuta todos los días a las 4 AM
    start_date=pendulum.datetime(2025, 3, 31, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["VTEX", "xConvenio","Master Data", "colaborador", "KEVIN"]
) as dag_diario:

    dag_diario.doc_md = "🔹 Limite de colaboradores y referidos."

    t0 = PythonOperator(
        task_id="get_users_for_limit",
        python_callable=get_users_for_limit
    )

    t0  # 🔹 Ejecuta diariamente


# 📌 DAG Mensual (Reasignation Task)
with DAG(
    'etl_reasignacion_colaborador',
    default_args=default_args,
    description="Proceso mensual de reasignacion de colaboradores excluidos.",
    schedule_interval="0 0 1 * *",  # 🔹 Se ejecuta el 1 de cada mes a las 00:00
    start_date=pendulum.datetime(2025, 4, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1, 
    tags=["VTEX","xConvenio","Master Data", "colaborador", "Mensual", "KEVIN"]
) as dag_mensual:

    dag_mensual.doc_md = "🔹 Proceso de reasignacion que se ejecuta una vez al mes."

    t0 = PythonOperator(
        task_id="reasign_process",
        python_callable=reasign_process
    )

    t0  # 🔹 Ejecuta mensualmente
