from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable

import pendulum

def query_to_df(query):
    import pandas as pd
    print(query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query)
    column_names = [desc[0] for desc in cursor.description]
    results = cursor.fetchall()
    results = pd.DataFrame(results, columns=column_names)
    print(results.head(20))
    cursor.close()
    pg_connection.close()
    return results

def get_and_send_top_productos():
    import pandas as pd
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError
    import requests
    import json
    import io
    import os

    interval = f'30 minutos'
    curr_working_directory = os.getcwd()
    print(os.getcwd())

    with open(curr_working_directory+f"/dags/unimarc/sql/alertas_sac_productos_no_pickeados.sql", "r") as query_file:
        query = query_file.read()
    
    print("Base query:")
    print(query)

    results = query_to_df(query)

    print(f"Number of records extracted: {len(results.index)}")

    if results.empty:
        client = WebClient(token=Variable.get("token_slack_bot"))
        print("No data returned from query")
        response = client.chat_postMessage(
                    channel="#alertas-foundrate",
                    text=f"<!channel> :uia No hay productos sustituidos en los últimos {interval} :uia:")
        print(response)
        return
    print(f"Resultados crudos desde SQL: {results.head(20)}")
    
    results=pd.DataFrame(results)
    results.columns = ["fecha_picking", "descripcion", "unidades_no_pickeadas", "pedidos_afectados"]
    print(results.head())

    results["unidades_no_pickeadas"] = results["unidades_no_pickeadas"].astype(float).round(2)

    df = pd.DataFrame(results, columns=["fecha_picking", "descripcion", "unidades_no_pickeadas", "pedidos_afectados"])

    # Crear archivo Excel en memoria
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Sheet1")
    buffer.seek(0)

    file_name = "top_productos.xlsx"
    file_size = buffer.getbuffer().nbytes
    token = Variable.get("token_slack_bot")
    channel_id = Variable.get("token_slack_channel_sac")
    upload_url_response = requests.post(
        "https://slack.com/api/files.getUploadURLExternal",
        data={
            "filename": file_name,
            "length": str(file_size),
            "token": token
        }
    ).json()

    upload_url = upload_url_response.get("upload_url")
    file_id = upload_url_response.get("file_id")

    if not upload_url:
        print("Error al obtener la URL de subida:", upload_url_response)
        return

    # Stage 2: Subir archivo usando buffer
    upload_response = requests.post(
        upload_url,
        data=buffer,
        headers={"Content-Type": "application/octet-stream"}
    )

    if upload_response.status_code != 200:
        print("Error al subir archivo:", upload_response.text)
        return

    # Stage 3: Completar la subida
    complete_payload = {
        "files": [{"id": file_id}],
        "channel_id": channel_id,
        "initial_comment": f"<!channel> ⚠️ 🚨 ⚠️ Aquí está el top de productos sustituidos en los últimos {interval} ⚠️ 🚨 ⚠️"
    }

    complete_response = requests.post(
        "https://slack.com/api/files.completeUploadExternal",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json;charset=utf-8"
        },
        data=json.dumps(complete_payload)
    ).json()

    if not complete_response.get("ok"):
        print("Error al completar la subida:", complete_response)
        return

    #Stage 4: Compartir públicamente (opcional)
    share_payload = {
        "channel": channel_id,
        "file": file_id,
        "initial_comment": "Archivo compartido 📢"
    }

    share_response = requests.post(
        "https://slack.com/api/files.sharedPublicURL",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json;charset=utf-8"
        },
        data=json.dumps(share_payload)
    ).json()

    if not share_response.get("ok"):
        print("Error al compartir públicamente:", share_response)
        return
    else:
        print("✅ Archivo enviado y compartido correctamente.")

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'etl_alertas_sac',
    default_args=default_args,
    description="Generación de alertas para SAC",
    schedule_interval="0,30 * * * *",
    start_date=pendulum.datetime(2025, 3, 30, tz="America/Santiago"),
    catchup=False,
    tags=["Alertas", "SAC", "Found Rate", "Coyhaique", "FRANCISCO"]
) as dag:
    
    dag.doc_md = """
    Alertas para SAC
    """ 

    # Single task to handle both fetching and sending data
    t0 = PythonOperator(
        task_id="get_and_send_top_productos",
        python_callable=get_and_send_top_productos,
    )

t0