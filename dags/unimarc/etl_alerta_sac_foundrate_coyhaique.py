from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.sensors.external_task import ExternalTaskSensor

import pendulum

from utils.postgres_utils import query_to_df
from utils.slack_utils import upload_df_as_excel, send_text_message,  dag_success_slack, dag_failure_slack

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
        send_text_message(
            channel_var_name="token_slack_channel_sac",
            text=f"<!channel> :uia: No hay productos sustituidos en los últimos {interval} :uia:",
        )
        return

    results.columns = ["fecha_picking", "descripcion", "unidades_no_pickeadas", "pedidos_afectados"]
    results["unidades_no_pickeadas"] = results["unidades_no_pickeadas"].astype(float).round(2)

    upload_df_as_excel(
        df=results,
        base_name="top_productos",
        channel_var_name="token_slack_channel_sac",
        initial_comment=f"<!channel> ⚠️ 🚨 ⚠️ Aquí está el top de productos sustituidos en los últimos {interval} ⚠️ 🚨 ⚠️",
    )
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
    'etl_alertas_sac_foundrate_coyhaique',
    default_args=default_args,
    description="Generación de alertas para SAC",
    schedule_interval="0,30 8-20 * * *",
    start_date=pendulum.datetime(2025, 3, 30, tz="America/Santiago"),
    catchup=False,
    tags=["Alertas", "SAC", "Found Rate", "Coyhaique", "FRANCISCO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:
    
    dag.doc_md = """
    Alertas para SAC, asociada al foundrate de productos para coyhaique.
    """ 

    t0 = ExternalTaskSensor(
        task_id="wait_for_found_rate_productos_unimarc",
        external_dag_id='etl_found_rate_productos_unimarc',
        external_task_id=None,
        allowed_states=['success'],
        failed_states=['failed']
    )

    t1 = PythonOperator(
        task_id="get_and_send_top_productos",
        python_callable=get_and_send_top_productos,
    )

    t0 >> t1