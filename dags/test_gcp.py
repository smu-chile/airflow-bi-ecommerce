from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import pendulum

# ---------------------------
#  1) Función que hace la chamba
# ---------------------------
def run_query():
    # Google libs
    from google.oauth2 import service_account
    from google.cloud import bigquery
    import pandas as pd

    # 1) Credenciales guardadas en Variables (marca “is JSON” al crearlas)
    sa_info = Variable.get("BIGQUERY_CREDENTIALS", deserialize_json=True)

    # 2) Objeto Credentials con scope “cloud-platform”
    creds = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    # 3) Cliente nativo de BigQuery usando esas creds
    client = bigquery.Client(
        project=sa_info["project_id"],
        credentials=creds,
    )

    # 4) Tu query
    sql = """
        SELECT *
        FROM `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_CATEGORIA`;
    """
    for row in client.query(sql).result():
        # row es un Row object → conviene cast a dict para que se imprima bonito
        print(dict(row))
    print("----------------------------------------")
    df = client.query(sql).to_dataframe()
    print(df.head(15))  # Imprime las primeras 5 filas del DataFrame

# ---------------------------
#  2) Definición del DAG
# ---------------------------
default_args = {
    "owner": "ecommerce_data",
    "retries": 0,
    "email_on_failure": False,
}

with DAG(
    dag_id="test_gcp_var_creds",
    description="Test BigQuery con SA en Variable",
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["TEST", "Francisco"],
    default_args=default_args,
    schedule=None,   # sólo lo disparas manual
) as dag:

    t1 = PythonOperator(
        task_id="query_bq",
        python_callable=run_query,
    )

    t1