import os
from io import StringIO
import pandas as pd
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from google.oauth2 import service_account
from google.cloud import bigquery

BASE_S3_PATH = "data_warehouse/"

def load_custom_bq_query_to_s3(ts, query, query_name, aws_conn_id="aws_s3_connection", extra_prefix=None):
    """
    Saca la data desde BigQuery, la manda a un CSV en memoria
    y la sube a S3 con la misma estructura de carpetas/archivos
    que Netezza Utils.
    """

    # ---------- 1) Armado de nombre ----------
    print("Execution datetime: " + ts)
    curr_datetime = ts[:16].replace("-", "/").replace("T", "/").replace(":", "")
    prefix = f"{BASE_S3_PATH}{query_name}/{curr_datetime}_"
    if extra_prefix:
        prefix += f"{extra_prefix}_"
    file_name = f"{prefix}{query_name}.csv"

    print("SQL Query:\n" + query)
    print("\nFile to be created: " + file_name)

    # ---------- 2) Credenciales ----------
    sa_info = Variable.get("BIGQUERY_CREDENTIALS", deserialize_json=True)
    creds = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client = bigquery.Client(
        project=sa_info["project_id"],
        credentials=creds,
    )

    # ---------- 3) Ejecutar Query ----------
    job = client.query(query)
    df = job.to_dataframe()  # trae todo a pandas

    print("Columnas:")
    print(list(df.columns))
    print("Number of records:")
    print(len(df.index))

    # ---------- 4) CSV en buffer ----------
    buffer = StringIO()
    df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    buffer.seek(0)

    # ---------- 5) Subida a S3 ----------
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    s3_hook.load_string(
        buffer.getvalue(),
        key=file_name,
        bucket_name=s3_bucket,
        replace=True,
        encrypt=False
    )

    print("✅ Archivo subido a S3:", file_name)
    return file_name