import os
from io import StringIO
import pandas as pd
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from google.oauth2 import service_account
from google.cloud import bigquery

BASE_S3_PATH = "data_warehouse/"

def load_custom_bq_query_to_s3(ts, query, query_name, aws_conn_id="aws_s3_connection", extra_prefix=None, base_path=None):
    """
    Saca la data desde BigQuery, la manda a un CSV en memoria
    y la sube a S3 con la misma estructura de carpetas/archivos
    que Netezza Utils.
    """
    # ---------- 1) Armado de nombre ----------
    print("Execution datetime: " + ts)
    curr_datetime = ts[:16].replace("-", "/").replace("T", "/").replace(":", "")
    
    if base_path:
        # Guardar en base_path/YYYY/MM/DD/query_name.csv
        exec_date = ts[:10].replace("-", "/")  # solo fecha, sin hora
        file_name = f"{base_path}{exec_date}/{query_name}.csv"
    else:
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
    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
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

def bigquery_full_table_load_to_s3(ts, table_name, where=None, date_query=None, aws_conn_id="aws_s3_connection", extra_prefix=None, base_path=None):
    print("Execution datetime: " + ts)
    curr_datetime = ts[:16].replace("-", "/").replace("T", "/").replace(":", "")
    
    if base_path:
        prefix = f"{base_path}{table_name}/{curr_datetime}_"
    else:
        prefix = BASE_S3_PATH+table_name+"/"+curr_datetime+"_"
    if extra_prefix is not None:
        prefix = prefix+extra_prefix+"_"
    file_name = prefix+table_name+".csv"

    if base_path:
        # Guardar en base_path/YYYY/MM/DD/query_name.csv
        exec_date = ts[:10].replace("-", "/")  # solo fecha, sin hora
        file_name = f"{base_path}{table_name}/{curr_datetime}.csv"
    else:
        prefix = f"{BASE_S3_PATH}{table_name}/{curr_datetime}_"
        if extra_prefix:
            prefix += f"{extra_prefix}_"
        file_name = f"{prefix}{table_name}.csv"

    print("File to be created: "+file_name)

    sql_str = f"SELECT * FROM {table_name}"
    if where is not None:
        sql_str = sql_str + " WHERE " + where
    if date_query is not None:
        date_query = date_query % ts[:10]
        sql_str = sql_str + " AND " + date_query

    print(sql_str)

    # ---------- Credenciales BQ ----------
    sa_info = Variable.get("BIGQUERY_CREDENTIALS", deserialize_json=True)
    creds = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client = bigquery.Client(
        project=sa_info["project_id"],
        credentials=creds,
    )

    # ---------- Ejecutar Query y traer a pandas ----------
    job = client.query(sql_str)
    df = job.to_dataframe()

    print("Columnas:")
    print(list(df.columns))

    buffer = StringIO()

    print("Number of records:")
    print(len(df.index))
    df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    buffer.seek(0)

    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    s3_hook.load_string(buffer.getvalue(),
                  key=file_name,
                  bucket_name=s3_bucket,
                  replace=True,
                  encrypt=False)

    return file_name

def bq_query_to_df(query, query_parameters=None, location="US"):
    """
    Ejecuta una consulta en BigQuery y retorna un DataFrame de pandas.
    
    Args:
        query (str): SQL a ejecutar (puede incluir parámetros @name).
        query_parameters (list|None): Lista de parámetros de BigQuery, p.ej.:
            [
              bigquery.ScalarQueryParameter("ds", "DATE", date(2025,8,20)),
              bigquery.ArrayQueryParameter("tiendas", "STRING", ["0089","0469"])
            ]
          Si no necesitas parámetros, deja None.
        location (str): Región del job de BQ (default "US").
    Returns:
        pandas.DataFrame df
    """
    print(f"Query to be executed: {query}")

    # Conexión/credenciales como en render_netezza_view
    sa_info = Variable.get("BIGQUERY_CREDENTIALS", deserialize_json=True)
    creds = service_account.Credentials.from_service_account_info(
        sa_info, scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    client = bigquery.Client(project=sa_info["project_id"], credentials=creds, location=location)

    job_config = None
    if query_parameters:
        job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)

    job = client.query(query, job_config=job_config)
    df = job.to_dataframe()

    print(df.head(20))
    return df