from airflow import DAG
from airflow.models import Variable
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime, timedelta
import pendulum

def _liquidacion_semanal():
    import pandas as pd
    import numpy as np
    import boto3
    import io

    file_name = "meli/liquidaciones/liquidacionsemana.xlsx"
    access_key = Variable.get("AWS_ACCESS_KEY")
    secret_key = Variable.get("AWS_SECRET_KEY")
    bucket_name = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name = "us-east-1"
    )
    obj = s3_client.get_object(Bucket = bucket_name, Key=file_name)
    data = obj['Body'].read()

    try:
        df_liquidaciones = pd.read_excel(io.BytesIO(data), skiprows=1, engine='openpyxl',usecols="A:O")
    except Exception as e:
        print(str(e))
        raise Exception("Deteniendo ejecución")

    columns = [
        'fecha', 
        'tipo_documento',
        'folio',
        'descripcion',
        'cantidad',
        'venta',
        'monto',
        'iva',
        'sku',
        'codigo_del_producto',
        'variacion',
        'folio_asociado',
        'devolucion',
        ]

    df_liquidaciones = df_liquidaciones.rename(columns={
        'tipo documento':'tipo_documento',
        'descripción':'descripcion',
        'código del producto':'codigo_del_producto',
        'variación':'variacion',
        'folio asociado':'folio_asociado',
        'devolución':'devolucion'}
        )

    df_liquidaciones = df_liquidaciones[columns]
    print (df_liquidaciones.dtypes)

    columns_query = ",".join(columns)
    values_query = ",".join(["%s" for column in columns])
    df_liquidaciones = df_liquidaciones.fillna("NULL")
    records = list(df_liquidaciones.to_records(index=False))

    fixed_records = []
    for record in records:
        fixed_record = []
        for value in record:
            if isinstance(value, np.generic):
                fixed_record.append(value.item())
            elif value == "NULL":
                fixed_record.append(None)
            else:
                fixed_record.append(value)
        fixed_records.append(tuple(fixed_record))
    print(f"Number of records to load: {str(len(fixed_records))}")
    incremental_query = """
        INSERT INTO ecommdata_meli.liquidacion ("""+columns_query+""") 
        VALUES ("""+values_query+""")
"""

    print(incremental_query)
    pg_hook = PostgresHook(conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.executemany(incremental_query, fixed_records)
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    print("Data loaded to Postgres")

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'etl_liquidaciones_semanales_MELI',
    default_args=default_args,
    description="Automatización de obtención de liquidaciones semanales MELI",
    schedule="0 21 * * 0",
    start_date=pendulum.datetime(2023, 1, 27, tz="America/Santiago"),
    catchup=False,
    tags=["MELI", "liquidaciones", "conciliacion","S3"],
) as dag:

    dag.doc_md = """
    Obtención de liquidaciones semanales en base a documento en S3.
    """ 

    t0 = PythonOperator(
        task_id = "liquidacion_semanal",
        python_callable = _liquidacion_semanal,
    )
