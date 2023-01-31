from airflow import DAG
from airflow.models import Variable
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime, timedelta
import pendulum

def _liquidacion_semanal():
    import pandas as pd
    import numpy as np
    import io

    # df_liquidaciones = pd.read_excel('liquidacion.xlsx', skiprows=1)

    #directorio s3: /meli/liquidaciones/liquidaciones.xlsx

    file_name = "meli/liquidaciones/liquidacionallin1.xlsx"
    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME')
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+file_name)
    if not s3_hook.check_for_key(file_name, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % file_name)
    
    liquidaciones_object = s3_hook.get_key(file_name, bucket_name = s3_bucket)

    df_liquidaciones = pd.read_excel(io.BytesIO(liquidaciones_object["Body"].read()))

    columns = ['fecha, tipo_documento','folio','descripcion','cantidad','orden','monto','iva','sku','codigo_del_producto',
    'variacion','folio_asociado','devolucion','venta']

    df_liquidaciones = df_liquidaciones.rename(columns={'tipo documento':'tipo_documento', 'descripción':'descripcion',
    'código del producto':'codigo_del_producto', 'variación':'variacion', 'folio asociado':'folio_asociado',
    'devolución':'devolucion'})

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
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.executemany(incremental_query, fixed_records)
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    print("Data loaded to Postgres")

default_args = {
    "owner": "capacity_and_planning",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'etl_liquidaciones_semanales_MELI',
    default_args=default_args,
    description="Automatización de obtención de liquidaciones semanales MELI",
    schedule_interval="0 21 * * 0",
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

t0