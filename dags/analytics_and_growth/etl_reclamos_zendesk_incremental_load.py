from airflow import DAG
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime, timedelta

def no_nan_join(list):
    no_nan_list = [element for element in list if element != ""]
    return "|".join(no_nan_list)

def _load_reclamos_zendesk(ts):
    import io
    import numpy as np
    import pandas as pd

    exec_date = datetime.strptime(ts[:10], "%Y-%m-%d") + timedelta(days=1)
    exec_date = exec_date.strftime("%Y/%m/%d")
    prefix = f"zendesk/manual/reclamos/{exec_date}/"
    file_name = prefix+"reclamos.xlsx"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+file_name)
    if not s3_hook.check_for_key(file_name, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % file_name)
    
    reclamos_object = s3_hook.get_key(file_name, bucket_name=s3_bucket)
    df = pd.read_excel(io.BytesIO(reclamos_object.get()["Body"]), encoding="utf-8")

    # tipo 1
    df["tipo1"] = df[[col for col in df.columns if col.startswith("tipo1_")]].fillna("").agg(no_nan_join, axis=1)
    df["tipo2"] = df[[col for col in df.columns if col.startswith("tipo2_")]].fillna("").agg(no_nan_join, axis=1)
    df["tipo3"] = df[[col for col in df.columns if col.startswith("tipo3_")]].fillna("").agg(no_nan_join, axis=1)

    df = df.drop(columns=[col for col in df.columns if col.startswith("tipo1_")])
    df = df.drop(columns=[col for col in df.columns if col.startswith("tipo2_")])
    df = df.drop(columns=[col for col in df.columns if col.startswith("tipo3_")])
    df = df.drop(columns=["no_pescar"])

    df = df.rename(columns={"ticket_id": "id_ticket"})
    print(df.columns)

    column_types = {
        "id_ticket": "int", 
        "fecha_actualizacion": "string", 
        "fecha_creacion": "string", 
        "fecha_cierre": "string",
        "motivo": "string", 
        "via_devolucion": "string", 
        "motivo_devolucion": "string", 
        "estado_devolucion": "string",
        "fecha_devolucion": "string", 
        "tienda": "string", 
        "gestion": "string", 
        "canal": "string", 
        "id_reclamo_sernac": "string",
        "numero_pedido": "int", 
        "numero_boleta": "int", 
        "id_caso_janis": "int", 
        "monto_devolucion": "int",
        "tipo1": "string", 
        "tipo2": "string", 
        "tipo3": "string"
    }

    df = df.astype(column_types, errors="ignore")

    # Drop duplicates
    df_full = df.drop_duplicates()

    columns = [
        "fecha_actualizacion",
        "fecha_creacion", 
        "fecha_cierre",
        "motivo", 
        "via_devolucion", 
        "motivo_devolucion", 
        "estado_devolucion",
        "fecha_devolucion", 
        "tienda", 
        "gestion", 
        "canal", 
        "id_reclamo_sernac",
        "numero_pedido", 
        "numero_boleta", 
        "id_caso_janis", 
        "monto_devolucion",
        "tipo1", 
        "tipo2", 
        "tipo3"
    ]

    columns_query = ",".join(columns)
    excluded_query = ",".join(["EXCLUDED."+column for column in columns])
    values_query = "%s,"+",".join(["%s" for column in columns])
    df = df.fillna("NULL")
    records = list(df.to_records(index=False))
    
    # Change data types to native python types
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
        INSERT INTO analytics_and_growth.reclamos_zendesk (id_ticket,"""+columns_query+""") 
        VALUES ("""+values_query+""")
        ON CONFLICT (id_ticket)
        DO UPDATE SET ("""+columns_query+""") = ("""+excluded_query+""") 
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

    print("Data saved to PostgreSQL. Table: analytics_and_growth.reclamos_zendesk")

    return

default_args = {
    "owner": "analytics_and_growth",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_reclamos_zendesk_incremental_load',
    default_args=default_args,
    description="Carga de datos de reclamos zendesk desde bucket de S3 al workspace de Postgresql.",
    schedule_interval="0 12 * * *",
    start_date=datetime(2022, 5, 1),
    catchup=True,
    max_active_runs = 1,
    tags=["DATA", "Zendesk", "analytics_and_growth", "reclamos_zendesk"],
) as dag:

    dag.doc_md = """
    Extracción de archivos csv de reclamos zendesk desde bucket de S3, transformación y carga de datos en tabla analytics_and_growth.reclamos_zendesk. \n
    Un sensor espera por 3 horas la presencia de un archivo bandera que indique que la carga de los csv de datos está completa.
    """ 
    t0 = S3KeySensor(
        task_id = "wait_for_reclamos_zendesk_flag_file",
        bucket_key = "zendesk/manual/reclamos/{{(execution_date + macros.timedelta(days=1)).strftime('%Y/%m/%d')}}/flag.txt",
        bucket_name = Variable.get("AWS_S3_BUCKET_NAME"),
        aws_conn_id = "aws_s3_connection",
        timeout = 60*60*3
    )

    t1 = PythonOperator(
        task_id = "load_reclamos_zendesk",
        python_callable = _load_reclamos_zendesk
    )

    t0 >> t1 
