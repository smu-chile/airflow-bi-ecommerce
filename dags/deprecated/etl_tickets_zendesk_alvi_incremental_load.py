from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime, timedelta

def _load_tickets_zendesk(ts):
    import io
    import numpy as np
    import pandas as pd
    import zipfile

    exec_date = datetime.strptime(ts[:10], "%Y-%m-%d") + timedelta(days=1)
    exec_date = exec_date.strftime("%Y%m%d")
    prefix = f"zendesk/manual/tickets/{exec_date}_"
    zip_file_name = prefix+"tickets_ALVI.zip"
    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+zip_file_name)
    if not s3_hook.check_for_key(zip_file_name, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % zip_file_name)
    
    tickets_zip_object = s3_hook.get_key(zip_file_name, bucket_name=s3_bucket)
    tickets_dataframes = []
    with io.BytesIO(tickets_zip_object.get()["Body"].read()) as tf:

        # rewind the file
        tf.seek(0)

        # Read the file as a zipfile and process the members
        with zipfile.ZipFile(tf, mode='r') as zipf:
            for subfile in zipf.namelist():
                print(subfile)
                df_temp = pd.read_csv(io.BytesIO(zipf.read(subfile)), encoding="utf-8")
                tickets_dataframes.append(df_temp)

    df = pd.concat(tickets_dataframes)

    df = df.rename(columns={
        "ticket_id": "id_ticket",
        "estado_del_ticket": "estado",
        "closed_by_merge": "cerrado_por_merge"
    })
    print(df.columns)

    column_types = {
        "id_ticket": "int", 
        "estado": "string",
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
        "tipo3": "string",
        "total_dias_hasta_resolucion": "float",
        "cerrado_por_merge": "string"
    }

    df["numero_pedido"] = df["numero_pedido"].astype("str").fillna("").str.split(".").str[0]
    df["numero_pedido"] = np.where(df["numero_pedido"].str.isnumeric(), df["numero_pedido"], None)
    df = df.astype(column_types, errors="ignore")
    df["id_tienda"] = np.where(df["tienda"].fillna("").str[:4].str.isnumeric(), df["tienda"].str[:4],None)
    df["id_tienda"] = df["id_tienda"].str[:4]
    df["id_tienda"] = np.where(df["id_tienda"].str.isnumeric(), df["id_tienda"], None)
    
    columns = [
        "estado",
        "fecha_actualizacion",
        "fecha_creacion",
        "fecha_cierre",
        "motivo",
        "motivo_devolucion",
        "via_devolucion",
        "estado_devolucion",
        "fecha_devolucion",
        "tienda",
        "id_tienda",
        "gestion",
        "canal",
        "id_reclamo_sernac",
        "numero_pedido",
        "numero_boleta",
        "id_caso_janis",
        "monto_devolucion",
        "tipo1",
        "tipo2",
        "tipo3",
        "total_dias_hasta_resolucion",
        "cerrado_por_merge"
    ]

    df = df[["id_ticket"]+columns]
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
            elif value in ["NULL", "nan"]:
                fixed_record.append(None)
            else:
                fixed_record.append(value)
        fixed_records.append(tuple(fixed_record))
    print(f"Number of records to load: {str(len(fixed_records))}")
    incremental_query = """
        INSERT INTO analytics_and_growth.tickets_zendesk_alvi (id_ticket,"""+columns_query+""") 
        VALUES ("""+values_query+""")
        ON CONFLICT (id_ticket)
        DO UPDATE SET ("""+columns_query+""") = ("""+excluded_query+""") 
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

    print("Data saved to PostgreSQL. Table: analytics_and_growth.tickets_zendesk_alvi")

    return

default_args = {
    "owner": "analytics_and_growth",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_tickets_zendesk_alvi_incremental_load',
    default_args=default_args,
    description="Carga de datos de tickets zendesk Alvi desde bucket de S3 al workspace de Postgresql.",
    schedule="0 12 * * *",
    start_date=datetime(2022, 8, 8),
    catchup=False,
    max_active_runs = 1,
    tags=["DATA", "Zendesk", "analytics_and_growth", "tickets_zendesk", "alvi"],
) as dag:

    dag.doc_md = """
    Extracción de archivo zip que incluye N archivos xlsx de tickets zendesk desde bucket de S3, transformación y carga de datos en tabla analytics_and_growth.reclamos_zendesk. \n
    Un sensor espera por 3 horas la presencia de un archivo bandera que indique que la carga del zip de datos está completa.
    """ 
    t0 = S3KeySensor(
        task_id = "wait_for_tickets_zendesk_flag_file",
        bucket_key = "zendesk/manual/tickets/{{(execution_date + macros.timedelta(days=1)).strftime('%Y%m%d')}}_flag_ALVI.txt",
        bucket_name = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket'),
        aws_conn_id = "aws_s3_connection",
        timeout = 60*60*3
    )

    t1 = PythonOperator(
        task_id = "load_tickets_zendesk",
        python_callable = _load_tickets_zendesk
    )

    t0 >> t1 
