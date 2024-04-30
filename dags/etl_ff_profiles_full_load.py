from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook

from utils.janis_utils import load_full_table_to_s3

from datetime import datetime

def _generate_ff_profiles_table(ti):
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text

    ff_profiles_file_name = ti.xcom_pull(key="return_value", task_ids=["load_full_table_to_s3"])[0]
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    if not s3_hook.check_for_key(ff_profiles_file_name, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % ff_profiles_file_name)
    
    ff_profiles_s3_object = s3_hook.get_key(ff_profiles_file_name, bucket_name=s3_bucket)
    df = pd.read_csv(ff_profiles_s3_object.get()["Body"])

    print("Number of records found::")
    print(len(df.index))

    df = df[[
        "id",
        "name",
        "code",
        "date_created",
        "date_modified",
        "user_created",
        "user_modified",
        "status"
    ]]

    df = df.rename(columns={
        "name": "nombre",
        "code": "codigo",
        "date_created": "fecha_creacion",
        "date_modified": "fecha_modificacion",
        "user_created": "creado_por",
        "user_modified": "modificado_por",
        "status": "estado"
    })

    # Cast date to local tz
    df["fecha_creacion"] = pd.to_datetime(df["fecha_creacion"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["fecha_modificacion"] = pd.to_datetime(df["fecha_modificacion"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")

    # Column data types
    df = df.astype({
        "nombre": "string",
        "codigo": "string",
        "fecha_creacion": "string",
        "fecha_modificacion": "string",
        "creado_por": "int",
        "modificado_por": "int",
        "estado": "int"
    })

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    connection = engine.connect()
    truncate_query = "TRUNCATE TABLE ecommdata.ff_perfiles"
    connection.execute(text(truncate_query))
    connection.close()

    # Save to PostgreSQL:
    df.to_sql(name="ff_perfiles",
                con=engine,         
                schema="ecommdata",         
                if_exists='append',         
                index=False,         
                chunksize=20000,         
                method='multi')

    print("Data saved to PostgreSQL.")

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0
}
with DAG(
    'etl_ff_perfiles_full_load',
    default_args=default_args,
    description="Extracción y carga de tabla ff_perfiles desde Janis Replica hasta Workspace.",
    schedule_interval="0 7 * * *",
    start_date=datetime(2022, 4, 1),
    catchup=False,
    tags=["DATA", "Janis", "ecommdata", "ff_perfiles", "MATIAS"],
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla ff_perfiles desde Janis Replica hasta Workspace.
    """ 
    t0 = PythonOperator(
        task_id = "load_full_table_to_s3",
        python_callable = load_full_table_to_s3,
        op_kwargs = {"table_name": "ff_profiles"}
    )

    t1 = PythonOperator(
        task_id = "load_calendar_table_to_postgres",
        python_callable = _generate_ff_profiles_table
    )

    t0 >> t1
