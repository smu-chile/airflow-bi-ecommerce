from airflow import DAG
from airflow import macros
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pendulum

def _load_pdv_wp_tables(ds):
    import pandas as pd
    import io
    wp_query = """
        SELECT * FROM ecommdata.cruce_wp_pdv cwp
            WHERE cwp.id_vtex is null
        """
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(wp_query)
    results = cursor.fetchall()
    cursor.close()
    pg_connection.close()
    df_wp = pd.DataFrame(results)

    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"carga_promociones/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    #Save to S3
    buffer = io.StringIO()
    df_wp.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"carga_promociones/{exec_date}/carga_promociones{date_aux}.csv"
    buffer.seek(0)
    print("se logro transformar el dataframe a un archivo .csv")
    print(f"con fecha {ds} y nombre de filename como {filename}")
    s3_hook.load_string(buffer.getvalue(),
                key=filename,
                bucket_name=s3_bucket,
                replace=True,
                encrypt=False)
    print(f"File load on S3: {prefix}")

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_promociones_workflow_load_unimarc',
    default_args=default_args,
    description="Extracción y carga de promociones a VTEX Unimarc.",
    schedule_interval="30 8 * * *",
    start_date=pendulum.datetime(2023, 8, 18, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "ecommdata", "promotions","VTEX", "unimarc"],
) as dag:

    dag.doc_md = """
    calculo distancia y tiempo ordenes dia anterior
    """ 
    t0 = PythonOperator(
        task_id = "_load_pdv_wp_tables",
        python_callable = _load_pdv_wp_tables,
    )

    t0