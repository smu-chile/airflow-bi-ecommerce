from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator as PostgresOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable

from utils.slack_utils import dag_success_slack, dag_failure_slack

import pendulum

from datetime import datetime, timedelta

def query_to_df(query):
    import pandas as pd
    print(query)
    pg_hook = PostgresHook(conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query)
    column_names = [desc[0] for desc in cursor.description]
    results = cursor.fetchall()
    results = pd.DataFrame(results, columns=column_names)
    print(results.head(20))
    cursor.close()
    pg_connection.close()
    return results

def maestra_slotting_to_s3(ds):
    import io
    from io import StringIO
    import pandas as pd
    exec_date = ds.replace("-", "/")
    date_aux_filename = ds.replace("-","_")
    prefix = f"maestra_slotting/{exec_date}/"
    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    query = "select * from ecommdata.maestra_informacion_slotting_mfc;"

    df = query_to_df(query)

    df["fecha"] = ds
    df.info()

    buffer = io.StringIO()
    df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"maestra_slotting/{exec_date}/maestra_slotting_{date_aux_filename}.csv"
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
    'etl_maestra_informacion_slotting_mfc',
    default_args=default_args,
    description="carga tabla maestra info slotting mfc",
    schedule= "0 14 * * 3",
    start_date=pendulum.datetime(2024, 7, 3, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "postgres", "ecommdata", "MFC","slotting", "S3","venta_regular", "PATRICIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:
    

    dag.doc_md = """
    Actualiza tabla todos los dias de maestra slotting en conjunto con la venta regular.
    """ 

    t0 = PostgresOperator(
        task_id = "venta_regular",
        conn_id="postgresql_conn",
        sql="sql/venta_regular.sql",
    )
    t1 = PostgresOperator(
        task_id = "maestra_sloting",
        conn_id="postgresql_conn",
        sql="sql/maestra_slotting.sql",
    )
    t2 = PythonOperator(
        task_id = "maestra_slotting_to_s3",
        python_callable = maestra_slotting_to_s3,
    )
    t0 >> t1 >> t2
