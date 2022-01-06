from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook

from utils.netezza_utils import netezza_full_table_load_to_s3

from datetime import datetime, timedelta

def _generate_calendar_table(ti):
    import pandas as pd
    import sqlalchemy

    dw_date_file_name = ti.xcom_pull(key="return_value", task_ids=["netezza_vm_dim_date_full_load"])[0]
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    if not s3_hook.check_for_key(dw_date_file_name, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % dw_date_file_name)
    
    dw_date_s3_object = s3_hook.get_key(dw_date_file_name, bucket_name=s3_bucket)
    df_dw_date = pd.read_csv(dw_date_s3_object.get()["Body"])

    print("Dates DW:")
    print(len(df_dw_date.index))

    print(df_dw_date.columns)
    print(df_dw_date.head(1))

    df = df_dw_date.drop(["DATE_KEY", "CALENDAR_YEAR_MONTH_KEY", "CALENDAR_YEAR_WEEK_KEY"], axis=1)
    df = df.rename(columns={
        	"DATE_VALUE": "fecha",
	        "CALENDAR_DAY_OF_MONTH": "dia_mes",
	        "CALENDAR_DAY_OF_QUARTER": "dia_trimestre",
	        "CALENDAR_DAY_OF_YEAR": "dia_anio",
            "WEEKDAY_NUMBER": "dia_semana_numerico",
            "WEEKDAY_NAME_ABBREVIATED": "dia_semana_abreviado",
            "WEEKDAY_NAME": "dia_semana_texto",
            "CALENDAR_MONTH_NUMBER": "mes_numerico",
            "CALENDAR_MONTH_NAME": "mes_texto",
            "CALENDAR_MONTH_ABBREVIATION": "mes_abreviado",
            "QUARTER": "trimestre_numerico",
            "QUARTER_TXT": "trimestre_texto",
            "SEMESTER": "semestre_numerico",
            "SEMESTER_TXT": "semestre_texto",
            "CALENDAR_YEAR": "anio",
            "WEEK_NUMBER": "semana_numerico"
    })

    df["fecha"] = pd.to_datetime(df["fecha"], format="%Y-%m-%d")

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = "postgresql+psycopg2://"+username+":"+password+"@"+host+":5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    # Save to PostgreSQL:
    df.to_sql(name="calendario",
                con=engine,         
                schema="ecommdata",         
                if_exists='replace',         
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
    'calendar_table_etl',
    default_args=default_args,
    description="Netezza vm_dim_date full table load to S3 and transformation-load to Postgres",
    schedule_interval="0 7 1 * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["DATA", "DW", "S3"],
) as dag:

    dag.doc_md = """
    Netezza VW_DIM_DATE full table load.
    Monthly process.
    """ 
    t0 = PythonOperator(
        task_id = "netezza_vm_dim_date_full_load",
        python_callable = netezza_full_table_load_to_s3,
        op_kwargs = {"table_name": "DWC_SMU.SMU.VW_DIM_DATE"},
        retries = 3,
        retry_delay = timedelta(minutes=5)
    )

    t1 = PythonOperator(
        task_id = "load_calendar_table_to_postgres",
        python_callable = _generate_calendar_table
    )

    t0 >> t1
