from sqlalchemy.engine import create_engine
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook

from utils.calendar import delta_yearweeks
from utils.bigquery_utils import bigquery_full_table_load_to_s3

from datetime import datetime, timedelta
import pendulum

def _generate_calendar_table(ti):
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text

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

    df = df_dw_date.drop(["DATE_KEY", "CALENDAR_YEAR_MONTH_KEY", "CALENDAR_YEAR_WEEK_KEY", "CALENDAR_DAY_OF_QUARTER"], axis=1)
    df = df.rename(columns={
        	"DATE_VALUE": "fecha",
	        "CALENDAR_DAY_OF_MONTH": "dia_mes",
	        "CALENDAR_DAY_OF_YEAR": "dia_ano",
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
            "CALENDAR_YEAR": "ano",
            "WEEK_NUMBER": "semana_numerico"
    })

    df["fecha"] = pd.to_datetime(df["fecha"], format="%Y-%m-%d")
    df["semana_ano_texto"] = df["ano"].astype("string") + "W" + df["semana_numerico"].astype("string")
    df["mes_ano_texto"] = df["ano"].astype("string") + " " + df["mes_abreviado"]

    years53 = df[(df["semana_numerico"] == 53) & (df["mes_numerico"] == 12)]["ano"].unique().tolist()
    print("Years with 53 weeks calendar: ")
    print(years53)
    base_row = df[df["fecha"] == datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)]

    base_year = base_row.iloc[0]["ano"]
    base_month = base_row.iloc[0]["mes_numerico"]
    base_week = base_row.iloc[0]["semana_numerico"]
    base_day = base_row.iloc[0]["dia_ano"]
    base_semester = base_row.iloc[0]["semestre_numerico"]
    base_quarter = base_row.iloc[0]["trimestre_numerico"]

    df["temp_semana_numerico"] = np.where((df["semana_numerico"].isin([52, 53])) & (df["mes_numerico"] == 1), 0, df["semana_numerico"])
    df["ano_relativo"] = df["ano"] - base_year
    df["mes_relativo"] = df["ano_relativo"]*12 + (df["mes_numerico"] - base_month)
    df["semana_relativa"] = df["ano"].apply(delta_yearweeks, args=[base_year, years53]) + (df["temp_semana_numerico"] - base_week)
    df["dia_relativo"] = df["ano_relativo"]*365 + df["dia_ano"] - base_day
    df["semestre_relativo"] = df["ano_relativo"]*2 + df["semestre_numerico"] - base_semester
    df["trimestre_relativo"] = df["ano_relativo"]*4 + df["trimestre_numerico"] - base_quarter

    df = df.drop(columns=["temp_semana_numerico"])

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    connection = engine.connect()
    truncate_query = "TRUNCATE TABLE ecommdata.calendario"
    connection.execute(text(truncate_query))
    connection.close()

    # Save to PostgreSQL:
    df.to_sql(name="calendario",
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
    'calendar_table_etl',
    default_args=default_args,
    description="Bigquery vm_dim_date full table load to S3 and transformation-load to Postgres",
    schedule_interval="30 7 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "DW", "S3", "MATIAS"],
) as dag:

    dag.doc_md = """
    Netezza VW_DIM_DATE full table load and relative dates calculation.
    """ 
    t0 = PythonOperator(
        task_id = "netezza_vm_dim_date_full_load",
        python_callable = bigquery_full_table_load_to_s3,
        op_kwargs = {"table_name": "`cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_DATE`"},
        retries = 3,
        retry_delay = timedelta(minutes=5)
    )

    t1 = PythonOperator(
        task_id = "load_calendar_table_to_postgres",
        python_callable = _generate_calendar_table
    )

    t0 >> t1
