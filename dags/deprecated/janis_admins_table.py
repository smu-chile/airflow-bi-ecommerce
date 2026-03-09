from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from utils.janis_utils import load_full_table_to_s3

from datetime import datetime
from io import StringIO

import boto3
import botocore
import mysql.connector
import pandas as pd
import psycopg2
import sqlalchemy

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'janis_admins_full_table_load',
    default_args=default_args,
    description="Extracción y carga de tabla admins desde Janis Replica.",
    schedule="0 3 * * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["DATA", "Janis", "S3"],
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla de admins de Janis.
    """ 
    t0 = PythonOperator(
        task_id = "load_full_table_to_s3",
        python_callable = load_full_table_to_s3,
        op_kwargs = {"table_name": "admins"}
    )
