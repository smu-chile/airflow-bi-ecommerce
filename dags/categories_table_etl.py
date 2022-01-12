from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook

from utils.janis_utils import load_full_table_to_s3

from datetime import datetime

import numpy as np
import pandas as pd
import sqlalchemy
from sqlalchemy import text

def process_categories_table(ti):
    file_name = ti.xcom_pull(key="return_value", task_ids=["load_full_table_to_s3"])[0]
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    if  not s3_hook.check_for_key(file_name, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % file_name)
    
    s3_object = s3_hook.get_key(file_name, bucket_name=s3_bucket)
    df0 = pd.read_csv(s3_object.get()["Body"])

    df = df0[["id", "ref_id", "name", "ref_parent", "status"]]
    # -----

    df1 = df[df["ref_parent"].isnull()].rename(columns={"id":"id1", "ref_id":"ref_id1", "name":"name1", "ref_parent":"ref_parent1", "status": "status1"})
    df2 = pd.merge(df1, df[df["ref_parent"].notnull()], left_on="ref_id1", right_on="ref_parent", how="inner").rename(columns={"id":"id2", "ref_id":"ref_id2", "name":"name2", "ref_parent":"ref_parent2", "status": "status2"})
    df3 = pd.merge(df2, df[df["ref_parent"].notnull()], left_on="ref_id2", right_on="ref_parent", how="inner").rename(columns={"id":"id3", "ref_id":"ref_id3", "name":"name3", "ref_parent":"ref_parent3", "status": "status3"})

    df = df3.append(df2).append(df1)

    print("Total records: ")
    print(len(df.index))

    df["id"] = np.select(
        [
            df["id3"].notnull(),
            df["id2"].notnull()
        ],
        [
            df["id3"],
            df["id2"]
        ],
        default=df["id1"]
    )

    df["status_code"] = np.select(
        [
            df["id3"].notnull(),
            df["id2"].notnull()
        ],
        [
            df["status3"],
            df["status2"]
        ],
        default=df["status1"]
    )

    df["status"] = np.where(df["status_code"].isin([0, 8]), "inactivo", "activo")
    df = df[["id", "name1", "name2", "name3", "status"]]
    df = df.rename(columns={"name1": "n1", "name2": "n2", "name3": "n3"})

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = "postgresql+psycopg2://"+username+":"+password+"@"+host+":5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    connection = engine.connect()
    truncate_query = "TRUNCATE TABLE ecommdata.categorias"
    connection.execute(text(truncate_query))
    connection.close()

    # Save to PostgreSQL:
    df.to_sql(name="categorias",
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
    "retries": 0,
}
with DAG(
    'categories_table_etl',
    default_args=default_args,
    description="Extracción, transformación y carga de tabla categories desde Janis Replica hasta Workspace.",
    schedule_interval="0 3 * * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["DATA", "Janis", "S3"],
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla de categories de Janis.
    """ 
    t0 = PythonOperator(
        task_id = "load_full_table_to_s3",
        python_callable = load_full_table_to_s3,
        op_kwargs = {"table_name": "categories"}
    )

    t1 = PythonOperator(
        task_id = "process_categories_table",
        python_callable = process_categories_table
    )

    t0 >> t1
