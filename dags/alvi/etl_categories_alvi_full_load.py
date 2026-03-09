from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator as PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from utils.janis_alvi_utils import load_full_table_to_s3
from utils.slack_utils import dag_success_slack, dag_failure_slack

from datetime import datetime

import pendulum

def process_categories_table(ti):
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text
    
    file_name = ti.xcom_pull(key="return_value", task_ids=["load_full_table_to_s3"])[0]
    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    if  not s3_hook.check_for_key(file_name, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % file_name)
    
    s3_object = s3_hook.get_key(file_name, bucket_name=s3_bucket)
    df0 = pd.read_csv(s3_object.get()["Body"])

    df = df0[["id", "ref_id", "name", "ref_parent", "status"]]

    df1 = df[
            (df["ref_id"].notnull()) & (df["ref_id"] != 0) &
            (df["ref_parent"].isnull() | (df["ref_parent"] == 0))
            ].rename(
                columns={
                    "id":"id1",
                    "ref_id":"ref_id1",
                    "name":"name1",
                    "ref_parent":"ref_parent1",
                    "status": "status1"
                }
            )
    df2 = pd.merge(df1, df[df["ref_parent"].notnull()], left_on="ref_id1", right_on="ref_parent", how="inner").rename(columns={"id":"id2", "ref_id":"ref_id2", "name":"name2", "ref_parent":"ref_parent2", "status": "status2"})
    

    df = df2.append(df1)

    print("Total records: ")
    print(len(df.index))

    df["id"] = np.select(
        [
            df["id2"].notnull()
        ],
        [
            df["id2"]
        ],
        default=df["id1"]
    )

    df["status_code"] = np.select(
        [
            df["id2"].notnull()
        ],
        [
            df["status2"]
        ],
        default=df["status1"]
    )

    df["ref_id"] = np.select(
        [
            df["id2"].notnull()
        ],
        [
            df["ref_id2"]
        ],
        default=df["ref_id1"]
    )

    df["status"] = np.where(df["status_code"].isin([0, 8]), "inactivo", "activo")
    df = df[["id", "ref_id", "name1", "name2", "status"]]
    df = df.rename(columns={"name1": "n1", "name2": "n2"})

    columns = ["ref_id", "n1", "n2", "status"]

    columns_query = ",".join(columns)
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
        INSERT INTO ecommdata_alvi.categorias (id,"""+columns_query+""") 
        VALUES ("""+values_query+""")
        ON CONFLICT (id)
        DO NOTHING; 
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

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_categories_alvi_full_load',
    default_args=default_args,
    description="Extracción, transformación y carga de tabla categories desde Janis Alvi A Replica hasta Workspace.",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "Janis_Alvi", "S3", "Alvi", "ecommdata_alvi", "MATIAS"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla de categories de Janis Alvi.
    """ 
    
    t0 = PythonOperator(
        task_id = "load_full_table_to_s3",
        python_callable = load_full_table_to_s3,
        op_kwargs = {"table_name": "categories"}
    )

    t1 = PostgresOperator(
        task_id = "truncate_table",
        conn_id="postgresql_conn",
        sql="""
        truncate ecommdata_alvi.categorias
        """,
    )

    t2 = PythonOperator(
        task_id = "process_categories_table",
        python_callable = process_categories_table
    )

    t0 >> t1 >> t2
    