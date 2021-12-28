from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook

from utils.janis_utils import load_full_table_to_s3
from utils.netezza_utils import netezza_full_table_load_to_s3

from datetime import datetime, timedelta

def _create_final_store_table(ti):
    """
    Read S3 files with store tables from Janis and Datawarehouse.
    Join them with Pandas, give format and save result to Postgres. 
    """
    # Prefer local import at Task level for better DAG run time.
    import numpy as np
    import pandas as pd
    import sqlalchemy

    dw_file_name = ti.xcom_pull(key="return_value", task_ids=["netezza_vm_dim_store_full_load_to_s3"])[0]
    janis_file_name = ti.xcom_pull(key="return_value", task_ids=["janis_stores_full_load_to_s3"])[0]
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    if not s3_hook.check_for_key(dw_file_name, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % dw_file_name)
    if not s3_hook.check_for_key(janis_file_name, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % janis_file_name)
    
    dw_s3_object = s3_hook.get_key(dw_file_name, bucket_name=s3_bucket)
    df_dw = pd.read_csv(dw_s3_object.get()["Body"])
    
    janis_s3_object = s3_hook.get_key(janis_file_name, bucket_name=s3_bucket)
    df_j = pd.read_csv(janis_s3_object.get()["Body"])

    print("Stores DW:")
    print(len(df_dw.index))
    print(df_dw.columns)
    print(df_dw.iloc[0])

    print("Stores Janis:")
    print(len(df_j.index))
    print(df_j.columns)
    print(df_j.iloc[0])

    return

default_args = {
    "owner": "dw_test",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'stores_table_etl',
    default_args=default_args,
    description="Extraction and transformation of store data.",
    schedule_interval="0 7 * * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["DATA", "DW", "S3", "Janis", "Workspace", "Tiendas"],
) as dag:

    dag.doc_md = """
    Extract store data from Janis replica and Datawarehouse to consolidate
    a single store table on Postgres workspace.
    """ 
    t0 = PythonOperator(
        task_id = "netezza_vm_dim_store_full_load_to_s3",
        python_callable = netezza_full_table_load_to_s3,
        op_kwargs = {"table_name": "DWC_SMU.SMU.VW_DIM_STORE"},
        retries = 2,
        retries_delay = timedelta(minutes=1)
    )

    t1 = PythonOperator(
        task_id = "janis_stores_full_load_to_s3",
        python_callable = load_full_table_to_s3,
        op_kwargs = {"table_name": "wms_stores"}
    )

    t2 = PythonOperator(
        task_id = "save_transformed_store_table",
        python_callable = _create_final_store_table
    )

    [t0, t1] >> t2
