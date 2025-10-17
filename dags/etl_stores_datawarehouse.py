from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from utils.bigquery_utils import bigquery_full_table_load_to_s3

from datetime import datetime, timedelta
import pendulum

def write_s3_file(ti):
    dw_stores_file_name = ti.xcom_pull(key="return_value", task_ids=["netezza_vm_dim_store_full_load_to_s3"])[0]
    dw_hierarchy_file_name = ti.xcom_pull(key="return_value" ,task_ids=["netezza_vm_dim_store_hierarchy_full_load_to_s3"])[0]
    s3_string = f"{dw_stores_file_name},{dw_hierarchy_file_name}"
    prefix = "data_warehouse/flags/"
    filename = "etl_stores_datawarehouse_raw_load.txt"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
    s3_hook.load_string(str(s3_string),prefix + filename,bucket_name=s3_bucket,replace=True)
    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'etl_stores_datawarehouse_raw_load',
    default_args=default_args,
    description="Extraction of raw data from data warehouse.",
    schedule_interval="15 7 * * *",
    start_date=pendulum.datetime(2022, 5, 1, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "DW", "S3", "Tiendas", "MATIAS"],
) as dag:

    dag.doc_md = """
    Extract store data from Datawarehouse and triggers both unimarc and alvi store dags.
    """ 
    t0 = PythonOperator(
        task_id = "netezza_vm_dim_store_full_load_to_s3",
        python_callable = bigquery_full_table_load_to_s3,
        op_kwargs = {"table_name": "`cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_STORE`"},
        retries = 2,
        retry_delay = timedelta(minutes=1)
    )

    t1 = PythonOperator(
        task_id = "netezza_vm_dim_store_hierarchy_full_load_to_s3",
        python_callable = bigquery_full_table_load_to_s3,
        op_kwargs = {"table_name": "`cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_STORE_HIERARCHY`"},
        retries = 2,
        retry_delay = timedelta(minutes=1)
    )

    td = PythonOperator(
        task_id = "Write_s3_file",
        python_callable = write_s3_file
    )

    t2 = TriggerDagRunOperator(
        task_id="trigger_stores_unimarc",
        trigger_dag_id="etl_tiendas_unimarc_incremental_load",
        wait_for_completion=False
    )

    t3 = TriggerDagRunOperator(
        task_id="trigger_stores_alvi",
        trigger_dag_id="etl_tiendas_alvi_incremental_load",
        wait_for_completion=False
    )

    [t0, t1] >> td >> [t2, t3]
    