from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.netezza_utils import netezza_full_table_load_to_s3

from datetime import datetime, timedelta

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5)
}
with DAG(
    'Netezza_vm_dim_date full load',
    default_args=default_args,
    description="Netezza vm_dim_date full table load",
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
        op_kwargs = {"table_name": "DWC_SMU.SMU.VW_DIM_DATE"}
    )

    t0
