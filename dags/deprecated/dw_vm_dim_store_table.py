from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from utils.netezza_utils import netezza_full_table_load_to_s3

from datetime import datetime

default_args = {
    "owner": "dw_test",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'Netezza_vm_dim_store',
    default_args=default_args,
    description="Netezza vm_dim_store full table load",
    schedule="30 10 * * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["DATA", "DW", "S3"],
) as dag:

    dag.doc_md = """
    Netezza VW_DIM_STORE full table load.
    """ 
    t0 = PythonOperator(
        task_id = "netezza_vm_dim_store_full_load",
        python_callable = netezza_full_table_load_to_s3,
        op_kwargs = {"table_name": "DWC_SMU.SMU.VW_DIM_STORE"}
    )

    t0
