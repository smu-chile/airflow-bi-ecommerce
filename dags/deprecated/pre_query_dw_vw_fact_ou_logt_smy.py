from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.netezza_utils import render_netezza_view

from datetime import datetime, timedelta

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'pre_query_vm_fact_ou_logt_smy',
    default_args=default_args,
    description="Simple select query to start view's rendering process.",
    schedule="30 10 * * *",
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["DATA", "DW"],
) as dag:

    dag.doc_md = """
    Simple select query to start view's rendering process.
    """ 
    t0 = PythonOperator(
        task_id = "netezza_vm_fact_ou_logt_smy_pre_query",
        python_callable = render_netezza_view,
        op_kwargs = {"view_name": "DWC_SMU.SMU.VW_FACT_OU_LOGT_SMY"},
        execution_timeout = timedelta(minutes=30),
        retries = 2,
        retry_delay = timedelta(minutes=1)
    )
