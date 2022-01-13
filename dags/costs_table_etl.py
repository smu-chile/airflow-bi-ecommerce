from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook

from utils.netezza_utils import netezza_full_table_load_to_s3

from datetime import datetime, timedelta

def _create_final_costs_table(ti):

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'costs_table_etl',
    default_args=default_args,
    description="Extraction and transformation of costs data.",
    schedule_interval="0 7 * * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["DATA", "DW", "S3", "Workspace", "Costos"],
) as dag:

    dag.doc_md = """
    Extract costs data from Datawarehouse to consolidate
    a single costs table on Postgres workspace.
    """ 
    t0 = PythonOperator(
        task_id = "netezza_vm_fact_ou_logt_smy_full_load",
        python_callable = netezza_full_table_load_to_s3,
        op_kwargs = {"table_name": "DWC_SMU.SMU.VW_FACT_OU_LOGT_SMY",
                     "where": "date_value = DATE(NOW() - interval '1 days')"
        },
        retries = 2,
        retry_delay = timedelta(minutes=1)
    )

    t1 = PythonOperator(
        task_id = "netezza_vm_dim_sku_attr_full_load",
        python_callable = netezza_full_table_load_to_s3,
        op_kwargs = {"table_name": "DWC_SMU.SMU.VW_DIM_SKU_ATTR"},
        retries = 2,
        retry_delay = timedelta(minutes=1)
    )

    t2 = PythonOperator(
        task_id = "save_transformed_store_table",
        python_callable = _create_final_costs_table
    )

    [t0, t1] >> t2
