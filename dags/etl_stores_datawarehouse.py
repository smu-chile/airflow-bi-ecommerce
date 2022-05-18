from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from utils.netezza_utils import netezza_full_table_load_to_s3

from datetime import datetime, timedelta


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
    schedule_interval="0 7 * * *",
    start_date=datetime(2022, 5, 1),
    catchup=False,
    tags=["DATA", "DW", "S3", "Tiendas"],
) as dag:

    dag.doc_md = """
    Extract store data from Datawarehouse and triggers both unimarc and alvi store dags.
    """ 
    t0 = PythonOperator(
        task_id = "netezza_vm_dim_store_full_load_to_s3",
        python_callable = netezza_full_table_load_to_s3,
        op_kwargs = {"table_name": "DWC_SMU.SMU.VW_DIM_STORE"},
        retries = 2,
        retry_delay = timedelta(minutes=1)
    )

    t1 = PythonOperator(
        task_id = "netezza_vm_dim_store_hierarchy_full_load_to_s3",
        python_callable = netezza_full_table_load_to_s3,
        op_kwargs = {"table_name": "DWC_SMU.SMU.VW_DIM_STORE_HIERARCHY"},
        retries = 2,
        retry_delay = timedelta(minutes=1)
    )

    td = DummyOperator(
        task_id = "Dummy"
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
    