from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

from utils.netezza_utils import netezza_full_table_load_to_s3

from datetime import datetime, timedelta

def _get_store_list():
    query = "SELECT id FROM ecommdata.tiendas"
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    print(results)
    cursor.close()
    pg_connection.close()
    return results

def _get_ou_key_list(ti):
    import pandas as pd
    store_ids = ti.xcom_pull(key="return_value", task_ids=["get_store_id_list_from_workspace"])[0]
    store_ids = [store_id[0] for store_id in store_ids]

    curr_datetime = datetime.utcnow()
    prefix = "/data_warehouse/DWC_SMU.SMU.VW_DIM_STORE/"+curr_datetime.strftime("%Y/%m/%d/")
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    store_object_list = s3_hook.list_keys(bucket_name=s3_bucket, prefix=prefix)
    store_object_key = store_object_list[0]
    store_object = s3_hook.get_key(store_object_key, bucket_name=s3_bucket)
    df_stores = pd.read_csv(store_object.get()["Body"])

    df_stores = df_stores[df_stores["STORE_ID"].isin(store_ids)]
    ou_key_list = df_stores["OU_KEY"].to_list()
    Variable.set(key="ou_key_list", value=ou_key_list)

    return ou_key_list

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
        task_id = "get_store_id_list_from_workspace",
        python_callable = _get_store_list
    )

    t1 = PythonOperator(
        task_id = "get_ou_key_list_from_datawarehouse",
        python_callable = _get_ou_key_list
    )

    ou_key_list = Variable.get(key="ou_key_list")

    with TaskGroup("ou_key_list_tasks") as dynamic_task_group:
        for ou_key in ou_key_list:
            dummy_ou_task = DummyOperator(
                task_id = "dummy_task_"+ou_key
            )
    
    end_task = DummyOperator(
        task_id = "end_task",
        trigger_rule='none_failed'
    )

    # t1 = PythonOperator(
    #     task_id = "netezza_vm_fact_ou_logt_smy_full_load",
    #     python_callable = netezza_full_table_load_to_s3,
    #     op_kwargs = {"table_name": "DWC_SMU.SMU.VW_FACT_OU_LOGT_SMY",
    #                  "where": "date_value = DATE(NOW() - interval '1 days')"
    #     },
    #     retries = 2,
    #     retry_delay = timedelta(minutes=1)
    # )

    # t2 = PythonOperator(
    #     task_id = "netezza_vm_dim_sku_attr_full_load",
    #     python_callable = netezza_full_table_load_to_s3,
    #     op_kwargs = {"table_name": "DWC_SMU.SMU.VW_DIM_SKU_ATTR"},
    #     retries = 2,
    #     retry_delay = timedelta(minutes=1)
    # )

    # t3 = PythonOperator(
    #     task_id = "save_transformed_store_table",
    #     python_callable = _create_final_costs_table
    # )

    # ou_key_list = Variable.delete(key="ou_key_list")

    t0 >> t1 >> dynamic_task_group >> end_task
