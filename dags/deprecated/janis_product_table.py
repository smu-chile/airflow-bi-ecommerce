from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.janis_utils import load_full_table_to_s3

from datetime import datetime

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'janis_product_full_table_load',
    default_args=default_args,
    description="Extracción y carga de tabla product desde Janis Replica.",
    schedule="0 7 * * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["DATA", "Janis"],
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla de products de Janis.
    """ 
    t0 = PythonOperator(
        task_id = "load_full_table_to_s3",
        python_callable = load_full_table_to_s3,
        op_kwargs = {"table_name": "products"}
    )
