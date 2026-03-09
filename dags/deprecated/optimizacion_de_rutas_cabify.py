from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from utils.cabify.Etapa_1_Get import janis_query
from utils.cabify.Etapa_2_Optimizador import report_generator
from utils.cabify.Etapa_3_Post import inyeccion

from datetime import datetime

def _check_stage_one(ti):
    stage_one_status = ti.xcom_pull(key="return_value", task_ids=["janis_query"])[0]
    if not stage_one_status:
        return False
    return True

def _check_stage_two(ti):
    stage_two_status = ti.xcom_pull(key="return_value", task_ids=["report_generator"])[0]
    if not stage_two_status:
        return False
    return True

def _check_stage_three(ti):
    stage_three_status = ti.xcom_pull(key="return_value", task_ids=["inyeccion"])[0]
    if not stage_three_status:
        raise Exception("Error en etapa 3.")
    return True

default_args = {
    "owner": "capacity",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'optimizador_de_rutas_cabify',
    default_args=default_args,
    description="Generación y optimización de rutas.",
    schedule="0 12,15,18,21 * * *", # 8, 11, 14, 17 (-4)
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["OPS", "Janis", "S3", "Cabify"],
) as dag:

    dag.doc_md = """
    Proceso de rutas Cabify
    """ 
    t0 = PythonOperator(
        task_id = "janis_query",
        python_callable = janis_query,
        op_kwargs = {
            "janis_api_secret": Variable.get("JANIS_API_SECRET"), 
            "janis_api_client": Variable.get("JANIS_CLIENT"), 
            "janis_api_key": Variable.get("JANIS_API_KEY"), 
            "aws_access_key": Variable.get("AWS_ACCESS_KEY"), 
            "aws_secret_key": Variable.get("AWS_SECRET_KEY"), 
            "aws_bucket_name": Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
        }
    )

    t1 = ShortCircuitOperator(
        task_id = "check_stage_one",
        python_callable = _check_stage_one
    )

    t2 = PythonOperator(
        task_id = "report_generator",
        python_callable = report_generator,
        op_kwargs = { 
            "aws_access_key": Variable.get("AWS_ACCESS_KEY"), 
            "aws_secret_key": Variable.get("AWS_SECRET_KEY"), 
            "aws_bucket_name": Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
        }
    )

    t3 = ShortCircuitOperator(
        task_id = "check_stage_two",
        python_callable = _check_stage_two
    )

    t4 = PythonOperator(
        task_id = "inyeccion",
        python_callable = inyeccion,
        op_kwargs = {
            "janis_api_secret": Variable.get("JANIS_API_SECRET"), 
            "janis_api_client": Variable.get("JANIS_CLIENT"), 
            "janis_api_key": Variable.get("JANIS_API_KEY"), 
            "aws_access_key": Variable.get("AWS_ACCESS_KEY"), 
            "aws_secret_key": Variable.get("AWS_SECRET_KEY"), 
            "aws_bucket_name": Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket'),
            "mongo_user": Variable.get("MONGODB_USER"), 
            "mongo_pass": Variable.get("MONGODB_PASSWORD"), 
            "cluster_name": Variable.get("MONGODB_CLUSTER"), 
            "db": "ecommerceOpsDB"
        }
    )

    t5 = ShortCircuitOperator(
        task_id = "check_stage_three",
        python_callable = _check_stage_three
    )

    t0 >> t1 >> t2 >> t3 >> t4 >> t5
