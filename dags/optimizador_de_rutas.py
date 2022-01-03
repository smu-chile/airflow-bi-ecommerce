from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from utils.routes.Etapa_1_Get import janis_query
from utils.routes.Etapa_2_Optimizador import report_generator
from utils.routes.Etapa_3_Post import inyeccion

from datetime import datetime


default_args = {
    "owner": "capacity",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'optimizador_de_rutas',
    default_args=default_args,
    description="Generación y optimización de rutas.",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["OPS", "Janis", "S3"],
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla de price de Janis.
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
            "aws_bucket_name": Variable.get("AWS_S3_BUCKET_NAME")
        }
    )

    t1 = PythonOperator(
        task_id = "report_generator",
        python_callable = report_generator,
        op_kwargs = { 
            "aws_access_key": Variable.get("AWS_ACCESS_KEY"), 
            "aws_secret_key": Variable.get("AWS_SECRET_KEY"), 
            "aws_bucket_name": Variable.get("AWS_S3_BUCKET_NAME")
        }
    )

    t2 = PythonOperator(
        task_id = "inyeccion",
        python_callable = inyeccion,
        op_kwargs = {
            "janis_api_secret": Variable.get("JANIS_API_SECRET"), 
            "janis_api_client": Variable.get("JANIS_CLIENT"), 
            "janis_api_key": Variable.get("JANIS_API_KEY"), 
            "aws_access_key": Variable.get("AWS_ACCESS_KEY"), 
            "aws_secret_key": Variable.get("AWS_SECRET_KEY"), 
            "aws_bucket_name": Variable.get("AWS_S3_BUCKET_NAME")
        }
    )

    t0 >> t1 >> t2
