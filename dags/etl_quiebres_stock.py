from airflow import DAG
from airflow import macros
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator


from datetime import datetime, timedelta
import pendulum


def funcion():
    from datetime import datetime, timedelta
    import pandas as pd

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    hoy = datetime.now().date() - timedelta(days=15)
    dias_a_restar = 5 #cambiar por 45
    fecha_inicial = hoy - timedelta(days=dias_a_restar)

    lista = []
    for i in range((hoy - fecha_inicial).days + 1):
        fecha_actual = fecha_inicial + timedelta(days=i)
        fecha_formateada = fecha_actual.strftime("%Y/%d/%m")
        aux = f"{fecha_formateada}/1100publicacion_catalogo_diario.csv"
        lista.append(aux)

    print(lista)

    dataframes_list = []

    for aux in lista:
        s3_filename = f"/{aux}"

        print("Loading file:", s3_filename)
        if not s3_hook.check_for_key(aux, bucket_name=s3_bucket):
            print(f"WARNING: File {s3_filename} not found.")
            continue

        s3_object = s3_hook.get_key(aux, bucket_name=s3_bucket)
        df = pd.read_csv(s3_object.get()["Body"])
        dataframes_list.append(df)

    result_df = pd.concat(dataframes_list, ignore_index=True)
    print("Final DataFrame:")
    print(result_df.info())
    return



default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_quiebre_stock',
    default_args=default_args,
    description="Carga de datos de quiebres stock 60 dias S3.",
    schedule_interval="0 5 1/15 * *",
    start_date=pendulum.datetime(2022, 8, 25, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["DATA", "ecommdata", "S3"],
) as dag:

    dag.doc_md = """
        Quiebres de stock a 60 dias desde el historico de publicacion catálogo
    """ 

    t1 = PythonOperator(
        task_id = "funcion1",
        python_callable = funcion
    )
    t1 
