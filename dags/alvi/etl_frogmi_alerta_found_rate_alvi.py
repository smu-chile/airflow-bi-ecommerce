from airflow import DAG
from airflow import macros
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from utils.slack_utils import dag_failure_slack, dag_success_slack

from datetime import datetime

import pendulum

def _load_json_to_s3(ts, ds):
    import requests
    import json
    import pandas as pd
    from io import StringIO
    import boto3 

    base_url = Variable.get("FROGMI_API_URL")
    url = f"{base_url}/api/v3/tasks_management/results?filters[period][from]={ds}&filters[period][to]={macros.ds_add(ds, 1)}&filters[activity][]=a6dbc4bd-64e6-4628-bb6b-66902cba3a7e&per_page=500&include=events"

    exec_datetime = datetime.strptime(ts[:16], "%Y-%m-%dT%H:%M")
    exec_datetime_utc = pendulum.timezone("utc").convert(exec_datetime)
    local_tz = pendulum.timezone("America/Santiago")
    exec_datetime_local = local_tz.convert(exec_datetime_utc)
    exec_datetime_local_str = exec_datetime_local.strftime("%Y-%m-%dT%H:%M")
    print(exec_datetime_local_str)

    if exec_datetime_local_str.split("T")[1] == "18:00":
        url = f"{base_url}/api/v3/tasks_management/results?filters[period][from]={macros.ds_add(ds, 1)}&filters[period][to]={macros.ds_add(ds, 2)}&filters[activity][]=a6dbc4bd-64e6-4628-bb6b-66902cba3a7e&per_page=500&include=events"
    print(url)
    api_key = Variable.get("FROGMI_API_TOKEN_SECRET")

    payload={}
    headers = {
    'Authorization': f'Bearer {api_key}',
    'X-Company-UUID': Variable.get("FROGMI_COMPANY_UUID_SECRET"),
    'Content-Type': 'application/vnd.api+json'
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    res = json.loads(response.text)
    lista_lineas = []

    for linea in res['data']:
        respuesta_0 = None
        respuesta_1 = None
        respuesta_2 = None
        respuesta_3 = None
        id = linea['id']
        realizado = linea['attributes']['done']
        fecha_inicio = linea['attributes']['start_date']
        fecha_fin = linea['attributes']['end_date']
        descripcion = linea['attributes']['external_data'][0]['main_text']
        material = linea['attributes']['external_data'][0]['second_text'][8:]
        tienda_frogmi = linea['relationships']['stores']['data']['id']
        for i in res['included']:
            if i['relationships']['task_action_events']['data']['id'] == id:
                pregunta = i['attributes']['name']
                id_respuesta = i['attributes']['answer']
                for j in i['attributes']['alternatives']['data']:
                    if id_respuesta == j['id'] and pregunta == "Producto se encuentra en la góndola?":
                        respuesta_0 = bool(j['attributes']['value'])
                    if id_respuesta == j['id'] and pregunta == "Hay stock para reponer?":
                        respuesta_1 = bool(j['attributes']['value'])
                    if id_respuesta == j['id'] and pregunta == "Hay stock en sistema?":
                        respuesta_2 = bool(j['attributes']['value'])
                    if id_respuesta == j['id'] and pregunta == "Se pudo reponer?":
                        respuesta_3 = bool(j['attributes']['value'])
        lista_lineas.append([id,realizado,fecha_inicio,fecha_fin,descripcion,material,tienda_frogmi,respuesta_0,respuesta_1,respuesta_2,respuesta_3])
    df = pd.DataFrame(lista_lineas, columns =['id','realizado','fecha_inicio','fecha_fin','descripcion','material','tienda_frogmi','gondola','stock_para_reponer','stock_en_sistema','repuesto'])
    
    df = df.astype({
        "id": "string",
        "realizado": "bool",
        "fecha_inicio": "string",
        "fecha_fin": "string",
        "descripcion": "string",
        "material": "string",
        "tienda_frogmi": "string",
        "gondola": "bool",
        "stock_para_reponer": "bool",
        "stock_en_sistema": "bool",
        "repuesto": "bool"
    }, errors="ignore")
    
    curr_datetime = ts[:16].replace("-", "/").replace("T", "/").replace(":", "")
    file_name = f"frogmi/alerta_found_rate/{curr_datetime}_alerta_found_rate.csv"
    buffer = StringIO()

    df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    buffer.seek(0)

    access_key = Variable.get("AWS_ACCESS_KEY")
    secret_key = Variable.get("AWS_SECRET_KEY")
    bucket_name = Variable.get("AWS_S3_BUCKET_NAME")
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name = "us-east-1"
    )
    response = s3_client.put_object(
        Bucket=bucket_name, Key=file_name, Body=buffer.getvalue()
    )

    return file_name

def _get_table_alerta_found_rate_from_S3(ti):
    import pandas as pd

    alerta_found_rate_file = ti.xcom_pull(key="return_value", task_ids=["load_json_to_s3"])[0]
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+alerta_found_rate_file)
    if not s3_hook.check_for_key(alerta_found_rate_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % alerta_found_rate_file)

    alerta_found_rate_object = s3_hook.get_key(alerta_found_rate_file, bucket_name=s3_bucket)

    df = pd.read_csv(alerta_found_rate_object.get()["Body"])
    print(f"Number of records found: {len(df.index)}")

    df = df.astype({
        "id": "string",
        "realizado": "bool",
        "fecha_inicio": "string",
        "fecha_fin": "string",
        "descripcion": "string",
        "material": "string",
        "tienda_frogmi": "string",
        "gondola": "bool",
        "stock_para_reponer": "bool",
        "stock_en_sistema": "bool",
        "repuesto": "bool"
    }, errors="ignore")

    return df

def _save_table_alerta_found_rate(ts, ti, ds):
    import pandas as pd
    import sqlalchemy

    df = _get_table_alerta_found_rate_from_S3(ti)
    df = df[['id','realizado','fecha_inicio','fecha_fin','descripcion','material','tienda_frogmi','gondola','stock_para_reponer','stock_en_sistema','repuesto']]

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")

    exec_date = ds

    exec_datetime = datetime.strptime(ts[:16], "%Y-%m-%dT%H:%M")
    exec_datetime_utc = pendulum.timezone("utc").convert(exec_datetime)
    local_tz = pendulum.timezone("America/Santiago")
    exec_datetime_local = local_tz.convert(exec_datetime_utc)
    exec_datetime_local_str = exec_datetime_local.strftime("%Y-%m-%dT%H:%M")
    print(exec_datetime_local_str)

    if exec_datetime_local_str.split("T")[1] == "18:00":
        exec_date = macros.ds_add(ds, 1)
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)
    with engine.begin() as conn:
        conn.execute(f"""
            DELETE FROM ecommdata_alvi.frogmi_alerta_found_rate
            WHERE fecha_inicio::date = '{exec_date}'
        """)
        df.to_sql(name="frogmi_alerta_found_rate",
                con=engine,         
                schema="ecommdata_alvi",         
                if_exists='append',         
                index=False,         
                chunksize=20000,         
                method='multi')
        conn.execute(f"""
            UPDATE ecommdata_alvi.frogmi_alerta_found_rate
            SET id_tienda = t.id
            FROM ecommdata_alvi.tiendas t
            WHERE fecha_inicio::date >= '{exec_date}' and tienda_frogmi = t.id_frogmi
        """)
        conn.execute(f"""
            DELETE FROM ecommdata_alvi.frogmi_alerta_found_rate
            WHERE id_tienda is NULL
        """)
        conn.close

    return


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'etl_frogmi_alerta_found_rate_alvi',
    default_args=default_args,
    description="Extracción y carga de tabla alerta frogmi Alvi desde API.",
    schedule_interval="0 12,15 * * *",
    start_date=pendulum.datetime(2022, 10, 12, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["frogmi", "found_rate", "alvi", "MATIAS"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla alerta frogmi desde API.
    """ 

    t0 = PythonOperator(
        task_id = "load_json_to_s3",
        python_callable = _load_json_to_s3
    )

    t1 = PythonOperator(
        task_id = "save_table_alerta_found_rate",
        python_callable = _save_table_alerta_found_rate
    )

    t2 = TriggerDagRunOperator(
        task_id="trigger_alerta_fr_encargado",
        trigger_dag_id="proc_frogmi_post_alerta_foundrate_encargado_ecommerce",
        wait_for_completion=False
    )


t0 >> t1 >> t2
