from airflow import DAG
from airflow import macros
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from datetime import datetime

import pendulum 

def _load_json_to_s3(ts, ds):
    import requests
    import json
    import pandas as pd
    from io import StringIO
    import boto3

    

    base_url = Variable.get("FROGMI_API_URL")
    url = f"{base_url}/api/v3/tasks_management/results?filters[period][from]={ds}&filters[period][to]={macros.ds_add(ds, 1)}&filters[activity][]=00a40e62-9eb3-443c-bb12-7239d2f0547f&per_page=500&include=events,stores"
    
    exec_datetime = datetime.strptime(ts[:16], "%Y-%m-%dT%H:%M")
    exec_datetime_utc = pendulum.timezone("utc").convert(exec_datetime)
    local_tz = pendulum.timezone("America/Santiago")
    exec_datetime_local = local_tz.convert(exec_datetime_utc)
    exec_datetime_local_str = exec_datetime_local.strftime("%Y-%m-%dT%H:%M")
    print(exec_datetime_local_str)

    if exec_datetime_local_str.split("T")[1] == "18:30":
        url = f"{base_url}/api/v3/tasks_management/results?filters[period][from]={macros.ds_add(ds, 1)}&filters[period][to]={macros.ds_add(ds, 2)}&filters[activity][]=00a40e62-9eb3-443c-bb12-7239d2f0547f&per_page=500&include=events,stores"
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
        tienda = None
        id = linea['id']
        realizado = linea['attributes']['done']
        fecha_inicio = linea['attributes']['start_date']
        fecha_fin = linea['attributes']['end_date']
        try:
            descripcion = linea['attributes']['external_data'][0]['main_text']
        except:
            descripcion = 'vacio'
        try:
            stock = linea['attributes']['external_data'][0]['second_text']
        except:
            stock = 'vacio'
        try:
            material = linea['attributes']['external_id']
        except:
            material = 'vacio'
        tienda_frogmi = linea['relationships']['stores']['data']['id']
        for i in res['included']:
                if i['type'] == "task_action_result":
                    if i['relationships']['task_action_events']['data']['id'] == id:
                        pregunta = i['attributes']['name']
                        id_respuesta = i['attributes']['answer']
                        if id_respuesta != None:
                            for j in i['attributes']['alternatives']['data']:
                                if id_respuesta[0] == j['id'] and pregunta == "¿El producto está disponible?":
                                    respuesta_0 = j['attributes']['name']
                                if id_respuesta[0] == j['id'] and pregunta == "¿Por qué el producto no ha tenido venta?":
                                    respuesta_1 = j['attributes']['name']
                                if id_respuesta[0] == j['id'] and pregunta == "¿Por qué no se encuentra el producto disponible?":
                                    respuesta_2 = j['attributes']['name']
                                if id_respuesta[0] == j['id'] and pregunta == "Ingrese comentarios adicionales en caso de requerirlo.":
                                    respuesta_3 = j['attributes']['answer']
                elif i['type'] == 'stores':
                    if i['id'] == tienda_frogmi:
                        tienda = i["attributes"]["code"].zfill(4)
        linea_f = [id, realizado, fecha_inicio, fecha_fin, descripcion, stock, material, tienda_frogmi, respuesta_0, respuesta_1, respuesta_2, respuesta_3, tienda]
        lista_lineas.append(linea_f)

    next_url = res["links"]["next"]

    while(len(res['data']) == 500):
        response = requests.request("GET", next_url, headers=headers, data=payload)
        res = json.loads(response.text)
        next_url = res["links"]["next"]
        print(next_url)

        for linea in res['data']:
            respuesta_0 = None
            respuesta_1 = None
            respuesta_2 = None
            respuesta_3 = None
            tienda = None
            id = linea['id']
            realizado = linea['attributes']['done']
            fecha_inicio = linea['attributes']['start_date']
            fecha_fin = linea['attributes']['end_date']
            try:
                descripcion = linea['attributes']['external_data'][0]['main_text']
            except:
                descripcion = 'vacio'
            try:
                stock = linea['attributes']['external_data'][0]['second_text']
            except:
                stock = 'vacio'
            try:
                material = linea['attributes']['external_id']
            except:
                material = 'vacio'
            tienda_frogmi = linea['relationships']['stores']['data']['id']
            for i in res['included']:
                if i['type'] == "task_action_results":
                    if i['relationships']['task_action_events']['data']['id'] == id:
                        pregunta = i['attributes']['name']
                        id_respuesta = i['attributes']['answer']
                        if id_respuesta != None:
                            for j in i['attributes']['alternatives']['data']:
                                if id_respuesta[0] == j['id'] and pregunta == "¿El producto está disponible?":
                                    respuesta_0 = j['attributes']['name']
                                if id_respuesta[0] == j['id'] and pregunta == "¿Por qué el producto no ha tenido venta?":
                                    respuesta_1 = j['attributes']['name']
                                if id_respuesta[0] == j['id'] and pregunta == "¿Por qué no se encuentra el producto disponible?":
                                    respuesta_2 = j['attributes']['name']
                                if id_respuesta[0] == j['id'] and pregunta == "Ingrese comentarios adicionales en caso de requerirlo.":
                                    respuesta_3 = j['attributes']['answer']
                elif i['type'] == 'stores':
                    if i['id'] == tienda_frogmi:
                        tienda = i["attributes"]["code"].zfill(4)
            linea_f = [id, realizado, fecha_inicio, fecha_fin, descripcion, stock, material, tienda_frogmi, respuesta_0, respuesta_1, respuesta_2, respuesta_3, tienda]
            lista_lineas.append(linea_f)

    df = pd.DataFrame(lista_lineas, columns =['id','realizado','fecha_inicio','fecha_fin','descripcion', 'stock', 'material','tienda_frogmi','disponibilidad','razon_de_porque_no_en_venta','razon_de_porque_no_disponible','comentarios', 'id_tienda'])
    
    df = df.astype({
        "id": "string",
        "realizado": "bool",
        "fecha_inicio": "string",
        "fecha_fin": "string",
        "descripcion": "string",
        "stock": "string",
        "material": "string",
        "tienda_frogmi": "string",
        "disponibilidad": "string",
        "razon_de_porque_no_en_venta": "string",
        "razon_de_porque_no_disponible": "string",
        "comentarios": "string",
        "id_tienda": "string"
    }, errors="ignore")
    
    curr_datetime = ts[:16].replace("-", "/").replace("T", "/").replace(":", "")
    file_name = f"frogmi/alerta_reposicion/{curr_datetime}_alerta_reposicion.csv"
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

def _get_table_alerta_reposicion_from_S3(ti):
    import pandas as pd

    alerta_reposicion_file = ti.xcom_pull(key="return_value", task_ids=["load_json_to_s3"])[0]
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+alerta_reposicion_file)
    if not s3_hook.check_for_key(alerta_reposicion_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % alerta_reposicion_file)

    alerta_found_rate_object = s3_hook.get_key(alerta_reposicion_file, bucket_name=s3_bucket)

    df = pd.read_csv(alerta_found_rate_object.get()["Body"])
    print(f"Number of records found: {len(df.index)}")

    df = df.astype({
        "id": "string",
        "realizado": "bool",
        "fecha_inicio": "string",
        "fecha_fin": "string",
        "descripcion": "string",
        "stock": "string",
        "material": "string",
        "tienda_frogmi": "string",
        "disponibilidad": "string",
        "razon_de_porque_no_en_venta": "string",
        "razon_de_porque_no_disponible": "string",
        "comentarios": "string",
        "id_tienda": "string"
    }, errors="ignore")

    return df

def _save_table_alerta_reposicion(ts, ti, ds):
    import pandas as pd
    import sqlalchemy

    df = _get_table_alerta_reposicion_from_S3(ti)
    df = df[['id','realizado','fecha_inicio','fecha_fin','descripcion', 'stock', 'material','tienda_frogmi','disponibilidad','razon_de_porque_no_en_venta','razon_de_porque_no_disponible','comentarios', 'id_tienda']]
    df['id_tienda'] = df['id_tienda'].str.zfill(4)

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    exec_date = ds

    if ts.split("T")[1] == "21:30:00+00:00":
        exec_date = macros.ds_add(ds, 1)

    with engine.begin() as conn:
        conn.execute(f"""
            DELETE FROM ecommdata.frogmi_alerta_reposicion
            WHERE fecha_inicio::date = '{exec_date}'
        """)
        df.to_sql(name="frogmi_alerta_reposicion",
                con=engine,         
                schema="ecommdata",         
                if_exists='append',         
                index=False,         
                chunksize=20000,         
                method='multi')

    return


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'etl_frogmi_alerta_reposicion',
    default_args=default_args,
    description="Extracción y carga de tabla alerta reposicion desde API.",
    schedule_interval="30 12,16,18 * * *",
    start_date=pendulum.datetime(2022, 10, 12, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["frogmi", "reposicion", "MATIAS"],
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla alerta reposicion desde API.
    """ 

    t0 = PythonOperator(
        task_id = "load_json_to_s3",
        python_callable = _load_json_to_s3
    )

    t1 = PythonOperator(
        task_id = "save_table_alerta_reposicion",
        python_callable = _save_table_alerta_reposicion
    )


t0 >> t1
