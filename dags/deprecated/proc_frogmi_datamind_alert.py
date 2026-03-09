from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from datetime import datetime, timedelta
import pendulum

def _get_time_interval(ts):
    # Data ranges:
    # 08:00 -  prev_date at 17:00 to curr_date at 08:00 (+14 hrs)
    # 13:00 -  curr_date at 08:00 to curr_date at 13:00 (+5 hrs)
    # 17:00 -  curr_date at 13:00 to curr_date at 17:00 (+5 hrs)

    hours_dictionary = {
        "08": 13,
        "13": 17,
        "17": 8
    }

    exec_datetime = datetime.strptime(ts[:16], "%Y-%m-%dT%H:%M")
    exec_datetime_utc = pendulum.timezone("utc").convert(exec_datetime)
    local_tz = pendulum.timezone("America/Santiago")
    exec_datetime_local = local_tz.convert(exec_datetime_utc)
    exec_datetime_local_str = exec_datetime_local.strftime("%Y-%m-%dT%H:%M")
    print(exec_datetime_local_str)

    current_exec_hour = exec_datetime_local_str.split("T")[1][:2]
    if current_exec_hour == "17":
        task_start_date = exec_datetime_local + timedelta(days=1)
        task_start_date = task_start_date.replace(hour=hours_dictionary[current_exec_hour], minute=0, second=0)
        return exec_datetime_local_str, "interval '15 hours'", task_start_date
    else:
        task_start_date = exec_datetime_local
        task_start_date = task_start_date.replace(hour=hours_dictionary[current_exec_hour], minute=0, second=0)
        return exec_datetime_local_str, "interval '5 hours'", task_start_date

def _pre_payload(id_tienda, product, descr, task_start_date, exec_date):
    if Variable.get("FROGMI_DATAMIND_ENV") != "prod":
        print("WARNING: THIS IS A TEST RUN OF THIS DAG! Change Env Var: FROGMI_ENV to perform a production run.")
        id_tienda = "93145c22-7f04-4b44-bbdc-505ba33f2dde"

    task_end_date = task_start_date + timedelta(hours=2)
    task_start_date_str = task_start_date.strftime("%Y-%m-%dT%H:%M:%S%z")
    task_end_date_str = task_end_date.strftime("%Y-%m-%dT%H:%M:%S%z")
    print(f"start_date: {task_start_date_str}")
    print(f"end_date: {task_end_date_str}")
    base_payload = {
        "data": [
            {
                "type": "task_sku",
                "attributes": {
                    "name": f"FR-Ecomm {task_start_date_str[:-5]}",
                    "template_id": "18e45453-4134-4b18-b470-4b18af2f0e0b",
                    "accountable_area_code": "ADMIN_LOCAL_PILOTO",
                    "stores": [
                        id_tienda
                    ],
                    "start_date": task_start_date_str[:-2]+":00",
                    "end_date": task_end_date_str[:-2]+":00",
                    "notification":[
                    ],
                    "instructions": "Alerta Found Rate",
                    "external_id": f"fr_ecomm_{task_start_date_str}",
                    "external_data": [
                        {
                            "main_text": f"{descr}",
                            "second_text": f"Código: {product['product_code']}",
                            "icon": "info"
                        }
                    ],
                    "products": [
                        product
                    ]
                }
            }
        ]
    }
    return base_payload

def _post_request_to_publish_task_endpoint(ts):
    import json
    import requests
    
    exec_date_local, time_interval, task_start_date = _get_time_interval(ts)
    query = f"""
        SELECT id,
            ref_id,
            id_tienda
        FROM soprole.feedback
        WHERE ultima_actualizacion 
            BETWEEN '{exec_date_local}'::timestamp and '{exec_date_local}'::timestamp + {time_interval}
        AND respuesta in ('no-encontrado')
        ; 
    """
    print(query)

    pg_datamind_hook = PostgresHook(conn_id="datamind_conn")
    pg_datamind_connection = pg_datamind_hook.get_conn()
    pg_datamind_cur = pg_datamind_connection.cursor()
    pg_datamind_cur.execute(query)
    records = pg_datamind_cur.fetchall()

    pg_datamind_cur.close()
    pg_datamind_connection.close()
    
    pg_hook = PostgresHook(conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    pg_cur = pg_connection.cursor()
    
    payload_data = []
    for record in records:
        product_ref_id = record[1]
        id_tienda = record[2]

        product_query = f"""
            SELECT material,
                nombre
            FROM ecommdata.productos
            WHERE ref_id = '{product_ref_id}';
        """

        pg_cur.execute(product_query)
        product_data = pg_cur.fetchall()[0]

        store_query = f"""
            SELECT id_frogmi
            FROM ecommdata.tiendas
            WHERE id = '{id_tienda}';
        """

        pg_cur.execute(store_query)
        store_data = pg_cur.fetchall()[0]

        if store_data[0] is None:
            print(f"WARNING! Store id: {id_tienda} does not have an id_frogmi. Skipped.")
            continue

        data_array = [product_data[0], product_data[1], store_data[0]]
        payload_data.append(data_array)


    print(f"Number of rows found: {len(payload_data)}")
    if len(payload_data) == 0:
        print("No records found. Exit.")
        return

    payloads = []
    for registro in payload_data:
        r_tienda = registro[2]
        r_material = registro[0]
        r_descripcion = registro[1]
        product_body = {
            "product_code": r_material,
            "place_code": "Productos_Ajustar"
        }
        payloads.append(_pre_payload(
            id_tienda=r_tienda, 
            product=product_body, 
            descr=r_descripcion,
            task_start_date=task_start_date, 
            exec_date=exec_date_local))

    # Send payloads to S3
    print(payloads)
    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    json_payloads_string = json.dumps(payloads)

    curr_datetime = ts[:16].replace("-", "/").replace("T", "/").replace(":", "")
    payloads_s3_path = "frogmi/api/post_publish_task_datamind/"+curr_datetime+".json"

    s3_hook.load_string(json_payloads_string,
                  key=payloads_s3_path,
                  bucket_name=s3_bucket,
                  replace=True,
                  encrypt=False)

    # POST requests:
    frogmi_url = Variable.get("FROGMI_API_URL")
    frogmi_publish_task_endpoint = frogmi_url + "api/v3/tasks_management/activities"
    headers = {
        "Authorization": "Bearer "+Variable.get("FROGMI_API_TOKEN_SECRET"),
        "X-Company-UUID": Variable.get("FROGMI_COMPANY_UUID_SECRET"),
        "Content-Type": "application/json"
    }
    jobs_ids = []
    for payload in payloads:
        response = requests.post(frogmi_publish_task_endpoint, json=payload, headers=headers)
        print(response.status_code)
        try:
            response_json = response.json()
            print(response.json())
            jobs_ids.append(response_json["data"]["id"])
        except Exception as e:
            print(e)
            print("Error on response. Can not get job id.")

    return

default_args = {
    "owner": "ecommerce_ops",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    "proc_frogmi_post_alerta_foundrate_datamind",
    default_args=default_args,
    description="Envío de tareas Alerta de Found Rate de Datamind a Frogmi",
    schedule="0 8,13,17 * * *",
    start_date=pendulum.datetime(2022, 10, 18, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,

    tags=["OPS", "Frogmi", "API", "POST", "foundrate", "datamind", "soprole"],
) as dag:

    dag.doc_md = """
    Envía tareas (tipo SKU) de Alerta de Found Rate a Frogmi. \n
    Utilizando las respuestas de Datamind se identifican los SKUs no-encontrados que deben ser enviados a Frogmi.\n
    Se genera una tarea por cada par Tienda / SKU. \n
    Este proceso considerará todas aquellas tiendas que tengan un valor no nulo en la columna id_frogmi en la tabla ecommdata.tiendas.\n
    Este proceso leerá la variable de entorno 'FROGMI_ENV' para determinar si usar tiendas reales o de prueba.
    """ 
    t0 = PythonOperator(
        task_id = "post_request_to_publish_task_endpoint",
        python_callable = _post_request_to_publish_task_endpoint
    )
