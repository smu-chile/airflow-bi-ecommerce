from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook

from utils.slack_utils import dag_success_slack, dag_failure_slack

from datetime import datetime, timedelta
import pendulum

def _get_time_interval(ts):
    # Data ranges:
    # 13:00 -  curr_date at 09:30 to curr_date at 13:00 (+3 hrs 30 min)
    # 17:00 -  curr_date at 13:00 to curr_date at 17:00 (+4 hrs)

    exec_datetime = datetime.strptime(ts[:16], "%Y-%m-%dT%H:%M")
    exec_datetime_utc = pendulum.timezone("utc").convert(exec_datetime)
    local_tz = pendulum.timezone("America/Santiago")
    exec_datetime_local = local_tz.convert(exec_datetime_utc)
    exec_datetime_local_str = exec_datetime_local.strftime("%Y-%m-%dT%H:%M")
    print(exec_datetime_local_str)
    current_exec_hour = exec_datetime_local_str.split("T")[1][:2]
    if current_exec_hour == "12":
         exec_datetime_local = exec_datetime_local - timedelta(minutes=30)
    return exec_datetime_local_str, "interval '4 hours'", exec_datetime_local

def _pre_payload(id_tienda, product, descr, task_start_date, template, accountable_area_code):
    if Variable.get("FROGMI_ENV") != "prod":
        print("WARNING: THIS IS A TEST RUN OF THIS DAG! Change Env Var: FROGMI_ENV to perform a production run.")
        id_tienda = "93145c22-7f04-4b44-bbdc-505ba33f2dde"

    task_end_date = task_start_date + timedelta(hours=3)
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
                    "template_id": f"{template}",
                    "accountable_area_code": f"{accountable_area_code}",
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
    import pandas as pd
    import json
    import requests
    
    exec_date_local, time_interval, task_start_date = _get_time_interval(ts)

    query = f"""
        select p.ref_id, p.nombre as descripcion, fafr.tienda_frogmi as id_tienda
        from ecommdata.frogmi_alerta_found_rate fafr
        inner join ecommdata.productos p on lpad(fafr.material, 18, '0') = p.material
        where fafr.gondola is true and (fecha_fin = date_trunc('hour','{task_start_date.strftime("%Y-%m-%d %H:%M:%S")}'::timestamp) + interval '3 hours' or fecha_fin = date_trunc('hour','{task_start_date.strftime("%Y-%m-%d %H:%M:%S")}'::timestamp) + interval '3 hours 30 minutes');
    """
    print(query)

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()

    df = pd.read_sql(query, pg_connection)

    print(f"Number of rows found: {len(df.index)}")
    if len(df.index) == 0:
        print("No records found. Exit.")
        return
    
    df["material"] = df["ref_id"].str.split("-").str[0]
    df = df.sort_values("id_tienda")
    tiendas = df["id_tienda"].drop_duplicates().tolist()
    print("Frogmi store ids:")
    print(tiendas)
    payloads = [] 

    registros = df.to_records(index=False)

    for registro in registros:
        r_tienda = registro[2]
        r_material = registro[3]
        r_descripcion = registro[1]
        product_body = {
            "product_code": str(int(r_material)),
            "place_code": "alerta_repo"
        }
        product_body_e = {
            "product_code": str(int(r_material)),
            "place_code": "Found_Rate_ecommerce"
        }
        payloads.append(_pre_payload(
            id_tienda=r_tienda, 
            product=product_body_e, 
            descr=r_descripcion,
            task_start_date=task_start_date,
            template='f1afd85f-a8dc-4aeb-8f3e-d91df8ab9444',
            accountable_area_code='Encargado_ecommerce'))

    # Send payloads to S3
    print(payloads)
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    json_payloads_string = json.dumps(payloads)

    curr_datetime = ts[:16].replace("-", "/").replace("T", "/").replace(":", "")
    payloads_s3_path = "frogmi/api/post_publish_task/"+curr_datetime+".json"

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
    
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    def send_request(payload):
        try:
            response = requests.post(frogmi_publish_task_endpoint, json=payload, headers=headers, timeout=15)
            return response, payload
        except Exception as e:
            print(f"Request failed or timed out: {e}")
            return None, payload

    print(f"Sending {len(payloads)} payloads concurrently...")
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(send_request, p) for p in payloads]
        for future in as_completed(futures):
            response, payload = future.result()
            if response is not None:
                print(response.status_code)
                try:
                    response_json = response.json()
                    print(response_json)
                    jobs_ids.append(response_json["data"]["id"])
                except Exception as e:
                    print(e)
                    print("Error on response. Can not get job id.")
            else:
                store_id = payload["data"][0]["attributes"]["stores"][0]
                print(f"Failed to send task for store {store_id} due to timeout/error.")

    return

default_args = {
    "owner": "ecommerce_ops",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    "proc_frogmi_post_alerta_foundrate_encargado_ecommerce",
    default_args=default_args,
    description="Envío de tareas Alerta de Found Rate a Frogmi",
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 8, 25, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    concurrency=2,
    tags=["OPS", "Frogmi", "API", "POST", "foundrate", "MATIAS"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Envía tareas (tipo SKU) de Alerta de Found Rate a Frogmi al encargado ecommerce. \n
    Se toman las respuestas de la alerta found rate original y se envia una pregunta de validación al encargado ecommerce sobre los productos en gondola. \n
    Por cada tienda arma un payload y es enviado al endpoint Publish Task de Frogmi.\n
    Se genera una tarea por cada par Tienda / SKU. \n
    Este proceso leerá la variable de entorno 'FROGMI_ENV' para determinar si usar tiendas reales o de prueba.
    """ 
    t0 = PythonOperator(
        task_id = "post_request_to_publish_task_endpoint",
        python_callable = _post_request_to_publish_task_endpoint
    )
