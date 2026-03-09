from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

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
    if current_exec_hour == "17":
        task_start_date = exec_datetime_local + timedelta(days=1)
        task_start_date = task_start_date.replace(hour=13, minute=0, second=0)
        exec_datetime_local = exec_datetime_local.replace(hour=9, minute=30, second=0) + timedelta(days=1)
        exec_datetime_local_str = exec_datetime_local.strftime("%Y-%m-%dT%H:%M")
        return exec_datetime_local_str, "interval '3 hours 30 minutes'", task_start_date
    else:
        task_start_date = exec_datetime_local
        task_start_date = task_start_date.replace(hour=17, minute=0, second=0)
        return exec_datetime_local_str, "interval '4 hours'", task_start_date

def _pre_payload(id_tienda, product, descr, task_start_date, template, accountable_area_code):
    if Variable.get("FROGMI_ENV") != "prod":
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
        select ref_id,
        descripcion,
        id_tienda,
        ordenes,
        unidades_faltantes,
        _rank,
        cantidad
        from
        (
            select ref_id
                , descripcion
                , id_frogmi as id_tienda
                , ordenes
                , unidades_faltantes
                , cantidad
                , dense_rank() over (partition by id_frogmi order by ordenes desc, unidades_faltantes desc) as _rank
            from 
            (
                select ref_id
                    , frp.descripcion
                    , id_frogmi 
                    , count(1) as ordenes
                    , sum(unidades_solicitadas - unidades_pickeadas) as unidades_faltantes --PAQ y DIS multiplicar por unidades_pack
                    , case 
                        when cpf.id_tienda is null then 5
                        else cpf.cantidad 
                    end as cantidad
                from operaciones_unimarc.found_rate_productos frp 
                join ecommdata.tiendas as t
                    on frp.id_tienda = t.id and t.id_frogmi is not null
                left join catalogo.cantidad_productos_frogmi cpf
                    on frp.id_tienda = cpf.id_tienda
                left join ecommdata.frogmi_alerta_reposicion far
                    on substring(frp.ref_id,1,18) = lpad(far.material, 18, '0') and frp.id_tienda = far.id_tienda
                where fecha_picking between '{exec_date_local}'::timestamp and '{exec_date_local}'::timestamp + {time_interval}
                and estado_foundrate <> 3
                and ((far.fecha_inicio not between '{exec_date_local}'::timestamp + interval '3 hours' and '{exec_date_local}'::timestamp + {time_interval} + interval '3 hours') or far.fecha_inicio is null)
                group by ref_id, frp.descripcion, id_frogmi, cpf.id_tienda, cpf.cantidad
            ) _t
        ) _resultado
        where _resultado._rank <= _resultado.cantidad
        order by id_tienda, _rank
        ; 
    """
    print(query)

    pg_hook = PostgresHook(conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()

    df = pd.read_sql(query, pg_connection)

    print(f"Number of rows found: {len(df.index)}")
    if len(df.index) == 0:
        print("No records found. Exit.")
        return
    
    df["material"] = df["ref_id"].str.split("-").str[0]
    df = df.groupby("id_tienda").head(15).reset_index().drop(columns=["index"])
    tiendas = df["id_tienda"].drop_duplicates().tolist()
    print("Frogmi store ids:")
    print(tiendas)
    payloads = [] 

    registros = df.to_records(index=False)

    for registro in registros:
        r_tienda = registro[2]
        r_material = registro[7]
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
            product=product_body, 
            descr=r_descripcion,
            task_start_date=task_start_date, 
            template='a6dbc4bd-64e6-4628-bb6b-66902cba3a7e',
            accountable_area_code='ADMIN_LOCAL_PILOTO'))
    if payloads:
        primer_product_code = payloads[0]["data"][0]["attributes"]["products"][0]["product_code"]
        print(f"[DEBUG] Primer product_code enviado: {primer_product_code}")

    # Send payloads to S3
    print(payloads)
    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
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
    "proc_frogmi_post_alerta_foundrate",
    default_args=default_args,
    description="Envío de tareas Alerta de Found Rate a Frogmi",
    schedule="0 13,17 * * *",
    start_date=pendulum.datetime(2022, 8, 25, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,

    tags=["OPS", "Frogmi", "API", "POST", "foundrate", "MATIAS"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Envía tareas (tipo SKU) de Alerta de Found Rate a Frogmi. \n
    Por cada tienda, toma los 5 SKUs con mayor número de incidencias de falta de stock e impacto por unidades solicitadas. \n
    Por cada tienda arma un payload y es enviado al endpoint Publish Task de Frogmi.\n
    Se genera una tarea por cada par Tienda / SKU. \n
    Este proceso considerará todas aquellas tiendas que tengan un valor no nulo en la columna id_frogmi en la tabla ecommdata.tienda.\n
    Este proceso leerá la variable de entorno 'FROGMI_ENV' para determinar si usar tiendas reales o de prueba.
    """ 
    t0 = PythonOperator(
        task_id = "post_request_to_publish_task_endpoint",
        python_callable = _post_request_to_publish_task_endpoint
    )
