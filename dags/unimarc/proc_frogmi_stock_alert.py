from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

from datetime import datetime
import pendulum

def _get_time_interval(ts):
    # Data ranges:
    # 08:00 -  prev_date at 18:00 to curr_date at 08:00 (+14 hrs)
    # 13:00 -  curr_date at 08:00 to curr_date at 13:00 (+5 hrs)
    # 18:00 -  curr_date at 13:00 to curr_date at 18:00 (+5 hrs)
    exec_datetime = datetime.strptime(ts[:16], "%Y-%m-%dT%H:%M")
    exec_datetime_utc = pendulum.timezone("utc").convert(exec_datetime)
    local_tz = pendulum.timezone("America/Santiago")
    exec_datetime_local = local_tz.convert(exec_datetime_utc)
    exec_datetime_local = exec_datetime_local.strftime("%Y-%m-%dT%H:%M")
    print(exec_datetime_local)

    current_exec_hour = exec_datetime_local.split("T")[1][:2]
    if current_exec_hour == "18":
        return exec_datetime_local, "interval '14 hours'"
    else:
        return exec_datetime_local, "interval '5 hours'"

def _pre_payload(id_tienda):
    if Variable.get("FROGMI_ENV") != "prod":
        print("WARNING: THIS IS A TEST RUN OF THIS DAG! Change Env Var: FROGMI_ENV to perform a production run.")
        id_tienda = "93145c22-7f04-4b44-bbdc-505ba33f2dde"

    base_payload = {
        "data": [
            {
                "type": "task_sku",
                "attributes": {
                    "name": "FR-Ecomm 2022-08-24_17:00:00",
                    "template_id": "a6dbc4bd-64e6-4628-bb6b-66902cba3a7e",
                    "accountable_area_code": "ADMIN_LOCAL",
                    "stores": [
                        id_tienda
                    ],
                    "start_date": "2022-08-24T00:00:00-04:00",
                    "end_date": "2022-08-24T20:00:00-04:00",
                    "notification":[
                    ],
                    "instructions": "Prueba API desde python",
                    "external_id": "1234567",
                    "external_data": [
                        {
                            "main_text": "Texto tarea de prueba API",
                            "second_text": "$1",
                            "icon": "info"
                        }
                    ],
                    "products": []
                }
            }
        ]
    }
    return base_payload

def _post_request_to_publish_task_endpoint(ts):
    import pandas as pd

    
    exec_date_local, time_interval = _get_time_interval(ts)
    query = f"""
        select *
        from
        (
            select ref_id
                , id_frogmi as id_tienda
                , ordenes
                , unidades_faltantes
                , dense_rank() over (partition by id_frogmi order by ordenes desc, unidades_faltantes desc) as _rank
            from 
            (
                select ref_id
                    , id_frogmi 
                    , count(1) as ordenes
                    , sum(unidades_solicitadas - unidades_pickeadas) as unidades_faltantes --PAQ y DIS multiplicar por unidades_pack
                from operaciones_unimarc.found_rate_productos frp 
                join ecommdata.tiendas as t
                    on frp.id_tienda = t.id and t.id_frogmi is not null
                where fecha_picking between '{exec_date_local}'::timestamp and '{exec_date_local}'::timestamp + {time_interval}
                and estado_foundrate <> 3
                group by ref_id, id_frogmi
            ) _t
        ) _resultado
        where _resultado._rank <= 5
        order by id_tienda, _rank
        ; 
    """
    print(query)

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()

    df = pd.read_sql(query, pg_connection)

    print(f"Number of rows found: {len(df.index)}")
    df["material"] = df["ref_id"].str.split("-").str[0]
    df = df.groupby("id_tienda").head(5).reset_index().drop(columns=["index"])
    print(df)
    print(df.to_records(index=False))
    tiendas = df["id_tienda"].drop_duplicates().tolist()
    print(tiendas)
    payloads = {tienda: _pre_payload(tienda) for tienda in tiendas}

    registros = df.to_records(index=False)

    for registro in registros:
        r_tienda = registro[1]
        r_material = registro[5]
        body = {
            "product_code": r_material,
            "place_code": "alerta_repo"
        }
        payloads[r_tienda]["data"][0]["attributes"]["products"].append(body)

    # Send payloads to S3
    print(payloads)

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
    schedule_interval="0 8,13,18 * * *",
    start_date=pendulum.datetime(2022, 8, 25, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    concurrency=2,
    tags=["OPS", "Frogmi", "API", "POST", "foundrate"],
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
