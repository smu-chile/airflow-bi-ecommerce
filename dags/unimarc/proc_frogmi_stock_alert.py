from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

import pendulum

def _get_time_interval(ts):
    # Data ranges:
    # 08:00 -  prev_date at 18:00 to curr_date at 08:00 (+14 hrs)
    # 13:00 -  curr_date at 08:00 to curr_date at 13:00 (+5 hrs)
    # 18:00 -  curr_date at 13:00 to curr_date at 18:00 (+5 hrs)
    print(f"ts: {ts}")
    current_exec_hour = ts.split("T")[1][:2]
    if current_exec_hour == "18":
        return "interval '14 hours'"
    else:
        return "interval '5 hours'"

def _post_request_to_publish_task_endpoint(ts):
    time_interval = _get_time_interval(ts)
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
                where fecha_picking between '{ts[:17]}' and '{ts[:17]}' + {time_interval}
                and estado_foundrate <> 3
                group by ref_id, id_frogmi
            ) _t
        ) _resultado
        where _resultado._rank <= 5
        order by id_tienda, _rank
        ; 
    """
    print(query)
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
