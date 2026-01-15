from airflow import DAG
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException

import pendulum

from utils.slack_utils import dag_failure_slack, dag_success_slack
from utils.postgres_utils import query_to_df

def get_vtex_category_id(janis_id: int):
    pg = PostgresHook(postgres_conn_id="postgresql_conn")
    sql = """
        SELECT ref_id
        FROM ecommdata.categorias
        WHERE id = %s
        LIMIT 1
    """
    row = pg.get_first(sql, parameters=(janis_id,))
    return int(row[0]) if row and row[0] is not None else None

def validar_y_despublicar(**kwargs):
    import requests
    import re

    local_tz = pendulum.timezone("America/Santiago")
    hoy = pendulum.now(local_tz)
    hoy_str = hoy.to_date_string()
    hora_str = hoy.to_time_string() # Para la columna hora_proceso

    pg_hook = PostgresHook(postgres_conn_id='postgresql_conn')
    conf = kwargs['dag_run'].conf
    refs = conf.get('refs', [])
    usuario = conf.get('requested_by', 'unknown')

    # Configuración de API vía Variables
    JANIS_URL_GET = "https://janis.in/api/products"
    JANIS_URL_POST = "https://janis.in/api/product/update"
    
    HEADERS = {
        "janis-api-key": Variable.get("JANIS_API_KEY"),
        "janis-api-secret": Variable.get("JANIS_API_SECRET"),
        "Janis-Client": Variable.get("JANIS_CLIENT"),
        "Content-Type": "application/json"
    }
    
    regex_pattern = r"^\d{18}-(?:KG|KGV|UN|CJ|DIS)$"
    validos = [r.strip().upper() for r in refs if re.match(regex_pattern, r.strip().upper())]

    if not validos:
        raise ValueError("❌ No hay referencias válidas para procesar.")

    print(f"🚀 Iniciando despublicación para {len(validos)} productos (Solicitado por @{usuario})")

    stats = {"procesados": 0, "actualizados": 0, "omitidos": 0, "errores": 0}

    for ref in validos:
        # --- VALIDACIÓN ---
        check_sql = f"""SELECT ref_id 
                        FROM catalogo.log_despublicaciones_0917 
                        WHERE ref_id = %s 
                        AND fecha_proceso = %s 
                        AND estado = 'EXITO'
                        """

        existente = pg_hook.get_first(check_sql, parameters=(ref, hoy_str))
            
        if existente:
            print(f"Skipping {ref}: ya se solicitó baja de producto hoy.")
            continue

        estado = "PENDIENTE"
        detalle = ""

        try:
            params = {"refId[]": ref}
            resp_get = requests.get(JANIS_URL_GET, headers=HEADERS, params=params, timeout=15)
            
            if resp_get.status_code != 200:
                estado, detalle = "ERROR", f"GET Status {resp_get.status_code}"
                stats["errores"] += 1
            else:
                data_list = resp_get.json()
                if not data_list:
                    estado, detalle = "OMITIDO", "No encontrado en Janis"
                    stats["omitidos"] += 1
                else:
                    producto = data_list[0]
                    stores = producto.get("Stores", [])
                    print(stores) # Debug: Mostrar las tiendas actuales, borrar
                    is_active = producto.get("IsActive", False)

                    if is_active and "0917" in stores:
                        
                        if stores and isinstance(stores[0], list):
                            stores_lista_simple = stores[0]
                        else:
                            stores_lista_simple = stores
                        
                        nuevas_stores = [s for s in stores_lista_simple if s != "0917"]

                        category_vtex = get_vtex_category_id(producto.get("Category"))

                        payload = {
                            "IdProduct": producto.get("IdProduct"), #1
                            "Name": producto.get("Name"), #2
                            "Brand": producto.get("Brand"),#3
                            "Category": category_vtex, #4
                            "CategoryErp": producto.get("CategoryErp"), #4.1
                            "Stores": nuevas_stores, #5
                            "IsActive": producto.get("IsActive"), #6
                            "IsAvailable": bool(producto.get("IsAvailable")), #7
                            "ShowWithoutStock": bool(producto.get("ShowWithoutStock")), #8
                        }

                        print(payload)  # Debug: Mostrar el payload que se enviará
                        payload_final = [payload]
                        resp_post = requests.post(JANIS_URL_POST, headers=HEADERS, json=payload_final, timeout=15)
                        
                        if resp_post.status_code in [200, 201, 204]:
                            estado, detalle = "EXITO", "Store 0917 removida"
                            stats["actualizados"] += 1
                            print(f"✅ {ref} actualizado.")
                        else:
                            estado, detalle = "ERROR", f"POST Error: {resp_post.text}"
                            stats["errores"] += 1
                    else:
                        estado, detalle = "OMITIDO", "Inactivo o sin Store 0917"
                        stats["omitidos"] += 1

        except Exception as e:
            estado, detalle = "ERROR", str(e)
            stats["errores"] += 1

        insert_sql = """
            INSERT INTO catalogo.log_despublicaciones_0917 
            (ref_id, usuario_slack, fecha_proceso, hora_proceso, estado, detalle)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        pg_hook.run(insert_sql, parameters=(ref, usuario, hoy_str, hora_str, estado, detalle))
        stats["procesados"] += 1

    return f"Resultado final: {stats}"

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_despublicacion_trapenses',
    default_args=default_args,
    description="""Despublicación de productos solicitados desde Slack.\n 
    Se ejecuta mediante POST desde backend de SAC con comando despublicar desde Slack.\n 
    Solo existe un grupo de usuarios quienes pueden ejecutar esta acción.\n
    Este grupo está dentro del control de SAC.""",
    schedule_interval=None,
    start_date=pendulum.datetime(2023, 7, 11, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["Trapenses", "Janis", "Slack", "Postgres", "Operaciones", "FRANCISCO", "KEVIN", "CK"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Despublicación de productos trapenses en Janis a partir de una lista de referencias enviada vía API.
    """ 
    
    t0 = PythonOperator(
        task_id = "despublicar_trapenses",
        python_callable = validar_y_despublicar
    )

    t0
