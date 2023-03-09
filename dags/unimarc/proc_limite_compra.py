from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

import pendulum

def db_get_ref_id_atributos_producto():
    import pandas as pd

    print(f"Iniciando obtencion de datos con limite de compra ...")
    print("Estableciendo conección con postgres db")

    query = """
        select pro.nombre, pro.ref_id, pro.vtex_id, att.valor
        from ecommdata.atributos_producto att
        full outer join ecommdata.productos pro
            on pro.id = att.id_producto_janis
        where att.id_atributo = 2847610
            and (att.valor not in ('12','11','10','9','8','7','6','5','4','3','2','1','12.0','17','1.0', '24', '24.0') or att.valor is null);
        """
    print(query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    columns_name = [i[0] for i in cursor.description]
    cursor.close()
    pg_connection.close()
    DF_atributos_producto_null = pd.DataFrame(results, columns=columns_name)
    lista_refid = DF_atributos_producto_null['ref_id'].to_list()
    print(f"Productos con valor sin Limite de Compra obtenidos: {lista_refid}")
    return lista_refid


def set_lim_compra(ti):
    import json
    import requests

    lista_ref_id = ti.xcom_pull(
        task_ids=["db_get_ref_id_atributos_producto"])[0]
    if len(lista_ref_id) == 0:
        print("No hay productos para cambiar, las tareas han finalizado")
        return

    headers = {
        "janis-api-key": Variable.get("JANIS_API_KEY"),
        "janis-api-secret": Variable.get("JANIS_API_SECRET"),
        "janis-client": Variable.get("JANIS_CLIENT"),
        "Connection": "keep-alive"
    }

    # Creación de big-json
    jst = []
    for x in lista_ref_id:
        item = {
            "item_id": x,
            "attributes": [
                {
                    "id": "409",
                    "values": ["12"]
                }
            ]
        }
        jst.append(item)

    # Partición de big-json
    lim_json = 500
    total_size = len(list(jst))
    if total_size > 500:
        jst = [json.dumps(jst[i:i+lim_json], indent=2)
               for i in range(0, len(jst), lim_json)]
  

    # Seteo vía API al atriubuto limite de compra de la lista de refid
    API_JANIS = Variable.get("JANIS_API_URL")
    cargando = 0
    for i, jsonString in enumerate(jst, start=1):
        json_loads = json.loads(jsonString)
        r = requests.post(f'{API_JANIS}attribute_value', headers = headers, json=json_loads)
        cargando += len(list(jsonString))
        if r.status_code == 200:
            print(f"Productos actualizados: {cargando} de {total_size} con EXITO")
        else:
            print(f"Carga sin éxito | Status_Code: {r.status_code} ")
            print(f"Response Print: {r.content}")
    print("La carga de límites a finalizado")          
    return 


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'proc_categoria_productos_sustitutos',
    default_args=default_args,
    description="Obtención de productos que entran y sale de la categoría Sustitutos",
    schedule_interval="0 10 * * *",
    start_date = pendulum.datetime(2023, 3, 8, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["limite_compra", "ecommdata_unimarc", "atributos_producto"],
) as dag:

    dag.doc_md = """
        Setea en 12 el valor del atributo "Limite de Compra" cuyos productos tienen valor = NULL 
        """

    t0 = PythonOperator(
        task_id='db_get_ref_id_atributos_producto',
        python_callable=db_get_ref_id_atributos_producto
    )

    t1 = PythonOperator(
        task_id='set_lim_compra',
        python_callable=set_lim_compra
    )

    t0 >> t1
