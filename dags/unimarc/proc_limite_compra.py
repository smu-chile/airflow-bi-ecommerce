from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from utils.slack_utils import dag_success_slack, dag_failure_slack

from utils.postgres_utils import is_empty_table
import pendulum

def db_get_ref_id_atributos_producto():
    import time

    print(f"Iniciando obtencion de lista de ref_id de productos sin limite de compra ...")
    print("Estableciendo conección con postgres db")

    id_atributo_limite_compra = Variable.get("JANIS_ID_ATRIBUTO_LIMITE_COMPRA") # dev:2839656 , prod:2847610 
    i = 0
    while is_empty_table("ecommdata","atributos_producto") == True:
        time.sleep(300)
        i +=1
        if i == 4:
            raise Exception("No se encuentra disponible la tabla ecommdata.atributos_producto")

    query = f"""select distinct p.ref_id from ecommdata.productos p
            inner join ecommdata.lista8 l on concat(l.material, '-', l.umv) = p.ref_id 
            where length(split_part(p.ref_id, '-', 1)) = 18 
            and length(split_part(p.ref_id,'-',2)) >= 2
            and split_part(p.ref_id,'-',2) not in ('KG', 'KGV')
            order by p.ref_id desc;
            """
    print(query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    ref_id_list = [result[0] for result in results]
    cursor.close()
    pg_connection.close()
    print(f"Productos con valor sin Limite de Compra obtenidos: {ref_id_list}")
    return ref_id_list


def set_lim_compra(ti):
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
                    "id": str(Variable.get("JANIS_REF_ID_ATRIBUTO_ID_CATEGORIA")),
                    "values": ["999"]
                }
            ]
        }
        jst.append(item)

    # Partición de big-json
    lim_json = 500
    total_size = len(jst)
    if total_size > lim_json:
        jst = [jst[i:i+lim_json] for i in range(0, len(jst), lim_json)]
    else:
        jst = [jst]

    # Seteo vía API al atriubuto limite de compra de la lista de refid
    API_JANIS = Variable.get("JANIS_API_URL")
    cargando = 0
    for arr_dic in jst:
        r = requests.post(f'{API_JANIS}attribute_value', headers = headers, json=arr_dic)
        cargando += len(arr_dic )
        if r.status_code == 200:
            print(f"Productos actualizados: {cargando} de {total_size} con EXITO")
        else:
            print(f"Carga sin éxito | Status_Code: {r.status_code} ")
            print(f"Response Print: {r.content}")
            raise ValueError("Janis API response != 200")
    print("La carga de límites a finalizado")          
    return 


default_args = {
    "owner": "ecommerce_ops",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'proc_janis_limite_compra',
    default_args=default_args,
    description=""" Busca en tabla ecommdata.atributos_producto productos que tengan su atributo 'Limite de Compra'  \n
    con valor = NULL, a estos productos se les rescata su ref_id para setear su valor a 12 mediante la API de Janis """,
    schedule_interval="55 8 * * *",
    start_date = pendulum.datetime(2023, 3, 8, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["janis", "limite_compra", "ecommdata_unimarc", "atributos_producto", "API", "SERGIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
        Busca en tabla ecommdata.atributos_producto productos que tengan su atributo 'Limite de Compra'  \n
        con valor = NULL, a estos productos se les rescata su ref_id para setear su valor a 12 mediante la API de Janis 
        """

    t0 = PythonOperator(
        task_id='db_get_ref_id_atributos_producto',
        python_callable=db_get_ref_id_atributos_producto
    )

    t1 = PythonOperator(
        task_id='set_lim_compra',
        python_callable=set_lim_compra
    )

    t2 = TriggerDagRunOperator(
        task_id="trigger_limite_promociones",
        trigger_dag_id="etl_limite_promociones",
        wait_for_completion=False
    )

    t0 >> t1 >> t2
