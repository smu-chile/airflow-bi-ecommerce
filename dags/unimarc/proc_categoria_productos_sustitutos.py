from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

from datetime import datetime
import pendulum

def get_sustitutos_and_not_sustitutos():
    import pandas as pd
    curr_working_directory = os.getcwd()

    with open(curr_working_directory+f"/dags/unimarc/sql/proc_categoria_sustituto.sql", "r") as query_file:
        rappi_stock_query = query_file.read()
    
    mycursor = conn_ecommdata()
    mycursor.execute(query_lista_sustitutos)
    results = mycursor.fetchall()
    columns = [i[0] for i in mycursor.description]
    df = pd.DataFrame(results, columns=columns)
    mycursor.close()
    return df[['ref_id', 'sustituto_total', 'id_category']].to_json(orient='records')


'''
Para aquellos productos donde producto.id_categoria --> 'sustituto'
antes de cambiar su categoría, actualizar atributos_producto.id_categoria si
su productos.id_categoria cambió. No considera categorias "NO TRABAJAR",
"INACTIVO", o con att.id_categoria = 48312581 (sustituto).
'''
def check_if_update_att_category(ti):
    import pandas as pd
    # CARGA DE INFORMACIÓN PARA ROLLBACK de Productos que entran a productos.categoria:sustituto
    # Checkear si hubo cambio de categoría en el producto
    json_categories = ti.xcom_pull(task_ids="get_sustitutos_and_not_sustitutos")[0]
    df = pd.read_json(json_categories, orient='index')
    list_refid_to_change = list(df[df['sustituto_total' == True]]['ref_id'])
    if len(list_refid_to_change) == 0:
        print("De los productos que entran a Categoria Sustitutos, todos mantienen su valor respecto a atributos_productos")
        return
    else:
        print("Actualizando att.id_categoria para aquellos productos que cambiarán de PRO.CAT.ID_CATEGORIA --> PRO.CAT.SUSTITUTOS")
        list_refid_to_change = tuple(list_refid_to_change)
        query_check = f"""
            select pro.ref_id, pro.vtex_id, pro.id_categoria
            from ecommdata.atributos_producto att
            inner join ecommdata.productos pro on pro.id = att.id_producto_janis
            where att.id_atributo = '11682839'
            and	(pro.id_categoria != 10531456 -- No trabajar
                or pro.id_categoria  != 11599085  -- Inactivo+
                or pro.id_categoria != 48312581 )  -- no debiera tener efecto alguno
            and pro.ref_id IN {list_refid_to_change}
            and split_part(att.valor,'.', 1)::INTEGER != pro.id_categoria
            and split_part(att.valor,'.', 1)::INTEGER != 48312581;
        """
        print(f"Iniciando obtencion de tabla...")
        mycursor = conn_ecommdata()
        mycursor.execute(query_check)
        results = mycursor.fetchall()
        columns = [i[0] for i in mycursor.description]
        df = pd.DataFrame(results, columns=columns)
        mycursor.close()
        return df.to_json(orient='records')


def create_payload_set_att_categoria(ti):
    import json
    import pandas as pd
    df_json = ti.xcom_pull(task_ids="check_if_update_att_category")[0]
    df = pd.read_json(df_json, orient='index')

    # Creación de big-json
    jst = []
    for index, row in df.iterrows():
        item = {
            "item_id": row['ref_id'],
            "attributes": [
                {
                    "id": "449",
                    "values": [str(row['id_categoria'])]
                }
            ]
        }
        jst.append(item)

    ti.xcom_push(key = 'jst', value = jst)
    # Partición de big-json y envío de data
    lim_json = 500
    total_size = len(list(jst))
    if total_size > lim_json:
        jst = [json.dumps(jst[i:i+lim_json], indent=2)
               for i in range(0, len(jst), lim_json)]
  
    API_JANIS = "https://janis.in/api/"

    # Actualizar atributos_producto.valor para productos que hayan cambiado de categoría
    return list_json

def set_by_api_att_category(ti, list_json):
    import requests

    print(f"Se inicia seteo de atributos_producto.id_categoria")
    # Crear sesión HTTP
    headers = {
        "janis-api-key": Variable.get("JANIS_API_KEY"),
        "janis-api-secret": Variable.get("JANIS_API_SECRET"),
        "janis-client": Variable.get("JANIS_CLIENT"),
        "Connection": "keep-alive"
    }

    session = requests.Session()
    session.headers.update(headers)
    environment = 'janis'

    # Enviar solicitudes POST en bucle for
    r_status = set()
    total_size = sum(len(lista) for lista in list_json)
    if total_size > 0:
        print(
            f"Productos que se actualizarán att.categoria via API: {total_size}")
        for i, jsonString in enumerate(list_json, start=1):
            endpoint = f'https://{environment}.in/api/attribute_value'
            r = session.post(endpoint, data=jsonString)
            r_status.add(str(r.status_code))
            print(
                f"Productos actualizados: {i} de {total_size} | Status_Code: {r.content}")
        print(f"carga finalizada con resultados: {r_status}")
    elif total_size == 0:
        print("No hay atributos_producto.valor que se deban actualizar")
        r_status = {'200'}
    ti.xcom_push(key = 'set_att', value= r_status )

def upload_refid_category(df, r_status, df_set_att_cat):
    if r_status != {'200'}:
        print("La actualización de atributos_producto.valor no fué exitosa, no se cargarán aquellos productos ")
        df = df[ df['ref_id'] not in list(df_set_att_cat['ref_id'])]
    lista_de_tuplas = [tuple(x) for x in df.to_numpy()]

    query_insert = """
    INSERT INTO catalogo.sustitutos (refid, category, active)
    values ( %s, %s, %s) """
    mycursor = conn_ecommdata()
    mycursor.execute("TRUNCATE catalogo.sustitutos")
    mycursor.execute(query_insert,  lista_de_tuplas)
    mycursor.close()


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
    start_date=datetime(2023, 3, 8),
    catchup=False,
    max_active_runs=1,
    tags=["sustituto", "ecommdata_unimarc", "lista8", "atributos_producto"],
) as dag:

    dag.doc_md = """
    Cambia de categoría aquellos productos que entran y salen de la categoría sustitutos
    """

    t0 = PythonOperator(
        task_id='get_sustitutos_and_not_sustitutos',
        python_callable=get_sustitutos_and_not_sustitutos
    )

    t1 = PythonOperator(
        task_id='check_if_update_att_category',
        python_callable=check_if_update_att_category,
    )

    t2 = PythonOperator(
        task_id='create_payload_set_att_categoria',
        python_callable = create_payload_set_att_categoria,
        op_kwargs = {'df': '{{ ti.xcom_pull(task_ids="check_if_update_att_category") }}'}
    )

    t3 = PythonOperator(
        task_id='set_by_api_att_category',
        python_callable = set_by_api_att_category,
        op_kwargs = {'list_json': '{{ ti.xcom_pull(task_ids="create_payload_set_att_categoria") }}'}
    )

    t4 = PythonOperator(
        task_id='upload_refid_category',
        python_callable = upload_refid_category,
        op_kwargs = {'df': '{{ ti.xcom_pull(task_ids="get_sustitutos_and_not_sustitutos", key = "df") }}',
                    'r_status': '{{ti.xcom_pull(task_ids="create_payload_set_att_categoria", key = "set_att")}}',
                    'df_set_att_cat': '{{ti.xcom_pull(task_ids="check_if_update_att_category")}}'}
    )

    t0 >> t1 >> t2 >> t3 >> t4 