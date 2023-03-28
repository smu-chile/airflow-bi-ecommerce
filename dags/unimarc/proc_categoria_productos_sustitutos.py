from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

from datetime import datetime
import pendulum

def get_in_sustitutos():
    import pandas as pd
    import os

    print("Iniciando obtención de productos que deban pasar a categoría sustitución: ")
    curr_working_directory = os.getcwd()
    with open(curr_working_directory+f"/dags/unimarc/sql/proc_categoria_in_sustituto.sql", "r") as query_file:
        query_in_sustitutos = query_file.read()
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query_in_sustitutos)
    results = cursor.fetchall()
    ref_id_list = [result[0] for result in results]
    cursor.close()
    pg_connection.close()
    print("Finalizada obtención de productos que deban cambiar de categoria")
    return ref_id_list

'''
Para aquellos productos que cambian de categoría a sustitutto, previamente se hará una revisión
de su atributos_producto.valor para el id_atributo: 11682839 (ID Categoría),
si este es diferente a su productos.id_categoria, se hará su actualización via API de Janis.
'''
def check_if_update_att_category(ti):
    import pandas as pd

    # Checkear si hubo cambio de categoría en ecommdata.productos que van a sustitutos
    list_refid_to_change = ti.xcom_pull(task_ids=["get_in_sustitutos"])[0]
    if len(list_refid_to_change) == 0:
        print("Ningún producto de Lista 8 entra a la categoría sustituto")
        return []
    print(f"""Se espera que los siguientes productos pasen a categoría sustitutos: 
    {list_refid_to_change}""")
    print("""Iniciando revisión de diferencias entre ecommdata.productos:id_categoria y
    ecommdata.atributos_producto:valor (id_categoria)""")
    list_refid_to_change = tuple(list_refid_to_change)
    print(query_check)
    query_check = f"""
            select pro.ref_id, pro.id_categoria, att.valor
            from ecommdata.atributos_producto att
            inner join ecommdata.productos pro 
                on pro.id = att.id_producto_janis
            where att.id_atributo = 11682839
                and	pro.id_categoria NOT IN (10531456, 11599085, 48312581) -- No trabajar, inactivo, sustituto
                and pro.ref_id IN {list_refid_to_change}
                and att.valor::float::int not in ( 48312581, pro.id_categoria);
    """
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query_check)
    results = cursor.fetchall()
    columns_name = [i[0] for i in cursor.description]
    cursor.close()
    pg_connection.close()
    DF_update_atr_pro_categoria = pd.DataFrame(results, columns=columns_name)
    refid_to_update = DF_update_atr_pro_categoria['ref_id']
    if refid_to_update.size == 0:
        print("No hay productos que necesiten actualizar atributos_productos.id_categoria")
        return [] 
    print(f"""Se debe actualizar atributos_productos en los siguientes productos: \n 
        {list(refid_to_update)}""")
    # Creación de big-json
    jst = []
    for index, row in DF_update_atr_pro_categoria.iterrows():
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
    return jst

def set_by_api_att_category(ti):
    import requests
    import json

    headers = {
        "janis-api-key": Variable.get("JANIS_API_KEY"),
        "janis-api-secret": Variable.get("JANIS_API_SECRET"),
        "janis-client": Variable.get("JANIS_CLIENT"),
        "Connection": "keep-alive"
    }

    ljst = ti.xcom_pull(task_ids=["check_if_update_att_category"])[0]
    if ljst == []:
        print("No hay update de atributos_producto.id_categoria")
        return []
    update_products = [item['item_id'] for item in ljst ]
    total_size = len(update_products)
    print(f"""Se deben actualizar {total_size} productos de ecommdata.atributos_producto. 
    Se actualizarán los siguientes productos: {update_products}""")

    # Partición de big-json y envío de data
    lim_json = 500
    total_size = len(ljst)
    if total_size > lim_json:
        jst = [ljst[i:i+lim_json] for i in range(0, len(ljst), lim_json)]
    else:
        jst = [ljst]
    # Actualizar atributos_producto.valor para productos que hayan cambiado de categoría
    cargando = 0
    API_JANIS = Variable.get("JANIS_API_URL") 
    set_response = {}
    ref_ids_not_updated = []
    for arr_dict in jst:
        r = requests.post(f'{API_JANIS}attribute_value', headers = headers, json= arr_dict)
        cargando += len(arr_dict)
        set_response.add(r.status_code)
        if r.status_code == 200:
            print(f"Productos actualizados: {cargando} de {total_size} con EXITO")
        else:
            print(f"Carga sin éxito | Status_Code: {r.status_code} ")
            print(f"Response Print: {r.text}")
            ref_id = json.loads(r.text)['errors'][0]["item_id"]
            print("ref_id not updated: ",ref_id)
            ref_ids_not_updated.append(ref_id)
    if set_response == {200}:
        print("Se han finalizado con EXITO las actualizaciones de las categorías en atributos_producto")
        productos_updated = [ id for id in update_products if id not in ref_ids_not_updated]
        print ("Productos actualizados: ",productos_updated)
        return "Productos actualizados"
    else:
        print(f"""Los siguientes productos no se lograron actualizar y no pasarán a categoría 'Sustitutos':
        {ref_ids_not_updated}
        Actualizacion de atributos_producto.id_categoría Finalizado.""")
        return ref_ids_not_updated
    
def get_out_sustitutos():
    import pandas as pd
    import os
    print("Getting ref_ids of products that are going out of sustitutive category:")
    curr_working_directory = os.getcwd()
    with open(curr_working_directory+f"/dags/unimarc/sql/proc_categoria_out_sustituto.sql", "r") as query_file:
        query_out_sustitutos = query_file.read()
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query_out_sustitutos)
    results = cursor.fetchall()
    columns_name = [i[0] for i in cursor.description]
    DF_update_atr_pro_categoria = pd.DataFrame(results, columns=columns_name)
    cursor.close()
    pg_connection.close()
    print("Finalizada obtención de productos que deban cambiar de categoria")
    return DF_update_atr_pro_categoria.to_json(orient='records')

def upload_refid_category(ti):
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text

    # Connection Data
    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    conn_url = "postgresql+psycopg2://"+username+":"+password+"@"+host+":5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    # Get products going out of substitutive  category
    json_out_sustitutos = ti.xcom_pull(task_ids=["get_out_sustitutos"])[0]
    # Get products going in of substitutive  category
    list_get_in_sustitutos = ti.xcom_pull(task_ids=["get_in_sustitutos"])[0]
    list_response_update =  ti.xcom_pull(task_ids=["set_by_api_att_category"])[0] 

    if json_out_sustitutos != '[]':
        df_out_sustituto = pd.read_json(json_out_sustitutos)
        df_out_sustituto.assign(active = 1)

    
    if list_get_in_sustitutos != []:
        if list_response_update != [] and  list_response_update != []: 
            products_no_updated = list_response_update
            list_in_sustituto = [ refid for refid in list_in_sustituto if refid not in products_no_updated ]
            print("products_no_updated: ", products_no_updated)
        df_in_sustitutos = pd.DataFrame(list_in_sustituto, columns=['refid']).assign(category = 48312581).assign(active = 1)

    if json_out_sustitutos == [] and list_in_sustituto == []:
        print("Finalmente no hay movimientos de productos entre categorias") 
        return
    elif json_out_sustitutos != [] and list_in_sustituto == []:
        df = df_out_sustituto
    elif json_out_sustitutos == [] and list_in_sustituto != []:
        df = df_in_sustitutos
    elif json_out_sustitutos != [] and list_in_sustituto != []:
        df = pd.concat([df_in_sustitutos, df_out_sustituto])
    
        # Save to PostgreSQL:
        print("Comienza la carga INSERT")
        df.to_sql(  name="sustitutos",
                    con=engine,         
                    schema="catalogo",         
                    if_exists='append',         
                    index=False,         
                    chunksize=10000,         
                    method='multi')
        print("Data loaded to Postgres")
    else:
        print("No hubo modificaciones en tabla Sustitutos")
    return 

default_args = {
    "owner": "ecommerce_ops",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'proc_janis_categoria_productos_sustitutos',
    default_args=default_args,
    description= """"Products that should enter and exit the 'Sustitución' category are obtained 
    by using the 'lista8' and 'productos' tables from the 'ecommdata'. 
    Then, the category of the products listed in 'lista8' is updated using Janis API."
    """,
    schedule_interval="0 10 * * *",
    start_date=pendulum.datetime(2023, 3, 28, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["API", "ecommdata", "lista8", "janis", "atributos_producto","productos", 'sustitutos', 'categorias'],
) as dag:

    dag.doc_md = """
    The products that must change categories are obtained, there are two cases:
    Enter substitution: Product that is found with its initial category and in list8 has all its stores marked 'substitution' 
    They come out of substitution: Product that is with a substitute category, in list8 not all of its stores marked 'substitution' 
    For those products that leave their initial category, it is verified that their current Category ID is the same as the one they are in.
    in ecommdata.product_attributes, which will be used as a backup for when it leaves the 'Substitution' category. If it's not the same,
    it is updated using the Janis API. If the backup category_id update is not updated, then these products are not
    they will be moved to the 'Substitution' category.
    """

    t0 = PythonOperator(
        task_id='get_sustitutos_and_not_sustitutos',
        python_callable = get_in_sustitutos
    )

    t1 = PythonOperator(
        task_id='check_if_update_att_category',
        python_callable = check_if_update_att_category,
    )

    t2 = PythonOperator(
        task_id='set_by_api_att_category',
        python_callable = set_by_api_att_category,
    )

    t3 = PythonOperator(
        task_id='get_sustitutos_and_not_sustitutos',
        python_callable = get_out_sustitutos
    )

    t4 = PostgresOperator(
        task_id='truncate_catalogo_sustitutos',
        postgres_conn_id='pg_connection',
        sql='TRUNCATE TABLE catalogo.sustitutos;'
    )
    
    t5 = PythonOperator(
        task_id='upload_refid_category',
        python_callable = upload_refid_category,
    )

    t0 >> t1 >> t2 >> t3 >> t4 >> t5