from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator as PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor

from datetime import datetime
import pendulum


def get_in_sustitutos():
    import os
    import pandas as pd

    print("Iniciando obtención de productos que deban pasar a categoría sustitución: ")
    curr_working_directory = os.getcwd()
    with open(curr_working_directory+f"/dags/unimarc/sql/proc_categoria_in_sustituto.sql", "r") as query_file:
        query_in_sustitutos = query_file.read()
    pg_hook = PostgresHook(conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    id_category_sustituto = Variable.get(
        "JANIS_SUSTITUTOS_ID_CATEGORIA_SUSTITUTO")
    id_category_static = Variable.get(
        "JANIS_SUSTITUTOS_STATIC_CATEGORIES")
    query_in_sustitutos = query_in_sustitutos.replace(
        '{ id_sustitutive_category_id }', id_category_sustituto).replace(
        '{ id_category_static }', id_category_static)
    print(query_in_sustitutos)
    cursor.execute(query_in_sustitutos)
    results = cursor.fetchall()
    columns_name = [i[0] for i in cursor.description]
    print(columns_name)
    DF_in_sustitutos = pd.DataFrame(results, columns=columns_name)
    cursor.close()
    pg_connection.close()
    print("Finalizada obtención de productos que deban cambiar de categoria")
    return DF_in_sustitutos.to_json(orient='records')


'''
Para aquellos productos que cambian de categoría a sustitutto, previamente se hará una revisión
de su atributos_producto.valor para el atributo: ID Categoría, si este es diferente 
a su productos.id_categoria, se actualizará via API de Janis.
'''


def check_if_update_att_category(ti):
    import pandas as pd
    import time
    from utils.postgres_utils import is_empty_table

    # Checkear si hubo cambio de categoría en ecommdata.productos que van a sustitutos
    json_refid_to_change = ti.xcom_pull(task_ids=["get_in_sustitutos"])[0]
    list_refid_to_change = []
    if json_refid_to_change != '[]':
        df_refid_to_change = pd.read_json(json_refid_to_change)
        print(df_refid_to_change)
        list_refid_to_change = list(df_refid_to_change['refid'])
    else:
        print("Ningún producto de Lista 8 entra a la categoría sustituto")
        return []
    print(f"""Se espera que los siguientes productos pasen a categoría sustitutos: 
    {list_refid_to_change}""")
    print("""Iniciando revisión de diferencias entre ecommdata.productos:id_categoria y
    ecommdata.atributos_producto:valor (id_categoria)""")
    print("list_refid_to_change", list_refid_to_change)
    list_refid_to_change = str(list_refid_to_change)[1:-1]
    id_atributo_idcategory = Variable.get(
        "JANIS_SUSTITUTOS_ID_ATT_IDCATEGORIA")  # dev: 5814502, prod: 11682839
    query_check = f"""
            select pro.ref_id as ref_id, c.ref_id as id_categoria
            from ecommdata.productos pro
            inner join ecommdata.atributos_producto att on pro.ref_id = att.ref_id
            inner join ecommdata.categorias c on pro.id_categoria = c.id 
            where pro.ref_id IN ({list_refid_to_change})
            and att.id_atributo = {id_atributo_idcategory} 
            and ( att.valor::float::int != c.ref_id or att.valor is null); 
    """
    print(query_check)
    
    i = 0
    while is_empty_table("ecommdata", "atributos_producto") == True:
        time.sleep(300)
        i += 1
        if i == 4:
            raise Exception(
                "No se encuentra disponible la tabla ecommdata.atributos_producto")

    pg_hook = PostgresHook(conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query_check)
    results = cursor.fetchall()
    columns_name = [i[0] for i in cursor.description]
    cursor.close()
    pg_connection.close()
    DF_update_atr_pro_categoria = pd.DataFrame(results, columns=columns_name)
    refid_to_update = DF_update_atr_pro_categoria['ref_id']
    print(refid_to_update)
    if refid_to_update.size == 0:
        print("No hay productos que necesiten actualizar atributos_productos.id_categoria")
        return []
    print(f"""Se debe actualizar atributos_productos en los siguientes productos: 
        {list(refid_to_update)}""")
    # Creación de big-json
    jst = []

    for index, row in DF_update_atr_pro_categoria.iterrows():
        item = {
            "item_id": row['ref_id'],
            "attributes": [
                {
                    # DEV:293, PRO:449
                    "id": Variable.get("JANIS_SUSTITUTOS_REF_ID_ATRIBUTE_ID_CATEGORIA"),
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
    update_products = [item['item_id'] for item in ljst]
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
    set_response = set()
    ref_ids_not_updated = []
    for arr_dict in jst:
        r = requests.post(f'{API_JANIS}attribute_value',
                          headers=headers, json=arr_dict)
        cargando += len(arr_dict)
        set_response.add(r.status_code)
        if r.status_code == 200:
            print(
                f"Productos actualizados: {cargando} de {total_size} con EXITO")
        else:
            print(f"Carga sin éxito | Status_Code: {r.status_code} ")
            print(f"Response Print: {r.text}")
            ref_id = [x["item_id"] for x in json.loads(r.text)['errors']]
            print("ref_id not updated: ", ref_id)
            ref_ids_not_updated = ref_ids_not_updated + ref_id
    if set_response == {200}:
        print("Se han finalizado con EXITO las actualizaciones de las categorías en atributos_producto")
        productos_updated = [
            id for id in update_products if id not in ref_ids_not_updated]
        print("Productos actualizados: ", productos_updated)
        return []
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
    pg_hook = PostgresHook(conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    id_atributo_idcategory = Variable.get(
        "JANIS_SUSTITUTOS_ID_ATT_IDCATEGORIA")
    id_category_sustituto = Variable.get(
        "JANIS_SUSTITUTOS_ID_CATEGORIA_SUSTITUTO")
    id_category_static = Variable.get("JANIS_SUSTITUTOS_STATIC_CATEGORIES")
    query_out_sustitutos = query_out_sustitutos.replace(
        '{ id_atributo_idcategory }', id_atributo_idcategory).replace(
        '{ id_sustitutive_category_id }', id_category_sustituto).replace(
        '{ id_category_static }', id_category_static)
    print(query_out_sustitutos)
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
    conn_url = "postgresql+psycopg2://"+username + \
        ":"+password+"@"+host+":5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    json_in_sustitutos = ti.xcom_pull(task_ids=["get_in_sustitutos"])[0]
    json_out_sustitutos = ti.xcom_pull(task_ids=["get_out_sustitutos"])[0]
    df_out_sustitutos = pd.read_json(json_out_sustitutos)
    if json_in_sustitutos == '[]' and json_out_sustitutos == '[]':
        print("Finalmente no hay movimientos de productos entre categorias")
        return

    df_in_sustitutos = pd.read_json(json_in_sustitutos)
    list_response_update = ti.xcom_pull(
        task_ids=["set_by_api_att_category"])[0]
    if list_response_update != []:
        print("products_no_updated: ", list_response_update)
        df_in_sustitutos = df_in_sustitutos.query(
            'refid not in @list_response_update')
    ref_id_categoria_sustituto = Variable.get(
        "JANIS_SUSTITUTOS_REFID_CATEGORIA_SUSTITUTO")
    df_in_sustitutos = df_in_sustitutos.assign(
        category=ref_id_categoria_sustituto)

    df = pd.concat([df_in_sustitutos, df_out_sustitutos])

    df = df.assign(active=1)
    df = df.rename(columns={'refid': 'refId'})
    # Save to PostgreSQL:
    print("Comienza la carga INSERT")
    df.to_sql(name="sustitutos",
              con=engine,
              schema="catalogo",
              if_exists='append',
              index=False,
              chunksize=10000,
              method='multi')

    if list_response_update != []:
        print(f"""atributo_producto.valor not updated of categoría:
        {list_response_update}""")
    else:
        print("Data full loaded to Postgres")
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
    description=""""Products that should enter and exit the 'Sustitución' category are obtained 
    by using the 'lista8' and 'productos' tables from the 'ecommdata'. 
    Then, the category of the products listed in 'lista8' is updated using Janis API."
    """,
    schedule="0 10 * * *",
    start_date=pendulum.datetime(2023, 3, 28, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["API", "ecommdata", "lista8", "janis", "atributos_producto",
          "productos", 'sustitutos', 'categorias'],
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
        task_id='get_in_sustitutos',
        python_callable=get_in_sustitutos
    )

    t1 = PythonOperator(
        task_id='check_if_update_att_category',
        python_callable=check_if_update_att_category,
    )

    t2 = PythonOperator(
        task_id='set_by_api_att_category',
        python_callable=set_by_api_att_category,
    )

    t3 = PythonOperator(
        task_id='get_out_sustitutos',
        python_callable=get_out_sustitutos
    )

    t4 = PostgresOperator(
        task_id='truncate_catalogo_sustitutos',
        conn_id='postgresql_conn',
        sql='TRUNCATE TABLE catalogo.sustitutos;'
    )

    t5 = PythonOperator(
        task_id='upload_refid_category',
        python_callable=upload_refid_category,
    )

    t0 >> t1 >> t2 >> t3 >> t4 >> t5
