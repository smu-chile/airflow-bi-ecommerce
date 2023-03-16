from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator, BranchPythonOperator

from datetime import datetime
import pendulum

def get_sustitutos_and_not_sustitutos():
    import pandas as pd
    import os

    print("""Iniciando obtención de productos que deban cambiar de categoria: \n
            sustitutos <---> no sustitutos desde lista8""")
    curr_working_directory = os.getcwd()
    with open(curr_working_directory+f"/dags/unimarc/sql/proc_categoria_sustituto.sql", "r") as query_file:
        query_lista_sustitutos = query_file.read()
    
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query_lista_sustitutos)
    results = cursor.fetchall()
    columns_name = [i[0] for i in cursor.description]
    cursor.close()
    pg_connection.close()
    DF_sustitutos_l8 = pd.DataFrame(results, columns=columns_name)
    print("Finalizada obtención de productos que deban cambiar de categoria")
    return DF_sustitutos_l8.to_json(orient='records')

'''
Para aquellos productoa que cambian de categoría a sustitutto, previamente se hará una revisión
de su atributos_producto.valor para el id_atributo: 11682839 (ID Categoría),
si este es diferente a su productos.id_categoria, se hará su actualización via API de Janis.
'''
def check_if_update_att_category(ti):
    import pandas as pd

    # Checkear si hubo cambio de categoría en ecommdata.productos que van a sustitutos
    json_categories = ti.xcom_pull(task_ids="get_sustitutos_and_not_sustitutos")[0]
    if json_categories == '[]':
        print('No hay movimientos entre categoría sustitutos en lista8')
        return
    df = pd.read_json(json_categories, orient='records')
    list_refid_to_change = list(df[df['sustituto_total' == True]]['ref_id'])  # productos que deben estrar a sustituto
    if len(list_refid_to_change) == 0:
        print("Ningún producto de Lista 8 entra a la categoría sustituto")
        return
    print(f"""Se espera que los siguientes productos pasen a categoría sustitutos: \n
    {list_refid_to_change}""")
    print("""Iniciando revisión de diferencias entre ecommdata.productos:id_categoria y \n
    ecommdata.atributos_producto:valor (id_categoria)""")
    list_refid_to_change = tuple(list_refid_to_change)
    print(query_check)
    query_check = f"""
            select pro.ref_id, pro.id_categoria
            from ecommdata.atributos_producto att
            inner join ecommdata.productos pro 
                on pro.id = att.id_producto_janis
            where att.id_atributo = 11682839
                and	pro.id_categoria NOT IN (10531456, 11599085, 48312581) -- No trabajar, inactivo, sustituto
                and pro.ref_id IN {list_refid_to_change}
                and att.valor::float::int != 48312581
                and att.valor::float::int != pro.id_categoria;
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
    if DF_update_atr_pro_categoria['ref_id'].size == 0:
        print("No hay productos que necesiten actualizar atributos_productos.id_categoria")
        return

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

    headers = {
        "janis-api-key": Variable.get("JANIS_API_KEY"),
        "janis-api-secret": Variable.get("JANIS_API_SECRET"),
        "janis-client": Variable.get("JANIS_CLIENT"),
        "Connection": "keep-alive"
    }

    ljst = ti.xcom_pull(task_ids="check_if_update_att_category")[0]
    if ljst == None:
        print("No hay update de atributos_producto.id_categoria")
        return "Sin actualizacion"
    update_productos = [item['item_id'] for item in ljst ]
    total_size = len(update_productos)
    print(f"""Se deben actualizar {total_size} productos de ecommdata.atributos_producto. \n
    Se actualizarán los siguientes productos: {update_productos}""")

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
    for arr_dict in jst:
        r = requests.post(f'{API_JANIS}attribute_value', headers = headers, json= arr_dict)
        cargando += len(arr_dict)
        if r.status_code == 200:
            print(f"Productos actualizados: {cargando} de {total_size} con EXITO")
        else:
            print(f"Carga sin éxito | Status_Code: {r.status_code} ")
            print(f"Response Print: {r.content}")
            print("""Dado que no se actualizaron las categorías de los productos, o no se actualizaron todos, \n
            estos productos no pasarán a categoría 'Sustitutos'""")
    print("Actualizacion de atributos_producto.id_categoría Finalizado.")    
    return update_productos

def upload_refid_category(ti):
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text

    json_categories = ti.xcom_pull(task_ids="get_sustitutos_and_not_sustitutos")[0]
    sustituto_sin_update =  ti.xcom_pull(task_ids="set_by_api_att_category")[0] 
    print("json_categories: ", json_categories)
    print("sustituto_sin_update: ", sustituto_sin_update)
    
    if json_categories != '[]':
        json_categories = json_categories.replace("true", "True")
        df = pd.DataFrame(eval(json_categories))[['ref_id', 'id_category']]
        if sustituto_sin_update != None:
            print("La actualización de atributos_producto.valor no fué exitosa, no se cargarán aquellos productos ")
            df = df[ ~df['ref_id'].isin(sustituto_sin_update)]
        df = df.assign(active = 1)
        df = df.rename(columns={'ref_id': 'refid', 'id_category': 'category'})

        host = Variable.get("POSTGRESQL_HOST")
        database = Variable.get("POSTGRESQL_DB")
        username = Variable.get("POSTGRESQL_USER")
        password = Variable.get("POSTGRESQL_PASSWORD")

        conn_url = "postgresql+psycopg2://"+username+":"+password+"@"+host+":5432/"+database
        engine = sqlalchemy.create_engine(conn_url)

        # Save to PostgreSQL:
        print("Comienza la carga INSERT")
        conn = engine.connect()
        truncate_query = "TRUNCATE ecommdata.atributos_producto"
        conn.execute(text(truncate_query))
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
    Cambia de categoría aquellos productos que entran y salen de la categoría sustitutos, \n
    actualizando atributos_producto.valor (id_categoria) si producto.id_categoria es  \n
    diferente.
    """

    t0 = PythonOperator(
        task_id='get_sustitutos_and_not_sustitutos',
        python_callable = get_sustitutos_and_not_sustitutos
    )

    t1 = BranchPythonOperator(
        task_id='check_if_update_att_category',
        python_callable = check_if_update_att_category,
    )

    t2 = PythonOperator(
        task_id='set_by_api_att_category',
        python_callable = set_by_api_att_category,
    )

    t3 = PythonOperator(
        task_id='upload_refid_category',
        python_callable = upload_refid_category,
    )

    t0 >> t1 >> t2 >> t3 