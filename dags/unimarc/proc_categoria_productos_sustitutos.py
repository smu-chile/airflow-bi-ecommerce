from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator


import pandas as pd
import time
import json
import requests
import psycopg2
import pandas
import stores

JANIS_API_KEY = Variable.get("JANIS_API_KEY")
JANIS_API_SECRET = Variable.get("JANIS_API_SECRET")
JANIS_CLIENT = Variable.get("JANIS_CLIENT")

headers = {
    "janis-api-key" : JANIS_API_KEY,
    "janis-api-secret" : JANIS_API_SECRET,
    "janis-client" : JANIS_CLIENT,
    "Connection" : "keep-alive"
    }

def conn_ecommdata():
    print("Estableciendo conexión con postgres db")
    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    conn = psycopg2.connect(
        host=host,
        user=username,
        password=password,
        database=database
    )
    mycursor = conn.cursor()
    return mycursor


def get_columns_name(name_table):
    mycursor = conn_ecommdata()
    mycursor.execute(f"""SELECT *
        FROM information_schema.columns
        WHERE table_schema = 'ecommdata'
        AND table_name   = '{name_table}'; """)
    columns_name = list(pd.DataFrame(mycursor.fetchall())[3])
    mycursor.close()
    # print(f"Columns name: {columns_name}")
    return columns_name


def get_sustitutos_y_no(ts):
    query_lista_sustitutos = """
        select
            LPAD(cast(l8.material as text), 18, '0') || '-' || l8.umv as ref_id,
            bool_and(l8.sustituto) as sustituto_total,
            case
                when bool_and(l8.sustituto) = false then pro.id_categoria
                else 48312581
            end as id_category
        from
            ecommdata.lista8 l8
        left join ecommdata.productos pro 
                    on
            LPAD(cast(l8.material as text), 18, '0') || '-' || l8.umv = pro.ref_id
        group by
            LPAD(cast(l8.material as text), 18, '0') || '-' || l8.umv,
            pro.id_categoria,
            l8.sustituto
        having
            (
            bool_and(l8.sustituto) = true
            and LPAD(cast(l8.material as text), 18, '0') || '-' || l8.umv not in (
                select
                        distinct ref_id
                from
                        ecommdata.productos pro
                where
                        pro.id_categoria = 48312581
                            )
                        )
            or (
            bool_and(l8.sustituto) = false
            and ( LPAD(cast(l8.material as text), 18, '0') || '-' || l8.umv in (
                select
                        distinct ref_id
                from
                        ecommdata.productos pro
                where
                        pro.id_categoria = 48312581
                            ))
            and LPAD(cast(l8.material as text), 18, '0') || '-' || l8.umv not in (
                        '000000000000761296-KG', '000000000000752499-KG', 
                        '000000000000542749-KG', '000000000000761299-KG', 
                        '000000000000752492-KG', '000000000000752501-KG', 
                        '000000000000752528-KG', '000000000000761291-KG', 
                        '000000000000761281-KG', '000000000000761292-KG', 
                        '000000000000761279-KG', '000000000000752510-KG',
                        '000000000000761276-KG', '000000000000542743-KG', 
                        '000000000000752519-KG', '000000000000542758-KG', 
                        '000000000000761285-KG', '000000000000761294-KG', 
                        '000000000000752496-KG', '000000000000752531-KG', 
                        '000000000000761287-KG', '000000000000752486-KG', 
                        '000000000000542755-KG', '000000000000542752-KG', 
                        '000000000000752507-KG'
                    ) 
                );"""

    mycursor = conn_ecommdata()
    mycursor.execute(query_lista_sustitutos)
    df_category_change = pd.DataFrame(mycursor.fetchall(), columns=['ref_id', 'sustituto_total', 'id_category'])
    mycursor.close()

    return df_category_change


# Para aquellos productos que están PRO.CAT:ID_CATEGORIA --> SUSTITUTOS
# antes de cambiar su categoría, veo si acaso cambió su PRO.CAT:ID_CATEGORÍA
# respecto al ATRIBUTO.CAT, si son diferentes entonces se hacer una carga
# del ID_CATEGORIA a ATRIBUTOS_PRODUCTO. No considerar categorias "NO TRABAJAR",
# "INACTIVO".
def check_if_update_id_category(list_refid_to_change):
    print("Actualizando att.id_categoria para aquellos productos que cambiarán de PRO.CAT.ID_CATEGORIA --> PRO.CAT.SUSTITUTOS")
    list_refid_to_change = str(list_refid_to_change)[1:-1]
    query_check = f"""
        select pro.ref_id, pro.vtex_id, pro.id_categoria
        from ecommdata.atributos_producto att 
        inner join ecommdata.productos pro on pro.id = att.id_producto_janis 
        where att.id_atributo = '11682839'
        and	(pro.id_categoria != 10531456 -- No trabajar
        or pro.id_categoria  != 11599085  -- Inactivo+
        or pro.id_categoria != 48312581 )  -- no debiera tener efecto alguno 
        and pro.ref_id IN ({list_refid_to_change})
        and split_part(att.valor,'.', 1)::INTEGER != pro.id_categoria;--and att.valor is null; -- full outer len: 397, inner 
    """
    columns_name = ['ref_id', 'vtex_id', 'id_categoria']
    print(f"Iniciando obtencion de tabla...")
    mycursor = conn_ecommdata()
    mycursor.execute(query_check)
    productos_need_update = pd.DataFrame(
        mycursor.fetchall(), columns = columns_name)
    mycursor.close()
    return productos_need_update


def create_big_json(df_lista_ref_id):
    # Creación de big-json
    json_list = []
    for index, row in df_lista_ref_id.iterrows():
        item = {
            "item_id": str(row['ref_id']),
            "attributes": [
                {
                    "id": "449",
                    "values": [str(row['id_categoria'])]
                }
            ]
        }
        json_list.append(item)
    jst = json.dumps(json_list)
    return jst

def set_by_api_att_category(list_json):
    print(f"Se inicia seteo de att.id_categoria")
    
    # Crear sesión HTTP
    headers = {
    "janis-api-key" : JANIS_API_KEY,
    "janis-api-secret" : JANIS_API_SECRET,
    "janis-client" : JANIS_CLIENT,
    "Connection" : "keep-alive"
    }

    session = requests.Session()
    session.headers.update(headers)
    environment = 'janis'

    # Enviar solicitudes POST en bucle for
    r_status = set()
    total_size = sum(len(lista) for lista in list_json)
    for i, jsonString in enumerate(list_json, start=1):
        endpoint = f'https://{environment}.in/api/attribute_value'
        r = session.post(endpoint, data=jsonString)
        r_status.add(str(r.status_code))
        print(f"Productos actualizados: {i} de {total_size} | Status_Code: {r.content}")

    # Imprimir resultados finales
    print(f"Carga finalizada con resultados: {r_status}")

def set_categoria(df_lista_ref_id):
    jst = create_big_json(df_lista_ref_id)
    lim_json = 500
    # Partición de big-json y envío de data
    sublists = [jst[i:i+lim_json] for i in range(0, len(jst), lim_json)]
    list_json = []
    # Convertir cada sublista a una cadena JSON
    for i, sublist in enumerate(sublists):
        json_string = json.dumps(sublist, indent=2)
        print(f'Sublista {i+1}: {json_string}')
        list_json.append(json_string)
    set_by_api_att_category(list_json)
    return list_json

# MAIN PROCES

def proceso_de_entrada_salida():
    print("Se inicia proceso de entrada y salida a PRO.CAT: SUSTITUTOS")
    # De lista8 obtengo la lista de sustitutos y no sustitutos
    df_sustitutos_y_no = get_sustitutos_y_no()

    # CARGA DE INFOR PARA ROLLBACK Productos entran a PRO.CAT.SUSTITUTOS
    # Checkear si hubo cambio de categoría en el producto
    poductos_entran_sustituto = list(df_sustitutos_y_no[df_sustitutos_y_no['sustituto_total' == True ]]['ref_id'])

    df_charge_and_change = check_if_update_id_category(
        poductos_entran_sustituto)
    # Aquellos con cambios setear nuevo id_categoria
    #####################################################################
    # ACTIVAR SETEO
    if len(df_charge_and_change) > 0:

        print(f"Productos que se actualizarán att.categoria via API: {len(df_charge_and_change)}")
        print(df_charge_and_change)
        r = set_categoria(df_charge_and_change)
        print(f"carga finalizada con {r}")
    elif len(df_charge_and_change) == 0:
        print("No hay productos que se deban actualizar")
    df_sustitutos_y_no = df_sustitutos_y_no[['ref_id', 'id_category']]
    df_sustitutos_y_no.to_csv('charge_category_sustitutos.csv') #GENERAR 
# proceso_de_entrada_salida()


def subir_data(df):
    lista_de_tuplas = [tuple(x) for x in df.to_numpy()]
    query_insert = """
    INSERT INTO catalogo.sustitutos (refid, category, active)
    values ( %s, %s, %s) """
    mycursor = conn_ecommdata('DEV')
    mycursor.execute("TRUNCATE catalogo.sustitutos")
    mycursor.execute(query_insert,  lista_de_tuplas)
    mycursor.close()

# subir_data(proceso_de_entrada_salida())
