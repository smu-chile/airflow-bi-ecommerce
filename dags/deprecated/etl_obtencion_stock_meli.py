from airflow import DAG
from airflow.models import Variable
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime, timedelta
import pendulum

def get_stock(ts):
    import pandas as pd
    import requests
    import io
    import numpy as np
    import time

    fecha_exec = (datetime.strptime(ts[:19], '%Y-%m-%dT%H:%M:%S')) + timedelta(hours=1)


    ### MONGO
    mongo_hook = MongoHook(conn_id="mongodb_meli_conn")
    pipeline_mongo = [{'$project':{
        'id':1, 
        'inventory_id':1, 
        'seller_id':1, 
        'status':1, 
        'title':1,
        'subtitle':1,
        'category_id':1,
        'price':1,
        'base_price':1,
        'original_price':1,
        'permalink':1,
        'seller_custom_field':1,
        'date_created':1,
        'last_updated':1,
        'health':1,
        'catalog_listing':1,
        'ean_list': {"$filter": {"input": "$attributes", "as": "list", "cond": {"$eq": ["$$list.id", "GTIN"]}}}}}, 
        {'$project' : {
        'id':1, 
        'inventory_id':1, 
        'seller_id':1, 
        'status':1, 
        'title':1,
        'subtitle':1,
        'category_id':1,
        'price':1,
        'base_price':1,
        'original_price':1,
        'permalink':1,
        'seller_custom_field':1,
        'date_created':1,
        'last_updated':1,
        'health':1,
        'catalog_listing':1,
        'eand': {"$arrayElemAt": ["$ean_list.value_name", 0]}
        }}]


    list_items_cursor = mongo_hook.aggregate(
        mongo_db = Variable.get('MELI_ITEMS_DB_MONGO'),
        mongo_collection="items",
        aggregate_query = pipeline_mongo,
    )

    list_items = list(list_items_cursor)
    if len(list_items) == 0:
        raise Exception('Error, lista vacía')
    df_items = pd.DataFrame(list_items)
    df_items['_id'] = df_items["_id"].astype(str)
    df_items['fecha'] = fecha_exec
    df_items['fecha'] = df_items['fecha'].astype(str)
    df_items = df_items[df_items['id'] != 'N/A']
    columns_main = [
        'id_mongo', 'product_id','inventory_id', 
        'seller_id', 'category_id', 'estado', 
        'nombre', 'subtitle', 'precio', 
        'precio_base','precio_original','fecha',
        'eand', 'seller_custom_field', 'fecha_creado', 
        'fecha_ultima_actualizacion', 'health', 'catalog_listing'
        ]
    df_items = df_items.rename(columns={'_id':'id_mongo','id':'product_id','status':'estado','title':'nombre', 'price':'precio',
    'original_price' : 'precio_original', 'base_price' : 'precio_base', 'date_created' : 'fecha_creado', 
    'last_updated' : 'fecha_ultima_actualizacion'})
    df_items = df_items[columns_main]
    print (df_items.dtypes)

    columns_query = ",".join(columns_main)
    values_query = ",".join(["%s" for column in columns_main])
    df_items = df_items.fillna("NULL")
    records = list(df_items.to_records(index=False))

    fixed_records = []
    for record in records:
        fixed_record = []
        for value in record:
            if isinstance(value, np.generic):
                fixed_record.append(value.item())
            elif value == "NULL":
                fixed_record.append(None)
            else:
                fixed_record.append(value)
        fixed_records.append(tuple(fixed_record))
    print(f"Number of records to load: {str(len(fixed_records))}")
    incremental_query = """
        INSERT INTO ecommdata_meli.productos ("""+columns_query+""") 
        VALUES ("""+values_query+""");
    """

    print(incremental_query)
    pg_hook = PostgresHook(conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.executemany(incremental_query, fixed_records)
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    print("Data loaded to Postgres")

    api_obtain_token = Variable.get('MELI_TOKEN_API')
    body_obtain_token = {
    "grant_type": "refresh_token",
    "client_id": Variable.get('MELI_CLIENT_ID'),
    "client_secret": Variable.get('MELI_CLIENT_SECRET'),
    "redirect_uri": Variable.get('MELI_REDIRECT_URI'),
    "refresh_token": Variable.get('MELI_REFRESH_TOKEN'),
    }
    header_obtain = {
        "Content-Type" : "application/json",
    }
    request_post = requests.post(api_obtain_token, data = body_obtain_token, headers=header_obtain)
    response = request_post.json()
    print (request_post.status_code)


    api_url = Variable.get('MELI_TOKEN_API')
    bearer_token = response['access_token']
    header = {
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type" : "application/json",}


    products_api = Variable.get('MELI_PRODUCTS_API_FORMAT')
    get_stock_seller = Variable.get('MELI_STOCK_API_FORMAT')
    get_non_available_stock = Variable.get('MELI_STOCK_DETAILS_API_FORMAT')

    total_inventory_id = []
    df_get_id = df_items['product_id'].dropna()
    largo = len(list(df_get_id))

    for z in range(largo):
        product_id_value = df_get_id.iat[z]
        r = requests.get(products_api.format(product_id_value), headers=header)
        if r.status_code == 404:
            print ('Respuesta 404, producto sin ID de inventario')
            print (r.status_code)
            continue
        response = r.json()
        if 'inventory_id' not in response:
            print ('producto sin columna inventory_id')
            print (response)
            continue
        total_inventory_id.append(response['inventory_id'])
        if z % 100 == 0:
            time.sleep(5)

    total_inventory_id = [w for w in total_inventory_id if w is not None]

    print (total_inventory_id)

    x = 0
    y = 0
    tabla_1 = []
    total_data_available = []

    for inventory_id_value in total_inventory_id:
        r = requests.get(get_non_available_stock.format(str(inventory_id_value)), headers=header)
        if r.status_code != 200:
            print (r.status_code)
            print (r.content)
            continue
        response = r.json()

        registro = []
        registro.append(response["total"])
        registro.append(response["available_quantity"])
        registro.append(response["not_available_quantity"])
        registro.append(response["inventory_id"])
        registro.append(response["external_references"][0]["id"])
        registro.append(fecha_exec)
        tabla_1.append(registro)
        x = x+1
        if x % 100 == 0:
            time.sleep(5)

    columns_t1 = ["cantidad_total",
                "cantidad_disponible",
                "cantidad_no_disponible",
                "inventory_id",
                "product_id",
                "fecha"
                ]

    for inventory_id_value in total_inventory_id:
        r = requests.get(get_non_available_stock.format(inventory_id_value), headers=header)
        response = r.json()

        registro = []
        try:
            registro.append(response["total"])
            registro.append(response["available_quantity"])
            registro.append(response["inventory_id"])
            registro.append(response["external_references"][0]["id"])
            registro.append(fecha_exec)
        except Exception as e:
            print (str(e))
            print (r.status_code)
            print (r.content)
            continue
        #segundo nivel
        not_available_status = response.get("not_available_detail",[])
        for not_available in not_available_status:
            registro_2 = registro.copy()
            registro_2.append(not_available["status"])
            registro_2.append(not_available["quantity"])

            # tercer nivel
            conditions = not_available.get("conditions", [])
            for condition in conditions:
                registro_3 = registro_2.copy()
                registro_3.append(condition["condition"])
                registro_3.append(condition["quantity"])

                total_data_available.append(registro_3)

            if len(conditions) == 0:
                registro_2 = registro_2 + [None, None]
                total_data_available.append(registro_2)
        if len(not_available_status) == 0:
            registro = registro + [None,None,None,None]
            total_data_available.append(registro)
        
        y = y+1
        if y % 100 == 0:
            time.sleep(5)

    df_list = pd.DataFrame(tabla_1, columns=columns_t1)
    df_list['fecha'] = df_list['fecha'].astype(str)

    columns = ["cantidad_total", "cantidad_disponible",
                "inventory_id",
                "product_id",
                "fecha",
                "estado",
                "cantidad_no_disponible_estado",
                "condicion",
                "cantidad_condicion",
                ]

    df_tot = pd.DataFrame(total_data_available, columns=columns)
    df_tot['fecha'] = df_tot['fecha'].astype(str)

    columns_insert = ["cantidad_total",
                "cantidad_disponible",
                "cantidad_no_disponible",
                "inventory_id",
                "product_id",
                "fecha"]

    columns_insert_tot = ["cantidad_total", "cantidad_disponible",
                "inventory_id",
                "product_id",
                "fecha",
                "estado",
                "cantidad_no_disponible_estado",
                "condicion",
                "cantidad_condicion",
                ]

    columns_query = ",".join(columns_insert)
    values_query = ",".join(["%s" for column in columns_insert])
    df_list = df_list.fillna("NULL")
    records = list(df_list.to_records(index=False))

    fixed_records = []
    for record in records:
        fixed_record = []
        for value in record:
            if isinstance(value, np.generic):
                fixed_record.append(value.item())
            elif value == "NULL":
                fixed_record.append(None)
            else:
                fixed_record.append(value)
        fixed_records.append(tuple(fixed_record))
    print(f"Number of records to load: {str(len(fixed_records))}")
    incremental_query = """
        INSERT INTO ecommdata_meli.stock ("""+columns_query+""") 
        VALUES ("""+values_query+""")
        ON CONFLICT (product_id,inventory_id, fecha)
        DO NOTHING; 
    """

    print(incremental_query)
    pg_hook = PostgresHook(conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.executemany(incremental_query, fixed_records)
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    print("Data loaded to Postgres")

    #-----------------

    columns_query = ",".join(columns_insert_tot)
    values_query = ",".join(["%s" for column in columns_insert_tot])
    df_tot = df_tot.fillna("NULL")
    records = list(df_tot.to_records(index=False))

    fixed_records = []
    for record in records:
        fixed_record = []
        for value in record:
            if isinstance(value, np.generic):
                fixed_record.append(value.item())
            elif value == "NULL":
                fixed_record.append(None)
            else:
                fixed_record.append(value)
        fixed_records.append(tuple(fixed_record))
    print(f"Number of records to load: {str(len(fixed_records))}")
    incremental_query = """
        INSERT INTO ecommdata_meli.detalle_no_encontrado ("""+columns_query+""") 
        VALUES ("""+values_query+""")
    """

    print(incremental_query)
    pg_hook = PostgresHook(conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.executemany(incremental_query, fixed_records)
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    print("Data loaded to Postgres")



default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'etl_obtencion_stock_meli',
    default_args=default_args,
    description="Automatización de obtención de stock de MELI",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2022, 12, 21, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "unimarc", "ecommerce_data", "MELI", "obtencion stock"],
) as dag:

    dag.doc_md = """
    Obtención de stock en base a documento, transformarlos y \n
    exportar a tabla de BDD.
    """ 

    t0 = PythonOperator(
        task_id = "obtener_stock_general",
        python_callable = get_stock,
    )
