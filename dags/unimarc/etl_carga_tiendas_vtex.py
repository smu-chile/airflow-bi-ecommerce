from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

import pendulum

def query_to_df(query):
    import pandas as pd
    print(query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query)
    column_names = [desc[0] for desc in cursor.description]
    results = cursor.fetchall()
    results = pd.DataFrame(results, columns=column_names)
    print(results.head(20))
    cursor.close()
    pg_connection.close()
    return results

def get(url, responses, session, exception_cases, X_VTEX_API_AppKey, X_VTEX_API_AppToken):
    r = session.get(url, headers = {
        "X-VTEX-API-AppKey" : X_VTEX_API_AppKey, 
        "X-VTEX-API-AppToken" : X_VTEX_API_AppToken
        })
    try:
        responses.append({'json':r.json(), 'url':url})
    except Exception as e:
        print(e)
        print(url)
        print(r)
        print(r.status_code)
        exception_cases.append(url)

def bulk_get(url_sublist, responses, session, exception_cases, X_VTEX_API_AppKey, X_VTEX_API_AppToken):
    for url in url_sublist:
        get(url, responses, session, exception_cases, X_VTEX_API_AppKey, X_VTEX_API_AppToken)
    return

def post(url, responses, session, exception_cases, X_VTEX_API_AppKey, X_VTEX_API_AppToken):
    r = session.post(url, headers={
        "X-VTEX-API-AppKey": X_VTEX_API_AppKey,
        "X-VTEX-API-AppToken": X_VTEX_API_AppToken
    })
    try:
        responses.append({'json': r.json(), 'url': url})
    except Exception as e:
        print(e)
        print(url)
        print(r)
        print(r.status_code)
        exception_cases.append(url)

def bulk_post(url_sublist, responses, session, exception_cases, X_VTEX_API_AppKey, X_VTEX_API_AppToken):
    for url in url_sublist:
        post(url, responses, session, exception_cases, X_VTEX_API_AppKey, X_VTEX_API_AppToken)
    return


def delete(url, responses, session, exception_cases, X_VTEX_API_AppKey, X_VTEX_API_AppToken):
    r = session.delete(url, headers={
        "X-VTEX-API-AppKey": X_VTEX_API_AppKey,
        "X-VTEX-API-AppToken": X_VTEX_API_AppToken
    })
    try:
        responses.append({'json': r.json(), 'url': url})
    except Exception as e:
        print(e)
        print(url)
        print(r)
        print(r.status_code)
        exception_cases.append(url)

def bulk_delete(url_sublist, responses, session, exception_cases, X_VTEX_API_AppKey, X_VTEX_API_AppToken):
    for url in url_sublist:
        delete(url, responses, session, exception_cases, X_VTEX_API_AppKey, X_VTEX_API_AppToken)
    return

def carga_tiendas_to_s3(ds):
    import pandas as pd
    import io
    from threading import Thread
    from io import StringIO
    import requests

    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"carga_tiendas_vtex/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    query_tiendas_producto = f"""WITH tablatemporal AS (
                SELECT cp."refId" as ref_id, 
                UNNEST(STRING_TO_ARRAY(cp.stores, ',')) AS stores
                FROM ecommdata.carga_productos cp	
            )
            SELECT tt.ref_id AS ref_id, 
            p.vtex_id AS vtex_id FROM tablatemporal tt 
            LEFT JOIN ecommdata.productos p ON p.ref_id = tt.ref_id"""
    
    df = query_to_df(query_tiendas_producto)
    
    lista_ref_ids = df['vtex_id'].unique()
    print(f"cantidad de skus unicos: {len(lista_ref_ids)}")

    account_name = Variable.get("VTEX_ACCOUNT_NAME")  
    env = Variable.get("VTEX_ENV") 
    
    url_list = []
    for sku in lista_ref_ids:
        url = f"https://{account_name}.{env}.com.br/api/catalog/pvt/product/{str(int(sku))}/salespolicy"
        url_list.append(url)

    session = requests.session()
    thread_num = 40
    task_num = len(url_list)//thread_num # division entera
    adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=thread_num)
    session.mount('https://', adapter)
    thread_tasks = []
    count = 0
    responses = []
    exception_cases = []
   
    X_VTEX_API_AppKey = Variable.get("X_VTEX_API_AppKey")
    X_VTEX_API_AppToken = Variable.get("X_VTEX_API_AppToken")
    
    for thr in range(thread_num):
        new_task = Thread(target=bulk_get, args=[url_list[task_num*count:task_num*(count+1)], responses, session, exception_cases, X_VTEX_API_AppKey, X_VTEX_API_AppToken], daemon=True)
        new_task.start()
        thread_tasks.append(new_task)
        count = count + 1
    # tareas resagadas:
    if task_num*thread_num != len(url_list):
        new_task = new_task = Thread(target=bulk_get, args=[url_list[task_num*thread_num:], responses, session, exception_cases, X_VTEX_API_AppKey, X_VTEX_API_AppToken], daemon=True)
        new_task.start()
        thread_tasks.append(new_task)
    for task in thread_tasks:
        task.join()
    thread_tasks = []
    print(responses)
    
    final_responses = []

    for response in responses:
            response_aux = response['json']
            try:
                for item in response_aux:
                    aux = (item['ProductId'], item['StoreId'])
                    final_responses.append(aux)
            except KeyError as e:
                print(e)
                print(response)
                exception_cases.append(response['url'])
    
    df_tiendas_productos = pd.DataFrame(final_responses, columns=["ProductId", "StoreId"])
    print(df_tiendas_productos.head(30))
        
    buffer = io.StringIO()
    df_tiendas_productos.to_csv(buffer, header=True, index=False, encoding="utf-8")
    buffer.seek(0)

    filename = f"carga_tiendas_vtex/{exec_date}/carga_tiendas_{date_aux}.csv"

    print(f"con fecha {ds} y nombre de filename como {filename}")
    s3_hook.load_string(buffer.getvalue(),
                key=filename,
                bucket_name=s3_bucket,
                replace=True,
                encrypt=False)

    print("se logro transformar los dataframes a archivos .csv")
    print(f"File load on S3: {prefix}")

    return filename

def carga_tiendas_vtex_to_postgresql(ti):
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text

    filename = ti.xcom_pull(key="return_value", task_ids=["carga_tiendas_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+filename)
    if not s3_hook.check_for_key(filename, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % filename)

    s_stock_object = s3_hook.get_key(filename, bucket_name=s3_bucket)

    df = pd.read_csv(s_stock_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")

    df.columns = ["vtex_id","canal_venta_vtex"]
    df.info()

    query_tiendas = """select id as id_tienda, nombre_tienda, canal_venta_vtex
            from ecommdata.tiendas t
            where canal_venta_vtex is not null"""   
    df_tiendas = query_to_df(query_tiendas)

    query_productos = """select ref_id, vtex_id , nombre 
            from ecommdata.productos p 
            where vtex_id is not null
            and ref_id is not null"""
    df_productos = query_to_df(query_productos)

    df_final = pd.merge(df, df_tiendas, how="left", on = ["canal_venta_vtex"])
    df_final = pd.merge(df_final, df_productos,how="left", on = ["vtex_id"])

    df_final = df_final[["id_tienda","ref_id","vtex_id","canal_venta_vtex"]]

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    with engine.begin() as conn:
        df_final.to_sql(name="producto_tiendas_vtex",
                    con=conn,         
                    schema="ecommdata",         
                    if_exists='append',         
                    index=False,         
                    chunksize=20000,         
                    method='multi')

    print("Data saved to PostgreSQL.")

    return

def send_data_to_vtex(ti): 
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text
    import requests
    from threading import Thread

    filename = ti.xcom_pull(key="return_value", task_ids=["carga_tiendas_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+filename)
    if not s3_hook.check_for_key(filename, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % filename)

    s_stock_object = s3_hook.get_key(filename, bucket_name=s3_bucket)

    df = pd.read_csv(s_stock_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")

    account_name = Variable.get("VTEX_ACCOUNT_NAME") #Cambiar a PROD luego 
    env = Variable.get("VTEX_ENV") #Cambiar a PROD luego 

    df.columns = ["vtex_id","canal_venta_vtex"]
    df.info()

    #################################################################################################
    #                                                                                               #
    # Para probar, se harán pruebas en tienda 0398, con un set limitado de 09 productos.            #
    # Para comenzar la prueba, debo ir primero a BORRAR estos skus                                  #
    # (no debería hacerlo después) y luego cargarlos. Osea, debo confirmar si                       #
    # están cargados (if) y si no lo están, cargarlos, sino, borrarlos y cargarlos                  #
    # Values to test:                                                                               #
    # Canal de venta vtex: 28 // Canal de venta real: 0398 - Constitución I                         #
    #                                                                                               #
    # Productos:                                                                                    # 
    #  000000000000146169-UN	13752	Levadura seca Lefersa sobre 10 g                            #
    #  000000000000712974-UN	70463	Servilleta cóctel Abolengo una hoja 40 un                   #
    #  000000000000222694-UN	4857	Albahaca Gourmet sobre 6 g                                  #
    #  000000000401724017-UN	1789	Jugo en polvo Livean mango rinde 1 L                        #
    #  000000000645975001-UN	69604	Jugo en polvo Sprim durazno rinde 1 L                       #
    #  000000000000665063-UN	86741	Sal fina Nuestra Cocina bolsa 1 Kg                          #
    #  000000000000661552-UN	82503	Pimienta negra molida Nuestra Cocina 15 g                   #
    #  000000000000210017-UN	17870	Block de papel lustre Proarte 24 hojas                      #
    #  000000000645975005-UN	74295	Jugo en polvo Sprim piña rinde 1 L                          #
    #  000000000000561651-UN	5455	Jugo en polvo Vivo equillibrio arándano ciruela rinde 1 L   #
    #  000000000000672593-UN	89803	Mondadientes Bioly bolsa 100 un                             #
    #                                                                                               #
    # Vtex_id id_tienda_vtex                                                                        #
    # 13752	    28                                                                                  #
    # 40362	    28                                                                                  #
    # 4857	    28                                                                                  #
    # 1789	    28                                                                                  #
    # 69604	    28                                                                                  #
    # 86741	    28                                                                                  #
    # 82503	    28                                                                                  #
    # 17870	    28                                                                                  #
    # 74295	    28                                                                                  #
    # 5455      28                                                                                  #
    # 89803     28                                                                                  #
    #################################################################################################

    id_tienda_filtrada = 28
    lista_ref_ids_a_filtrar = [13752, 40362, 4857, 1789, 69604, 86741, 82503, 17870, 74295, 5455, 89803]
    df = df[df["canal_venta_vtex"] == id_tienda_filtrada]
    df = df[df["vtex_id"].isin(lista_ref_ids_a_filtrar)]
    
    post_urls = []
    delete_urls = []

    for index, row in df.iterrows():
        sku = row["vtex_id"]
        sales_policy = row["canal_venta_vtex"]
        url = f"https://{account_name}.{env}.com.br/api/catalog/pvt/product/{str(int(sku))}/salespolicy/{str(int(sales_policy))}"
        post_urls.append(url)
        delete_urls.append(url)
    
    
    session = requests.session()
    thread_num = 3
    task_num = len(post_urls)//thread_num # division entera
    adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=thread_num)
    session.mount('https://', adapter)
    thread_tasks = []
    count = 0
    responses = []
    exception_cases = []
   
    X_VTEX_API_AppKey = Variable.get("X_VTEX_API_AppKey")  
    X_VTEX_API_AppToken = Variable.get("X_VTEX_API_AppToken")      

    # Primero, lanzar los DELETE antes de los POST
    delete_thread_tasks = []
    count = 0
    for thr in range(thread_num):
        new_task = Thread(target=bulk_delete, args=[delete_urls[task_num*count:task_num*(count+1)], responses, session, exception_cases, X_VTEX_API_AppKey, X_VTEX_API_AppToken], daemon=True)
        new_task.start()
        delete_thread_tasks.append(new_task)
        count = count + 1
    # tareas rezagadas:
    if task_num*thread_num != len(delete_urls):
        new_task = Thread(target=bulk_delete, args=[delete_urls[task_num*thread_num:], responses, session, exception_cases, X_VTEX_API_AppKey, X_VTEX_API_AppToken], daemon=True)
        new_task.start()
        delete_thread_tasks.append(new_task)
    for task in delete_thread_tasks:
        task.join()
    delete_thread_tasks = []
    print("Delete terminado")

    count = 0    
    for thr in range(thread_num):
        new_task = Thread(target=bulk_post, args=[post_urls[task_num*count:task_num*(count+1)], responses, session, exception_cases, X_VTEX_API_AppKey, X_VTEX_API_AppToken], daemon=True)
        new_task.start()
        thread_tasks.append(new_task)
        count = count + 1
    # tareas resagadas:
    if task_num*thread_num != len(post_urls):
        new_task = new_task = Thread(target=bulk_post, args=[post_urls[task_num*thread_num:], responses, session, exception_cases, X_VTEX_API_AppKey, X_VTEX_API_AppToken], daemon=True)
        new_task.start()
        thread_tasks.append(new_task)
    for task in thread_tasks:
        task.join()
    thread_tasks = []
    print(responses)

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_carga_tiendas_vtex',
    default_args=default_args,
    description="Carga y elimina tradePolicy de tiendas a los productos en vtex",
    schedule_interval="0 10 * * *",
    start_date=pendulum.datetime(2024, 7, 30, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "tiendas", "Productos", "ecommdata", "VTEX", "unimarc", "PATRICIO"],
) as dag:
    

    dag.doc_md = """
    Elimina y carga tradePolicy de tiendas a los productos en vtex\n
    guardar en S3.
    """ 

    t0 = PythonOperator(
        task_id = 'carga_tiendas_to_s3',
        python_callable=carga_tiendas_to_s3,
    )

    t1 = PostgresOperator(
        task_id="truncate_tiendas_vtex",
        postgres_conn_id="postgresql_conn",
        sql="""TRUNCATE ecommdata.producto_tiendas_vtex;"""
    )

    t2 = PythonOperator(
        task_id = "carga_tiendas_vtex_to_postgresql",
        python_callable = carga_tiendas_vtex_to_postgresql,
    )

    t3 = PythonOperator(
        task_id = "send_data_to_vtex",
        python_callable = send_data_to_vtex,
    )

    t0 >> t1 >> t2 >> t3