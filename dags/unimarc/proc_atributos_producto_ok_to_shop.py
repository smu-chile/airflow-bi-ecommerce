from airflow import DAG
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

from utils.postgres_utils import is_empty_table
from utils.postgres_utils import get_max_updated_at_value
from datetime import datetime, timedelta

import pendulum

def get(url, responses, session, exception_cases, ok_to_shop_api_key):
    r = session.get(url, headers = {"x-auth-token" : ok_to_shop_api_key,"okts-lat" : "0","okts-lon" : "0"})
    try:
        responses.append({'json':r.json(), 'url':url})
    except Exception as e:
        print(e)
        print(url)
        print(r)
        print(r.status_code)
        exception_cases.append(url)


def bulk_get(url_sublist, responses, session, exception_cases, ok_to_shop_api_key):
    for url in url_sublist:
        get(url, responses, session, exception_cases, ok_to_shop_api_key)
    return

def _evaluate_full_load(ti, schema, table_name):
    if is_empty_table(schema, table_name):
        ti.xcom_push(key="load_method", value="full_load")
        return "load_full_table_to_s3"
    else:
        ti.xcom_push(key="load_method", value="incremental_load")
        return "get_max_updated_at_value"
    
def full_load_ok_to_shop_table_to_s3(ds):
    import requests
    import pandas as pd
    import numpy as np
    import io
    from threading import Thread

    ok_to_shop_url = Variable.get("OK_TO_SHOP_URL")
    ok_to_shop_api_key = Variable.get("OK_TO_SHOP_API_KEY")

    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"ok_to_shop/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    headers = {
        "x-auth-token" : ok_to_shop_api_key,
        "okts-lat" : "0",
        "okts-lon" : "0",
        "Connection" : "keep-alive"
    }
    
    i=1
    df_oktoshop = pd.DataFrame()

    while True:
        url = f"{ok_to_shop_url}/products?page={i}&pageSize=10000&since=0&showIdentifiers=1"
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            page_data = response.json()
            page_df = pd.DataFrame(page_data["response"])
            page_df['barcode'] = page_df['identifiers'].apply(lambda x: x[0]['value'] if len(x) > 0 else None)
            selected_columns = ['id', 'description', 'lastUpdate', 'barcode']
            page_df = page_df[selected_columns]
            df_oktoshop = pd.concat([df_oktoshop, page_df], ignore_index=True)
            if page_data["pagination"]["hasMore"] == True:
                i += 1
            else:
                break
        else:
            print(f"Error fetching page {i}. Status code: {response.status_code}")
            break
    
    sku_query = """select ref_id,
                ean_primario,
                nombre_sku
                from ecommdata.skus"""
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(sku_query)
    skus = cursor.fetchall()
    skus=pd.DataFrame(skus)
    skus.columns = ["ref_id","ean_primario","nombre_sku"]

    df_oktoshop = pd.merge(df_oktoshop, skus, left_on='barcode', right_on='ean_primario', how='inner')
    df_oktoshop.drop(columns=['ean_primario'], inplace=True)

    url_list = []

    for index, row in df_oktoshop.iterrows():
        id_oktoshop = row["id"]
        url_list.append(f"{ok_to_shop_url}/products/{id_oktoshop}")

    session = requests.session()
    thread_num = 2
    task_num = len(url_list)//thread_num # division entera
    adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=thread_num)
    session.mount('https://', adapter)
    thread_tasks = []
    count = 0
    responses = []
    exception_cases = []

    for thr in range(thread_num):
        new_task = Thread(target=bulk_get, args=[url_list[task_num*count:task_num*(count+1)], responses, session, exception_cases, ok_to_shop_api_key], daemon=True)
        new_task.start()
        thread_tasks.append(new_task)
        count = count + 1
    # tareas resagadas:
    if task_num*thread_num != len(url_list):
        new_task = new_task = Thread(target=bulk_get, args=[url_list[task_num*thread_num:], responses, session, exception_cases, ok_to_shop_api_key], daemon=True)
        new_task.start()
        thread_tasks.append(new_task)
    for task in thread_tasks:
        task.join()
        thread_tasks = []
    
    columns = ['ean','fecha_creacion','ultima_modificacion', 'libre_lacteos', 'libre_lactosa', 'libre_gluten', 'libre_tacc',
           'libre_soya', 'libre_huevos', 'libre_peces', 'libre_mariscos', 'libre_frutos_secos',
           'libre_mani', 'libre_nueces_arbol', 'libre_sulfitos', 'vegano', 'vegetariano', 'halal',
           'kosher', 'basado_plantas', 'libre_transgenicos', 'organico', 'carbono_neutral',
           'libre_maltrato_animal', 'comercio_justo', 'marca_chile']
    data = {col: [] for col in columns}
    df = pd.DataFrame(data)
    exception_cases = []

    df['ean'] = df['ean'].astype('int8')
    df['fecha_creacion'] = df['fecha_creacion'].astype('int8')
    df['ultima_modificacion'] = df['ultima_modificacion'].astype('int8')
    for col in columns[3:]:
        df[col] = df[col].astype('boolean')

    suitability_to_variable = {
        "dairy_free": "libre_lacteos",
        "lactose_free": "libre_lactosa",
        "gluten_free": "libre_gluten",
        "tacc_free": "libre_tacc",
        "soy_free": "libre_soya",
        "egg_free": "libre_huevos",
        "fish_free": "libre_peces",
        "seafood_free": "libre_mariscos",
        "nuts_free": "libre_nueces",
        "peanut_free": "libre_mani",
        "walnuts_free": "libre_nueces_arbol",
        "sulphite_free": "libre_sulfitos",
        "vegan": "vegano",
        "vegetarian": "vegetariano",
        "plant_based": "basado_plantas",
        "halal": "halal",
        "kosher": "kosher",
        "plant_based": "basado_plantas",
        "non_gmo": "libre_transgenicos",
        "organic": "organico",
        "carbon_neutral": "carbono_neutral",
        "cruelty_free": "libre_maltrato_animal",
        "fair_trade": "comercio_justo",
        "marca_chile": "marca_chile"
    }

    for response in responses:
        try:
            body = response['json']['response']  
            ean = body["identifiers"][0]["value"]
            in_date = body["chronology"]["timestampIn"]
            last_modified = body["chronology"]["lastUpdate"]
            variables = {var: False for var in columns[3:]}
            for suitability in body["suitability"]:
                if suitability["declaredBy"][0]["degreeId"] >= 2:
                    variable_name = suitability_to_variable.get(suitability["code"])
                    if variable_name:
                        variables[variable_name] = True
            df = df.append({'ean': ean, 'fecha_creacion': in_date, 'ultima_modificacion': last_modified, **variables}, ignore_index=True)
        except KeyError as e:
            print(e)
            print(response)
            exception_cases.append(response['url'])
    print(df)
    print(df.info())
    
    buffer = io.StringIO()
    df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"ok_to_shop/{exec_date}/ok_to_shop_{date_aux}.csv"
    buffer.seek(0)
    print("se logro transformar el dataframe a un archivo .csv")
    print(f"con fecha {ds} y nombre de filename como {filename}")
    s3_hook.load_string(buffer.getvalue(),
                key=filename,
                bucket_name=s3_bucket,
                replace=True,
                encrypt=False)
    print(f"File load on S3: {prefix}")

    return filename

def incremental_load_to_s3(ti,ds):
    import requests
    import io
    import numpy as np
    import pandas as pd

    max_updated_at_value = ti.xcom_pull(key="return_value", task_ids=["get_max_updated_at_value"])[0]
    ok_to_shop_url = Variable.get("OK_TO_SHOP_URL")
    ok_to_shop_api_key = Variable.get("OK_TO_SHOP_API_KEY")

    print(max_updated_at_value)

    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"ok_to_shop/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    headers = {
        "x-auth-token" : ok_to_shop_api_key,
        "okts-lat" : "0",
        "okts-lon" : "0",
        "Connection" : "keep-alive"
    }
    
    i=1
    df_oktoshop = pd.DataFrame()
    print("df initialized")

    while True:
        url = f"{ok_to_shop_url}/products?page={i}&pageSize=10000&since={max_updated_at_value + 1}&showIdentifiers=1"
        print (url)
        response = requests.get(url, headers=headers)
        print(response.status_code)
        if response.status_code == 200:
            page_data = response.json()
            page_df = pd.DataFrame(page_data["response"])
            page_df['barcode'] = page_df['identifiers'].apply(lambda x: x[0]['value'] if len(x) > 0 else None)
            selected_columns = ['id', 'description', 'lastUpdate', 'barcode']
            page_df = page_df[selected_columns]
            df_oktoshop = pd.concat([df_oktoshop, page_df], ignore_index=True)
            print(i)
            if page_data["pagination"]["hasMore"] == True:
                i += 1
            else:
                break
        else:
            print(f"Error fetching page {i}. Status code: {response.status_code}")
            break
    print(df_oktoshop.info())

    if df_oktoshop.empty:
        return 'rows_not_found'

    sku_query = """select ref_id,
                ean_primario,
                nombre_sku
                from ecommdata.skus"""
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(sku_query)
    skus = cursor.fetchall()
    skus=pd.DataFrame(skus)
    skus.columns = ["ref_id","ean_primario","nombre_sku"]

    df_oktoshop = pd.merge(df_oktoshop, skus, left_on='barcode', right_on='ean_primario', how='inner')
    df_oktoshop.drop(columns=['ean_primario'], inplace=True)

    if df_oktoshop.empty:
        return 'rows_not_found'

    columns = ['ean','fecha_creacion','ultima_modificacion', 'libre_lacteos', 'libre_lactosa', 'libre_gluten', 'libre_tacc',
           'libre_soya', 'libre_huevos', 'libre_peces', 'libre_mariscos', 'libre_frutos_secos',
           'libre_mani', 'libre_nueces_arbol', 'libre_sulfitos', 'vegano', 'vegetariano', 'halal',
           'kosher', 'basado_plantas', 'libre_transgenicos', 'organico', 'carbono_neutral',
           'libre_maltrato_animal', 'comercio_justo', 'marca_chile']
    data = {col: [] for col in columns}
    df = pd.DataFrame(data)

    df['ean'] = df['ean'].astype('int8')
    df['fecha_creacion'] = df['fecha_creacion'].astype('int8')
    df['ultima_modificacion'] = df['ultima_modificacion'].astype('int8')
    for col in columns[3:]:
        df[col] = df[col].astype('boolean')

    suitability_to_variable = {
        "dairy_free": "libre_lacteos",
        "lactose_free": "libre_lactosa",
        "gluten_free": "libre_gluten",
        "tacc_free": "libre_tacc",
        "soy_free": "libre_soya",
        "egg_free": "libre_huevos",
        "fish_free": "libre_peces",
        "seafood_free": "libre_mariscos",
        "nuts_free": "libre_nueces",
        "peanut_free": "libre_mani",
        "walnuts_free": "libre_nueces_arbol",
        "sulphite_free": "libre_sulfitos",
        "vegan": "vegano",
        "vegetarian": "vegetariano",
        "plant_based": "basado_plantas",
        "halal": "halal",
        "kosher": "kosher",
        "plant_based": "basado_plantas",
        "non_gmo": "libre_transgenicos",
        "organic": "organico",
        "carbon_neutral": "carbono_neutral",
        "cruelty_free": "libre_maltrato_animal",
        "fair_trade": "comercio_justo",
        "marca_chile": "marca_chile"
    }

    for index, row in df_oktoshop.iterrows():
        id_oktoshop = row["id"]
        url = f"{ok_to_shop_url}/products/{id_oktoshop}"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            body = response.json()['response']
            ean = body["identifiers"][0]["value"]
            in_date = body["chronology"]["timestampIn"]
            last_modified = body["chronology"]["lastUpdate"]
            variables = {var: False for var in columns[3:]}
            for suitability in body["suitability"]:
                if suitability["declaredBy"][0]["degreeId"] >= 2:
                    variable_name = suitability_to_variable.get(suitability["code"])
                    if variable_name:
                        variables[variable_name] = True
            df = df.append({'ean': ean, 'fecha_creacion': in_date, 'ultima_modificacion': last_modified, **variables}, ignore_index=True)
        else:
            print(f"Error fetching {id_oktoshop}, Status code: {response.status_code}")
            break
    print(df)
    print(df.info())

    buffer = io.StringIO()
    df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"ok_to_shop/{exec_date}/ok_to_shop_{date_aux}.csv"
    buffer.seek(0)
    print("se logro transformar el dataframe a un archivo .csv")
    print(f"con fecha {ds} y nombre de filename como {filename}")
    s3_hook.load_string(buffer.getvalue(),
                key=filename,
                bucket_name=s3_bucket,
                replace=True,
                encrypt=False)
    print(f"File load on S3: {prefix}")

    return filename

def rows_not_found(ds):
    print(f"no updates found at: {ds}")
    return

def load_oktoshop_table_to_postgres(ti):
    import pandas as pd
    import numpy as np

    load_method = ti.xcom_pull(key="load_method", task_ids=["evaluate_full_load"])[0]
    print(f"Load method: {load_method}")
    if load_method == "full_load":
        filename = ti.xcom_pull(key="return_value", task_ids=["load_full_table_to_s3"])[0]
    else:
        filename = ti.xcom_pull(key="return_value", task_ids=["incremental_unixtime_load_table_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+filename)
    if not s3_hook.check_for_key(filename, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % filename)

    s_oktoshop_object = s3_hook.get_key(filename, bucket_name=s3_bucket)

    df = pd.read_csv(s_oktoshop_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")

    columns = ['fecha_creacion','ultima_modificacion', 'libre_lacteos', 'libre_lactosa', 'libre_gluten', 'libre_tacc',
           'libre_soya', 'libre_huevos', 'libre_peces', 'libre_mariscos', 'libre_frutos_secos',
           'libre_mani', 'libre_nueces_arbol', 'libre_sulfitos', 'vegano', 'vegetariano', 'halal',
           'kosher', 'basado_plantas', 'libre_transgenicos', 'organico', 'carbono_neutral',
           'libre_maltrato_animal', 'comercio_justo', 'marca_chile']

    columns_query = ",".join(columns)
    excluded_query = ",".join(["EXCLUDED."+column for column in columns])
    values_query = "%s,"+",".join(["%s" for column in columns])
    records = list(df.to_records(index=False))

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
            INSERT INTO ecommdata.ok_to_shop (ean,"""+columns_query+""") 
            VALUES ("""+values_query+""")
            ON CONFLICT (ean)
            DO UPDATE SET ("""+columns_query+""") = ("""+excluded_query+""") 
        """
    print(incremental_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.executemany(incremental_query, fixed_records)
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    print("Data loaded to Postgres")
    return

def set_janis_atributos(ti):
    import requests
    import pandas as pd

    stock_tiendas_query = """select s.ref_id,
        ots.libre_lacteos,
        ots.libre_lactosa,
        ots.libre_gluten,
        ots.libre_tacc,
        ots.libre_soya,
        ots.libre_huevos,
        ots.libre_peces,
        ots.libre_mariscos,
        ots.libre_frutos_secos,
        ots.libre_mani,
        ots.libre_nueces_arbol,
        ots.libre_sulfitos,
        ots.vegano,
        ots.vegetariano,
        ots.halal,
        ots.kosher,
        ots.basado_plantas,
        ots.libre_transgenicos,
        ots.organico,
        ots.carbono_neutral,
        ots.libre_maltrato_animal,
        ots.comercio_justo,
        ots.marca_chile
    from ecommdata.ok_to_shop ots
    left join ecommdata.skus s on s.ean_primario::text = ots.ean::text"""
    print(stock_tiendas_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(stock_tiendas_query)
    df_ots = cursor.fetchall()
    df_ots=pd.DataFrame(df_ots)
    print(df_ots)
    df_ots.columns = ["ref_id","libre_lacteos","libre_lactosa","libre_gluten","libre_tacc","libre_soya","libre_huevos",
                       "libre_peces","libre_mariscos","libre_frutos_secos","libre_mani","libre_nueces_arbol","libre_sulfitos","vegano",
                       "vegetariano","halal","kosher","basado_plantas","libre_transgenicos","organico","carbono_neutral","libre_maltrato_animal",
                       "comercio_justo","marca_chile"]
    cursor.close()
    pg_connection.close()

    df_ots = df_ots.rename(columns={
        "ref_id": "ref_id",
        "libre_lacteos": "Libre de Lácteos",
        "libre_lactosa": "Libre de Lactosa",
        "libre_gluten": "Libre de Gluten",
        "libre_tacc": "Libre de TACC",
        "libre_soya": "Libre de Soya",
        "libre_huevos": "Libre de Huevo",
        "libre_peces": "Libre de Peces",
        "libre_mariscos": "Libre de Mariscos",
        "libre_frutos_secos": "Libre de Frutos Secos",
        "libre_mani": "Libre de Maní",
        "libre_nueces_arbol": "Libre de Nuez",
        "libre_sulfitos": "Libre de Sulfitos",
        "vegano": "Vegano",
        "vegetariano": "Vegetariano",
        "halal": "Halal",
        "kosher": "kosher",
        "basado_plantas": "Basado en Plantas",
        "libre_transgenicos": "Libre de Transgénicos",
        "organico": "Orgánico",
        "carbono_neutral": "Carbono Neutral",
        "libre_maltrato_animal": "Libre de Maltrato Animal",
        "comercio_justo": "Comercio Justo",
        "marca_chile": "Marca Chile"
    })

    print(df_ots.info())

    API_JANIS = Variable.get("JANIS_API_URL")
    headers = {
        "janis-api-key": Variable.get("JANIS_API_KEY"),
        "janis-api-secret": Variable.get("JANIS_API_SECRET"),
        "janis-client": Variable.get("JANIS_CLIENT"),
        "Connection": "keep-alive"}

    jst = []
    for index, row in df_ots.iterrows():
        item = {
            "item_id": row["ref_id"],
            "attributes": []
        }

        values = [col for col in df_ots.columns[1:] if row[col]]
        
        if values:
            attribute = {
                "id": str(Variable.get("JANIS_REF_ID_ATRIBUTO_ID_SELLOS")),
                "values": values
            }
            item["attributes"].append(attribute)

        jst.append(item)

    print(jst)

    lim_json = 500
    total_size = len(jst)
    if total_size > lim_json:
        jst = [jst[i:i+lim_json] for i in range(0, len(jst), lim_json)]
    else:
        jst = [jst]

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
    'proc_janis_attributes_product_ok_to_shop',
    default_args=default_args,
    description=""" With the extractions of .csv files from ftp connection, it's been made
    an update of catalogo.ok_to_shop table, and an insert of attributes of products that match EAN's 
    of sku_ean and skus using the API of Janis attribute_value. After this, we hope to observe
    atributos_producto table updated.""",

    schedule_interval="0 10 * * *",
    start_date=pendulum.datetime(2023, 5, 21, tz="America/Santiago"),
    catchup=False,
    tags=["API", "Janis", "ok_to_shop", 'atributos', 'atributos_producto'],
) as dag:

    dag.doc_md = """
    Extraction and insert of attributes from ok_to_shop API to Janis.
    """

    t0 = BranchPythonOperator(
        task_id = "evaluate_full_load",
        python_callable = _evaluate_full_load,
        op_kwargs = {
            "schema": "ecommdata",
            "table_name": "ok_to_shop"
        }
    )

    t1 = PythonOperator(
        task_id = "full_load_ok_to_shop_table_to_s3",
        python_callable = full_load_ok_to_shop_table_to_s3,
    )

    t2 = PythonOperator(
        task_id = "get_max_updated_at_value",
        python_callable = get_max_updated_at_value,
        op_kwargs = {
            "schema": "ecommdata",
            "table_name": "ok_to_shop", 
            "updated_at_field": "ultima_modificacion",
            "is_unixtime": True
        }
    )
    
    t3 =  BranchPythonOperator(
        task_id = "incremental_load_to_s3",
        python_callable = incremental_load_to_s3,
    )

    t3_none = PythonOperator(
        task_id = "rows_not_found",
        python_callable = rows_not_found,
    )

    t4 = PythonOperator(
        task_id = "load_oktoshop_table_to_postgres",
        python_callable = load_oktoshop_table_to_postgres,
    )
    

    t0 >> t1 >> t4
    t0 >> t2 >> t3 >> t4
    t3 >> t3_none

