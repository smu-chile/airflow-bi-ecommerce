from airflow import DAG
from airflow import macros
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

from utils.slack_utils import dag_success_slack, dag_failure_slack

from datetime import datetime

import pendulum

def _load_json_to_s3(ts, ds):
    import requests
    import json
    import pandas as pd
    from io import StringIO
    import boto3 

    accountName = Variable.get("VTEX_ACCOUNT_NAME")
    env = Variable.get("VTEX_ENV")

    url = f"https://{accountName}.{env}.com.br/api/rnb/pvt/benefits/calculatorconfiguration"

    X_VTEX_API_AppKey = Variable.get("X_VTEX_API_AppKey")
    X_VTEX_API_AppToken = Variable.get("X_VTEX_API_AppToken")

    payload={}
    headers = {
    "X-VTEX-API-AppKey" : X_VTEX_API_AppKey,
    "X-VTEX-API-AppToken" :  X_VTEX_API_AppToken
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    res = json.loads(response.text)
    lista_lineas = []

    for linea in res['items']:
        id = linea['idCalculatorConfiguration']
        ultima_modificacion = linea['lastModifiedUtc']
        nombre = linea['name']
        fecha_inicio = linea['beginDate']
        fecha_fin = linea['endDate']
        activo = linea['isActive']
        descripcion = linea['description']
        try:
            valores_generales = linea['generalValues']
        except:
            valores_generales = None
        tipo = linea['type']
        estado = linea['status']
        archivado = linea['isArchived']
        tipo_efecto = linea['effectType']
        lista_lineas.append([id,ultima_modificacion,nombre,fecha_inicio,fecha_fin,activo,descripcion,valores_generales,tipo,estado,archivado,tipo_efecto])
    df = pd.DataFrame(lista_lineas, columns =['id','ultima_modificacion','nombre','fecha_inicio','fecha_fin','activo','descripcion','valores_generales','tipo','estado','archivado','tipo_efecto'])
    
    df = df.astype({
        "id": "string",
        "ultima_modificacion": "string",
        "nombre": "string",
        "fecha_inicio": "string",
        "fecha_fin": "string",
        "activo": "bool",
        "descripcion": "string",
        "valores_generales": "object",
        "tipo": "string",
        "estado": "string",
        "archivado": "bool",
        "tipo_efecto": "string"
    }, errors="ignore")
    
    curr_datetime = ts[:16].replace("-", "/").replace("T", "/").replace(":", "")
    file_name = f"vtex/promociones_vtex/{curr_datetime}_promociones_vtex.csv"
    buffer = StringIO()

    df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    buffer.seek(0)

    access_key = Variable.get("AWS_ACCESS_KEY")
    secret_key = Variable.get("AWS_SECRET_KEY")
    bucket_name = Variable.get("AWS_S3_BUCKET_NAME")
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name = "us-east-1"
    )
    response = s3_client.put_object(
        Bucket=bucket_name, Key=file_name, Body=buffer.getvalue()
    )

    return file_name

def _get_table_promociones_from_S3(ti):
    import pandas as pd

    promociones_file = ti.xcom_pull(key="return_value", task_ids=["load_json_to_s3"])[0]
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+promociones_file)
    if not s3_hook.check_for_key(promociones_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % promociones_file)

    promociones_object = s3_hook.get_key(promociones_file, bucket_name=s3_bucket)

    df = pd.read_csv(promociones_object.get()["Body"])
    print(f"Number of records found: {len(df.index)}")

    df = df.astype({
        "id": "string",
        "ultima_modificacion": "string",
        "nombre": "string",
        "fecha_inicio": "string",
        "fecha_fin": "string",
        "activo": "bool",
        "descripcion": "string",
        "valores_generales": "object",
        "tipo": "string",
        "estado": "string",
        "archivado": "bool",
        "tipo_efecto": "string"
    }, errors="ignore")

    return df

def _save_table_promociones(ts, ti, ds):
    import pandas as pd
    import sqlalchemy

    df = _get_table_promociones_from_S3(ti)
    df = df[['id','ultima_modificacion','nombre','fecha_inicio','fecha_fin','activo','descripcion','valores_generales','tipo','estado','archivado','tipo_efecto']]

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)
    df.to_sql(name="promociones_vtex",
            con=engine,         
            schema="ecommdata",         
            if_exists='append',         
            index=False,         
            chunksize=20000,         
            method='multi')

    return

def _load_vtex_id_list():
    query = """
        select pv.id
        from ecommdata.promociones_vtex pv
        where pv.activo is true
        """
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    pg_connection.close()
    return results

def get(url, responses, session, exception_cases, X_VTEX_API_AppKey, X_VTEX_API_AppToken):
    r = session.get(url, headers = {"X-VTEX-API-AppKey" : X_VTEX_API_AppKey, "X-VTEX-API-AppToken" : X_VTEX_API_AppToken})
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

def _load_promociones_detalle_vtex_to_S3(final_responses, ts, file_name):
    import pandas as pd
    import sqlalchemy
    import boto3
    from io import StringIO

    df = pd.DataFrame(final_responses)

    df = df[[
        "idCalculatorConfiguration",
        "name",
        "generalValues",
        "beginDateUtc",
        "endDateUtc",
        "lastModified",
        "isActive",
        "isArchived",
        "priceTableName",
        "brands",
        "products",
        "skus",
        "collections1BuyTogether",
        "collections2BuyTogether",
        "listSku1BuyTogether",
        "listSku2BuyTogether",
        "coupon",
        "maximumUnitPriceDiscount",
        "minimumQuantityBuyTogether",
        "quantityToAffectBuyTogether",
        "collections",
        "percentualDiscountValue",
        "accumulateWithManualPrice",
        "utmCampaign",
        "absoluteShippingDiscountValue",
        "nominalShippingDiscountValue",
        "percentualShippingDiscountValue"
    ]]

    df["idCalculatorConfiguration"] = df["idCalculatorConfiguration"].astype("str")
    df["name"] = df["name"].astype("str")
    df["beginDateUtc"] = df["beginDateUtc"].astype("str")
    df["endDateUtc"] = df["endDateUtc"].astype("str")
    df["lastModified"] = df["lastModified"].astype("str")
    df["isActive"] = df["isActive"].astype("bool")
    df["isArchived"] = df["isArchived"].astype("bool")
    df["accumulateWithManualPrice"] = df["accumulateWithManualPrice"].astype("bool")
    df["utmCampaign"] = df["utmCampaign"].astype("str")

    columns_rename = {
        "idCalculatorConfiguration" : "id",
        "name" : "nombre",
        "generalValues" : "valores_generales",
        "beginDateUtc": "fecha_inicio",
        "endDateUtc" : "fecha_fin",
        "lastModified" : "ultima_modificacion",
        "isActive" : "activo",
        "isArchived" : "archivado",
        "priceTableName" : "tabla_nombre_precio",
        "brands" : "marcas",
        "products" : "productos",
        "skus" : "skus",
        "collections1BuyTogether" : "collections1BuyTogether",
        "collections2BuyTogether" : "collections2BuyTogether",
        "listSku1BuyTogether" : "listSku1BuyTogether",
        "listSku2BuyTogether" : "listSku2BuyTogether",
        "coupon" : "cupon",
        "maximumUnitPriceDiscount": "maximumUnitPriceDiscount",
        "minimumQuantityBuyTogether": "minimumQuantityBuyTogether",
        "quantityToAffectBuyTogether": "quantityToAffectBuyTogether",
        "collections": "collections",
        "percentualDiscountValue": "percentualDiscountValue",
        "accumulateWithManualPrice": "accumulateWithManualPrice",
        "utmCampaign" : "campana_cupon",
        "absoluteShippingDiscountValue": "absoluteShippingDiscountValue",
        "nominalShippingDiscountValue": "nominalShippingDiscountValue",
        "percentualShippingDiscountValue": "percentualShippingDiscountValue"
    }

    df = df.rename(columns=columns_rename)

    curr_datetime = ts[:16].replace("-", "/").replace("T", "/").replace(":", "")
    file_name = f"vtex/promociones_detalle_vtex/{curr_datetime}_promociones_detalle_vtex.csv"
    buffer = StringIO()

    df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    buffer.seek(0)

    access_key = Variable.get("AWS_ACCESS_KEY")
    secret_key = Variable.get("AWS_SECRET_KEY")
    bucket_name = Variable.get("AWS_S3_BUCKET_NAME")
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name = "us-east-1"
    )
    response = s3_client.put_object(
        Bucket=bucket_name, Key=file_name, Body=buffer.getvalue()
    )

    return file_name

def _save_detalle_promociones_in_s3(ti, ts):
    import requests
    from threading import Thread
    import pandas as pd
    import sqlalchemy
    
    l_vtex_id = _load_vtex_id_list()

    if len(l_vtex_id) == 0:
        print('the list of vtex id was empty')
        return

    accountName = Variable.get("VTEX_ACCOUNT_NAME")
    env = Variable.get("VTEX_ENV")
    url_list = [f"https://{accountName}.{env}.com.br/api/rnb/pvt/calculatorconfiguration/{i[0]}" for i in l_vtex_id]
    
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
    
    final_responses = []

    for response in responses:
        try:
            aux = response['json']['idCalculatorConfiguration']
            final_responses.append(response['json'])
        except KeyError as e:
            print(e)
            print(response)
            exception_cases.append(response['url'])
    
    file_name = _load_promociones_detalle_vtex_to_S3(final_responses, ts, 'promociones_detalle_vtex')

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    date_path = ts[:10].replace("-","/")
    s3_path = f"vtex/api/get_stock_url_retries/{date_path}/"
    retries = s3_path+"retries"

    s3_hook.load_string(str(exception_cases),retries,bucket_name=s3_bucket,replace=True)
    ti.xcom_push(key = 'vtex_retries', value = retries)

    return file_name

def _get_table_detalle_promociones_from_S3(ti):
    import pandas as pd

    detalle_promociones_file = ti.xcom_pull(key="return_value", task_ids=["save_detalle_promociones_in_s3"])[0]
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+detalle_promociones_file)
    if not s3_hook.check_for_key(detalle_promociones_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % detalle_promociones_file)

    detalle_promociones_object = s3_hook.get_key(detalle_promociones_file, bucket_name=s3_bucket)

    df = pd.read_csv(detalle_promociones_object.get()["Body"])
    print(f"Number of records found: {len(df.index)}")

    df["id"] = df["id"].astype("str")
    df["nombre"] = df["nombre"].astype("str")
    df["fecha_inicio"] = df["fecha_inicio"].astype("str")
    df["fecha_fin"] = df["fecha_fin"].astype("str")
    df["ultima_modificacion"] = df["ultima_modificacion"].astype("str")
    df["activo"] = df["activo"].astype("bool")
    df["archivado"] = df["archivado"].astype("bool")

    return df

def _save_table_detalle_promociones(ts, ti, ds):
    import pandas as pd
    import sqlalchemy
    import ast
    import requests

    accountName = Variable.get("VTEX_ACCOUNT_NAME")
    env = Variable.get("VTEX_ENV")
    url = f"https://{accountName}.{env}.com.br/api/catalog/pvt/collection/"

    X_VTEX_API_AppKey = Variable.get("X_VTEX_API_AppKey")
    X_VTEX_API_AppToken = Variable.get("X_VTEX_API_AppToken")

    headers = {
    "X-VTEX-API-AppKey" : X_VTEX_API_AppKey,
    "X-VTEX-API-AppToken" :  X_VTEX_API_AppToken
    }

    df = _get_table_detalle_promociones_from_S3(ti)
    df = df[["id",
        "nombre",
        "valores_generales",
        "fecha_inicio",
        "fecha_fin",
        "ultima_modificacion",
        "activo",
        "archivado",
        "tabla_nombre_precio",
        "marcas",
        "productos",
        "skus",
        "collections1BuyTogether",
        "collections2BuyTogether",
        "listSku1BuyTogether",
        "listSku2BuyTogether",
        "cupon",
        "maximumUnitPriceDiscount",
        "minimumQuantityBuyTogether",
        "quantityToAffectBuyTogether",
        "collections",
        "percentualDiscountValue",
        "accumulateWithManualPrice",
        "campana_cupon",
        "absoluteShippingDiscountValue",
        "nominalShippingDiscountValue",
        "percentualShippingDiscountValue"
        ]]

    aux_list = []

    for ind in df.index:
        id = df['id'][ind]
        nombre_promocion = df['nombre'][ind]
        valores_generales = df['valores_generales'][ind]
        fecha_inicio = df['fecha_inicio'][ind]
        fecha_fin = df['fecha_fin'][ind]
        ultima_modificacion = df['ultima_modificacion'][ind]
        activo = df['activo'][ind]
        archivado = df['archivado'][ind]
        tabla_nombre_precio = df['tabla_nombre_precio'][ind]
        marcas = df['marcas'][ind]
        cupon = df['cupon'][ind]
        maxima_unidad_pd = df['maximumUnitPriceDiscount'][ind]
        min_cantidad_bt = df['minimumQuantityBuyTogether'][ind]
        cantidad_a_afectar_bt = df['quantityToAffectBuyTogether'][ind]
        valor_descuento_percentual = df['percentualDiscountValue'][ind]
        acumular_precio_fijo= df['accumulateWithManualPrice'][ind]
        campana_cupon = df['campana_cupon'][ind]
        for col in ['productos', 'skus', 'collections1BuyTogether', 'collections2BuyTogether', 'listSku1BuyTogether', 'listSku2BuyTogether', 'collections']:
            if len(df[col][ind]) == 0:
                total_carro = True
                vtex_id_producto = None
                nombre_producto = None
                vtex_id_sku = None
                nombre_sku = None
                vtex_id_coleccion = None
                nombre_coleccion = None
                print(id)
                tipo = "total_boleta"
                aux_list.append([id,nombre_promocion,valores_generales,fecha_inicio,fecha_fin,ultima_modificacion,activo,archivado,tabla_nombre_precio,marcas,cupon,vtex_id_producto,nombre_producto, vtex_id_sku, nombre_sku, tipo,maxima_unidad_pd,min_cantidad_bt,cantidad_a_afectar_bt,valor_descuento_percentual,acumular_precio_fijo,vtex_id_coleccion,nombre_coleccion,campana_cupon,total_carro,afecta_despacho])
            else:
                total_carro = False
        if "XXXX" in df['nombre'][ind]:
            total_carro = False
        if df['absoluteShippingDiscountValue'][ind] != 0 or df['nominalShippingDiscountValue'][ind] != 0 or df['percentualShippingDiscountValue'][ind] != 0 :
            afecta_despacho = True
        else:
            afecta_despacho = False
        for i in ast.literal_eval(df['productos'][ind]):
            vtex_id_producto = i.get('id',None)
            nombre_producto = i.get('name',None)
            vtex_id_sku = None
            nombre_sku = None
            vtex_id_coleccion = None
            nombre_coleccion = None
            tipo = "producto"
            aux_list.append([id,nombre_promocion,valores_generales,fecha_inicio,fecha_fin,ultima_modificacion,activo,archivado,tabla_nombre_precio,marcas,cupon,vtex_id_producto,nombre_producto, vtex_id_sku, nombre_sku, tipo,maxima_unidad_pd,min_cantidad_bt,cantidad_a_afectar_bt,valor_descuento_percentual,acumular_precio_fijo,vtex_id_coleccion,nombre_coleccion,campana_cupon,total_carro,afecta_despacho])
        for i in ast.literal_eval(df['skus'][ind]):
            vtex_id_producto = None
            nombre_producto = None
            vtex_id_sku = i.get('id',None)
            nombre_sku = i.get('name',None)
            tipo = "sku"
            vtex_id_coleccion = None
            nombre_coleccion = None
            aux_list.append([id,nombre_promocion,valores_generales,fecha_inicio,fecha_fin,ultima_modificacion,activo,archivado,tabla_nombre_precio,marcas,cupon,vtex_id_producto,nombre_producto, vtex_id_sku, nombre_sku, tipo,maxima_unidad_pd,min_cantidad_bt,cantidad_a_afectar_bt,valor_descuento_percentual,acumular_precio_fijo,vtex_id_coleccion,nombre_coleccion,campana_cupon,total_carro,afecta_despacho])
        for i in ast.literal_eval(df['collections1BuyTogether'][ind]):
            vtex_id_producto = i.get('id',None)
            nombre_producto = i.get('name',None)
            vtex_id_sku = None
            nombre_sku = None
            vtex_id_coleccion = None
            nombre_coleccion = None
            tipo = "collections1BuyTogether"
            aux_list.append([id,nombre_promocion,valores_generales,fecha_inicio,fecha_fin,ultima_modificacion,activo,archivado,tabla_nombre_precio,marcas,cupon,vtex_id_producto,nombre_producto, vtex_id_sku, nombre_sku, tipo,maxima_unidad_pd,min_cantidad_bt,cantidad_a_afectar_bt,valor_descuento_percentual,acumular_precio_fijo,vtex_id_coleccion,nombre_coleccion,campana_cupon,total_carro,afecta_despacho])
        for i in ast.literal_eval(df['collections2BuyTogether'][ind]):
            vtex_id_producto = i.get('id',None)
            nombre_producto = i.get('name',None)
            vtex_id_sku = None
            nombre_sku = None
            vtex_id_coleccion = None
            nombre_coleccion = None
            tipo = "collections2BuyTogether"
            aux_list.append([id,nombre_promocion,valores_generales,fecha_inicio,fecha_fin,ultima_modificacion,activo,archivado,tabla_nombre_precio,marcas,cupon,vtex_id_producto,nombre_producto, vtex_id_sku, nombre_sku, tipo,maxima_unidad_pd,min_cantidad_bt,cantidad_a_afectar_bt,valor_descuento_percentual,acumular_precio_fijo,vtex_id_coleccion,nombre_coleccion,campana_cupon,total_carro,afecta_despacho])
        for i in ast.literal_eval(df['listSku1BuyTogether'][ind]):
            vtex_id_producto = None
            nombre_producto = None
            vtex_id_sku = i.get('id',None)
            nombre_sku = i.get('name',None)
            vtex_id_coleccion = None
            nombre_coleccion = None
            tipo = "listSku1BuyTogether"
            aux_list.append([id,nombre_promocion,valores_generales,fecha_inicio,fecha_fin,ultima_modificacion,activo,archivado,tabla_nombre_precio,marcas,cupon,vtex_id_producto,nombre_producto, vtex_id_sku, nombre_sku, tipo,maxima_unidad_pd,min_cantidad_bt,cantidad_a_afectar_bt,valor_descuento_percentual,acumular_precio_fijo,vtex_id_coleccion,nombre_coleccion,campana_cupon,total_carro,afecta_despacho])
        for i in ast.literal_eval(df['listSku2BuyTogether'][ind]):
            vtex_id_producto = None
            nombre_producto = None
            vtex_id_sku = i.get('id',None)
            nombre_sku = i.get('name',None)
            vtex_id_coleccion = None
            nombre_coleccion = None
            tipo = "listSku2BuyTogether"
            aux_list.append([id,nombre_promocion,valores_generales,fecha_inicio,fecha_fin,ultima_modificacion,activo,archivado,tabla_nombre_precio,marcas,cupon,vtex_id_producto,nombre_producto, vtex_id_sku, nombre_sku, tipo,maxima_unidad_pd,min_cantidad_bt,cantidad_a_afectar_bt,valor_descuento_percentual,acumular_precio_fijo,vtex_id_coleccion,nombre_coleccion,campana_cupon,total_carro,afecta_despacho])
        for i in ast.literal_eval(df['collections'][ind]):
            vtex_id_producto = None
            nombre_producto = None
            vtex_id_sku = None
            nombre_sku = None
            vtex_id_coleccion = i.get('id',None)
            nombre_coleccion = i.get('name',None)
            tipo = "collections"
            max_retries = 3  # Set the maximum number of retries
            page=1
            df_collections = pd.DataFrame()
            retry_count = 0
            while True:
                params = {'pageSize': 1000, 'page': page}
                products_url = f'{url}{vtex_id_coleccion}/products'
                try:
                    response = requests.get(products_url, params=params, headers=headers)
                    response.raise_for_status()
                    if response.status_code == 200:
                        collection_skus = response.json()
                        df_collections = df_collections.append(pd.DataFrame(collection_skus))
                        if page < response.json()['TotalPage']:
                            page += 1
                        else:
                            break
                    else:
                        print(f"Request failed with status code {response.status_code}")
                        break
                except Exception as e:
                    print(e)
                    if retry_count < max_retries - 1:
                        retry_count += 1
                        print(f"Retrying... ({retry_count}/{max_retries})")
                    else:
                        print("Max retries reached. Exiting.")
                        break
            for index, row in df_collections.iterrows():
                data_column = row['Data']
                vtex_id_sku = data_column['SkuId']
                nombre_sku = data_column['ProductName']
                aux_list.append([id,nombre_promocion,valores_generales,fecha_inicio,fecha_fin,ultima_modificacion,activo,archivado,tabla_nombre_precio,marcas,cupon,vtex_id_producto,nombre_producto, vtex_id_sku, nombre_sku, tipo,maxima_unidad_pd,min_cantidad_bt,cantidad_a_afectar_bt,valor_descuento_percentual,acumular_precio_fijo,vtex_id_coleccion,nombre_coleccion,campana_cupon,total_carro,afecta_despacho])
        if str(tabla_nombre_precio) != 'nan':
            vtex_id_producto = None
            nombre_producto = None
            vtex_id_sku = None
            nombre_sku = None
            vtex_id_coleccion = None
            nombre_coleccion = None
            tipo = "tabla_nombre_precio"
            aux_list.append([id,nombre_promocion,valores_generales,fecha_inicio,fecha_fin,ultima_modificacion,activo,archivado,tabla_nombre_precio,marcas,cupon,vtex_id_producto,nombre_producto, vtex_id_sku, nombre_sku, tipo,maxima_unidad_pd,min_cantidad_bt,cantidad_a_afectar_bt,valor_descuento_percentual,acumular_precio_fijo,vtex_id_coleccion,nombre_coleccion,campana_cupon,total_carro,afecta_despacho])
        if marcas != '[]':
            vtex_id_producto = None
            nombre_producto = None
            vtex_id_sku = None
            nombre_sku = None
            vtex_id_coleccion = None
            nombre_coleccion = None
            tipo = "marcas"
            aux_list.append([id,nombre_promocion,valores_generales,fecha_inicio,fecha_fin,ultima_modificacion,activo,archivado,tabla_nombre_precio,marcas,cupon,vtex_id_producto,nombre_producto, vtex_id_sku, nombre_sku, tipo,maxima_unidad_pd,min_cantidad_bt,cantidad_a_afectar_bt,valor_descuento_percentual,acumular_precio_fijo,vtex_id_coleccion,nombre_coleccion,campana_cupon,total_carro,afecta_despacho])
        if str(campana_cupon) != 'nan':
            vtex_id_producto = None
            nombre_producto = None
            vtex_id_sku = None
            nombre_sku = None
            vtex_id_coleccion = None
            nombre_coleccion = None
            tipo = "cupon"
            aux_list.append([id,nombre_promocion,valores_generales,fecha_inicio,fecha_fin,ultima_modificacion,activo,archivado,tabla_nombre_precio,marcas,cupon,vtex_id_producto,nombre_producto, vtex_id_sku, nombre_sku, tipo,maxima_unidad_pd,min_cantidad_bt,cantidad_a_afectar_bt,valor_descuento_percentual,acumular_precio_fijo,vtex_id_coleccion,nombre_coleccion,campana_cupon,total_carro,afecta_despacho])

    df2 = pd.DataFrame(aux_list, columns = ['id','nombre_promocion','valores_generales','fecha_inicio','fecha_fin','ultima_modificacion','activo','archivado','tabla_nombre_precio','marcas','cupon','vtex_id_producto','nombre_producto','vtex_id_sku','nombre_sku','tipo','maxima_unidad_pd','min_cantidad_bt','cantidad_a_afectar_bt','valor_descuento_percentual','acumular_precio_fijo','vtex_id_coleccion','nombre_coleccion','campana_cupon','total_carro','afecta_despacho'])
        
    int_cols = [
        'vtex_id_producto', 
        'vtex_id_sku', 
        'vtex_id_coleccion', 
        'maxima_unidad_pd', 
        'min_cantidad_bt', 
        'cantidad_a_afectar_bt'
    ]
    for col in int_cols:
        if col in df2.columns:
            df2[col] = pd.to_numeric(df2[col], errors='coerce').astype('Int64')

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)
    df2.to_sql(name="promociones_detalle_vtex",
            con=engine,         
            schema="ecommdata",         
            if_exists='append',         
            index=False,         
            chunksize=20000,         
            method='multi')

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'etl_promociones_vtex',
    default_args=default_args,
    description="Extracción y carga de tablas promociones_vtex y promociones_detalle_vtex desde API.",
    schedule_interval="30 8,15 * * *",
    start_date=pendulum.datetime(2022, 10, 20, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["vtex", "promociones", "SERGIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Extracción y carga de tablas promociones_vtex y promociones_detalle_vtex desde API.
    """ 

    t0 = PostgresOperator(
        task_id = "truncate_promociones_vtex",
        postgres_conn_id="postgresql_conn",
        sql="""
        TRUNCATE ecommdata.promociones_vtex
        """,
    )

    t1 = PostgresOperator(
        task_id = "truncate_promociones_detalle_vtex",
        postgres_conn_id="postgresql_conn",
        sql="""
        TRUNCATE ecommdata.promociones_detalle_vtex
        """,
    )
    
    t2 = PythonOperator(
        task_id = "load_json_to_s3",
        python_callable = _load_json_to_s3
    )

    t3 = PythonOperator(
        task_id = "save_table_promociones",
        python_callable = _save_table_promociones
    )

    t4 = PythonOperator(
        task_id = "save_detalle_promociones_in_s3",
        python_callable = _save_detalle_promociones_in_s3
    )

    t5 = PythonOperator(
        task_id = "save_table_detalle_promociones",
        python_callable = _save_table_detalle_promociones
    )


t0 >> t1 >> t2 >> t3 >> t4 >> t5
