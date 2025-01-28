from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator

import pendulum

from datetime import datetime, timedelta

def _check_time(ts):
    
    exec_datetime = datetime.strptime(ts[:16], "%Y-%m-%dT%H:%M")
    exec_datetime_utc = pendulum.timezone("utc").convert(exec_datetime)
    local_tz = pendulum.timezone("America/Santiago")
    exec_datetime_local = local_tz.convert(exec_datetime_utc)
    exec_datetime_local_str = exec_datetime_local.strftime("%Y-%m-%dT%H:%M")
    print(exec_datetime_local_str)

    time_str = exec_datetime_local_str.split("T")[1]
    if (time_str == "17:30") or (time_str == "21:30") or (time_str == "01:30") or (time_str == "13:30"):
        return "task_skip"
    elif (time_str == "05:30"):
        return "stock_ventas_tiendas_to_s3_am"
    else:
        return "stock_ventas_tiendas_to_s3_pm"

def stock(ds):
    import pandas as pd
    stock_tiendas_query = f"""select distinct c.*, date_part('dow','{ds}'::date) as dia, date_part('week','{ds}'::date) as semana
                    from(select pt.ref_id, pt.id_tienda
                            from ecommdata.productos_tienda pt
                            left join ecommdata.tiendas t on t.id = pt.id_tienda 
                            where pt.id_tienda not in ('9212', '1917')
                            and t.status = 1
                            union
                            select distinct
                            "refId" as ref_id,
                            unnest(string_to_array(stores, ',')) AS id_tienda
                            from ecommdata.carga_productos cp) as c
                            where c.id_tienda not in ('9212', '1917');
                            """
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    print(stock_tiendas_query)
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(stock_tiendas_query)
    results = cursor.fetchall()
    results=pd.DataFrame(results)
    results.columns = ["ref_id","id_tienda","dia","semana"]
    results.info()
    cursor.close()
    pg_connection.close()
    return results

def matriz_ss():
    import pandas as pd
    matriz_query = """select *
                    from catalogo.matriz_ss ms """
    print(matriz_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(matriz_query)
    results = cursor.fetchall()
    results=pd.DataFrame(results)
    results.columns = ["id_tienda","peso"]
    cursor.close()
    pg_connection.close()
    results.info()
    return results

def venta_tienda(ds):
    import pandas as pd
    ventas_skus_tienda_query = f"""select LPAD(v.id_tienda , 4, '0') as id_tienda,
                    CONCAT(LPAD(v.material, 18, '0'), '-', v.umv) as ref_id,
                    v.venta_umv as cantidad,
                    date_part('dow',v.fecha)::int as dia,
                    date_part('week',v.fecha)::int as semana
                    from ecommdata.venta_sku_tienda as v
                    left join ecommdata.tiendas as t
                    on LPAD(v.id_tienda , 4, '0') = t.id
                    where v.fecha >= '{ds}'::date -70
                    and v.organizacion = 'Unimarc'
                    and v.id_tienda <>'1917'
                    """
    print(ventas_skus_tienda_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(ventas_skus_tienda_query)
    results = cursor.fetchall()
    results=pd.DataFrame(results)
    results.columns = ["id_tienda","ref_id","cantidad","dia","semana"]
    results.info()
    cursor.close()
    pg_connection.close()
    return results

def minimos_exhibicion():
    import pandas as pd
    query = """select concat(meio.material,'-',meio.umv) as ref_id, meio.id_tienda, meio.minimo_exhibicion
                from ecommdata.minimos_exhibicion_in_out meio
                left join ecommdata.tiendas t 
                on t.id = meio.id_tienda
                left join ecommdata.lista8 l 
                on l.material = meio.material and l.umv = meio.umv  and l.id_tienda = meio.id_tienda 
                where t.status = 1
                and l.material  is not null
                and l.id_tienda <> '1917'
                and l.id_tienda  is not null
                and l.umv  is not null
                and t.id is not null
                and meio.minimo_exhibicion > 2"""
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

def stock_ventas_tiendas_to_s3_am(ds):
    import pandas as pd
    import numpy as np
    import io
    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"stock_seguridad/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    df_stock = stock(ds)
    print("se ha cargado stock\n")
    print(df_stock)

    df_venta_tienda = venta_tienda(ds)
    print("se ha cargado ventas\n")
    print(df_venta_tienda)
    
    print("\nse ha terminado de extraer data \n")

    #########################
    #transformacion de datos#
    #########################

    fecha_str = ds
    formato_str = "%Y-%m-%d"

    dia = datetime.strptime(fecha_str, formato_str) 
    dia = dia.weekday()
    dia = (dia + 1) % 7

    #filtramos columnas necesarias de dataframe de ventas
    df_venta_tienda = df_venta_tienda[["id_tienda","ref_id","cantidad","dia","semana"]]

    #sumamos venta umv a nivel tienda, sku, dia, semana
    df_aux1 = df_venta_tienda.groupby(by=["id_tienda","ref_id","dia","semana"], as_index=False).sum()

    #promediamos venta a nivel tienda,sku, dia
    df_aux2 = df_aux1.groupby(by=["id_tienda","ref_id","dia"], as_index=False).mean()

    #filtramos columnas necesarias de venta con la venta promedio de dia de la semana
    df_aux2 = df_aux2[["id_tienda","ref_id","dia","semana","cantidad"]]
    df_aux2["cantidad"] = df_aux2["cantidad"].fillna(0)

    #hacemos merge de stock con venta promedio a nivel tienda, sku, dia
    df_stock_seguridad = df_stock.merge(df_aux2, how='left', on=["id_tienda","ref_id","dia"])

    #rellenamos los registros con dia y venta 0 para los que no hubo venta en el merge
    df_stock_seguridad["dia"] = df_stock_seguridad["dia"].fillna(dia)
    df_stock_seguridad["cantidad"] = df_stock_seguridad["cantidad"].fillna(0)

    #multiplicamos la venta por 0.5 para cargar la mitad del stock de seguridad por regla de negocio
    df_stock_seguridad["cantidad"] = df_stock_seguridad["cantidad"]*0.5
    print(df_stock_seguridad["cantidad"])

    #Condicion para que si la venta fue menor a dos setear stock seguridad igual a 2
    condlist = [df_stock_seguridad["cantidad"]>=2,
                df_stock_seguridad["cantidad"]<2]
    choicelist = [df_stock_seguridad["cantidad"], 2]

    df_stock_seguridad["nuevo_stock_seguridad"] = np.select(condlist, choicelist)
    df_stock_seguridad["nuevo_stock_seguridad"] = round(df_stock_seguridad["nuevo_stock_seguridad"],2)

    #filtrar columnas necesarias del nuevo dataFrame      
    df_stock_seguridad = df_stock_seguridad[["ref_id","id_tienda","dia","nuevo_stock_seguridad"]]

    df_stock_seguridad_aux = df_stock_seguridad.groupby(by=["id_tienda","ref_id","dia"], as_index=False).mean()
    df_stock_seguridad_aux["nuevo_stock_seguridad"] =round(df_stock_seguridad_aux["nuevo_stock_seguridad"],0)
    df_stock_seguridad_aux['nuevo_stock_seguridad'] = pd.to_numeric(df_stock_seguridad_aux['nuevo_stock_seguridad'], errors='coerce').astype('Int64')
    df_stock_seguridad_aux['dia'] = pd.to_numeric(df_stock_seguridad_aux['dia'], errors='coerce').astype('Int64')

    ###############################################
    #        filtrado por dia #y promociones      #
    ###############################################

    df_stock_seguridad_aux.info()
    df_stock_seguridad_aux["dia"] = df_stock_seguridad_aux["dia"].fillna(dia)
    df_stock_seguridad_aux = df_stock_seguridad_aux[df_stock_seguridad_aux["dia"] == dia]
    df_stock_seguridad_aux.info()

    df_final = df_stock_seguridad_aux
    df_final.reset_index()
    df_final.info()

    df_final = df_final[["ref_id","id_tienda","dia","nuevo_stock_seguridad"]]

    #Agregar logica minimos exhibicion
    df_minimos = minimos_exhibicion()
    print(f"\nCantidad de registros antes del merge con minimos de exhibicion: {len(df_final.index)}")
    df_final = df_final.merge(df_minimos, how='left', on=["id_tienda","ref_id"])
    print(f"\nCantidad de registros despues del merge con minimos de exhibicion: {len(df_final.index)}")
    df_final.info()
    df_final['minimo_exhibicion'] = df_final['minimo_exhibicion'].fillna(2)
    df_final.info()
    df_final['minimo_exhibicion'] = pd.to_numeric(df_final['minimo_exhibicion'], errors='coerce').astype('Int64')

    condlist_1 = [
            df_final["nuevo_stock_seguridad"] > df_final["minimo_exhibicion"],
            df_final["nuevo_stock_seguridad"] <= df_final["minimo_exhibicion"]
            ]
    choicelist_1 = [
                df_final["minimo_exhibicion"],
                df_final["nuevo_stock_seguridad"]
                ]
    
    df_final["nuevo_stock_seguridad"] = np.select(np.array(condlist_1).astype(bool), choicelist_1)

    df_final["dia"] = df_final["dia"].astype(int)
    df_final["nuevo_stock_seguridad"] = df_final["nuevo_stock_seguridad"].astype(int)

    df_final.info()

    print("transformacion de datos listo \n")
    #################
    #Matrix de Pesos#
    #################
    
    df_matriz = matriz_ss()

    df_final = df_final.merge(df_matriz, how='left', on="id_tienda")
    df_final["peso"] = df_final["peso"].fillna(1)
    df_final["nuevo_stock_seguridad"] = round(df_final["nuevo_stock_seguridad"] * df_final["peso"],0)


    df_final = df_final[["id_tienda","ref_id","dia","nuevo_stock_seguridad"]]

    ##############
    #cargar datos#
    ##############

    condlist = [df_final["nuevo_stock_seguridad"]>=2,
                df_final["nuevo_stock_seguridad"]<2]
    choicelist = [df_final["nuevo_stock_seguridad"], 2]

    df_final["nuevo_stock_seguridad"] = np.select(condlist, choicelist)
    df_final["nuevo_stock_seguridad"] = round(df_final["nuevo_stock_seguridad"],2)
    print(df_final.head())

    condlist = [df_final["nuevo_stock_seguridad"]>=100,
                df_final["nuevo_stock_seguridad"]<100]
    choicelist = [100, df_final["nuevo_stock_seguridad"]]

    df_final["nuevo_stock_seguridad"] = np.select(condlist, choicelist)

    df_final = df_final[df_final['id_tienda'] != '1917']
    df_final = df_final[df_final['id_tienda'] != '9212']
    print(df_final.head())

    buffer = io.StringIO()
    df_final.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"stock_seguridad/{exec_date}/stock_seguridad_am_{date_aux}.csv"
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

def stock_ventas_tiendas_to_s3_pm(ds):
    import pandas as pd
    import numpy as np
    import io
    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"stock_seguridad/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    df_stock = stock(ds)
    print("se ha cargado stock\n")
    print(df_stock)

    df_venta_tienda = venta_tienda(ds)
    print("se ha cargado ventas\n")
    print(df_venta_tienda)
    
    print("\nse ha terminado de extraer data \n")

    #########################
    #transformacion de datos#
    #########################
    fecha_str = ds
    formato_str = "%Y-%m-%d"

    dia = datetime.strptime(fecha_str, formato_str) 
    dia = dia.weekday()
    dia = (dia + 1) % 7

    df_venta_tienda = df_venta_tienda[["id_tienda","ref_id","cantidad","dia","semana"]]

    df_aux1 = df_venta_tienda.groupby(by=["id_tienda","ref_id","dia","semana"], as_index=False).sum()
    df_aux2 = df_aux1.groupby(by=["id_tienda","ref_id","dia"], as_index=False).mean()
    df_aux2 = df_aux2[["id_tienda","ref_id","dia","semana","cantidad"]]
    print("\nventa promedio:\n")
    df_aux2.info()

    df_aux2["cantidad"] = df_aux2["cantidad"].fillna(0)

    df_aux2.info()
    df_stock_seguridad = df_stock.merge(df_aux2, how='left', on=["id_tienda","ref_id","dia"])
    df_stock_seguridad["dia"] = df_stock_seguridad["dia"].fillna(dia)
    df_stock_seguridad["cantidad"] = df_stock_seguridad["cantidad"].fillna(0)
    df_stock_seguridad.info()
    df_stock_seguridad["cantidad"] = df_stock_seguridad["cantidad"]*0.5

    condlist = [df_stock_seguridad["cantidad"]>=2,
                df_stock_seguridad["cantidad"]<2]
    choicelist = [df_stock_seguridad["cantidad"], 2]

    df_stock_seguridad["nuevo_stock_seguridad"] = np.select(condlist, choicelist)
    df_stock_seguridad["nuevo_stock_seguridad"] = round(df_stock_seguridad["nuevo_stock_seguridad"],2)

    df_stock_seguridad = df_stock_seguridad[["ref_id","id_tienda","dia","nuevo_stock_seguridad"]]
    df_stock_seguridad_aux = df_stock_seguridad.groupby(by=["id_tienda","ref_id","dia"], as_index=False).mean()
    df_stock_seguridad_aux["nuevo_stock_seguridad"] = round(df_stock_seguridad_aux["nuevo_stock_seguridad"],0)
    df_stock_seguridad_aux['nuevo_stock_seguridad'] = pd.to_numeric(df_stock_seguridad_aux['nuevo_stock_seguridad'], errors='coerce').astype('Int64')
    df_stock_seguridad_aux['dia'] = pd.to_numeric(df_stock_seguridad_aux['dia'], errors='coerce').astype('Int64')
    
    ###############################################
    #        filtrado por dia y promociones       #
    ###############################################

    print(f"\ndia: {dia}\n")
    df_stock_seguridad_aux.info()
    df_stock_seguridad_aux["dia"] = df_stock_seguridad_aux["dia"].fillna(dia)
    df_stock_seguridad_aux = df_stock_seguridad_aux[df_stock_seguridad_aux["dia"] == dia]
    df_stock_seguridad_aux.info()

    df_final = df_stock_seguridad_aux
    df_final.reset_index()
    df_final.info()

    #df_final = df_final[["id_tienda","ref_id","dia","nuevo_stock_seguridad"]]
    df_final = df_final[["ref_id","id_tienda","dia","nuevo_stock_seguridad"]]
    print(df_final)

    #Agregar logica minimos exhibicion
    df_minimos = minimos_exhibicion()
    print(f"\nCantidad de registros antes del merge con minimos de exhibicion: {len(df_final.index)}")
    df_final = df_final.merge(df_minimos, how='left', on=["id_tienda","ref_id"])
    print(f"\nCantidad de registros despues del merge con minimos de exhibicion: {len(df_final.index)}")
    df_final.info()
    df_final['minimo_exhibicion'] = df_final['minimo_exhibicion'].fillna(2)
    df_final.info()
    df_final['minimo_exhibicion'] = pd.to_numeric(df_final['minimo_exhibicion'], errors='coerce').astype('Int64')
    print(df_final[['nuevo_stock_seguridad', 'minimo_exhibicion']].dtypes)

    condlist_1 = [
            df_final["nuevo_stock_seguridad"] > df_final["minimo_exhibicion"],
            df_final["nuevo_stock_seguridad"] <= df_final["minimo_exhibicion"]
            ]
    choicelist_1 = [
                df_final["minimo_exhibicion"],
                df_final["nuevo_stock_seguridad"]
                ]
    
    df_final["nuevo_stock_seguridad"] = np.select(np.array(condlist_1).astype(bool), choicelist_1)
    #df_final["nuevo_stock_seguridad"] = round(df_final["nuevo_stock_seguridad"],2)

    df_final["dia"] = df_final["dia"].astype(int)
    df_final["nuevo_stock_seguridad"] = df_final["nuevo_stock_seguridad"].astype(int)

    df_final.info()

    print("transformacion de datos listo \n")
    #################
    #Matrix de Pesos#
    #################
    
    df_matriz = matriz_ss()
    print(df_matriz)
    print("\n")
    print(df_final)
    df_final = df_final.merge(df_matriz, how='left', on=["id_tienda"])
    df_final["peso"] = df_final["peso"].fillna(1) ##
    print("QA_test")
    print(df_final)
    df_final["nuevo_stock_seguridad"] = round(df_final["nuevo_stock_seguridad"] * df_final["peso"],0)

    df_final = df_final[["id_tienda","ref_id","dia","nuevo_stock_seguridad"]]

    print("\n")
    print(df_final)
    ##############
    #cargar datos#
    ##############

    condlist = [df_final["nuevo_stock_seguridad"]>=2,
                df_final["nuevo_stock_seguridad"]<2]
    choicelist = [df_final["nuevo_stock_seguridad"], 2]

    df_final["nuevo_stock_seguridad"] = np.select(condlist, choicelist)
    df_final["nuevo_stock_seguridad"] = round(df_final["nuevo_stock_seguridad"],2)

    condlist = [df_final["nuevo_stock_seguridad"]>=100,
                df_final["nuevo_stock_seguridad"]<100]
    choicelist = [100, df_final["nuevo_stock_seguridad"]]

    df_final["nuevo_stock_seguridad"] = np.select(condlist, choicelist)
    df_final = df_final[df_final['id_tienda'] != '1917']
    df_final = df_final[df_final['id_tienda'] != '9212']

    print(df_final)

    buffer = io.StringIO()
    df_final.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"stock_seguridad/{exec_date}/stock_seguridad_pm_{date_aux}.csv"
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

def carga_stock_seguridad_janis_pm(ds,ti):
    import requests
    import pandas as pd
    import datetime
    import json
    exec_date = ds.replace("-", "/")
    prefix = f"stock_seguridad/{exec_date}/"
    print(prefix)

    filename = ti.xcom_pull(key="return_value", task_ids=["stock_ventas_tiendas_to_s3_pm"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+filename)
    if not s3_hook.check_for_key(filename, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % filename)

    s_stock_object = s3_hook.get_key(filename, bucket_name=s3_bucket)

    df = pd.read_csv(s_stock_object.get()["Body"])
    print(df)
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")
    df.info()

    dia_semana = datetime.datetime.today().weekday()
    print(dia_semana, type(dia_semana))

    print(df)

    base_url = Variable.get("JANIS_API_URL")

    url = f"{base_url}stock"

    JANIS_API_KEY = Variable.get("JANIS_API_KEY")
    JANIS_API_SECRET = Variable.get("JANIS_API_SECRET")
    JANIS_CLIENT = Variable.get("JANIS_CLIENT")

    headers = {
    "janis-api-key" : JANIS_API_KEY,
    "janis-api-secret" : JANIS_API_SECRET,
    "janis-client" : JANIS_CLIENT,
    "Connection" : "keep-alive"
    }
    
    payload=[]
    for i in df.index:
        material = df.ref_id[i].split("-")[0]
        id_tienda = str(int(df['id_tienda'][i])).zfill(4)
        stock_seguridad = int(df.nuevo_stock_seguridad[i])
        row = {"IdSku": material,
                "Quantity": 0,
                "Store": id_tienda,
                "MinStockDiff": True,
                "MinStock": stock_seguridad,
                "Type": 2}
        payload.append(row)    
        if i % 499 == 0:
            payload_json = json.dumps(payload, ensure_ascii=False).replace('"true"', 'true').replace('"false"', 'false')
            response = requests.post(url, headers=headers, data=payload_json)
            print(response.text)
            payload = []
    payload_json = json.dumps(payload, ensure_ascii=False).replace('"true"', 'true').replace('"false"', 'false')
    response = requests.post(url, headers=headers, data=payload_json)
    print(response.text)

    return


def carga_stock_seguridad_janis_am(ds,ti):
    import requests
    import pandas as pd
    import datetime
    import json
    exec_date = ds.replace("-", "/")
    prefix = f"stock_seguridad/{exec_date}/"
    print(prefix)

    filename = ti.xcom_pull(key="return_value", task_ids=["stock_ventas_tiendas_to_s3_am"])[0]

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
    df.info()

    dia_semana = datetime.datetime.today().weekday()
    print(dia_semana, type(dia_semana))

    print(df)
    df.info()

    base_url = Variable.get("JANIS_API_URL")

    url = f"{base_url}stock"

    JANIS_API_KEY = Variable.get("JANIS_API_KEY")
    JANIS_API_SECRET = Variable.get("JANIS_API_SECRET")
    JANIS_CLIENT = Variable.get("JANIS_CLIENT")

    headers = {
    "janis-api-key" : JANIS_API_KEY,
    "janis-api-secret" : JANIS_API_SECRET,
    "janis-client" : JANIS_CLIENT,
    "Connection" : "keep-alive"
    }
    
    payload=[]
    for i in df.index:
        material = df.ref_id[i].split("-")[0]
        id_tienda = str(int(df['id_tienda'][i])).zfill(4)
        stock_seguridad = int(df.nuevo_stock_seguridad[i])
        row = {"IdSku": material,
                "Quantity": 0,
                "Store": id_tienda,
                "MinStockDiff": True,
                "MinStock": stock_seguridad,
                "Type": 2}
        payload.append(row)    
        if i % 499 == 0:
            payload_json = json.dumps(payload, ensure_ascii=False).replace('"true"', 'true').replace('"false"', 'false')
            response = requests.post(url, headers=headers, data=payload_json)
            print(response.text)
            payload = []
    payload_json = json.dumps(payload, ensure_ascii=False).replace('"true"', 'true').replace('"false"', 'false')
    response = requests.post(url, headers=headers, data=payload_json)
    print(response.text)

    return

def stock_ventas_tiendas_to_postgresql_am(ti):
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text

    filename = ti.xcom_pull(key="return_value", task_ids=["stock_ventas_tiendas_to_s3_am"])[0]

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
    df["id_tienda"] = df["id_tienda"].apply(lambda x: str(x).zfill(4))
    df.info()
    print(df.head())

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    with engine.begin() as conn:
        conn.execute("TRUNCATE ecommdata.stock_seguridad_tiendas") 
        df.to_sql(name="stock_seguridad_tiendas",
                    con=conn,         
                    schema="ecommdata",         
                    if_exists='append',         
                    index=False,         
                    chunksize=20000,         
                    method='multi')

    print("Data saved to PostgreSQL.")

    return


def stock_ventas_tiendas_to_postgresql_pm(ti):
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text

    filename = ti.xcom_pull(key="return_value", task_ids=["stock_ventas_tiendas_to_s3_pm"])[0]

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
    df["id_tienda"] = df["id_tienda"].apply(lambda x: str(x).zfill(4))
    df.info()
    print(df.head())

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    with engine.begin() as conn:
        conn.execute("TRUNCATE ecommdata.stock_seguridad_tiendas") 
        df.to_sql(name="stock_seguridad_tiendas",
                    con=conn,         
                    schema="ecommdata",         
                    if_exists='append',         
                    index=False,         
                    chunksize=20000,         
                    method='multi')

    print("Data saved to PostgreSQL.")

    return


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_stock_seguridad',
    default_args=default_args,
    description="cargar stock de seguridad",
    schedule_interval="30 1/4 * * *",
    start_date=pendulum.datetime(2023, 6, 12, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "Janis", "ecommdata_unimarc", "stock", "stock_seguidad", "ventas", "unimarc", "PATRICIO"],
) as dag:
    

    dag.doc_md = """
    Carga stock de seguridad \n
    guardar en S3.
    """ 
    t0 = BranchPythonOperator(
        task_id='check_time',
        python_callable=_check_time,
    )

    t_dummy = DummyOperator(
            task_id='task_skip',
        )

    t1_am = PythonOperator(
        task_id = "stock_ventas_tiendas_to_s3_am",
        python_callable = stock_ventas_tiendas_to_s3_am,
    )

    t1_pm = PythonOperator(
        task_id = "stock_ventas_tiendas_to_s3_pm",
        python_callable = stock_ventas_tiendas_to_s3_pm,
    )

    t2_am = PythonOperator(
        task_id = "stock_ventas_tiendas_to_postgresql_am",
        python_callable = stock_ventas_tiendas_to_postgresql_am,
    )

    t2_pm = PythonOperator(
        task_id = "stock_ventas_tiendas_to_postgresql_pm",
        python_callable = stock_ventas_tiendas_to_postgresql_pm,
    )

    t3_am = PythonOperator(
        task_id = "carga_stock_seguridad_janis_am",
        python_callable = carga_stock_seguridad_janis_am
    )

    t3_pm = PythonOperator(
        task_id = "carga_stock_seguridad_janis_pm",
        python_callable = carga_stock_seguridad_janis_pm
    )

    t0 >> t1_am >> t2_am >> t3_am
    t0 >> t1_pm >> t2_pm >> t3_pm
    t0 >> t_dummy


