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
    if (time_str == "18:00") or (time_str == "22:00") or (time_str == "02:00") or (time_str == "14:00"):
        return "task_skip"
    elif (time_str == "06:00"):
        return "stock_ventas_tienda_1917_to_s3_am"
    else:
        return "stock_ventas_tienda_1917_to_s3_pm"

def ubicaciones_flo(ds):
    stock_tiendas_query = """select sap_code||'-'|| measurement_unit as ref_id,
                        '1917' as id_tienda,
                        date_part('dow','"""+ds+"""'::date) as dia,
                        date_part('week','"""+ds+"""'::date) as semana,
                        mfc_is_item_side
                        from ecommdata.ubicacion_mfc
                        where mfc_is_item_side = 'FLO'"""
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    print(stock_tiendas_query)
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(stock_tiendas_query)
    results = cursor.fetchall()
    cursor.close()
    pg_connection.close()
    return results

def lista_eliminar_ss():
    lista_material_reg_query = """select sap_code
                            from ecommdata.ubicacion_mfc um 
                            where mfc_is_item_side = 'REG'"""
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    print(lista_material_reg_query)
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(lista_material_reg_query)
    results = cursor.fetchall()
    cursor.close()
    pg_connection.close()
    return results


def promociones(ds):
    import pandas as pd
    promociones_query = f"""select distinct
                    concat(wp.material, '-',
                    case 
                        when wp.umv = 'ST' then 'UN'
                        else wp.umv
                    end) as ref_id 
                    from ecommdata.workflow_promociones wp 
                    where wp.fecha_inicio_de_promocion <= '{ds}'::date
                    and wp.fecha_fin_de_promocion >= '{ds}'::date
                    and wp.tipo_promocion IN (1,4)
                    and wp.registro_valido = True
                    and wp.organizacion_ventas = '1000'
                    and wp.canal_distribucion = '10'
                    and wp.id_mecanica NOT IN (25, 27, 36, 37, 50, 51, 53, 67, 72, 77, 93, 99, 123,124)
                    AND wp.nombre_promocion::text !~~ '%MFC%'::text
                    AND wp.nombre_promocion::text !~~ '%BANCO%'::text 
                    AND wp.nombre_promocion::text !~~ '%UNIPAY%'::text
                    AND wp.nombre_promocion::text !~~ '%TERCERA%'::text 
                    AND wp.nombre_promocion::text !~~ '%917%'::text
                    AND wp.nombre_promocion::text !~~ '%ESTADO%'::text
                    and wp.nombre_promocion::text !~~ '% LOC%'::text
                    and wp.nombre_promocion::text !~~ '%LIQ%'::text
                    group by wp.umv, wp.material
                            """
    print(promociones_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(promociones_query)
    results = cursor.fetchall()
    results=pd.DataFrame(results)
    results.columns = ["ref_id"]
    cursor.close()
    pg_connection.close()

    return results


def venta_tienda(ds):
    ventas_skus_tienda_query = """select _t.*
                    from ( 
                        select LPAD(v.id_tienda , 4, '0') as id_tienda,
                        CONCAT(LPAD(v.material, 18, '0'), '-', v.umv) as ref_id,
                        case
                            when (v.umv in ('UN','DIS','KG')) then round(v.venta_bruta/v.venta_umv,0)
                            else v.venta_bruta
                        end as precio_venta,
                        p.precio_lista,
                        v.venta_umv,
                        date_part('dow',v.fecha) as dia,
                        date_part('week',v.fecha) as semana
                        from ecommdata.venta_sku_tienda as v
                        left join ecommdata.tiendas as t
                        on LPAD(v.id_tienda , 4, '0') = t.id
                        left join ecommdata.precios as p
                        on CONCAT(LPAD(v.material, 18, '0'), '-', v.umv) = p.ref_id
                        and p.id_tienda_janis = t.id_janis  
                        where v.fecha >= '"""+ds+"""'::date -70
                        and v.venta_umv > 0 
                        and v.venta_bruta <> 0 
                        and v.organizacion = 'Unimarc'
                        and p.precio_lista is not null
                        and LPAD(v.id_tienda , 4, '0') in ('0917')) as _t
                        where precio_venta/precio_lista > 0.8 
                        """
    print(ventas_skus_tienda_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(ventas_skus_tienda_query)
    results = cursor.fetchall()
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
                left join ecommdata.ubicacion_mfc um 
                on um.sap_code = meio.material and um.measurement_unit = meio.umv 
                where t.status = 1
                and t.id = '0917'
                and l.material  is not null
                and l.id_tienda  is not null
                and l.umv  is not null
                and t.id is not null
                and meio.minimo_exhibicion > 2
                and um.mfc_is_item_side = 'FLO'"""
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

def stock_ventas_tienda_1917_to_s3_am(ds):
    import pandas as pd
    import numpy as np
    import io
    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"stock_seguridad_mfc/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    df_stock = pd.DataFrame(ubicaciones_flo(ds))
    print("se ha cargado stock\n")
    df_venta_tienda = pd.DataFrame(venta_tienda(ds))
    print("se ha cargado ventas\n")
    df_stock.columns=["ref_id","id_tienda","dia","semana","mfc_is_item_side"]
    df_venta_tienda.columns =["id_tienda","ref_id","venta","precio_lista","cantidad","dia","semana"]
    df_promociones = promociones(ds)
    df_promociones=df_promociones.drop_duplicates(subset='ref_id')
    print("se ha cargado promociones \n")

    print("se ha terminado de extraer data \n")

    #########################
    #transformacion de datos#
    #########################

    df_venta_tienda.precio_lista.fillna(df_venta_tienda.venta, inplace=True)
    df_venta_tienda = df_venta_tienda[["id_tienda","ref_id","cantidad","dia","semana"]]

    df_aux1 = df_venta_tienda.groupby(by=["id_tienda","ref_id","dia","semana"], as_index=False).sum()
    df_aux2 = df_aux1.groupby(by=["id_tienda","ref_id","dia"], as_index=False).mean()
    df_aux2=df_aux2[["id_tienda","ref_id","dia","semana","cantidad"]]

    df_stock_seguridad = df_stock.merge(df_aux2, how='left', on=["id_tienda","ref_id","dia"])
    df_stock_seguridad = df_stock_seguridad.fillna(0)
    df_stock_seguridad["cantidad"] = df_stock_seguridad["cantidad"]/2

    condlist = [df_stock_seguridad["cantidad"]>=2,
                df_stock_seguridad["cantidad"]<2]
    choicelist = [df_stock_seguridad["cantidad"], 2]

    df_stock_seguridad["nuevo_stock_seguridad"] = np.select(condlist, choicelist)
    df_stock_seguridad["nuevo_stock_seguridad"] = round(df_stock_seguridad["nuevo_stock_seguridad"],2)
    print(df_stock_seguridad)
    df_stock_seguridad=df_stock_seguridad[["ref_id","id_tienda","dia","nuevo_stock_seguridad"]]
    df_stock_seguridad_aux = df_stock_seguridad.groupby(by=["id_tienda","ref_id","dia"], as_index=False).mean()
    df_stock_seguridad_aux["nuevo_stock_seguridad"] =round(df_stock_seguridad_aux["nuevo_stock_seguridad"],0)
    df_stock_seguridad_aux
    ###############################################
    #        filtrado por dia y promociones       #
    ###############################################
    fecha_str = ds
    formato_str = "%Y-%m-%d"

    dia = datetime.strptime(fecha_str, formato_str) 
    dia = dia.weekday()
    dia = (dia + 1) % 7
    df_stock_seguridad_aux=df_stock_seguridad_aux[df_stock_seguridad_aux["dia"] == dia] #cambiar por ds

    df_final=(df_stock_seguridad_aux.merge(df_promociones, on='ref_id', how='left', indicator=True)
        .query('_merge == "left_only"')
        .drop('_merge', 1))

    df_final = df_final[["id_tienda","ref_id","dia","nuevo_stock_seguridad"]]
    print(df_final)

    df_final["dia"]=df_final["dia"].astype(int)
    df_final["nuevo_stock_seguridad"]=df_final["nuevo_stock_seguridad"].astype(int)

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
    print("Verificación de condiciones:")
    print((df_final["nuevo_stock_seguridad"] > df_final["minimo_exhibicion"]).head())
    print((df_final["nuevo_stock_seguridad"] <= df_final["minimo_exhibicion"]).head())

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

    df_final = df_final[["id_tienda","ref_id","dia","nuevo_stock_seguridad"]]

    condlist = [df_final["nuevo_stock_seguridad"]>=100,
                df_final["nuevo_stock_seguridad"]<100]
    choicelist = [100, df_final["nuevo_stock_seguridad"]]

    df_final["nuevo_stock_seguridad"] = np.select(condlist, choicelist)

    print("transformacion de datos listo \n")


    ##############
    #cargar datos#
    ##############

    buffer = io.StringIO()
    df_final.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"stock_seguridad_mfc/{exec_date}/stock_seguridad_mfc_am_{date_aux}.csv"
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

def carga_stock_seguridad_1917_janis_am(ds,ti):
    import requests
    import pandas as pd
    import datetime
    import json

    exec_date = ds.replace("-", "/")
    prefix = f"stock_seguridad_mfc/{exec_date}/"
    print(prefix)

    filename = ti.xcom_pull(key="return_value", task_ids=["stock_ventas_tienda_1917_to_s3_am"])[0]

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
    print(df.info())

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
        id_tienda = "1917"
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

def stock_ventas_tienda_1917_to_s3_pm(ds):
    import pandas as pd
    import numpy as np
    import io
    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"stock_seguridad_mfc/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    df_stock = pd.DataFrame(ubicaciones_flo(ds))
    print("se ha cargado stock\n")
    df_venta_tienda = pd.DataFrame(venta_tienda(ds))
    print("se ha cargado ventas\n")
    df_stock.columns=["ref_id","id_tienda","dia","semana","mfc_is_item_side"]
    df_venta_tienda.columns =["id_tienda","ref_id","venta","precio_lista","cantidad","dia","semana"]
    df_promociones = promociones(ds)
    df_promociones = df_promociones.drop_duplicates(subset='ref_id')
    print("se ha cargado promociones \n")

    print("se ha terminado de extraer data \n")

    #########################
    #transformacion de datos#
    #########################

    df_venta_tienda.precio_lista.fillna(df_venta_tienda.venta, inplace=True)
    df_venta_tienda = df_venta_tienda[["id_tienda","ref_id","cantidad","dia","semana"]]

    df_aux1 = df_venta_tienda.groupby(by=["id_tienda","ref_id","dia","semana"], as_index=False).sum()
    df_aux2 = df_aux1.groupby(by=["id_tienda","ref_id","dia"], as_index=False).mean()
    df_aux2=df_aux2[["id_tienda","ref_id","dia","semana","cantidad"]]

    df_stock_seguridad = df_stock.merge(df_aux2, how='left', on=["id_tienda","ref_id","dia"])
    df_stock_seguridad = df_stock_seguridad.fillna(0)

    condlist = [df_stock_seguridad["cantidad"]>=2,
                df_stock_seguridad["cantidad"]<2]
    choicelist = [df_stock_seguridad["cantidad"], 2]

    df_stock_seguridad["nuevo_stock_seguridad"] = np.select(condlist, choicelist)
    df_stock_seguridad["nuevo_stock_seguridad"] = round(df_stock_seguridad["nuevo_stock_seguridad"],2)
    print(df_stock_seguridad)
    df_stock_seguridad=df_stock_seguridad[["ref_id","id_tienda","dia","nuevo_stock_seguridad"]]
    df_stock_seguridad_aux = df_stock_seguridad.groupby(by=["id_tienda","ref_id","dia"], as_index=False).mean()
    df_stock_seguridad_aux["nuevo_stock_seguridad"] =round(df_stock_seguridad_aux["nuevo_stock_seguridad"],0)
    df_stock_seguridad_aux
    ###############################################
    #        filtrado por dia y promociones       #
    ###############################################
    fecha_str = ds
    formato_str = "%Y-%m-%d"

    dia = datetime.strptime(fecha_str, formato_str) 
    dia = dia.weekday()
    dia = (dia + 1) % 7
    df_stock_seguridad_aux=df_stock_seguridad_aux[df_stock_seguridad_aux["dia"] == dia] #cambiar por ds

    df_final=(df_stock_seguridad_aux.merge(df_promociones, on='ref_id', how='left', indicator=True)
        .query('_merge == "left_only"')
        .drop('_merge', 1))

    df_final = df_final[["id_tienda","ref_id","dia","nuevo_stock_seguridad"]]
    print(df_final)

    df_final["dia"]=df_final["dia"].astype(int)
    df_final["nuevo_stock_seguridad"]=df_final["nuevo_stock_seguridad"].astype(int)

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
    print("Verificación de condiciones:")
    print((df_final["nuevo_stock_seguridad"] > df_final["minimo_exhibicion"]).head())
    print((df_final["nuevo_stock_seguridad"] <= df_final["minimo_exhibicion"]).head())

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

    df_final = df_final[["id_tienda","ref_id","dia","nuevo_stock_seguridad"]]

    condlist = [df_final["nuevo_stock_seguridad"]>=100,
                df_final["nuevo_stock_seguridad"]<100]
    choicelist = [100, df_final["nuevo_stock_seguridad"]]

    df_final["nuevo_stock_seguridad"] = np.select(condlist, choicelist)

    print("transformacion de datos listo \n")

    ##############
    #cargar datos#
    ##############

    buffer = io.StringIO()
    df_final.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"stock_seguridad_mfc/{exec_date}/stock_seguridad_mfc_pm_{date_aux}.csv"
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

def carga_stock_seguridad_1917_janis_pm(ds,ti):
    import requests
    import pandas as pd
    import datetime
    import json
    
    exec_date = ds.replace("-", "/")
    prefix = f"stock_seguridad_mfc/{exec_date}/"
    print(prefix)

    filename = ti.xcom_pull(key="return_value", task_ids=["stock_ventas_tienda_1917_to_s3_pm"])[0]

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
    print(df.info())

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
        id_tienda = "1917"
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

def eliminar_stock_seguridad_reg():
    import pandas as pd
    import requests
    import json

    df = pd.DataFrame(lista_eliminar_ss())
    df.columns = ["material"]

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
    for i in range(len(df.index)):
        print(i)
        material = df.material[i]
        store = "1917"
        stock_seguridad = 0
        row = {"IdSku": material,
                "Quantity": 0,
                "Store": store,
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

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_stock_seguridad_mfc',
    default_args=default_args,
    description="cargar stock de seguridad MFC",
    schedule_interval="0 2/4 * * *",
    start_date=pendulum.datetime(2023, 7, 11, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "Janis", "ecommdata_unimarc", "stock", "stock_seguidad", "ventas", "unimarc", "MFC", "PATRICIO"],
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
        task_id = "stock_ventas_tienda_1917_to_s3_am",
        python_callable = stock_ventas_tienda_1917_to_s3_am,
    )

    t2_am = PythonOperator(
        task_id = "carga_stock_seguridad_1917_janis_am",
        python_callable = carga_stock_seguridad_1917_janis_am
    )

    t1_pm = PythonOperator(
        task_id = "stock_ventas_tienda_1917_to_s3_pm",
        python_callable = stock_ventas_tienda_1917_to_s3_pm,
    )

    t2_pm = PythonOperator(
        task_id = "carga_stock_seguridad_1917_janis_pm",
        python_callable = carga_stock_seguridad_1917_janis_pm
    )

    t3_am = PythonOperator(
        task_id = "eliminar_stock_seguridad_reg_am",
        python_callable = eliminar_stock_seguridad_reg
    )

    t3_pm = PythonOperator(
        task_id = "eliminar_stock_seguridad_reg_pm",
        python_callable = eliminar_stock_seguridad_reg
    )

    t0 >> t1_am >> t2_am >> t3_am
    t0 >> t1_pm >> t2_pm >> t3_pm
    t0 >> t_dummy


