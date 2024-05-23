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
    stock_tiendas_query = """select id_tienda,
                            glosa_tienda,
                            ref_id,
                            stock_janis,
                            stock_seguridad_janis,
                            date_part('dow',fecha) as dia,
                            date_part('week',fecha) as semana
                            from ecommdata_alvi.stock as s
                            left join ecommdata_alvi.tiendas as t
                            on t.id = s.id_tienda
                            where fecha = '"""+ds+"""'::date
                            and surtido_ecommerce is true
                            and stock_infinito_janis is not true
                            and t.status = 1"""
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    print(stock_tiendas_query)
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(stock_tiendas_query)
    results = cursor.fetchall()
    cursor.close()
    pg_connection.close()
    return results

def matriz_ss():
    import pandas as pd
    matriz_query = """select *
                    from catalogo.matriz_ss_alvi"""
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
                                    v.venta_umv,
                                    date_part('dow',v.fecha) as dia,
                                    date_part('week',v.fecha) as semana
                                    from ecommdata_alvi.venta_sku_tienda as v
                                    left join ecommdata_alvi.tiendas as t
                                    on LPAD(v.id_tienda , 4, '0') = t.id
                                    where v.fecha >= '"""+ds+"""'::date -30
                                    and v.venta_umv > 0 
                                    and v.venta_bruta <> 0) as _t
                                    group by _t.id_tienda,
                                    _t.ref_id,
                                    _t.precio_venta,
                                    _t.venta_umv, 
                                    _t.dia,
                                    _t.semana"""
    print(ventas_skus_tienda_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(ventas_skus_tienda_query)
    results = cursor.fetchall()
    cursor.close()
    pg_connection.close()
    return results

def stock_ventas_tiendas_to_s3_am(ds):
    import pandas as pd
    import numpy as np
    import io
    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"stock_seguridad_alvi_/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    df_stock = pd.DataFrame(stock(ds))
    print("se ha cargado stock\n")
    print(df_stock)
    df_stock.columns=["id_tienda","glosa_tienda","ref_id","stock_janis","stock_seguridad","dia","semana"]
    df_venta_tienda = pd.DataFrame(venta_tienda(ds))
    print("se ha cargado ventas\n")
    print(df_venta_tienda)
    df_venta_tienda.columns =["id_tienda","ref_id","venta","cantidad","dia","semana"]
    
    print("\nse ha terminado de extraer data \n")

    #########################
    #transformacion de datos#
    #########################

    df_venta_tienda = df_venta_tienda[["id_tienda","ref_id","cantidad","dia","semana"]]

    df_aux1 = df_venta_tienda.groupby(by=["id_tienda","ref_id","dia","semana"], as_index=False).sum()
    df_aux2 = df_aux1.groupby(by=["id_tienda","ref_id","dia"], as_index=False).mean()
    df_aux2=df_aux2[["id_tienda","ref_id","dia","semana","cantidad"]]

    df_stock_seguridad = df_stock.merge(df_aux2, how='left', on=["id_tienda","ref_id","dia"])
    df_stock_seguridad = df_stock_seguridad.fillna(0)
    print(df_stock_seguridad["cantidad"])
    df_stock_seguridad["cantidad"] = df_stock_seguridad["cantidad"]*0.5
    print(df_stock_seguridad["cantidad"])

    condlist = [df_stock_seguridad["cantidad"]>=2,
                df_stock_seguridad["cantidad"]<2]
    choicelist = [df_stock_seguridad["cantidad"], 2]

    df_stock_seguridad["nuevo_stock_seguridad"] = np.select(condlist, choicelist)
    df_stock_seguridad["nuevo_stock_seguridad"] = round(df_stock_seguridad["nuevo_stock_seguridad"],2)

    df_stock_seguridad=df_stock_seguridad[["ref_id","id_tienda","dia","stock_janis","stock_seguridad","nuevo_stock_seguridad"]]
    df_stock_seguridad_aux = df_stock_seguridad.groupby(by=["id_tienda","ref_id","dia"], as_index=False).mean()
    df_stock_seguridad_aux["nuevo_stock_seguridad"] =round(df_stock_seguridad_aux["nuevo_stock_seguridad"],0)
    df_stock_seguridad_aux
    ##################################
    #        filtrado por dia        #
    ##################################

    fecha_str = ds
    formato_str = "%Y-%m-%d"

    dia = datetime.strptime(fecha_str, formato_str) 
    dia = dia.weekday()
    dia = (dia + 1) % 7
    df_stock_seguridad_aux=df_stock_seguridad_aux[df_stock_seguridad_aux["dia"] == dia] #cambiar por ds

    df_final = df_stock_seguridad_aux

    df_final = df_final[["id_tienda","ref_id","dia","stock_janis","stock_seguridad","nuevo_stock_seguridad"]]
    print(df_final)
    df_final = df_final[["id_tienda","ref_id","dia","nuevo_stock_seguridad"]]
    print(df_final)

    df_final["dia"]=df_final["dia"].astype(int)
    df_final["nuevo_stock_seguridad"]=df_final["nuevo_stock_seguridad"].astype(int)

    print("transformacion de datos listo \n")
    #################
    #Matrix de Pesos#
    #################
    
    df_matriz = matriz_ss()

    df_final = df_final.merge(df_matriz, how='left', on=["id_tienda"])
    df_final["nuevo_stock_seguridad"] = round(df_final["nuevo_stock_seguridad"] * df_final["peso"],0)

    df_final = df_final[["id_tienda","ref_id","dia","nuevo_stock_seguridad"]]

    ##############
    #cargar datos#
    ##############

    condlist = [df_final["nuevo_stock_seguridad"]>=200,
                df_final["nuevo_stock_seguridad"]<200]
    choicelist = [200, df_final["nuevo_stock_seguridad"]]

    df_final["nuevo_stock_seguridad"] = np.select(condlist, choicelist)

    buffer = io.StringIO()
    df_final.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"stock_seguridad_alvi_/{exec_date}/stock_seguridad_am_{date_aux}.csv"
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
    prefix = f"stock_seguridad_alvi_/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    df_stock = pd.DataFrame(stock(ds))
    print("se ha cargado stock\n")
    print(df_stock)
    df_stock.columns=["id_tienda","glosa_tienda","ref_id","stock_janis","stock_seguridad","dia","semana"]
    df_venta_tienda = pd.DataFrame(venta_tienda(ds))
    print("se ha cargado ventas\n")
    print(df_venta_tienda)
    df_venta_tienda.columns =["id_tienda","ref_id","venta","cantidad","dia","semana"]
    
    print("\nse ha terminado de extraer data \n")

    #########################
    #transformacion de datos#
    #########################

    df_venta_tienda = df_venta_tienda[["id_tienda","ref_id","cantidad","dia","semana"]]

    df_aux1 = df_venta_tienda.groupby(by=["id_tienda","ref_id","dia","semana"], as_index=False).sum()
    df_aux2 = df_aux1.groupby(by=["id_tienda","ref_id","dia"], as_index=False).mean()
    df_aux2=df_aux2[["id_tienda","ref_id","dia","semana","cantidad"]]

    df_stock_seguridad = df_stock.merge(df_aux2, how='left', on=["id_tienda","ref_id","dia"])
    df_stock_seguridad = df_stock_seguridad.fillna(0)
    print(df_stock_seguridad["cantidad"])

    condlist = [df_stock_seguridad["cantidad"]>=2,
                df_stock_seguridad["cantidad"]<2]
    choicelist = [df_stock_seguridad["cantidad"], 2]

    df_stock_seguridad["nuevo_stock_seguridad"] = np.select(condlist, choicelist)
    df_stock_seguridad["nuevo_stock_seguridad"] = round(df_stock_seguridad["nuevo_stock_seguridad"],2)

    df_stock_seguridad=df_stock_seguridad[["ref_id","id_tienda","dia","stock_janis","stock_seguridad","nuevo_stock_seguridad"]]
    df_stock_seguridad_aux = df_stock_seguridad.groupby(by=["id_tienda","ref_id","dia"], as_index=False).mean()
    df_stock_seguridad_aux["nuevo_stock_seguridad"] =round(df_stock_seguridad_aux["nuevo_stock_seguridad"],0)
    ################################
    #        filtrado por dia      #
    ################################
    fecha_str = ds
    formato_str = "%Y-%m-%d"

    dia = datetime.strptime(fecha_str, formato_str) 
    dia = dia.weekday()
    dia = (dia + 1) % 7
    df_stock_seguridad_aux=df_stock_seguridad_aux[df_stock_seguridad_aux["dia"] == dia] #cambiar por ds

    df_final = df_stock_seguridad_aux

    df_final = df_final[["id_tienda","ref_id","dia","stock_janis","stock_seguridad","nuevo_stock_seguridad"]]
    print(df_final)
    df_final = df_final[["id_tienda","ref_id","dia","nuevo_stock_seguridad"]]
    print(df_final)

    df_final["dia"]=df_final["dia"].astype(int)
    df_final["nuevo_stock_seguridad"]=df_final["nuevo_stock_seguridad"].astype(int)

    print("transformacion de datos listo \n")
    #################
    #Matrix de Pesos#
    #################
    
    df_matriz = matriz_ss()

    df_final = df_final.merge(df_matriz, how='left', on=["id_tienda"])
    df_final["nuevo_stock_seguridad"] = round(df_final["nuevo_stock_seguridad"] * df_final["peso"],0)

    df_final = df_final[["id_tienda","ref_id","dia","nuevo_stock_seguridad"]]

    ##############
    #cargar datos#
    ##############

    condlist = [df_final["nuevo_stock_seguridad"]>=200,
                df_final["nuevo_stock_seguridad"]<200]
    choicelist = [200, df_final["nuevo_stock_seguridad"]]

    df_final["nuevo_stock_seguridad"] = np.select(condlist, choicelist)

    buffer = io.StringIO()
    df_final.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"stock_seguridad_alvi_/{exec_date}/stock_seguridad_am_{date_aux}.csv"
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
    prefix = f"stock_seguridad_alvi_/{exec_date}/"
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
    print(df.info())

    dia_semana = datetime.datetime.today().weekday()
    print(dia_semana, type(dia_semana))

    print(df)

    base_url = Variable.get("JANIS_API_URL")

    url = f"{base_url}stock"

    JANIS_API_KEY = Variable.get("JANIS_ALVI_API_KEY")
    JANIS_API_SECRET = Variable.get("JANIS_ALVI_API_SECRET")
    JANIS_CLIENT = Variable.get("JANIS_ALVI_CLIENT")

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
    prefix = f"stock_seguridad_alvi_/{exec_date}/"
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
    print(df.info())

    dia_semana = datetime.datetime.today().weekday()
    print(dia_semana, type(dia_semana))

    print(df)
    print(df.info())

    base_url = Variable.get("JANIS_API_URL")

    url = f"{base_url}stock"

    JANIS_API_KEY = Variable.get("JANIS_ALVI_API_KEY")
    JANIS_API_SECRET = Variable.get("JANIS_ALVI_API_SECRET")
    JANIS_CLIENT = Variable.get("JANIS_ALVI_CLIENT")

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

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_stock_seguridad_alvi',
    default_args=default_args,
    description="cargar stock de seguridad alvi",
    schedule_interval="30 1/4 * * *",
    start_date=pendulum.datetime(2023, 6, 12, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "Janis", "ecommdata_alvi", "stock", "stock_seguidad", "ventas", "alvi", "PATRICIO"],
) as dag:
    

    dag.doc_md = """
    Carga stock de seguridad alvi \n
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
        task_id = "carga_stock_seguridad_janis_am",
        python_callable = carga_stock_seguridad_janis_am
    )

    t2_pm = PythonOperator(
        task_id = "carga_stock_seguridad_janis_pm",
        python_callable = carga_stock_seguridad_janis_pm
    )

    t0 >> t1_am >> t2_am
    t0 >> t1_pm >> t2_pm
    t0 >> t_dummy


