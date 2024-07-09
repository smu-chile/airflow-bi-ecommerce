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
        return "ventas_maximos_apo_to_s3_am"
    else:
        return "ventas_maximos_apo_to_s3_pm"

def materiales_dentro_ventas(list_material,ds):
    import pandas as pd
    stock_tiendas_query = """select material,
                    max(fecha_inicio_de_promocion) as fecha_inicio,
                    max( fecha_fin_de_promocion) as fecha_fin
                    from ecommdata.workflow_promociones wp
                    where fecha_fin_de_promocion < '"""+ds+"""'::date 
                    and fecha_inicio_de_promocion >= '"""+ds+"""'::date -30
                    and material in ('"""+list_material+"""')
                    and id_mecanica not in (12,22,25,26,27,36,50,67,72,84,99,37,51,53,59,77,82,93,96,123)
                    and id_evento not in (551)
                    group by material 
                    order by max( fecha_fin_de_promocion)"""
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    print(stock_tiendas_query)
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(stock_tiendas_query)
    results = cursor.fetchall()
    results=pd.DataFrame(results)
    results.columns = ["material","fecha_inicio","fecha_fin"]
    cursor.close()
    pg_connection.close()
    return results

def promociones(ds):
    import pandas as pd
    promociones_query = f"""select distinct
                    wp.material
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
                    group by wp.umv, wp.material"""
    print(promociones_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(promociones_query)
    results = cursor.fetchall()
    if len(results)==0:
        print("\n\n\tNo hay productos en periodo promocional")
        return 
    else:
        results=pd.DataFrame(results)
        results.columns = ["material"]
        cursor.close()
        pg_connection.close()

    return results


def ventas(list_material,fecha_inicio,fecha_fin):
    import pandas as pd
    ventas_skus_tienda_query = f"""
        select lpad(vst.id_tienda, 4, '0') as id_tienda,
            lpad(vst.material, 18, '0') as material,
            (sum(vst.venta_umv) / ('{fecha_inicio}'::date - '{fecha_fin}'::date)) * -1 as prom_ventas
        from ecommdata.venta_sku_tienda vst 
        left join ecommdata.tiendas t on t.id = lpad(vst.id_tienda, 4, '0')
        where vst.fecha >= '{fecha_inicio}'::date
            and vst.fecha <= '{fecha_fin}'::date
            and lpad(vst.material, 18, '0') in ('{list_material}')
            and t.status = 1
            and lpad(vst.id_tienda, 4, '0') not in ('1917','0917')
        group by vst.id_tienda, vst.material
    """
    
    print(ventas_skus_tienda_query)
    
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(ventas_skus_tienda_query)
    results = cursor.fetchall()

    try:
        results = pd.DataFrame(results, columns=["id_tienda", "material", "prom_ventas"])
    except ValueError:
        print("No se encontraron datos para los parámetros dados.")
        results = pd.DataFrame(columns=["id_tienda", "material", "prom_ventas"])

    cursor.close()
    pg_connection.close()
    return results


def ventas_maximas(list_material,ds):
    import pandas as pd
    ventas_maximos_query = """WITH filtered_data AS (
                            SELECT id_tienda, material, venta_umv 
                            FROM ecommdata.venta_sku_tienda
                            WHERE fecha >= '"""+ds+"""'::date - 15
                            AND material IN ("""+list_material+"""))
                            SELECT fd.id_tienda, fd.material, fd.venta_umv
                            FROM filtered_data fd
                            LEFT JOIN ecommdata.tiendas t ON t.id = lpad(fd.id_tienda,4,'0')
                            WHERE t.status = 1
                            and fd.id_tienda not in ('1917','917');"""
    print(ventas_maximos_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(ventas_maximos_query)
    results = cursor.fetchall()
    results=pd.DataFrame(results)
    results.columns = ["id_tienda","material","venta_maxima"]
    cursor.close()
    pg_connection.close()
    return results

def ventas_maximos_apo_to_s3_am(ds):
    import pandas as pd
    import numpy as np
    import math
    import io
    
    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"stock_seguridad_apo/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    #####################
    #extraccion de datos#
    #####################

    df_materiales = promociones(ds)
    list_material = df_materiales['material'].tolist()
    print(len(list_material))
    list_material = list(dict.fromkeys(list_material))
    list_material = ' '.join(list_material)
    list_material = list_material.replace(" ", "','")
    print(list_material)

    df = materiales_dentro_ventas(list_material,ds)
    
    df_grouped = df.groupby(['fecha_inicio', 'fecha_fin']).agg(lista_materiales=('material', list)).reset_index()

    df_final = pd.DataFrame()
    for i in range(len(df_grouped.index)):
        aux_list = []
        aux_list = df_grouped.lista_materiales[i]
        aux_list = ' '.join(aux_list)
        aux_list = aux_list.replace(" ", "','")
        df_aux = ventas(aux_list,str(df_grouped.fecha_inicio[i]),str(df_grouped.fecha_fin[i]))
        df_final = pd.concat([df_final, df_aux])

    df_final["prom_ventas"]= df_final["prom_ventas"].apply(np.ceil)
    
    list_material_aux = []
    list_material_aux = df_materiales['material'].tolist()
    list_material_aux = [item.lstrip('0') for item in list_material_aux]
    n=10
    output=[list_material_aux[i:i + n] for i in range(0, len(list_material_aux), n)]
    print(output)
    df_maximas = pd.DataFrame()
    for x in range(len(output)):
        aux_df = ventas_maximas(str(output[x]).replace('[','').replace(']',''),ds)
        aux_df = aux_df.groupby(['id_tienda', 'material'])['venta_maxima'].max().reset_index()
        df_maximas = pd.concat([df_maximas, aux_df])
    df_maximas['id_tienda'] = df_maximas['id_tienda'].astype(str).str.rjust(4, '0')
    df_maximas['material'] = df_maximas['material'].astype(str).str.rjust(18, '0')
    print(df_maximas)

    df_final_final = df_maximas.merge(df_final, how='left', on=["id_tienda","material"])

    condlist = [df_final_final["prom_ventas"].isnull() == True,
                df_final_final["prom_ventas"]<2,
                df_final_final["prom_ventas"]>=2]
    choicelist = [df_final_final["venta_maxima"], 2,df_final_final["prom_ventas"]]

    df_final_final["stock_seguridad"] = np.select(condlist, choicelist)

    df_final_final["stock_seguridad"] = df_final_final["stock_seguridad"]*0.5

    print(df_final_final)

    condlist = [df_final_final["stock_seguridad"] < 2,
                df_final_final["stock_seguridad"]>=2]
    choicelist = [2,df_final_final["stock_seguridad"]]

    df_final_final["stock_seguridad"] = np.select(condlist, choicelist)

    df_final_final["stock_seguridad"] = df_final_final["stock_seguridad"].apply(np.ceil)

    condlist = [df_final_final["stock_seguridad"]>100,
                df_final_final["stock_seguridad"]<=100
    ]
    choicelist = [100,df_final_final["stock_seguridad"]]

    df_final_final["stock_seguridad"] = np.select(condlist, choicelist)

    df_final_final["stock_seguridad"] = df_final_final["stock_seguridad"]*0.5

    print(df_final_final)
  
    ##############
    #cargar datos#
    ##############

    buffer = io.StringIO()
    df_final_final.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"stock_seguridad_apo/{exec_date}/stock_seguridad_apo_am_{date_aux}.csv"
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

def carga_stock_seguridad_janis_am(ds,ti):
    import requests
    import pandas as pd
    import datetime
    import json

    exec_date = ds.replace("-", "/")
    prefix = f"stock_seguridad_apo/{exec_date}/"
    print(prefix)

    filename = ti.xcom_pull(key="return_value", task_ids=["ventas_maximos_apo_to_s3_am"])[0]

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
        material = str(int(df['material'][i])).zfill(18)
        id_tienda = str(int(df['id_tienda'][i])).zfill(4)
        stock_seguridad = int(df.stock_seguridad[i])
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

def ventas_maximos_apo_to_s3_pm(ds):
    import pandas as pd
    import numpy as np
    import math
    import io
    
    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"stock_seguridad_apo_/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    #####################
    #extraccion de datos#
    #####################

    df_materiales = promociones(ds)
    list_material = df_materiales['material'].tolist()
    print(len(list_material))
    list_material = list(dict.fromkeys(list_material))
    list_material = ' '.join(list_material)
    list_material = list_material.replace(" ", "','")
    print(list_material)

    df = materiales_dentro_ventas(list_material,ds)
    
    df_grouped = df.groupby(['fecha_inicio', 'fecha_fin']).agg(lista_materiales=('material', list)).reset_index()

    df_final = pd.DataFrame()
    for i in range(len(df_grouped.index)):
        aux_list = []
        aux_list = df_grouped.lista_materiales[i]
        aux_list = ' '.join(aux_list)
        aux_list = aux_list.replace(" ", "','")
        df_aux = ventas(aux_list,str(df_grouped.fecha_inicio[i]),str(df_grouped.fecha_fin[i]))
        print(df_aux)
        df_final = pd.concat([df_final, df_aux])

    df_final["prom_ventas"]= df_final["prom_ventas"].apply(np.ceil)

    list_material_aux = []
    list_material_aux = df_materiales['material'].tolist()
    list_material_aux = [item.lstrip('0') for item in list_material_aux]
    n=10
    output=[list_material_aux[i:i + n] for i in range(0, len(list_material_aux), n)]
    print(output)
    df_maximas = pd.DataFrame()
    for x in range(len(output)):
        aux_df = ventas_maximas(str(output[x]).replace('[','').replace(']',''),ds)
        aux_df = aux_df.groupby(['id_tienda', 'material'])['venta_maxima'].max().reset_index()
        df_maximas = pd.concat([df_maximas, aux_df])
    df_maximas['id_tienda'] = df_maximas['id_tienda'].astype(str).str.rjust(4, '0')
    df_maximas['material'] = df_maximas['material'].astype(str).str.rjust(18, '0')
    print(df_maximas)

    df_final_final = df_maximas.merge(df_final, how='left', on=["id_tienda","material"])

    condlist = [df_final_final["prom_ventas"].isnull() == True,
                df_final_final["prom_ventas"]<2,
                df_final_final["prom_ventas"]>=2]
    choicelist = [df_final_final["venta_maxima"], 2,df_final_final["prom_ventas"]]

    df_final_final["stock_seguridad"] = np.select(condlist, choicelist)
    
    condlist = [df_final_final["stock_seguridad"]>100,
                df_final_final["stock_seguridad"]<= 100
    ]
    choicelist = [100,df_final_final["stock_seguridad"]]

    df_final_final["stock_seguridad"] = np.select(condlist, choicelist)

    print(df_final_final)
  
    ##############
    #cargar datos#
    ##############

    buffer = io.StringIO()
    df_final_final.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"stock_seguridad_apo/{exec_date}/stock_seguridad_apo_pm_{date_aux}.csv"
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
    prefix = f"stock_seguridad_apo/{exec_date}/"
    print(prefix)

    filename = ti.xcom_pull(key="return_value", task_ids=["ventas_maximos_apo_to_s3_pm"])[0]

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
        material = str(int(df['material'][i])).zfill(18)
        id_tienda = str(int(df['id_tienda'][i])).zfill(4)
        stock_seguridad = int(df.stock_seguridad[i])
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

    filename = ti.xcom_pull(key="return_value", task_ids=["ventas_maximos_apo_to_s3_am"])[0]

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

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    with engine.begin() as conn:
        conn.execute("TRUNCATE ecommdata.stock_seguridad_tiendas_apo") 
        df.to_sql(name="stock_seguridad_tiendas_apo",
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

    filename = ti.xcom_pull(key="return_value", task_ids=["ventas_maximos_apo_to_s3_pm"])[0]

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

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    with engine.begin() as conn:
        conn.execute("TRUNCATE ecommdata.stock_seguridad_tiendas_apo") 
        df.to_sql(name="stock_seguridad_tiendas_apo",
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
    'etl_stock_seguridad_apoteosicos',
    default_args=default_args,
    description="cargar stock de seguridad de apoteosicos",
    schedule_interval="30 1/4 * * *",
    start_date=pendulum.datetime(2023, 9, 21, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "Janis", "ecommdata_unimarc", "stock", "stock_seguidad", "ventas", "unimarc", "apoteosicos", "PATRICIO"],
) as dag:
    

    dag.doc_md = """
    Carga stock de seguridad a los productos en periodo promocional\n
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
        task_id = "ventas_maximos_apo_to_s3_am",
        python_callable = ventas_maximos_apo_to_s3_am,
    )

    t2_am = PythonOperator(
        task_id = "stock_ventas_tiendas_to_postgresql_am",
        python_callable = stock_ventas_tiendas_to_postgresql_am,
    )

    t3_am = PythonOperator(
        task_id = "carga_stock_seguridad_janis_am",
        python_callable = carga_stock_seguridad_janis_am
    )

    t1_pm = PythonOperator(
        task_id = "ventas_maximos_apo_to_s3_pm",
        python_callable = ventas_maximos_apo_to_s3_pm,
    )

    t2_pm = PythonOperator(
        task_id = "stock_ventas_tiendas_to_postgresql_pm",
        python_callable = stock_ventas_tiendas_to_postgresql_pm,
    )

    t3_pm = PythonOperator(
        task_id = "carga_stock_seguridad_janis_pm",
        python_callable = carga_stock_seguridad_janis_pm
    )

    t0 >> t1_am >> t2_am >> t3_am
    t0 >> t1_pm >> t2_pm >> t3_pm
    t0 >> t_dummy


