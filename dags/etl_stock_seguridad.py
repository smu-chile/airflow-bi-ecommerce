from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

import pendulum
from datetime import datetime, timedelta

def stock(tienda,ds):
    stock_tiendas_query = """select id_tienda, 
    glosa_tienda, 
    ref_id, 
    stock_janis, 
    stock_seguridad_janis, 
    date_part('dow',fecha) as dia, 
    date_part('week',fecha) as semana 
    from ecommdata.stock 
    where fecha >= '"""+ds+"""'::date -30 
    and stock_janis is not null 
    and surtido_ecommerce = 'True' 
    and id_tienda ='"""+tienda+"""'"""
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    #print(stock_tiendas_query)
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(stock_tiendas_query)
    results = cursor.fetchall()
    cursor.close()
    pg_connection.close()
    return results

def venta_tienda(tienda,ds):
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
        where v.fecha >= '"""+ds+"""'::date -30  
        and v.venta_umv > 0  
        and v.venta_bruta <> 0  
        and v.organizacion = 'Unimarc' 
        and p.precio_lista is not null 
        and LPAD(v.id_tienda , 4, '0') = '"""+tienda+"""') as _t 
        where precio_venta/precio_lista > 0.8 
        group by 
        _t.id_tienda, 
        _t.ref_id, 
        _t.precio_venta, 
        _t.precio_lista, 
        _t.venta_umv, 
        _t.dia, 
        _t.semana"""
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(ventas_skus_tienda_query)
    results = cursor.fetchall()
    cursor.close()
    pg_connection.close()
    return results

def stock_ventas_tiendas_to_s3(ds):
    import pandas as pd
    import numpy as np
    import io
    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"stock_seguridad/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    tiendas = ['Mirador','Los Militares','Los Leones','Coyhaique','La Chimba']
    id_tiendas = {'Los Militares':'0813','Los Leones':'3097','Coyhaique':'1917','Mirador':'0581','La Chimba': '0034'}
    #diccionario_glosa = {'Los Militares':'0469 - LOS MILITARES','Los Leones':'0333 - LOS LEONES','Coyhaique':'0442 - COYHAIQUE','Mirador':'0581 - MIRADOR'}
    ventas_tiendas_data = []
    stock_data = []
    for x in tiendas:
        print(x+" "+id_tiendas[x]+" ventas")
        #print(ds)
        data = pd.DataFrame(venta_tienda(id_tiendas[x],ds))
        #print(data)
        ventas_tiendas_data.append(data)
        print(x+" "+id_tiendas[x]+" stock")
        data1 = pd.DataFrame(stock(id_tiendas[x],ds))
        stock_data.append(data1)
        #print(data1)
    ventas_tiendas_data = pd.concat(ventas_tiendas_data)
    stock_data = pd.concat(stock_data)
    stock_data.columns=["id_tienda","glosa_tienda","ref_id","stock_janis","stock_seguridad","dia","semana"]
    ventas_tiendas_data.columns =["id_tienda","ref_id","venta","precio_lista","cantidad","dia","semana"]

    ventas_tiendas_data.precio_lista.fillna(ventas_tiendas_data.venta, inplace=True)
    ventas_tiendas_data["venta"]=ventas_tiendas_data["venta"].astype(str).astype(int)
    ventas_tiendas_data["promo"] = ((ventas_tiendas_data["venta"]/ventas_tiendas_data["precio_lista"])-1)*-100
    ventas_tiendas_data = ventas_tiendas_data[ventas_tiendas_data["promo"] <= 20]
    ventas_tiendas_data=ventas_tiendas_data[["id_tienda","ref_id","cantidad","dia","semana"]]

    aux1 = ventas_tiendas_data.groupby(by=["id_tienda","ref_id","dia","semana"], as_index=False).sum()
    axu2 = aux1.groupby(by=["id_tienda","ref_id","dia"], as_index=False).mean()
    axu2=axu2[["id_tienda","ref_id","dia","semana","cantidad"]]

    df_stock_seguridad = stock_data.merge(axu2, how='left', on=["id_tienda","ref_id","dia"])
    df_stock_seguridad=df_stock_seguridad.fillna(0)

    condlist = [df_stock_seguridad["cantidad"]>=2,
                df_stock_seguridad["cantidad"]<2]
    choicelist = [df_stock_seguridad["cantidad"], 2]
    
    df_stock_seguridad["nuevo_stock_seguridad"] = np.select(condlist, choicelist)
    df_stock_seguridad["nuevo_stock_seguridad"] = round(df_stock_seguridad["nuevo_stock_seguridad"],2)

    df_stock_seguridad=df_stock_seguridad[["ref_id","id_tienda","dia","nuevo_stock_seguridad"]]
    df_stock_seguridad_aux = df_stock_seguridad.groupby(by=["id_tienda","ref_id","dia"], as_index=False).mean()
    df_stock_seguridad_aux["nuevo_stock_seguridad"] =round(df_stock_seguridad_aux["nuevo_stock_seguridad"],0)

    df_stock_seguridad_aux["dia"]=df_stock_seguridad_aux["dia"].astype(int)
    df_stock_seguridad_aux["nuevo_stock_seguridad"]=df_stock_seguridad_aux["nuevo_stock_seguridad"].astype(int)

    print(df_stock_seguridad_aux)
    print(df_stock_seguridad_aux.info())

    buffer = io.StringIO()
    df_stock_seguridad_aux.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"stock_seguridad/{exec_date}/stock_seguridad_{date_aux}.csv"
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

def stock_ventas_tiendas_to_postgres(ti):
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text
    filename = ti.xcom_pull(key="return_value", task_ids=["stock_ventas_tiendas_to_s3"])[0]

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

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = "postgresql+psycopg2://"+username+":"+password+"@"+host+":5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    df.to_sql(name="stock_seguridad",
                con=engine,         
                schema="operaciones_unimarc",         
                if_exists='append',         
                index=False,         
                chunksize=20000,         
                method='multi')

    print("Data saved to PostgreSQL.")

    return

def carga_stock_seguridad_janis(ds,ti):
    import requests
    import pandas as pd
    import datetime
    exec_date = ds.replace("-", "/")
    prefix = f"stock_seguridad/{exec_date}/"
    print(prefix)

    filename = ti.xcom_pull(key="return_value", task_ids=["stock_ventas_tiendas_to_s3"])[0]

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
    for i in range(len(df.index)):
        print(i)
        material = df.ref_id[i].split("-")[0]
        id_tienda = str(int(df['id_tienda'][i])).zfill(4)
        stock_seguridad = int(df.nuevo_stock_seguridad[i])
        row = {"IdSku": material, "Quantity": 0, "Store": id_tienda, "MinStock": stock_seguridad, "Type": 2}
        print(row)
        payload.append(row)    
        if i % 99 == 0:
            payload = str(payload).replace("'", '"')
            response = requests.request("POST", url, headers=headers, data=payload)
            print(response.text)
            payload = []
    payload = str(payload).replace("'", '"')
    response = requests.request("POST", url, headers=headers, data=payload)
    print(response.text)
    #material = df.ref_id[0].split("-")[0]
    #id_tienda = str(int(df['id_tienda'][0])).zfill(4)
    #stock_seguridad = int(df.nuevo_stock_seguridad[0])
    #row = {"IdSku": material, "Quantity": 0, "Store": id_tienda, "MinStock": stock_seguridad, "Type": 2}
    #print(row)
    #payload.append(row)
    #response = requests.request("POST", url, headers=headers, data=payload)
    #print(response.text)

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
    description="cargar stock de sugirdad",
    schedule_interval=None,    #preguntar a mati k va por acá
    start_date=pendulum.datetime(2022, 8, 1, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "Janis", "ecommdata_unimarc", "stock", "stock_seguidad", "ventas", "unimarc"],
) as dag:
    

    dag.doc_md = """
    Extracción y carga de tabla ventas_skus y stock filtrado por lista 8 Replica hasta Workspace. \n
    UPSERT incremental basado en fecha_modificacion_unixtime.
    """ 

    t0 = PythonOperator(
        task_id = "stock_ventas_tiendas_to_s3",
        python_callable = stock_ventas_tiendas_to_s3,
        op_kwargs = {
            "schema": "ecommdata_unimarc",
            "table_name": "stock", 
            "updated_at_field": "fecha_modificacion_unixtime",
            "is_unixtime": True
        }
    )

    t1 = PythonOperator(
        task_id = "stock_ventas_tiendas_to_postgres",
        python_callable = stock_ventas_tiendas_to_postgres,
        op_kwargs = {
            "table_name": "stock", 
            "xcom_updated_date_task_id": "get_max_updated_at_date_atributos", 
            "updated_column": "date_modified"
        }
    )

    t2 = PythonOperator(
        task_id = "carga_stock_seguridad_janis",
        python_callable = carga_stock_seguridad_janis
    )

    t0 >> t1 >> t2


