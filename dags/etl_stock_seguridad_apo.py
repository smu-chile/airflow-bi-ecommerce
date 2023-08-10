from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

import pendulum
from datetime import datetime, timedelta

def stock(ds):
    stock_tiendas_query = """ """
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    print(stock_tiendas_query)
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(stock_tiendas_query)
    results = cursor.fetchall()
    cursor.close()
    pg_connection.close()
    return results

def promociones(ds):
    import pandas as pd
    promociones_query = """ """
    print(promociones_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(promociones_query)
    results = cursor.fetchall()
    results=pd.DataFrame(results)
    results.columns = ["ref_id","fecha_inicio","fecha_final","id_mecanica"]
    cursor.close()
    pg_connection.close()

    return results


def venta_tienda(ds):
    ventas_skus_tienda_query = """ """
    print(ventas_skus_tienda_query)
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

    #####################
    #extraccion de datos#
    #####################
    
  

    ##############
    #cargar datos#
    ##############

    buffer = io.StringIO()
    df_final.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"stock_seguridad_apo/{exec_date}/stock_seguridad_{date_aux}.csv"
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
        row = {"IdSku": material, "Quantity": 0, "Store": id_tienda,"MinStockDiff": True, "MinStock": stock_seguridad, "Type": 2}
        print(row)
        payload.append(row)    
        if i % 499 == 0:
            payload = str(payload).replace("'", '"')
            response = requests.request("POST", url, headers=headers, data=payload)
            print(response.text)
            payload = []
    payload = str(payload).replace("'", '"')
    response = requests.request("POST", url, headers=headers, data=payload)
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
    'etl_stock_seguridad',
    default_args=default_args,
    description="cargar stock de seguridad",
    schedule_interval="0 10 * * *",
    start_date=pendulum.datetime(2023, 6, 12, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "Janis", "ecommdata_unimarc", "stock", "stock_seguidad", "ventas", "unimarc"],
) as dag:
    

    dag.doc_md = """
    Carga stock de seguridad \n
    guardar en S3.
    """ 

    t0 = PythonOperator(
        task_id = "stock_ventas_tiendas_to_s3",
        python_callable = stock_ventas_tiendas_to_s3,
    )

    t1 = PythonOperator(
        task_id = "carga_stock_seguridad_janis",
        python_callable = carga_stock_seguridad_janis
    )

    t0 >> t1


