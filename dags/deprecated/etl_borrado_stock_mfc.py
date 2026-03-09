from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import pendulum


from datetime import datetime, timedelta

def ubicaciones_mfc():
    import pandas as pd
    ubi_mfc_query = """select CONCAT(LPAD(sap_code, 18, '0'), '-', measurement_unit) as ref_id,
                    mfc_is_item_side
                    from ecommdata.ubicacion_mfc
                    where mfc_is_item_side = 'FLO'"""
    print(ubi_mfc_query)
    pg_hook = PostgresHook(conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(ubi_mfc_query)
    results = cursor.fetchall()
    results=pd.DataFrame(results)
    print(results)
    results.columns = ["ref_id","mfc_side"]
    cursor.close()
    pg_connection.close()
    return results

def lista8_0917():
    import pandas as pd
    lista8_0917_query = """select material ||'-' ||umv, id_tienda
                    from ecommdata.lista8_lite
                    where id_tienda = '0917'"""
    print(lista8_0917_query)
    pg_hook = PostgresHook(conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(lista8_0917_query)
    results = cursor.fetchall()
    results=pd.DataFrame(results)
    print(results)
    results.columns = ["ref_id","id_tienda"]
    cursor.close()
    pg_connection.close()
    return results

def load_to_s3(ds):
    import pandas as pd
    import numpy as np
    import io
    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"borrado_stock_mfc/{exec_date}/"
    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    #####################
    #Extraccion de datos#
    #####################

    df_ubicaciones = ubicaciones_mfc()
    df_lista8_0917 = lista8_0917()

    #########################
    #Transformación de datos#
    #########################

    df_final = (df_ubicaciones.merge(df_lista8_0917, on=["ref_id"], how='left', indicator=True)
     .query('_merge == "left_only"')
     .drop('_merge', 1))
    df_final = df_final["ref_id","mfc_side"]
    print(df_final)

    ###################
    #Carga de datos S3#
    ###################

    buffer = io.StringIO()
    df_final.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"borrado_stock_mfc/{exec_date}/borrado_stock_mfc_{date_aux}.csv"
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

def load_to_janis(ds,ti):

    import requests
    import pandas as pd
    import datetime
    exec_date = ds.replace("-", "/")
    prefix = f"borrado_stock_mfc/{exec_date}/"
    print(prefix)
    filename = ti.xcom_pull(key="return_value", task_ids=["load_to_s3"])[0]

    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
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
        row = {"IdSku": material, "Quantity": 0, "Store": '1917'}
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
    "retries": 0
}

with DAG(
    'etl_borrado_stock_mfc',
    default_args=default_args,
    description="Borrado de stock janis mfc para productos FLO que no se encuentren en surtido 0917.",
    schedule="0 10 * * *",
    start_date=pendulum.datetime(2023, 7, 4, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["DATA", "SAP", "mfc", "lista8", "stock", "janis"],
) as dag:

    dag.doc_md = """
    Borrado de stock janis mfc en base a productos FLO que no se encuentran en surtido 0917."
    """ 
    t0 = PythonOperator(
        task_id = "load_to_s3",
        python_callable = load_to_s3
    )
    t1 = PythonOperator(
        task_id = "load_to_janis",
        python_callable = load_to_janis
    )
    t0 >> t1