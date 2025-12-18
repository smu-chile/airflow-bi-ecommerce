from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

from utils.slack_utils import dag_success_slack, dag_failure_slack

import pendulum

from datetime import datetime, timedelta

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

def bulk_get(url_list, responses, session, exception_cases, X_VTEX_API_AppKey, X_VTEX_API_AppToken):
    """
    Función para realizar múltiples solicitudes en un hilo.
    """
    import pandas as pd
    headers = {
        'Accept': "application/json",
        'Content-Type': "application/json",
        "X-VTEX-API-AppKey": X_VTEX_API_AppKey,
        "X-VTEX-API-AppToken": X_VTEX_API_AppToken,
        "Connection": "keep-alive"
    }
    for url in url_list:
        try:
            response = session.get(url, headers=headers)
            if response.status_code == 200:
                responses.append({"url": url, "json": response.json()})
            else:
                exception_cases.append({"url": url, "status_code": response.status_code})
        except Exception as e:
            exception_cases.append({"url": url, "error": str(e)})

def fetch_vtex_prices_to_df_multithread_legacy(vtex_ids, thread_num=10):
    """
    Realiza solicitudes a la API VTEX usando hilos.
    """
    import pandas as pd
    import requests
    import threading
    url_list = [f"https://api.vtex.com/unimarc/pricing/prices/{vtex_id}" for vtex_id in vtex_ids]
    session = requests.Session()
    task_num = len(url_list) // thread_num
    adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=thread_num)
    session.mount('https://', adapter)

    thread_tasks = []
    responses = []
    exception_cases = []

    X_VTEX_API_AppKey = Variable.get("VTEX_API_KEY")
    X_VTEX_API_AppToken = Variable.get("VTEX_API_TOKEN")

    # Crear hilos
    count = 0
    for thr in range(thread_num):
        new_task = threading.Thread(
            target=bulk_get,
            args=(
                url_list[task_num * count:task_num * (count + 1)],
                responses,
                session,
                exception_cases,
                X_VTEX_API_AppKey,
                X_VTEX_API_AppToken
            ),
            daemon=True
        )
        new_task.start()
        thread_tasks.append(new_task)
        count += 1

    # Tareas rezagadas
    if task_num * thread_num != len(url_list):
        new_task = threading.Thread(
            target=bulk_get,
            args=(
                url_list[task_num * thread_num:],
                responses,
                session,
                exception_cases,
                X_VTEX_API_AppKey,
                X_VTEX_API_AppToken
            ),
            daemon=True
        )
        new_task.start()
        thread_tasks.append(new_task)

    # Esperar que los hilos terminen
    for task in thread_tasks:
        task.join()

    # Procesar respuestas
    results = []
    print(responses)
    for response in responses:
        try:
            fixed_prices = response["json"].get("fixedPrices", [])
            for price in fixed_prices:
                processed_data = {
                    "itemId": response["json"].get("itemId"),
                    "listPrice": response["json"].get("listPrice"),
                    "costPrice": response["json"].get("costPrice"),
                    "markup": response["json"].get("markup"),
                    "basePrice": response["json"].get("basePrice"),
                    "vtex_id": response["url"].split("/")[-1],
                    "tradePolicyId": price.get("tradePolicyId"),
                    "value": price.get("value"),
                    "fixedListPrice": price.get("listPrice"),
                    "minQuantity": price.get("minQuantity"),
                    "dateRangeFrom": price.get("dateRange", {}).get("from"),
                    "dateRangeTo": price.get("dateRange", {}).get("to"),
                }
                results.append(processed_data)
        except KeyError as e:
            exception_cases.append({"url": response["url"], "error": str(e)})

    # Convertir a DataFrame
    df = pd.DataFrame(results)
    return df

def precios_vtex_to_s3(ds):
    import pandas as pd
    import numpy as np
    import io
    from io import StringIO

    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    query = """select distinct pt.ref_id , s.vtex_id as vtex_sku , p.vtex_id as vtex_producto , p.nombre 
            from ecommdata.productos_tienda pt 
            left join ecommdata.skus s on s.ref_id = pt.ref_id 
            left join ecommdata.productos p on p.ref_id = pt.ref_id 
            where pt.id_tienda in ('0054','0053')"""
    df = query_to_df(query)

    df = df[~df['vtex_sku'].isna()]  # Eliminar filas con NaN
    df = df[np.isfinite(df['vtex_sku'])]  # Eliminar filas con infinitos (inf o -inf)

    df['vtex_sku'] = df['vtex_sku'].astype(int)
    vtex_lista = df['vtex_sku'].tolist()

    # Ejecutar con hilos
    df_prices = fetch_vtex_prices_to_df_multithread_legacy(vtex_lista, thread_num=2)

    # Filtrar el DataFrame donde tradePolicyId sea 40 o 39
    df_filtered = df_prices[df_prices['tradePolicyId'].isin(['40', '39'])]

    # Realizar un merge left entre df_filtered y df
    df_final = pd.merge(df_filtered, df, how='left', left_on='vtex_id', right_on='vtex_sku')

    buffer = io.StringIO()
    df_final.to_csv(buffer, header=True, index=False, encoding="utf-8")
    buffer.seek(0)

    filename = f"precios_vtex/{exec_date}/precios_vtex_{date_aux}.csv"

    print(f"con fecha {ds} y nombre de filename como {filename}")
    s3_hook.load_string(buffer.getvalue(),
                key=filename,
                bucket_name=s3_bucket,
                replace=True,
                encrypt=False)

    print("se logro transformar los dataframes a archivos .csv")
    print(f"File load on S3: {filename}")

    return filename

def precios_vtex_to_postgres(ti):
    import pandas as pd
    import sqlalchemy
    import numpy as np

    filename = ti.xcom_pull(key="return_value", task_ids=["precios_vtex_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+filename)
    if not s3_hook.check_for_key(filename, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % filename)

    hook_object = s3_hook.get_key(filename, bucket_name=s3_bucket)

    df = pd.read_csv(hook_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")

    df.info()

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    with engine.begin() as conn:
        conn.execute("TRUNCATE ecommdata.precios_vtex") 
        df.to_sql(name="precios_vtex",
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
    'etl_precios_vtex',
    default_args=default_args,
    description="descarga por API vtex los precios ",
    schedule_interval="30 9 * * *",
    start_date=pendulum.datetime(2023, 6, 12, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "Janis", "precios", "PATRICIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:
    

    dag.doc_md = """
    precios vtex \n
    guardar en S3.
    """ 
    t0 = PythonOperator(
        task_id = "precios_vtex_to_s3",
        python_callable = precios_vtex_to_s3,
    )

    t1 = PythonOperator(
        task_id = "precios_vtex_to_postgres",
        python_callable = precios_vtex_to_postgres,
    )

    t0 >> t1