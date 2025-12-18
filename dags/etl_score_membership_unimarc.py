from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from utils.slack_utils import dag_success_slack, dag_failure_slack

from datetime import datetime

import pendulum

################################################################################################################
#                                                                                                              #
#                                  Creacion de archivo s3                                                      #
#                                                                                                              # 
################################################################################################################
def _join_stock_from_s3(ds, ti):
    import json
    import pandas as pd
    import io

    exec_date = ds.replace("-", "/")

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
    
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()

    # Obtén la fecha de ejecución en formato YYYYMMDD
    exec_date_formatted = datetime.now().strftime("%Y%m%d")

    join_file_name = f"Membresia/score/{exec_date}/{exec_date_formatted}.csv"
    if s3_hook.check_for_key(join_file_name, bucket_name=s3_bucket):
            print(f"File {join_file_name} already exists on bucket: {s3_bucket}. Skipping...")

    wf_membership_query = f""" 
            select wp.n_promocion ,
            wp.nombre_promocion ,
            wp.descripcion_material,
            s.ref_id ,
            s.vtex_id 
            from ecommdata.workflow_promociones wp 
            left join ecommdata.skus s on s.erp_id = wp.material 
            WHERE wp.fecha_inicio_de_promocion <= CURRENT_DATE 
            AND wp.fecha_fin_de_promocion >= CURRENT_DATE
            and wp.nombre_promocion like '%MEMB%' ;
    """

    cursor.execute(wf_membership_query)
    results = cursor.fetchall()
    columns = [i[0] for i in cursor.description]

    if len(results) == 0:
        print(f"No records found. Skipping...")
        cursor.close()
        pg_connection.close()
        return
    

    df = pd.DataFrame(results, columns=columns)
    print(f"Number of records found on stock: {len(df.index)}")
    
    buffer = io.StringIO()
    df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    buffer.seek(0)
        
    s3_hook.load_string(buffer.getvalue(),
                key=join_file_name,
                bucket_name=s3_bucket,
                replace=True,
                encrypt=False)
    print(f"File load on S3: {join_file_name}")
    
    cursor.close()
    pg_connection.close()
    return
################################################################################################################
#                                                                                                              #
#                                  Crear lista de productos de hoy                                             #
#                                                                                                              # 
################################################################################################################
def _get_ref_ids_from_s3_today(ds, ti):
    import pandas as pd
    import io
    from airflow.models import Variable
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from datetime import datetime

    exec_date = ds.replace("-", "/")
    exec_date_formatted = datetime.now().strftime("%Y%m%d")

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
    
    file_key = f"Membresia/score/{exec_date}/{exec_date_formatted}.csv"

    if not s3_hook.check_for_key(file_key, bucket_name=s3_bucket):
        print(f"File {file_key} not found in bucket {s3_bucket}")
        return []

    file_content = s3_hook.read_key(key=file_key, bucket_name=s3_bucket)
    
    df = pd.read_csv(io.StringIO(file_content))

    if 'ref_id' not in df.columns:
        print("Column 'ref_id' not found in CSV.")
        return []

    ref_ids = df['ref_id'].dropna().unique().tolist()
    return ref_ids
################################################################################################################
#                                                                                                              #
#                                  Crear lista de productos de ayer                                            #
#                                                                                                              # 
################################################################################################################
def _get_ref_ids_from_s3_yesterday(ds, ti):
    import pandas as pd
    import io
    from airflow.models import Variable
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from datetime import datetime, timedelta

    # Fecha de ejecución - 1 día
    execution_date = datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=1)
    exec_date = execution_date.strftime("%Y/%m/%d")
    exec_date_formatted = execution_date.strftime("%Y%m%d")

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
    
    file_key = f"Membresia/score/{exec_date}/{exec_date_formatted}.csv"

    if not s3_hook.check_for_key(file_key, bucket_name=s3_bucket):
        print(f"File {file_key} not found in bucket {s3_bucket}")
        return []

    file_content = s3_hook.read_key(key=file_key, bucket_name=s3_bucket)
    
    df = pd.read_csv(io.StringIO(file_content))

    if 'ref_id' not in df.columns:
        print("Column 'ref_id' not found in CSV.")
        return []

    ref_ids = df['ref_id'].dropna().unique().tolist()
    return ref_ids
################################################################################################################
#                                                                                                              #
#                                  Crear lista de product id                                                   #
#                                                                                                              # 
################################################################################################################
def _get_vtex_product_ids_today(ds, ti):
    import requests
    import time

    # Recuperar ref_ids desde XCom
    ref_ids = ti.xcom_pull(task_ids='_get_ref_ids_from_s3_today')  # Ajusta el task_id según el tuyo

    if not ref_ids:
        print("No ref_ids found. Exiting...")
        return []
    
    X_VTEX_API_AppKey = Variable.get("X_VTEX_API_AppKey")
    X_VTEX_API_AppToken = Variable.get("X_VTEX_API_AppToken")

    headers = {
        "x-vtex-api-appKey": X_VTEX_API_AppKey,
        "X-vtex-api-appToken": X_VTEX_API_AppToken 
    }

    base_url = "https://unimarc.vtexcommercestable.com.br/api/catalog_system/pvt/products/productgetbyrefid/"
    product_ids = []

    for ref_id in ref_ids:
        url = f"{base_url}{ref_id}"
        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, dict) and 'Id' in data:
                    product_ids.append(data['Id'])
                else:
                    print(f"Ref ID {ref_id} - No 'Id' in response.")
            else:
                print(f"Ref ID {ref_id} - Failed with status {response.status_code}")
        except Exception as e:
            print(f"Error fetching ref_id {ref_id}: {e}")
        time.sleep(0.3)  # evitar rate limit

    return product_ids
################################################################################################################
#                                                                                                              #
#                                  Crear lista de product id ayer                                              #
#                                                                                                              # 
################################################################################################################
def _get_vtex_product_ids_yesterday(ds, ti):
    import requests
    import time

    # Recuperar ref_ids desde XCom
    ref_ids = ti.xcom_pull(task_ids='_get_ref_ids_from_s3_yesterday')  # Ajusta el task_id según el tuyo

    if not ref_ids:
        print("No ref_ids found. Exiting...")
        return []

    X_VTEX_API_AppKey = Variable.get("X_VTEX_API_AppKey")
    X_VTEX_API_AppToken = Variable.get("X_VTEX_API_AppToken")

    headers = {
        "x-vtex-api-appKey": X_VTEX_API_AppKey,
        "X-vtex-api-appToken": X_VTEX_API_AppToken 
    }


    base_url = "https://unimarc.vtexcommercestable.com.br/api/catalog_system/pvt/products/productgetbyrefid/"
    product_ids = []

    for ref_id in ref_ids:
        url = f"{base_url}{ref_id}"
        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, dict) and 'Id' in data:
                    product_ids.append(data['Id'])
                else:
                    print(f"Ref ID {ref_id} - No 'Id' in response.")
            else:
                print(f"Ref ID {ref_id} - Failed with status {response.status_code}")
        except Exception as e:
            print(f"Error fetching ref_id {ref_id}: {e}")
        time.sleep(0.3)  # evitar rate limit

    return product_ids

################################################################################################################
#                                                                                                              #
#                          Coloca score 0 a los productos de ayer                                              #
#                                                                                                              # 
################################################################################################################
def _update_score_for_products_yesterday(ds, ti):
    import requests
    import time

    # Recuperar product_ids desde XCom
    product_ids = ti.xcom_pull(task_ids='_get_vtex_product_ids_today')  # Ajusta el task_id si es distinto

    if not product_ids:
        print("No product_ids found. Exiting...")
        return

    # Parámetros base
    account_name = "unimarc"
    environment = "vtexcommercestable"

    X_VTEX_API_AppKey = Variable.get("X_VTEX_API_AppKey")
    X_VTEX_API_AppToken = Variable.get("X_VTEX_API_AppToken")

    headers = {
        "X-VTEX-API-AppKey": X_VTEX_API_AppKey,
        "X-VTEX-API-AppToken": X_VTEX_API_AppToken,
        "Content-Type": "application/json"
    }

    for product_id in product_ids:
        url = f"https://{account_name}.{environment}.com.br/api/catalog/pvt/product/{product_id}"
        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                product_data = response.json()

                # Modificar el campo Score
                product_data["Score"] = 0

                # PUT con los cambios
                put_response = requests.put(url, headers=headers, json=product_data)

                print(f"Product {product_id} - PUT Status: {put_response.status_code}")
                if put_response.status_code != 200:
                    print(f"Error: {put_response.text}")
            else:
                print(f"Product {product_id} - GET failed with status {response.status_code}")
        except Exception as e:
            print(f"Error with product {product_id}: {e}")
        time.sleep(0.3)  # Para evitar throttling

    return

################################################################################################################
#                                                                                                              #
#                          Coloca score 1 a los produtos de hoy                                                #
#                                                                                                              # 
################################################################################################################

def _update_score_for_products_today(ds, ti):
    import requests
    import time

    # Recuperar product_ids desde XCom
    product_ids = ti.xcom_pull(task_ids='get_vtex_product_ids')  # Ajusta el task_id si es distinto

    if not product_ids:
        print("No product_ids found. Exiting...")
        return

    # Parámetros base
    account_name = "unimarc"
    environment = "vtexcommercestable"

    X_VTEX_API_AppKey = Variable.get("X_VTEX_API_AppKey")
    X_VTEX_API_AppToken = Variable.get("X_VTEX_API_AppToken")

    headers = {
        "X-VTEX-API-AppKey": X_VTEX_API_AppKey,
        "X-VTEX-API-AppToken": X_VTEX_API_AppToken,
        "Content-Type": "application/json"
    }

    for product_id in product_ids:
        url = f"https://{account_name}.{environment}.com.br/api/catalog/pvt/product/{product_id}"
        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                product_data = response.json()

                # Modificar el campo Score
                product_data["Score"] = 1

                # PUT con los cambios
                put_response = requests.put(url, headers=headers, json=product_data)

                print(f"Product {product_id} - PUT Status: {put_response.status_code}")
                if put_response.status_code != 200:
                    print(f"Error: {put_response.text}")
            else:
                print(f"Product {product_id} - GET failed with status {response.status_code}")
        except Exception as e:
            print(f"Error with product {product_id}: {e}")
        time.sleep(0.3)  # Para evitar throttling

    return


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

# Definir el DAG

with DAG(
    'etl_score_membresia',
    default_args=default_args,
    description='Guarda promociones comparadas en S3 y las carga en la base de datos',
    schedule_interval='0 9 * * *',
    start_date=pendulum.datetime(2024, 5, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["DATA", "postgres", "VTEX", "score_vtex", "S3", "NICOLAS"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
        Coloca el score necesario para los productos de membresia
        """ 
    # Definir las tareas

    t0 = PythonOperator(
            task_id='_join_stock_from_s3',
            python_callable=_join_stock_from_s3
        )
    t1 = PythonOperator(
            task_id='List_ref_id_yesterday',
            python_callable=_get_ref_ids_from_s3_yesterday

        )
    t2 = PythonOperator(
            task_id='put_score_0_yesterday',
            python_callable=_update_score_for_products_yesterday
        )
    t3 = PythonOperator(
            task_id='list_ref_id_today',
            python_callable=_get_vtex_product_ids_today
        )
    t4 = PythonOperator(
            task_id='put_score_1_today',
            python_callable=_update_score_for_products_today
        )
    t0 >> t1 >> t2 >> t3 >> t4