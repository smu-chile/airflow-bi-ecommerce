from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook

from datetime import datetime
import pendulum

def get_nutritional_data(url, exception_cases, retries=3, backoff_factor=0.3):
    import requests
    import time

    headers = {
        'version': '1.0.0',
        'source': 'web',
        'Connection': 'keep-alive'
    }

    ean = url.split('/')[-1]

    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=10)  # Timeout para evitar colgaduras
            
            if response.status_code == 200:
                print(f"Encontramos data de para este EAN: {ean}")
                data = response.json()
                
                # Extracción de datos básicos
                ingredients = ", ".join([ingredient["name"] for ingredient in data.get("ingredients", [])])
                has_alcohol = data.get("hasAlcohol", False)

                # Extracción de datos de la tabla nutricional
                nutritional_table = data.get("nutritionalTable", {})
                portion_text = nutritional_table.get("portionText", "")
                portion_value = nutritional_table.get("portionValue", "")
                total_portions = nutritional_table.get("totalPortions", "")
                table_data = nutritional_table.get("table", [])

                # Crear un diccionario con los valores de los nutrientes
                nutrient_dict = {}
                if table_data:
                    for row in table_data[1:]:  # Saltar la primera fila que es el encabezado
                        nutrient_key = row[0].lower().replace(" ", "_")  # Ej. "Energía (kCal)" -> "energía_(kcal)"
                        nutrient_dict[f"{nutrient_key}_valor"] = row[1]
                        nutrient_dict[f"{nutrient_key}_peso"] = row[2] if len(row) > 2 else None
                        nutrient_dict[f"{nutrient_key}_cant"] = row[2] if len(row) > 2 else None

                # Extracción de suitabilities
                suitabilities_dict = {}
                for suit in data.get("suitabilities", []):
                    suitabilities_dict[suit["code"]] = suit["description"]

                # Extracción de stamps
                stamps_dict = {}
                for stamp in data.get("stamps", []):
                    stamps_dict[stamp["code"]] = stamp["description"]

                # Consolidar todos los datos en un solo diccionario
                return {
                    "ean": ean,
                    "ingredients": ingredients,
                    "has_alcohol": has_alcohol,
                    "portion_text": portion_text,
                    "portion_value": portion_value,
                    "total_portions": total_portions,
                    **nutrient_dict,
                    **suitabilities_dict,
                    **stamps_dict
                }
            else:
                print(f"Error: Received status code {response.status_code} for URL: {url}")
                exception_cases.append(url)
                return None
            
        except requests.exceptions.Timeout:
            print(f"Timeout occurred for URL: {url}")
            exception_cases.append(url)
        except requests.exceptions.RequestException as e:
            print(f"Request exception occurred for URL: {url}: {e}")
            exception_cases.append(url)
        
        # Exponential backoff for retries
        time.sleep(backoff_factor * (2 ** attempt))
    
    print(f"Failed to retrieve data after {retries} attempts for URL: {url}")
    return None

def query_to_df(query):
    import pandas as pd
    print(query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn_prod")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query)
    column_names = [desc[0] for desc in cursor.description]
    results = cursor.fetchall()
    results = pd.DataFrame(results, columns=column_names)
    cursor.close()
    pg_connection.close()
    return results

def bulk_get(url_sublist, responses, exception_cases):
    for url in url_sublist:
        response = get_nutritional_data(url,exception_cases)  # Solo se pasa el URL a la función
        if response:
            responses.append(response)
        else:
            exception_cases.append(url)
    return

def ok_to_shop_api_to_s3(ds):
    import requests
    from threading import Thread
    import pandas as pd
    import io

    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"ok_to_shop_v2/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    query_eans = """select distinct se.ean::int8
        from ecommdata.lista8 l 
        left join ecommdata.sku_ean se on se.ref_id = concat(l.material,'-',l.umv)
        where l.material is not null
            """
    
    df = query_to_df(query_eans)
    df = df.dropna(subset=['ean'])
    lista_eans = df['ean'].unique()

    url_list = [
        f"https://bff-unimarc-ecommerce.unimarc.cl//catalog/product/nutritional-data/{str(int(ean))}"
        for ean in lista_eans
        ]

    session = requests.session()
    thread_num = 4#40
    task_num = len(url_list)//thread_num # division entera
    adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=thread_num)
    session.mount('https://', adapter)
    thread_tasks = []
    count = 0
    responses = []
    exception_cases = []

    for thr in range(thread_num):
        new_task = Thread(target=bulk_get, args=[url_list[task_num*count:task_num*(count+1)], responses, exception_cases], daemon=True)
        new_task.start()
        thread_tasks.append(new_task)
        count = count + 1
    # tareas resagadas:
    if task_num*thread_num != len(url_list):
        new_task = new_task = Thread(target=bulk_get, args=[url_list[task_num*thread_num:], responses, exception_cases], daemon=True)
        new_task.start()
        thread_tasks.append(new_task)
    for task in thread_tasks:
        task.join()
        thread_tasks = []

    df_final = pd.DataFrame(responses)

    print(f"\nCantidad de eans que no se encuentran en ok to shop: {len(exception_cases)}")

    print(df_final)
    df_final.info()

    buffer = io.StringIO()
    df_final.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"ok_to_shop_v2/{exec_date}/ok_to_shop_{date_aux}.csv"
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


default_args = {
    "owner": "ecommerce_ops",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_ok_to_shop_v2',
    default_args=default_args,
    description="""Cargar datos de eans de productos al consumir API ok_to_shop""",

    schedule_interval="0 10 * * *",
    start_date=pendulum.datetime(2023, 5, 21, tz="America/Santiago"),
    catchup=False,
    tags=["API", "ok_to_shop", "PATRICIO"],
) as dag:

    dag.doc_md = """
    Cargar datos de eans de productos al consumir API ok_to_shop a postgres y S3
    upsert
    """

    t0 = PythonOperator(
        task_id="ok_to_shop_api_to_s3",
        python_callable=ok_to_shop_api_to_s3,
    )