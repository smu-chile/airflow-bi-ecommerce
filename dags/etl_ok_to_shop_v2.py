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
    import re

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
                data = response.json()
                
                # Extracción de datos básicos
                ingredients = ", ".join([ingredient["name"] for ingredient in data.get("ingredients", [])])
                has_alcohol = data.get("hasAlcohol", False)

                # Extracción de datos de la tabla nutricional
                nutritional_table = data.get("nutritionalTable", {})
                portion_text = nutritional_table.get("portionText", "")
                portion_value = nutritional_table.get("portionValue", "").split()[0] if nutritional_table.get("portionValue") else None
                total_portions = nutritional_table.get("totalPortions", "")
                table_data = nutritional_table.get("table", [])

                # Crear un diccionario con los valores de los nutrientes
                nutrient_dict = {}
                if table_data:
                    for row in table_data[1:]:  # Saltar la primera fila que es el encabezado
                        nutrient_key = row[0].lower().replace(" ", "_")  # Ej. "Energía (kCal)" -> "energía_(kcal)"
                        medida_match = re.search(r"\((.*?)\)", nutrient_key)  # Busca lo que está entre paréntesis
                        if medida_match:
                            medida = medida_match.group(1)
                            nutrient_key = re.sub(r"\(.*?\)", "", nutrient_key).strip()  # Elimina el paréntesis y el contenido
                        else:
                            medida = "g" if nutrient_key == "nutrientes" else None  # Asigna "g" a "Nutrientes"

                        nutrient_dict[f"{nutrient_key}_medida"] = medida
                        nutrient_dict[f"{nutrient_key}_cada_100"] = row[1]
                        nutrient_dict[f"{nutrient_key}_porcion"] = row[2] if len(row) > 2 else None

                # Extracción de suitabilities
                suitabilities_dict = {}
                for suit in data.get("suitabilities", []):
                    # Si "No declara" está en la descripción, asigna 0; de lo contrario, asigna 1
                    suitabilities_dict[suit["code"]] = 1 if "No declara" in suit["description"].lower() else 0


                # Extracción de stamps
                stamps_dict = {stamp["code"]: 1 for stamp in data.get("stamps", [])}

                # Consolidar todos los datos en un solo diccionario
                return {
                    "ean": int(ean),
                    "ingredients": ingredients,
                    "has_alcohol": has_alcohol,
                    "portion_text": portion_text,
                    "portion_value": str(portion_value) if portion_value else None,
                    "total_portions": float(total_portions) if total_portions else None,
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
        limit 1000
            """
    
    df = query_to_df(query_eans)
    df = df.dropna(subset=['ean'])
    lista_eans = df['ean'].unique()

    url_list = [
        f"https://bff-unimarc-ecommerce.unimarc.cl//catalog/product/nutritional-data/{str(int(ean))}"
        for ean in lista_eans
        ]

    session = requests.session()
    thread_num = 5#40
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

    print(f"Cantidad de eans que no se encuentran en ok to shop: {len(exception_cases)}")

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

def ok_to_shop_api_to_postgres(ti):
    import numpy as np
    import pandas as pd
    
    admins_file = ti.xcom_pull(key="return_value", task_ids=["ok_to_shop_api_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+admins_file)
    if not s3_hook.check_for_key(admins_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % admins_file)

    admins_object = s3_hook.get_key(admins_file, bucket_name=s3_bucket)

    df = pd.read_csv(admins_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")

    print(f"Estas son las columnas: {df.columns}\n")
    print(f"Estos son los tipos de las columnas: {df.dtypes}\n")

    # Select only relevant columns:
    df = df[['ean', 'ingredients', 'has_alcohol', 'portion_text', 'portion_value',
       'total_portions', 'energía_(kcal)_valor', 'energía_(kcal)_peso',
       'energía_(kcal)_cant', 'proteínas_(g)_valor', 'proteínas_(g)_peso',
       'proteínas_(g)_cant', 'grasas_totales_(g)_valor',
       'grasas_totales_(g)_peso', 'grasas_totales_(g)_cant',
       'grasas_saturadas_(g)_valor', 'grasas_saturadas_(g)_peso',
       'grasas_saturadas_(g)_cant', 'colesterol_(mg)_valor',
       'colesterol_(mg)_peso', 'colesterol_(mg)_cant',
       'h._de_c._disponibles_(g)_valor', 'h._de_c._disponibles_(g)_peso',
       'h._de_c._disponibles_(g)_cant', 'azúcares_totales_(g)_valor',
       'azúcares_totales_(g)_peso', 'azúcares_totales_(g)_cant',
       'sodio_(mg)_valor', 'sodio_(mg)_peso', 'sodio_(mg)_cant',
       'lactose_free', 'egg_free', 'fish_free', 'seafood_free', 'nuts_free',
       'peanut_free', 'walnuts_free', 'sulphite_free', 'kosher', 'dairy_free',
       'soy_free', 'vegan', 'vegetarian', 'minsal_cl_alcohol',
       'minsal_cl_high_sugar', 'grasas_monoinsaturadas_(g)_valor',
       'grasas_monoinsaturadas_(g)_peso', 'grasas_monoinsaturadas_(g)_cant',
       'grasas_poliinsaturadas_(g)_valor', 'grasas_poliinsaturadas_(g)_peso',
       'grasas_poliinsaturadas_(g)_cant', 'grasas_trans_(g)_valor',
       'grasas_trans_(g)_peso', 'grasas_trans_(g)_cant',
       'minsal_cl_high_sodium', 'gluten_free', 'colesterol_(g)_valor',
       'colesterol_(g)_peso', 'colesterol_(g)_cant',
       'minsal_cl_high_saturated_fat', 'minsal_cl_high_calories',
       'fibra_(g)_valor', 'fibra_(g)_peso', 'fibra_(g)_cant', 'halal',
       'organic']]

    columns = ['ingredients', 'has_alcohol', 'portion_text', 'portion_value',
       'total_portions', 'energía_(kcal)_valor', 'energía_(kcal)_peso',
       'energía_(kcal)_cant', 'proteínas_(g)_valor', 'proteínas_(g)_peso',
       'proteínas_(g)_cant', 'grasas_totales_(g)_valor',
       'grasas_totales_(g)_peso', 'grasas_totales_(g)_cant',
       'grasas_saturadas_(g)_valor', 'grasas_saturadas_(g)_peso',
       'grasas_saturadas_(g)_cant', 'colesterol_(mg)_valor',
       'colesterol_(mg)_peso', 'colesterol_(mg)_cant',
       'h._de_c._disponibles_(g)_valor', 'h._de_c._disponibles_(g)_peso',
       'h._de_c._disponibles_(g)_cant', 'azúcares_totales_(g)_valor',
       'azúcares_totales_(g)_peso', 'azúcares_totales_(g)_cant',
       'sodio_(mg)_valor', 'sodio_(mg)_peso', 'sodio_(mg)_cant',
       'lactose_free', 'egg_free', 'fish_free', 'seafood_free', 'nuts_free',
       'peanut_free', 'walnuts_free', 'sulphite_free', 'kosher', 'dairy_free',
       'soy_free', 'vegan', 'vegetarian', 'minsal_cl_alcohol',
       'minsal_cl_high_sugar', 'grasas_monoinsaturadas_(g)_valor',
       'grasas_monoinsaturadas_(g)_peso', 'grasas_monoinsaturadas_(g)_cant',
       'grasas_poliinsaturadas_(g)_valor', 'grasas_poliinsaturadas_(g)_peso',
       'grasas_poliinsaturadas_(g)_cant', 'grasas_trans_(g)_valor',
       'grasas_trans_(g)_peso', 'grasas_trans_(g)_cant',
       'minsal_cl_high_sodium', 'gluten_free', 'colesterol_(g)_valor',
       'colesterol_(g)_peso', 'colesterol_(g)_cant',
       'minsal_cl_high_saturated_fat', 'minsal_cl_high_calories',
       'fibra_(g)_valor', 'fibra_(g)_peso', 'fibra_(g)_cant', 'halal',
       'organic']

    columns_query = ",".join(columns)
    excluded_query = ",".join(["EXCLUDED."+column for column in columns])
    values_query = "%s,"+",".join(["%s" for column in columns])
    df = df.fillna("NULL")
    records = list(df.to_records(index=False))
    
    # Change data types to native python types
    fixed_records = []
    for record in records:
        fixed_record = []
        for value in record:
            if isinstance(value, np.generic):
                fixed_record.append(value.item())
            elif value == "NULL":
                fixed_record.append(None)
            else:
                fixed_record.append(value)
        fixed_records.append(tuple(fixed_record))
    print(f"Number of records to load: {str(len(fixed_records))}")
    incremental_query = """
        INSERT INTO catalogo.ok_to_shop_v2 (ean,"""+columns_query+""") 
        VALUES ("""+values_query+""")
        ON CONFLICT (ean)
        DO UPDATE SET ("""+columns_query+""") = ("""+excluded_query+""") 
    """
    print(incremental_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.executemany(incremental_query, fixed_records)
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    print("Data loaded to Postgres en catalogo.ok_to_shop_v2 ")

    return


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
    t1 = PythonOperator(
        task_id="ok_to_shop_api_to_postgres",
        python_callable=ok_to_shop_api_to_postgres,
    )

    t0 >> t1