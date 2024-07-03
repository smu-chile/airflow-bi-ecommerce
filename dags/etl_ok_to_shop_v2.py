from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook

from datetime import datetime
import pendulum

def get_nutritional_data(url):
    import requests
    headers = {
    'version': '1.0.0',
    'source': 'web',
    'Connection': 'keep-alive'
    }
    #print(f"\n {url}")
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        #print(f"Error: {response.status_code}")
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

def ok_to_shop_api_to_s3(ds):
    import pandas as pd
    import requests
    import math
    import io

    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"stock_seguridad_apo/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    query_eans = """select distinct ean
            from ecommdata.sku_ean se 
            where ref_id in (select concat(material,'-',umv)as ref_id from ecommdata.lista8 l)
            """
    
    df = query_to_df(query_eans)
    df = df.head(50)
    data = []

    for index, value in df['ean'].iteritems():
        url = f"https://bff-unimarc-ecommerce.unimarc.cl/catalog/product/nutritional-data/{str(value)}"
        json_data = get_nutritional_data(url)
        if json_data:
            product_data = {
                'EAN': value,
                'ingredients': [ingredient['name'] for ingredient in json_data.get('ingredients', [])],
                **json_data
            }
            data.append(product_data)

    df = pd.DataFrame(data)

    print(df)
    df.info()

    buffer = io.StringIO()
    df.to_csv(buffer, header=True, index=False, encoding="utf-8")
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
    """

    t0 = PythonOperator(
        task_id="ok_to_shop_api_to_s3",
        python_callable=ok_to_shop_api_to_s3,
    )