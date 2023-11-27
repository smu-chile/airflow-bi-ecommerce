from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pendulum


def load_fixed_prices_to_s3(ds):
    import pandas as pd
    import requests
    import numpy as np
    import io
    from io import StringIO

    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"vtex_alvi/listas_precios/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()

    print("Getting vtex_ids of products in WP with Fixed Price")
    query = """select distinct s.vtex_id
        from ecommdata_alvi.workflow_promociones wp 
        inner join ecommdata_alvi.skus s on s.ref_id = wp.material||'-'|| CASE WHEN wp.umv = 'ST' THEN 'UN' WHEN wp.umv = 'CS' THEN 'CJ' WHEN wp.umv = 'DIS' THEN 'DIS' END
        where wp.tipo_promocion = 10
        and wp.umv not in ('KG','KGV')
        and wp.fecha_inicio_de_promocion  <= current_date + interval '1 days'
        and wp.fecha_fin_de_promocion >= current_date  
        AND s.vtex_id IS NOT NULL; """
    cursor.execute(query)
    results = cursor.fetchall()
    list_skus = [result[0] for result in results]
    cursor.close()
    pg_connection.close()
    print(f"Se obtuvieron {len(list_skus)} skus")

    X_VTEX_API_AppKey = Variable.get("X_VTEX_ALVI_API_Appkey")
    X_VTEX_API_AppToken = Variable.get("X_VTEX_ALVI_API_Apptoken")
    accountName = Variable.get("VTEX_ALVI_ACCOUNT_NAME")
    headers = {
        'Accept': "application/json",
        'Content-Type': "application/json",
        "X-VTEX-API-AppKey": X_VTEX_API_AppKey,
        "X-VTEX-API-AppToken":  X_VTEX_API_AppToken,
        "Connection": "keep-alive"
    }

    df_final = pd.DataFrame()
    for itemId in list_skus:
        df = pd.DataFrame()
        GET_FIXED_PRICES = f"https://api.vtex.com.br/{accountName}/pricing/prices/{str(itemId)}/fixed"
        print("GET_FIXED_PRICES: ", GET_FIXED_PRICES)
        r = requests.get(GET_FIXED_PRICES, headers=headers)
        print(r.content)
        if r.status_code == 404:
            print("Error 404: Recurso no encontrado")
            continue
        elif r.status_code == 200:
            r.raise_for_status()
            data = r.json()
            df = pd.DataFrame(data)
            if not df.empty:
                if 'dateRange' not in df.columns:
                    continue
                df = df.assign(vtex_id=itemId)
                for index, row in df.iterrows():
                    if pd.isna(row['dateRange']) or (row['dateRange'] == ''):
                        df.at[index, 'Date From'] = 'NULL'
                        df.at[index, 'Date To'] = 'NULL'
                    else:
                        df.at[index, 'Date From'] = row['dateRange']['from']
                        df.at[index, 'Date To'] = row['dateRange']['to']
                df = df.drop('dateRange', axis=1)
            df = df.reindex(columns=['vtex_id', 'tradePolicyId', 'value',
                            'listPrice', 'minQuantity', 'Date From', 'Date To'])
            df_final = pd.concat([df_final, df])
        else:
            print(f"No se obtuvo info del producto {itemId}")

    df_final = df_final.rename(columns={'vtex_id': 'SKU ID', 'tradePolicyId': 'Trade Policy',
                                        'value': 'Price', 'listPrice': 'List Price',
                                        'minQuantity': 'Min Quantity'})
    df_final = df_final.astype(
        {'SKU ID': 'int', 'Price': 'int64', 'Min Quantity': 'int64'})
    
    print(df_final.info())
    
    buffer = io.StringIO()
    df_final.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"vtex_alvi/listas_precios/{exec_date}/listas_precios_{date_aux}.csv"
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

def fixed_prices_to_postgres(ti):
    import pandas as pd
    import sqlalchemy

    fixed_prices_file = ti.xcom_pull(key="return_value", task_ids=["load_fixed_prices_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+fixed_prices_file)
    if not s3_hook.check_for_key(fixed_prices_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % fixed_prices_file)

    fixed_prices_object = s3_hook.get_key(fixed_prices_file, bucket_name=s3_bucket)

    df = pd.read_csv(fixed_prices_object.get()["Body"])

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")

    conn_url = "postgresql+psycopg2://"+username + \
        ":"+password+"@"+host+":5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    df.to_sql(name="listas_precios_vtex",
                   con=engine,
                   schema="ecommdata_alvi",
                   if_exists='append',
                   index=False,
                   chunksize=20000,
                   method='multi')
    return


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'etl_listas_precios_vtex_alvi',
    default_args=default_args,
    description="Extracción y carga de la tabla listas_precios_vtex desde API.",
    schedule_interval="0 4 * * *",
    start_date=pendulum.datetime(2023, 6, 6, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["vtex", "ALVI","promociones", "listas_precios", "workflow_promociones"],
) as dag:

    dag.doc_md = """
    Extracción y carga de la tabla listas_precios_vtex desde API.
    """

    t0 = PythonOperator(
        task_id="load_fixed_prices_to_s3",
        python_callable=load_fixed_prices_to_s3
    )

    t1 = PostgresOperator(
        task_id="truncate_listas_precios",
        postgres_conn_id="postgresql_conn",
        sql="TRUNCATE ecommdata_alvi.listas_precios_vtex",
    )

    t2 = PythonOperator(
        task_id="fixed_prices_to_postgres",
        python_callable=fixed_prices_to_postgres
    )

    t0 >> t1 >> t2
