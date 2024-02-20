from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pendulum


def get_fixed_prices(ti):
    import pandas as pd
    import requests
    import json

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    print("Getting vtex_ids of products in WP with Fixed Price")
    query = """select distinct s.vtex_id
        from ecommdata.workflow_promociones wp 
        inner join ecommdata.skus s on s.ref_id = wp.material||'-'|| CASE WHEN wp.umv = 'ST' THEN 'UN' WHEN wp.umv = 'CS' THEN 'CJ' WHEN wp.umv = 'DIS' THEN 'DIS' END
        where wp.tipo_promocion = 4
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

    X_VTEX_API_AppKey = Variable.get("X_VTEX_API_AppKey")
    X_VTEX_API_AppToken = Variable.get("X_VTEX_API_AppToken")
    accountName = Variable.get("VTEX_ACCOUNT_NAME")
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
    return df_final.to_json(orient='records')


def upload_fixed_prices(ti):
    import pandas as pd
    import sqlalchemy

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")

    conn_url = "postgresql+psycopg2://"+username + \
        ":"+password+"@"+host+":5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    json_data = ti.xcom_pull(task_ids=["get_fixed_prices"])[0]
    df_data = pd.read_json(json_data, orient='records')

    df_data.to_sql(name="listas_precios_vtex",
                   con=engine,
                   schema="catalogo",
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
    'etl_listas_precios_vtex',
    default_args=default_args,
    description="Extracción y carga de la tabla listas_precios_vtex desde API.",
    schedule_interval="0 4 * * *",
    start_date=pendulum.datetime(2023, 6, 6, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["vtex", "promociones", "listas_precios", "workflow_promociones", "SERGIO"],
) as dag:

    dag.doc_md = """
    Extracción y carga de la tabla listas_precios_vtex desde API.
    """

    t0 = PythonOperator(
        task_id="get_fixed_prices",
        python_callable=get_fixed_prices
    )

    t1 = PostgresOperator(
        task_id="truncate_listas_precios",
        postgres_conn_id="postgresql_conn",
        sql="TRUNCATE catalogo.listas_precios_vtex",
    )

    t2 = PythonOperator(
        task_id="upload_fixed_prices",
        python_callable=upload_fixed_prices
    )

    t0 >> t1 >> t2
