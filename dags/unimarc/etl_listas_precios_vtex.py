from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

import pendulum

def get_fixed_prices(ti):
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text
    import requests

    host = Variable.get('POSTGRESQL_HOST')
    database = Variable.get('POSTGRESQL_DB')
    username = Variable.get('POSTGRESQL_USER')
    password = Variable.get('POSTGRESQL_PASSWORD')
    conn_url = "postgresql+psycopg2://"+username + \
        ":"+password+"@"+host+":5432/"+database
    engine = sqlalchemy.create_engine(conn_url)
    connection = engine.connect()

    print("Getting vtex_ids from ecommdata.skus of products within lista8")
    query = """SELECT distinct s.vtex_id FROM ecommdata.lista8 l8
        INNER JOIN ecommdata.skus s ON s.ref_id = l8.material||'-'||l8.umv"""
    df_skus = pd.read_sql(query, con=engine)
    df_skus = df_skus.dropna()
    df_skus = df_skus.astype({'vtex_id': 'int64'})
    print(df_skus)
    connection.close()

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

    list_skus = list(df_skus)[:10]
    df_final = pd.DataFrame()
    for itemId in list_skus:
        df = pd.DataFrame()
        GET_FIXED_PRICES = f"https://api.vtex.com/{accountName}/pricing/prices/{itemId}/fixed"
        print(GET_FIXED_PRICES)
        r = requests.get(GET_FIXED_PRICES, headers=headers)
        print("status_code: ", r.status_code)
        data = r.json()
        df = pd.DataFrame(data)
        df = df.assign(vtex_id=itemId)
        if 'dateRange' not in df.columns:
            # Si no existe, crear la columna y llenarla con una marca
            df['dateRange'] = 'No existe'
        for index, row in df.iterrows():
            if pd.isna(row['dateRange']):
                df.at[index, 'date-from'] = ''
                df.at[index, 'date-to'] = ''
            elif row['dateRange'] == '':
                df.at[index, 'date-from'] = ''
                df.at[index, 'date-to'] = ''
            else:
                df.at[index, 'date-from'] = row['dateRange']['from']
                df.at[index, 'date-to'] = row['dateRange']['to']
        df = df.drop('dateRange', axis=1)
        df = df.reindex(columns=['vtex_id', 'tradePolicyId', 'value',
                        'listPrice', 'minQuantity', 'date-from', 'date-to'])
        df = df.rename({'value': 'price'})
        df_final = pd.concat([df_final, df])
    df_final.rename({'vtex_id': 'SKU ID', 'tradePolicyId': 'Trade Policy',
                    'value': 'Price', 'listPrice': 'List Price',
                     'minQuantity': 'Min Quantity', 'date-from': 'Date From',
                     'date-to': 'Date To'})
    return df_final.to_json(orient='records')


def upload_fixed_prices(ti):
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text

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
    'etl_vtex_listas_precios',
    default_args=default_args,
    description="Extracción y carga de la tabla listas_precios_vtex desde API.",
    schedule_interval="0 4 * * *",
    start_date=pendulum.datetime(2023, 6, 6, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["vtex", "promociones", "listas_precios", "workflow_promociones"],
) as dag:

    dag.doc_md = """
    Extracción y carga de la tabla listas_precios_vtex desde API.
    """

    t0 = PythonOperator(
        task_id="get_fixed_prices",
        python_callable=get_fixed_prices
    )
 t1 = PostgresOperator(
        task_id = "truncate_listas_precios",
        postgres_conn_id="postgresql_conn",
        sql="""
        TRUNCATE catalogo.listas_precios_vtex
        """,
    )

    t2 = PythonOperator(
        task_id="upload_fixed_prices",
        python_callable=upload_fixed_prices
    )

    t0 >> t1 >> t2
