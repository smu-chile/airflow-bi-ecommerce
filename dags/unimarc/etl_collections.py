from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.slack_utils import dag_success_slack, dag_failure_slack

import pendulum

def get_all_collections(ti):
    import pandas as pd
    import requests
    import sqlalchemy
    from sqlalchemy import text

    X_VTEX_API_AppKey = Variable.get("X_VTEX_API_AppKey")
    X_VTEX_API_AppToken = Variable.get("X_VTEX_API_AppToken")
    headers = {
        'Accept': "application/json",
        'Content-Type': "application/json",
        "X-VTEX-API-AppKey": X_VTEX_API_AppKey,
        "X-VTEX-API-AppToken":  X_VTEX_API_AppToken,
        "Connection": "keep-alive"
    }
    accountName = Variable.get("VTEX_ACCOUNT_NAME")
    environment = Variable.get("VTEX_ENV")
    URL_VTEX = f'https://{accountName}.{environment}.com.br'
    GET_ALL_COLLECTIONS_BASE = f'/api/catalog_system/pvt/collection/search'

    pages = requests.request("GET", f"{URL_VTEX}{GET_ALL_COLLECTIONS_BASE}",
                             headers=headers).json()['paging']['pages']
    print("pages: ", pages)
    df_final = pd.DataFrame()
    page = 1
    while page <= pages:
        url_page = f"{URL_VTEX}{GET_ALL_COLLECTIONS_BASE}?page={page}"
        r = requests.request("GET", url_page, headers=headers)
        if r.status_code == 400:
            print("Error 404: Recurso no encontrado")
            print(r.content)
            continue
        elif r.status_code == 200:
            collections = r.json()['items']
            df_collections = pd.DataFrame(collections)
            df_final = pd.concat([df_final, df_collections], ignore_index=True)
            resultado = "Exito" if r.status_code == 200 else "Falla"
        print(f"Obteniendo pagina {page} de {pages} con {resultado}")
        page += 1

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")

    conn_url = "postgresql+psycopg2://"+username + \
        ":"+password+"@"+host+":5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    connection = engine.connect()
    truncate_query = "TRUNCATE TABLE catalogo.vtex_collections"
    connection.execute(text(truncate_query))
    connection.close()

    df_final.to_sql(name="vtex_collections",
                    con=engine,
                    schema="catalogo",
                    if_exists='append',
                    index=False,
                    chunksize=20000,
                    method='multi')
    return


def get_products_from_collection(ti):
    import pandas as pd
    import requests

    query = f"SELECT * FROM catalogo.vtex_collections;"
    print(query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    columns_name = [i[0] for i in cursor.description]
    df_all_collections = pd.DataFrame(results, columns=columns_name)
    cursor.close()
    pg_connection.close()

    X_VTEX_API_AppKey = Variable.get("X_VTEX_API_AppKey")
    X_VTEX_API_AppToken = Variable.get("X_VTEX_API_AppToken")
    headers = {
        'Accept': "application/json",
        'Content-Type': "application/json",
        "X-VTEX-API-AppKey": X_VTEX_API_AppKey,
        "X-VTEX-API-AppToken":  X_VTEX_API_AppToken,
        "Connection": "keep-alive"
    }
    accountName = Variable.get("VTEX_ACCOUNT_NAME")
    environment = Variable.get("VTEX_ENV")
    URL_VTEX = f'https://{accountName}.{environment}.com.br'
    
    df_final = pd.DataFrame()
    for index, row in df_all_collections.iterrows():
        if row['totalSku'] > 0:
            collectionId = row['id']
            print(collectionId)
            GET_PRODUCTS_FROM_COLLECTION = f'/api/catalog/pvt/collection/{collectionId}/products'
            url_pages = f"{URL_VTEX}{GET_PRODUCTS_FROM_COLLECTION}"
            pages = requests.request("GET", url_pages, headers=headers).json()[
                'TotalPage']
            print("Pages: ", pages)
            page = 1
            while page <= pages:
                url_page = f"{URL_VTEX}{GET_PRODUCTS_FROM_COLLECTION}?page={page}"
                r = requests.request("GET", url_page, headers=headers)
                if r.status_code == 400:
                    print("Error 404: Recurso no encontrado")
                    print(r.content)
                    continue
                elif r.status_code == 200:
                    r.raise_for_status()
                    products = r.json()['Data']
                    df_products = pd.DataFrame(products)
                    df_products['collection_id'] = collectionId
                    df_final = pd.concat(
                        [df_final, df_products], ignore_index=True)
                    resultado = "Exito" if r.status_code == 200 else "Falla"
                print(f"Obteniendo pagina {page} de {pages} con {resultado}")
                page += 1
        else:
            continue
    return df_final.to_json(orient='records')


def upload_products_from_collections(ti):
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

    json_data = ti.xcom_pull(task_ids=["get_products_from_collection"])[0]
    df_data = pd.read_json(json_data, orient='records')

    if ~df_data.empty:
        connection = engine.connect()
        truncate_query = "TRUNCATE TABLE catalogo.vtex_products_collections"
        connection.execute(text(truncate_query))
        connection.close()
    df_data.to_sql(name="vtex_products_collections",
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
    'etl_collections',
    default_args=default_args,
    description="Extracción y carga de las tablas vtex_collections y vtex_products_collections desde API.",
    schedule_interval="0 3 * * *",
    start_date=pendulum.datetime(2023, 6, 26, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["vtex", "promociones", "colecciones", "workflow_promociones", "SERGIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Extracción y carga de las tablas vtex_collections y vtex_products_collections desde API
    """

    t0 = PythonOperator(
        task_id="get_all_collections",
        python_callable=get_all_collections
    )

    t1 = PythonOperator(
        task_id="get_products_from_collection",
        python_callable=get_products_from_collection
    )

    t2 = PythonOperator(
        task_id="upload_products_from_collections",
        python_callable=upload_products_from_collections
    )

    t0 >> t1 >> t2
