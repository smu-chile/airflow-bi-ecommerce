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
    import numpy as np

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    print("Getting vtex_ids of products in WP with Fixed Price")
    query = """select distinct s.vtex_id
        from ecommdata_alvi.workflow_promociones wp 
        inner join ecommdata_alvi.skus s on s.ref_id = wp.material||'-'|| CASE WHEN wp.umv = 'ST' THEN 'UN' WHEN wp.umv = 'CS' THEN 'CJ' WHEN wp.umv = 'DIS' THEN 'DIS' END
        where wp.tipo_promocion = 10
        and wp.umv not in ('KG','KGV')
        and wp.fecha_inicio_de_promocion  <= current_date
        and wp.fecha_fin_de_promocion >= current_date  + interval '1 days'
        AND s.vtex_id IS NOT NULL
        limit 30;"""
    cursor.execute(query)
    results = cursor.fetchall()
    list_skus = [result[0] for result in results]
    cursor.close()
    pg_connection.close()
    print(f"Se obtuvieron {len(list_skus)} skus")

    X_VTEX_API_AppKey = Variable.get("X_VTEX_ALVI_API_Appkey")
    X_VTEX_API_AppToken = Variable.get("X_VTEX_ALVI_API_Apptoken")
    accountName = Variable.get("VTEX_ALVI_accountName")
    headers = {
        'Accept': "application/json",
        'Content-Type': "application/json",
        "X-VTEX-API-AppKey": X_VTEX_API_AppKey,
        "X-VTEX-API-AppToken":  X_VTEX_API_AppToken,
        "Connection": "keep-alive"
    }

    df_final = pd.DataFrame()

    for itemId in list_skus:
        get_fixed_prices = f"https://api.vtex.com.br/{accountName}/pricing/prices/{str(itemId)}/fixed"
        print("GET_FIXED_PRICES: ", get_fixed_prices)

        r = requests.get(get_fixed_prices, headers=headers)

        if r.status_code == 404:
            print(f"Error 404: Resource not found for SKU ID {itemId}")
            continue
        elif r.status_code == 200:
            r.raise_for_status()
            response = r.json()
            df = pd.DataFrame()
            df['vtexId'] = itemId
            df['tradePolicyId'] = response.get("rows", [])[0]

            if not df.empty and 'dateRange' in df.columns:
                df['Date From'] = df['dateRange'].apply(lambda x: x['from'] if pd.notna(x) else 'NULL')
                df['Date To'] = df['dateRange'].apply(lambda x: x['to'] if pd.notna(x) else 'NULL')

                df = df.drop(['dateRange'], axis=1)
                df = df.reindex(columns=['vtexId', 'tradePolicyId', 'listPrice', 'value', 'minQuantity', 'Date From', 'Date To'])
                df['vtex_id'] = itemId
                df = df.sort_values(by='minQuantity')
                for i in range(1, 4):
                    quantity_col = f'{i}_quantity'
                    price_col = f'{i}_price'
                    df[quantity_col] = df['minQuantity'].apply(lambda x: x if x == i else 0)
                    df[price_col] = df['value']
                    df_final = pd.concat([df_final, df[['vtexId', 'tradePolicyId', 'listPrice', quantity_col, price_col, 'dateFrom', 'dateTo']]], ignore_index=True)

            else:
                print(f"No pricing info obtained for SKU ID {itemId}")
        else:
            print(f"Failed to retrieve data for SKU ID {itemId}. Status code: {r.status_code}")
    df_final.info()

    df_final['vtexId'] = df_final['vtexId'].apply(lambda x: int(x) if pd.to_numeric(x, errors='coerce') == x else np.nan)
    df_final = df_final.rename(columns={
        "vtexId": "vtex_id",
        "tradePolicyId": "id_lista_precios",
        "listPrice": "precio_modal",
        "1_quantity": "primera_cantidad",
        "1_price": "primer_precio",
        "Date From": "fecha_inicio",
        "Date To": "fecha_termino",
        "2_quantity": "segunda_cantidad",
        "2_price": "segundo_precio",
        "3_quantity": "tercera_cantidad",
        "3_price": "tercer_precio"
    })

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
        task_id="truncate_listas_precios",
        postgres_conn_id="postgresql_conn",
        sql="TRUNCATE catalogo.listas_precios_vtex",
    )

    t2 = PythonOperator(
        task_id="upload_fixed_prices",
        python_callable=upload_fixed_prices
    )

    t0 >> t1 >> t2
