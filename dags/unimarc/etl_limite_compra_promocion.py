from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator

import pendulum

def _load_limite_compra_promocion_table(ti,ds):
    import pandas as pd
    import numpy as np
    import os
    import sqlalchemy

    curr_working_directory = os.getcwd()
    print(os.getcwd())

    with open(curr_working_directory+f"/dags/unimarc/sql/limite_compra_promocion.sql", "r") as query_file:
        limite_promocion_query = query_file.read()
    
    limite_promocion_query = limite_promocion_query.replace("{ds}", ds)

    print("Base query:")
    print(limite_promocion_query)

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()

    df_limite = pd.read_sql_query(limite_promocion_query, pg_connection)

    df_limite.info()

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    # Save to PostgreSQL:
    df_limite.to_sql(name="limite_compra_promocion",
                con=engine,         
                schema="ecommdata",         
                if_exists='append',         
                index=False,         
                chunksize=20000,         
                method='multi')
    print("Data loaded to Postgres")

    return

def _set_lim_compra(ts):
    import requests
    from datetime import datetime
    import pandas as pd

    query_lista8 = """select * from ecommdata.limite_compra_promocion lcp;
    """
    print(query_lista8)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query_lista8)
    results = cursor.fetchall()
    results=pd.DataFrame(results)
    print(results)
    results.columns = ["ref_id","n_promocion","nombre_promocion","descuento_pesos",
                       "porcentaje_descuento","fecha_inicio_de_promocion","fecha_fin_de_promocion"]
    cursor.close()
    pg_connection.close()
    
    print(f"Number of records extracted: {len(results.index)}")

    headers = {
        "janis-api-key": Variable.get("JANIS_API_KEY"),
        "janis-api-secret": Variable.get("JANIS_API_SECRET"),
        "janis-client": Variable.get("JANIS_CLIENT"),
        "Connection": "keep-alive"
    }
    #seleccionar promociones con mayor duracion de promocion
    results['fecha_fin_de_promocion'] = pd.to_datetime(results['fecha_fin_de_promocion'])
    productos_promo = results.loc[results.groupby('ref_id')['fecha_fin_de_promocion'].idxmax()]
    # Creación de big-json
    jst = []
    unix_time = int(datetime.fromisoformat(ts).timestamp())
    for index, row in productos_promo.iterrows():
        if(int(row["fecha_fin_de_promocion"].strftime('%s')) < unix_time):
            item = {
                "item_id": row["ref_id"],
                "attributes": [
                    {
                        "id": str(Variable.get("JANIS_REF_ID_ATRIBUTO_ID_CATEGORIA")),
                        "values": ["999"]
                    }
                ]
            }
        else:
            item = {
                "item_id": row["ref_id"],
                "attributes": [
                    {
                        "id": str(Variable.get("JANIS_REF_ID_ATRIBUTO_ID_CATEGORIA")),
                        "values": ["22"]
                    }
                ]
            }
        jst.append(item)

    print(jst)
    
    # Partición de big-json
    lim_json = 500
    total_size = len(jst)
    if total_size > lim_json:
        jst = [jst[i:i+lim_json] for i in range(0, len(jst), lim_json)]
    else:
        jst = [jst]

    # Seteo vía API al atriubuto limite de compra de la lista de refid
    API_JANIS = Variable.get("JANIS_API_URL")
    cargando = 0
    for arr_dic in jst:
        r = requests.post(f'{API_JANIS}attribute_value', headers = headers, json=arr_dic)
        cargando += len(arr_dic )
        if r.status_code == 200:
            print(f"Productos actualizados: {cargando} de {total_size} con EXITO")
        else:
            print(f"Carga sin éxito | Status_Code: {r.status_code} ")
            print(f"Response Print: {r.content}")
            raise ValueError("Janis API response != 200")
    print("La carga de límites a finalizado")          
    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_limite_promociones',
    default_args=default_args,
    description="Extraer productos con promociones vigentes y setear limite de compra estatico",
    schedule_interval="30 9 * * *",
    start_date=pendulum.datetime(2023, 6, 1, tz="America/Santiago"),
    catchup=False,
    tags=["ecommdata", "promociones", "limite_compra", "unimarc", "SERGIO"],
) as dag:
    
    dag.doc_md = """
    Extraer productos con promociones vigentes y setear limite de compra estatico
    """ 
    t0 = PythonOperator(
        task_id = "_load_limite_compra_promocion_table",
        python_callable = _load_limite_compra_promocion_table
    )

    t1 = PythonOperator(
        task_id = "_set_lim_compra",
        python_callable = _set_lim_compra
    )
    t0 >> t1