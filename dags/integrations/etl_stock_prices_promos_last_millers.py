from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule


import pendulum

from utils.netezza_utils import load_custom_query_to_s3

from datetime import datetime, timedelta
    
def _get_last_millers_stores():
    last_millers_stores_query = """
        SELECT id
        FROM integraciones.tiendas_last_millers;
    """
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn_prod")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(last_millers_stores_query)
    results = cursor.fetchall()
    cursor.close()
    pg_connection.close()
    return results

def extract_stock_from_dw(ti,ds,ts):
    import os
    import pandas as pd
    import io
    from io import StringIO
    from utils.netezza_utils import load_custom_query_to_s3

    ids_tiendas = ti.xcom_pull(key="return_value", task_ids=["get_last_millers_stores"])[0]
    ids_tiendas = [id[0] for id in ids_tiendas]
    ids_tiendas_str = str(tuple(ids_tiendas))
    print(ids_tiendas_str)

    exec_date = ds.replace("-", "/")
    date_aux = ts.replace("-", "_")
    prefix = f"ls_millers_patito/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    query = f"""SELECT S.NBR_ITM 
                , S.SKU_KEY
                , SA.SKU_PRODUCT 
                , OU.OU_ID 
            FROM DWC_SMU.SMU.VW_FACT_STOCK S
            LEFT JOIN DWC_SMU.SMU.VW_DIM_SKU_ATTR SA ON SA.SKU_KEY  = S.SKU_KEY
            LEFT JOIN DWC_SMU.SMU.VW_DIM_ORGANIZATION_UNIT OU ON OU.OU_KEY = S.OU_KEY
            LEFT JOIN DWC_SMU.SMU.VW_DIM_ALMACEN A ON A.ALMACEN_KEY =S.ALMACEN_KEY
            LEFT JOIN DWC_SMU.SMU.VW_DIM_PARTICULARIDAD PART ON S.PARTICULARIDAD_KEY =PART.PARTICULARIDAD_KEY
            WHERE OU.OU_ID in {ids_tiendas_str}
            AND S.DATE_VALUE = '{ds}'
            AND S.APLICA_STOCK = 'S'
            AND A.ALMACEN_COD = '0001'
            AND S.TIPO_STOCK_KEY IN (9161419180, 9145314683)
            AND PART.PARTICULARIDAD_COD = 'A'
            AND S.NBR_ITM > 0
            limit 5000
            ;"""
    print(query)

    try:
        filename = load_custom_query_to_s3(ts,query,"stock_sap_query")
        print("Searching file: "+filename)
        return "stock_to_postgresql"
    except Exception as err:
        print(f"error: {err}")
        return "fallo_dw_stock"
    
def extract_product_from_dw(ts):
    import os
    import pandas as pd
    import io
    from io import StringIO
    from utils.netezza_utils import load_custom_query_to_s3

    query = f"""SELECT P.SKU_KEY
                    , P.EAN 
                    , P.CONT_CONV_UMB
                    , P.NM
                    , P.BRAND_DESC
                    , P.UNIDAD_DE_MEDIDA
                FROM DWC_SMU.SMU.VW_DIM_PRODUCT P
                WHERE p.indic_ean_ppal = 'X';
            """
    print(query)

    try:
        filename = load_custom_query_to_s3(ts,query,"product_dw")
        print("Searching file: "+filename)
        return "product_to_postgresql"
    except Exception as err:
        print(f"error: {err}")
        return "fallo_dw_producto"


def stock_to_postgresql(ti,ts):
    print('\n carga de stock sap a postgresql')
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text
    BASE_S3_PATH = "data_warehouse/"
    query_name = "stock_sap_query"
    curr_datetime = ts[:16].replace("-", "/").replace("T", "/").replace(":", "")
    prefix = BASE_S3_PATH+query_name+"/"+curr_datetime+"_"

    filename = prefix+query_name+".csv"  

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+filename)
    if not s3_hook.check_for_key(filename, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % filename)

    s_stock_object = s3_hook.get_key(filename, bucket_name=s3_bucket)

    df = pd.read_csv(s_stock_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")
    df.columns = ["nbr_itm","sku_key","sku_product","ou_id"]
    df = df[["sku_key","sku_product","ou_id","nbr_itm"]]
    df['sku_product'] = df['sku_product'].apply(lambda x: str(x).zfill(18))
    df['ou_id'] = df['ou_id'].apply(lambda x: str(x).zfill(4))
    df.info()
    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    # Save to PostgreSQL:
    with engine.begin() as conn:
        conn.execute("TRUNCATE integraciones.stock_2") 
        df.to_sql(name="stock_2",
                    con=conn,         
                    schema="integraciones",         
                    if_exists='append',         
                    index=False,         
                    chunksize=20000,         
                    method='multi')

    print("Data loaded to Postgres: integraciones.stock_2")
    return

def product_to_postgresql(ti,ts):
    print('\n carga de productos sap a postgresql')
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text
    BASE_S3_PATH = "data_warehouse/"
    query_name = "product_dw"
    curr_datetime = ts[:16].replace("-", "/").replace("T", "/").replace(":", "")
    prefix = BASE_S3_PATH+query_name+"/"+curr_datetime+"_"

    filename = prefix+query_name+".csv"  

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+filename)
    if not s3_hook.check_for_key(filename, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % filename)

    s_stock_object = s3_hook.get_key(filename, bucket_name=s3_bucket)

    df = pd.read_csv(s_stock_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")
    df.columns = map(str.lower, df.columns)
    df = df.dropna(subset=df.columns[:3])
    df.info()
    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    # Save to PostgreSQL:
    with engine.begin() as conn:
        conn.execute("TRUNCATE integraciones.productos_2") 
        df.to_sql(name="productos_2",
                    con=conn,         
                    schema="integraciones",         
                    if_exists='append',         
                    index=False,         
                    chunksize=20000,         
                    method='multi')

    print("Data loaded to Postgres: integraciones.productos_2")
    return

def prices_to_integrations(ti):
    try:
        #filename = load_custom_query_to_s3(ts,query,"product_dw")
        print("Searching file: ")#+filename)
        return "precios_postgres"
    except Exception as err:
        print(f"error: {err}")
        return "fallo_postgres_precios"

def promos_to_integrations(ti):
    try:
        #filename = load_custom_query_to_s3(ts,query,"product_dw")
        print("Searching file: ")#+filename)
        return "promos_postgres"
    except Exception as err:
        print(f"error: {err}")
        return "fallo_postgres_promos"

def stock_prices_promos_lss_to_s3():
    print("Compilando la informacion de todas las tablas de insumos a S3")
    return

def stock_prices_promos_lss_to_postgres(ti):
    print("Cargando la data de last millers a postgres")
    return
def promos_postgres(ti):
    print("Cargando promociones del workflow promociones en postgres")
    return
def check_promos():
    print("Revisando que exista data en la tabla de promociones")
    return
def check_prices():
    print("Revisando que exista data en la tabla de precios")
    return
def check_stock():
    print("Revisando que exista data en la tabla de stock")

    return
def check_product():
    print("Revisando que exista data en la tabla de productos")
    return


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_stock_prices_promos_last_millers',
    default_args=default_args,
    description="cargar stock,precios y promos a la tabla lss_millers_promos",
    schedule_interval="20 9,15 * * *",
    start_date=pendulum.datetime(2023, 6, 12, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "last_millers", "integraciones", "stock", "prices", "promos"],
) as dag:
    

    dag.doc_md = """
    cargar stock,precios y promos a la tabla lss_millers_promos\n
    guardar en S3 y postgresql.
    """ 

    t_dummy_p = DummyOperator(
            task_id='fallo_dw_producto',
        )
    
    t_dummy_s = DummyOperator(
            task_id='fallo_dw_stock',
        )
    t_dummy_prom = DummyOperator(
            task_id='fallo_postgres_promos',
        )
    
    t_dummy_price = DummyOperator(
            task_id='fallo_postgres_precios',
        )
    
    t0  = PythonOperator(
        task_id = "get_last_millers_stores",
        python_callable = _get_last_millers_stores
    )

    t1 = BranchPythonOperator(
        task_id = "extract_stock_from_dw",
        python_callable = extract_stock_from_dw,
    )

    t2 = BranchPythonOperator(
        task_id = "extract_product_from_dw",
        python_callable = extract_product_from_dw,
    )

    t3 = PythonOperator(
        task_id = "stock_to_postgresql",
        python_callable = stock_to_postgresql,
    )
    t4 = PythonOperator(
        task_id = "product_to_postgresql",
        python_callable = product_to_postgresql,
    )
    t5 = BranchPythonOperator(
        task_id="prices_to_integrations_s3",
        python_callable=prices_to_integrations,
    )
    t6 = BranchPythonOperator(
        task_id="promos_to_integrations_s3",
        python_callable=promos_to_integrations,
    )
    t7 = PythonOperator(
        task_id="check_stock",
        python_callable=check_stock,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )
    t8 = PythonOperator(
        task_id="check_product",
        python_callable=check_product,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )
    t9 = PythonOperator(
        task_id="stock_prices_promos_lss_to_s3",
        python_callable=stock_prices_promos_lss_to_s3,
    )
    t10 = PythonOperator(
        task_id="stock_prices_promos_lss_to_postgres",
        python_callable=stock_prices_promos_lss_to_postgres,
    )
    t11 = PythonOperator(
        task_id="precios_postgres",
        python_callable=check_product,
    )

    t12 = PythonOperator(
        task_id="check_prices",
        python_callable=check_prices,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )
    t13 = PythonOperator(
        task_id="promos_postgres",
        python_callable=promos_postgres,
    )
    t14 = PythonOperator(
        task_id="check_promos",
        python_callable=check_promos,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    t1 >>  t_dummy_s 
    t2 >>  t_dummy_p
    t0 >> [t1,t2,t5,t6]
    t1 >> t3
    t2 >> t4
    t3 >> t7 
    t_dummy_s >> t7
    t4 >> t8 
    t_dummy_p >> t8
    t5 >> t_dummy_price
    t5 >> t11
    t_dummy_price >> t12
    t11 >> t12
    t6 >> t_dummy_prom
    t6 >> t13
    t_dummy_prom >> t14
    t13 >> t14
    [t7,t8,t12,t14] >> t9
    t9 >> t10

    
