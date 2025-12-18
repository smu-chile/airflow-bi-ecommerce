from airflow import DAG
from airflow import macros
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.slack_utils import dag_success_slack, dag_failure_slack

from datetime import datetime

import pendulum

def _load_dw_stock_to_s3(ds,ts):
    import pandas as pd
    import sqlalchemy
    from io import StringIO
    import boto3

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()

    query_stores = """
        SELECT id
        FROM ecommdata.tiendas
        WHERE id_frogmi is not null;
    """

    cursor = pg_connection.cursor()
    cursor.execute(query_stores)
    stores = cursor.fetchall()
    cursor.close()

    print(f"Number of stores found: {len(stores)}")
    print(f"checking date {ds}")

    df_list = []

    for store in stores:
        query_products = f"""
            select material
            from ecommdata.frogmi_alerta_found_rate
            where id_tienda = '{store[0]}'
            and stock_en_sistema is True
            and fecha_inicio::date = '{ds}'
        """
        cursor = pg_connection.cursor()
        cursor.execute(query_products)
        products = cursor.fetchall()
        cursor.close()

        if len(products) == 0:
            print(f"No products to check in store {store[0]}")
            continue
        elif len(products) == 1:
            products = tuple([str(item[0]).zfill(18) for item in products])
            products = str(products[0])
            products = f"('{products}')"
        else:
            products = tuple([str(item[0]).zfill(18) for item in products])

        print(f"Products in store {store[0]}: {products}")
        
        query_stock_dw = f"""
            select sdb.sku_product as MATERIAL
            , sdb.nbr_item as STOCK
            , sdb.id_tienda as ID_TIENDA
            , sdb.nombre as NOMBRE
            , sdb.fecha as FECHA
            from ecommdata.stock_dw_bq sdb
                where sdb.fecha = '{ds}'
            and sdb.id_tienda = '{str(store[0])}'
            and sdb.sku_product in {products}
        """
        cursor = pg_connection.cursor()
        cursor.execute(query_stock_dw)
        rows = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        cursor.close()

        if len(rows) == 0:
            print(f"No DW stock data for store {store[0]}")
            continue

        df = pd.DataFrame(rows, columns=column_names)
        df_list.append(df)

    if not df_list:
        print("⚠️ No se encontraron registros de stock en ningún local.")
        return None
    df_full = pd.concat(df_list, ignore_index=True)

    column_names = {
        "MATERIAL": "material",
        "STOCK": "stock",
        "ID_TIENDA": "id_tienda",
        "NOMBRE": "nombre",
        "FECHA": "fecha"
    }

    df_full = df_full.rename(columns=column_names)
    print(df_full)
    df_full["id_tienda"] = df_full["id_tienda"].str.zfill(4)
    df_full["material"] = df_full["material"].str.zfill(18)
    
    curr_datetime = ts[:16].replace("-", "/").replace("T", "/").replace(":", "")
    prefix = "frogmi/check_found_rate_sap/"+curr_datetime
    file_name = prefix+"check_found_rate_sap.csv"

    buffer = StringIO()

    df_full.to_csv(buffer, header=True, index=False, encoding="utf-8")
    buffer.seek(0)

    access_key = Variable.get("AWS_ACCESS_KEY")
    secret_key = Variable.get("AWS_SECRET_KEY")
    bucket_name = Variable.get("AWS_S3_BUCKET_NAME")
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name = "us-east-1"
    )
    response = s3_client.put_object(
        Bucket=bucket_name, Key=file_name, Body=buffer.getvalue()
    )

    return file_name

def _get_table_chequeo_found_rate_from_S3(ti):
    import pandas as pd

    chequeo_found_rate_file = ti.xcom_pull(key="return_value", task_ids=["load_dw_stock_to_s3"])[0]
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+chequeo_found_rate_file)
    if not s3_hook.check_for_key(chequeo_found_rate_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % chequeo_found_rate_file)

    chequeo_reposicion_object = s3_hook.get_key(chequeo_found_rate_file, bucket_name=s3_bucket)

    df = pd.read_csv(chequeo_reposicion_object.get()["Body"])
    print(f"Number of records found: {len(df.index)}")

    df = df.astype({
        "material": "string",
        "stock": "int",
        "id_tienda": "string",
        "nombre": "string",
        "fecha": "string",
    }, errors="ignore")

    return df

def _save_table_chequeo_found_rate(ts, ti, ds):
    import pandas as pd
    import sqlalchemy

    df = _get_table_chequeo_found_rate_from_S3(ti)
    df["id_tienda"] = df["id_tienda"].str.zfill(4)
    df["material"] = df["material"].str.zfill(18)

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    with engine.begin() as conn:
        df.to_sql(name="frogmi_chequeo_found_rate",
                con=engine,         
                schema="ecommdata",         
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
    'etl_frogmi_chequeo_found_rate',
    default_args=default_args,
    description="Extracción y carga de stock DW para chequear foundrate de productos.",
    schedule_interval="0 8 * * *",
    start_date=pendulum.datetime(2022, 10, 12, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["frogmi", "foundrate", "MATIAS"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Extracción y carga de stock DW para chequear reposicion de productos
    """ 

    t0 = PythonOperator(
        task_id = "load_dw_stock_to_s3",
        python_callable = _load_dw_stock_to_s3
    )

    t1 = PythonOperator(
        task_id = "save_table_chequeo_found_rate",
        python_callable = _save_table_chequeo_found_rate
    )


t0 >> t1
