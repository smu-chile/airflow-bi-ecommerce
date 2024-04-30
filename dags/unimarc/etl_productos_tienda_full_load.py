from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from utils.janis_utils import load_custom_query_to_s3

from datetime import datetime

import pendulum

def _load_products_store_data(ti):
    import pandas as pd
    import sqlalchemy
    
    products_store_file = ti.xcom_pull(key="return_value", task_ids=["load_full_table_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+products_store_file)
    if not s3_hook.check_for_key(products_store_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % products_store_file)

    products_store_object = s3_hook.get_key(products_store_file, bucket_name=s3_bucket)

    df = pd.read_csv(products_store_object.get()["Body"], dtype={"id_tienda": "string"})
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")

    df = df.astype({
        "id_producto": "int",
        "ref_id": "string", 
        "id_tienda": "string", 
        "glosa_tienda": "string",
        "activo": "boolean" 
    })

    print("Number of records to be loaded: "+str(len(df.index)))

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    # Save to PostgreSQL:
    df.to_sql(name="productos_tienda",
                con=engine,         
                schema="ecommdata",         
                if_exists='append',         
                index=False,         
                chunksize=20000,         
                method='multi')
    print("Data loaded to Postgres")

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_productos_tienda_unimarc_full_load',
    default_args=default_args,
    description="Extracción y carga de tabla productos_tienda desde Janis Replica hasta Workspace.",
    schedule_interval="0 4 * * *",
    start_date=pendulum.datetime(2022, 5, 1, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "Janis", "ecommdata", "productos_tienda", "Unimarc", "MATIAS"],
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla de productos_tienda de Janis a Workspace. \n
    TRUNCATE - INSERT.
    """ 
    t0 = PythonOperator(
        task_id = "load_full_table_to_s3",
        python_callable = load_custom_query_to_s3,
        op_kwargs = {
            "query": """
                select ps.product as id_producto
                    , p.ref_id 
                    , ws.ref_id as id_tienda 
                    , ws.name as glosa_tienda
                    , is_active as activo
                from product_stores ps
                left join products p on ps.product =p.id
                left join wms_stores ws on ws.id = ps.store 
            """,
            "query_name": "products_store"
        }
    )

    t1 = PostgresOperator(
        task_id = "truncate_table",
        postgres_conn_id="postgresql_conn",
        sql = "TRUNCATE ecommdata.productos_tienda"
    )

    t2 = PythonOperator(
        task_id = "load_products_store_data",
        python_callable = _load_products_store_data
    )

    t0 >> t1 >> t2
