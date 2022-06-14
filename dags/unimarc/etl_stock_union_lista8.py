from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from utils.janis_utils import load_custom_query_to_s3

from datetime import datetime

def _load_stock_data(ti):
    import pandas as pd
    import sqlalchemy
    
    stock_file = ti.xcom_pull(key="return_value", task_ids=["load_full_table_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+stock_file)
    if not s3_hook.check_for_key(stock_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % stock_file)

    stock_object = s3_hook.get_key(stock_file, bucket_name=s3_bucket)

    df = pd.read_csv(stock_object.get()["Body"], dtype={"id_tienda": "string"})
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")

    df = df.astype({
        "ref_id": "int",
        "vtex_id": "int",
        "id_tienda": "string",
        "stock": "int", 
        
    })

    print("Number of records to be loaded: "+str(len(df.index)))

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = "postgresql+psycopg2://"+username+":"+password+"@"+host+":5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    lista8 = pd.read_sql("select * from ecommdata.", engine)
    # Save to PostgreSQL:
    df.to_sql(name="stock",
                con=engine,         
                schema="ecommdata",         
                if_exists='append',         
                index=False,         
                chunksize=20000,         
                method='multi')
    print("Data from stock loaded to Postgres")

    lista8 = pd.read_sql(""" insert into ecommdata.stock
    select CONCAT(l.material , '-', l.umv) as ref_id, p.vtex_id , l.id_tienda, 0 as stock
    from ecommdata.lista8 as l
    left join ecommdata.productos as p on CONCAT(l.material , '-', l.umv) = p.ref_id
    on conflict do nothing""", engine)
    
    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_stock_unimarc_full_load',
    default_args=default_args,
    description="Extracción y carga de tabla stock desde Janis Replica hasta Workspace.",
    schedule_interval="0 7 * * *",
    start_date=datetime(2022, 5, 1),
    catchup=False,
    tags=["DATA", "Janis", "ecommdata", "stock", "Unimarc"],
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla de stock de Janis a Workspace. \n
    TRUNCATE - INSERT.
    """ 
    t0 = PythonOperator(
        task_id = "load_full_table_to_s3",
        python_callable = load_custom_query_to_s3,
        op_kwargs = {
            "query": """
                select p.ref_id, p.vtex_id, ws.ref_id as id_tienda, s.stock
                from janis_jackie.stock s
                left join janis_jackie.products p on s.item_id = p.id
                left join janis_jackie.wms_stores ws on s.store_id = ws.id
                where s.stock > 0
            """,
            "query_name": "stock"
        }
    )

    t1 = PostgresOperator(
        task_id = "truncate_table",
        postgres_conn_id="postgresql_conn",
        sql = "TRUNCATE ecommdata.stock"
    )

    t2 = PythonOperator(
        task_id = "load_stock_data",
        python_callable = _load_stock_data
    )

    t0 >> t1 >> t2
