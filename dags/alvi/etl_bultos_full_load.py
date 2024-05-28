from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from utils.janis_alvi_utils import load_custom_query_to_s3

from datetime import datetime

def _full_load_bultos(ti):
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text
    
    packages_file = ti.xcom_pull(key="return_value", task_ids=["load_full_table_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+packages_file)
    if not s3_hook.check_for_key(packages_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % packages_file)

    packages_object = s3_hook.get_key(packages_file, bucket_name=s3_bucket)

    df = pd.read_csv(packages_object.get()["Body"])
    df = df[[
            "id_orden",
            "cantidad_bultos",
            "tipo_bulto"
    ]]  

    # # Ensure correct datatypes:
    df["id_orden"] = df["id_orden"].astype("int", errors="ignore")
    df["cantidad_bultos"] = df["cantidad_bultos"].astype("int", errors="ignore")
    df["tipo_bulto"] = df["tipo_bulto"].astype("str")
    print("Number of records to be loaded: "+str(len(df.index)))

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    connection = engine.connect()
    truncate_query = "TRUNCATE TABLE ecommdata_alvi.bultos"
    connection.execute(text(truncate_query))
    connection.close()

    # Save to PostgreSQL:
    df.to_sql(name="bultos",
                con=engine,         
                schema="ecommdata_alvi",         
                if_exists='append',         
                index=False,         
                chunksize=20000,         
                method='multi')

    print("Data saved to PostgreSQL. Table: ecommdata_alvi.bultos")

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_bultos_full_load',
    default_args=default_args,
    description="Extracción y carga tabla bultos y su relación con la tabla ordenes desde Janis Replica hasta Workspace.",
    schedule_interval="0 9 * * *",
    start_date=datetime(2022, 3, 15),
    catchup=False,
    tags=["DATA", "Janis", "ecommdata_alvi", "bultos", "MATIAS"],
) as dag:

    dag.doc_md = """
    Extracción y carga tabla bultos y su relación con la tabla ordenes desde Janis Replica hasta Workspace.
    """ 

    t0 = PythonOperator(
        task_id = "load_full_table_to_s3",
        python_callable = load_custom_query_to_s3,
        op_kwargs = {
            "query": """
                select wo.seq_id as id_orden
                , op.quantity as cantidad_bultos
                , p.name as tipo_bulto
                from order_packages op
                left join packages p on op.package_id = p.id
                left join package_types pt on p.package_type = pt.id
                left join wms_orders wo on op.order_id = wo.id;
            """,
            "query_name": "wms_logistic_warehouses",
        }
    )

    t1 = PythonOperator(
        task_id = "full_load_bultos",
        python_callable = _full_load_bultos
    )

    t0 >> t1
