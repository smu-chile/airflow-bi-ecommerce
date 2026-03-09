from airflow import DAG
from airflow.models import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator as PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from utils.janis_utils import load_custom_query_to_s3
from utils.slack_utils import dag_success_slack, dag_failure_slack

import pendulum

def _full_load_table_to_workspace(ts, ti):
    import pandas as pd
    import sqlalchemy

    sku_ean_items_file = ti.xcom_pull(key="return_value", task_ids=["extract_sku_ean_table"])[0]

    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+sku_ean_items_file)
    if not s3_hook.check_for_key(sku_ean_items_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % sku_ean_items_file)

    sku_ean_items_object = s3_hook.get_key(sku_ean_items_file, bucket_name=s3_bucket)

    column_types = {
        "ean": "string",
        "ref_id": "string",
        "umv": "string",
        "material": "string"
    } 

    df = pd.read_csv(sku_ean_items_object.get()["Body"], dtype=column_types)
    print(f"Records found: {len(df.index)}")

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    # Save to PostgreSQL:
    df.to_sql(name="sku_ean",
                con=engine,         
                schema="ecommdata",         
                if_exists='append',         
                index=False,         
                chunksize=20000,         
                method='multi')

    print("Data saved to PostgreSQL. Table: ecommdata.sku_ean")

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_sku_ean_unimarc_full_load',
    default_args=default_args,
    description="Extracción y carga de tabla sku_ean desde Janis Replica hasta Workspace.",
    schedule="0 8 * * *",
    start_date=pendulum.datetime(2022, 11, 2, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,

    tags=["DATA", "Janis", "ecommdata", "sku_ean", "unimarc", "MATIAS"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla de sku_ean, cruzada con tabla sku para obtener ref_id desde Janis a Workspace. \n
    FULL LOAD con un TRUNCATE antes de cada carga.
    """ 
    t0 = PythonOperator(
        task_id = "extract_sku_ean_table",
        python_callable = load_custom_query_to_s3,
        op_kwargs = {
            "query": """
                SELECT se.ean
                    , s.ref_id 
                    , SUBSTRING_INDEX(s.ref_id, "-", -1) as umv
                    , SUBSTRING_INDEX(s.ref_id, "-", 1) as material
                FROM janis_jackie.sku_ean se 
                JOIN janis_jackie.skus s 
                    ON se.id_sku = s.id;
            """,
            "query_name": "sku_ean",
        }
    )

    t1 = PostgresOperator(
        task_id = "truncate_table_sku_ean",
        conn_id="postgresql_conn",
        sql = """
        truncate ecommdata.sku_ean;
        """,
    )

    t2 = PythonOperator(
        task_id = "full_load_table_to_workspace",
        python_callable = _full_load_table_to_workspace
    )

t0 >> t1 >> t2
