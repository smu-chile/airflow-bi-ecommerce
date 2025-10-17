from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from utils.bigquery_utils import load_custom_bq_query_to_s3

from datetime import datetime

import pendulum

def _load_stores_table(ti, ds):
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import Table, MetaData
    from sqlalchemy.dialects.postgresql import insert
    
    stores_file = ti.xcom_pull(key="return_value", task_ids=["load_custom_query_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+stores_file)
    if not s3_hook.check_for_key(stores_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % stores_file)

    stores_object = s3_hook.get_key(stores_file, bucket_name=s3_bucket)

    df = pd.read_csv(stores_object.get()["Body"])
    
    print(f"Number of records extracted: {len(df.index)}")

    # Rename columns to match workspace schema:
    columns_rename = {
            "STORE_ID" : "id_tienda",
            "STORE_NAME" : "nombre_tienda",
            "CANAL_DIST" : "canal_dist",
            "ORG_COMPRAS" : "org_compras",
            "ORG_VENTAS" : "org_ventas",
            "CITY_ID" : "city_id",
            "COUNTY_DESC" : "county_desc"
    }
    df = df.rename(columns=columns_rename)


    # Cast numeric values to int

    df = df.astype({
        "id_tienda": "string",
        "canal_dist": "string"
    }, errors="ignore")

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    # Save to PostgreSQL:
    md = MetaData(schema="ecommdata")
    tiendas = Table("tiendas_dw", md, autoload_with=engine)

    # Preparar el INSERT…ON CONFLICT
    records = df.to_dict(orient="records")
    stmt = insert(tiendas).values(records)
    # `id_tienda` es PRIMARY KEY
    stmt = stmt.on_conflict_do_nothing(index_elements=["id_tienda"])

    # Sólo actualizamos las columnas que queremos, excluyendo 'zona' y PK:
    excluded = stmt.excluded
    cols_to_upd = {
        c.name: getattr(excluded, c.name)
        for c in tiendas.columns
        if c.name not in ("id_tienda", "zona")
    }

    stmt = stmt.on_conflict_do_update(
        index_elements=["id_tienda"],
        set_=cols_to_upd
    )

    # Ejecución
    with engine.begin() as conn:
        result = conn.execute(stmt)
        print(f"{result.rowcount} filas nuevas insertadas (el resto se ignoró por conflicto)")

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_tiendas_dw',
    default_args=default_args,
    description="Extracción y carga de tiendas desde DW hasta Workspace.",
    schedule_interval="30 8 * * *",
    start_date=pendulum.datetime(2022, 2, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["DW", "ecommdata", "tiendas", "MATIAS"],
) as dag:

    dag.doc_md = """
    Extracción y carga de tiendas desde DW hasta Workspace.\n
    Es un Upsert de la tabla `ecommdata.tiendas_dw` en el Workspace.
    """ 
    
    t0 = PythonOperator(
        task_id = "load_custom_query_to_s3",
        python_callable = load_custom_bq_query_to_s3,
        op_kwargs = {
            "query": """SELECT STORE_ID, STORE_NAME, CANAL_DIST, ORG_COMPRAS, ORG_VENTAS, CITY_ID, COUNTY_DESC
            FROM `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_STORE`
            WHERE STORE_ID <> '200' AND CANAL_DIST IS NOT NULL;
            """,
            "query_name": "tiendas_dw",
        }
    )

    #t1 = PostgresOperator(
    #    task_id = "clear_table",
    #    postgres_conn_id="postgresql_conn",
    #    sql="""
    #    truncate ecommdata.tiendas_dw
    #    """
    #)

    t1 = PythonOperator(
        task_id = "load_stores_table",
        python_callable = _load_stores_table
    )

    t0 >> t1
