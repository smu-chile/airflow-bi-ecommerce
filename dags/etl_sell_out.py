from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

from utils.bigquery_utils import bq_query_to_df
from utils.slack_utils import dag_success_slack, dag_failure_slack

import pendulum

from datetime import datetime, timedelta

def render_netezza_view(ds):
    
    sql_str= f"""
    select *
        from `cl-cda-prod.DS_CDA_BI_USR.FACT_CUBO_ECOMMERCE_PRINCIPAL` 
        where DATE(SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', FECHA_CREACION_VTEX)) >= date_sub(cast('{ds}' as date), interval 1 day)
        and DATE(SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', FECHA_CREACION_VTEX)) < cast('{ds}' as date)
        """
    print(sql_str)

    df = bq_query_to_df(sql_str)
    
    return df


def sell_out_to_s3(ds):
    import pandas as pd
    import numpy as np
    import io
    from io import StringIO

    print("comenzando S3")
    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"sell_out_/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    df = render_netezza_view(ds)

    print("Todo bien hasta acá en la extracción de DWC")

    # Cambiando columnas a minusculas
    df.columns = df.columns.str.lower()

    df = df[['desc_organizacion',
    'id_centro',
    'desc_centro',
    'canal_wf',
    'fecha_vta',
    'fecha_creacion_vtex',
    'numtrx_vta',
    'nro_cotiza',
    'id_proveedor',
    'id_wf',
    'desc_promo_wf',
    'id_cod_cat',
    'des_cod_cat',
    'id_grupo_articulo',
    'desc_grupo_articulo',
    'id_seccion',
    'id_negocio',
    'id_linea',
    'desc_linea',
    'desc_negocio',
    'cod_mat',
    'des_mat',
    'ean',
    'umv',
    'umv_cnt',
    'marca',
    'tipo_promo',
    'tipo_doc',
    'unid_vta_promo',
    'unid_vtex',
    'venta_bruta',
    'venta_neta',
    'gasto_sellout']]
    
    print("\nHasta acá todo bien al filtrar las columnas :D\n")
    
    df.columns = ['desc_organizacion',
    'id_tienda',
    'desc_centro',
    'canal_wf',
    'fecha_vta',
    'fecha_creacion_vtex',
    'numtrx_vta',
    'nro_cotiza',
    'id_proveedor',
    'id_wf',
    'desc_promo_wf',
    'id_cod_cat',
    'des_cod_cat',
    'id_grupo_articulo',
    'desc_grupo_articulo',
    'id_seccion',
    'id_negocio',
    'id_linea',
    'desc_linea',
    'desc_negocio',
    'material',
    'des_mat',
    'ean',
    'umv',
    'umv_cnt',
    'marca',
    'tipo_promo',
    'tipo_doc',
    'unid_vta_promo',
    'unid_vtex',
    'venta_bruta',
    'venta_neta',
    'gasto_sellout']

    print("\nHasta acá todo bien renombrando las columnas :D\n")

    print(df.info())

    buffer = io.StringIO()
    df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"sell_out_/{exec_date}/sell_out_{date_aux}.csv"
    buffer.seek(0)
    print("se logro transformar el dataframe a un archivo .csv")
    print(f"con fecha {ds} y nombre de filename como {filename}")
    s3_hook.load_string(buffer.getvalue(),
                key=filename,
                bucket_name=s3_bucket,
                replace=True,
                encrypt=False)
    
    print(f"File load on S3: {prefix}")

    return filename

def sell_out_to_postgresql(ti):
    print("todo bien por acá")
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text

    filename = ti.xcom_pull(key="return_value", task_ids=["sell_out_to_s3"])[0]

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
    print(df.info())
    df['umv_cnt'] = df.apply(lambda row: row['umv_cnt'] / 1000 if row['umv'] in ["KG", "KGV"] else row['umv_cnt'], axis=1)
    df['material'] = df['material'].apply(lambda x: str(x).zfill(18))
    df['id_tienda'] = df['id_tienda'].apply(lambda x: str(x).zfill(4))

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    with engine.begin() as conn:
        df.to_sql(name="sell_out",
                    con=conn,         
                    schema="catalogo",         
                    if_exists='append',         
                    index=False,         
                    chunksize=20000,         
                    method='multi')

    print("Data saved to PostgreSQL.")

    return
    

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_sell_out',
    default_args=default_args,
    description="cargar tabla sell_out",
    schedule_interval= "0 11 * * *",
    start_date=pendulum.datetime(2023, 10, 9, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "postgres", "ecommdata", "sell_out", "S3", "PATRICIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:
    

    dag.doc_md = """
    Extrae tabla sell_out de dwC, lo carga a S3 y postgresql en un intervalo de 30 dias por fecha creacion. \n
    Insert diario 11 am.
    """ 

    t0 = PythonOperator(
        task_id = "sell_out_to_s3",
        python_callable = sell_out_to_s3,
    )

    t1 = PythonOperator(
        task_id = "sell_out_to_postgresql",
        python_callable = sell_out_to_postgresql,
    )


    t0 >> t1