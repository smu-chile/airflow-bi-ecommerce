from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

from utils.netezza_utils import load_custom_query_to_s3

from datetime import datetime, timedelta
import pendulum


def materiales_lista8():
    import pandas as pd
    stock_carnes_padre_hijo = """select distinct split_part(ref_id , '-', 1) AS material
                            from ecommdata.productos_tienda pt ;"""
    print(stock_carnes_padre_hijo)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(stock_carnes_padre_hijo)
    results = cursor.fetchall()
    results=pd.DataFrame(results)
    print(results)
    results.columns = ["material"] 
    cursor.close()
    pg_connection.close()
    return results

def master_sku_to_s3(ds,ti):
    import pandas as pd
    import numpy as np
    import io
    from io import StringIO

    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"master_skus_/{exec_date}/"

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    ventas_sala_dw_file = ti.xcom_pull(key="return_value", task_ids=["extract_data_from_dw"])[0]

    print("Searching file: "+ventas_sala_dw_file)
    if not s3_hook.check_for_key(ventas_sala_dw_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % ventas_sala_dw_file)

    ventas_sala_dw_object = s3_hook.get_key(ventas_sala_dw_file, bucket_name=s3_bucket)

    column_types = {
    "SKU_KEY": "str",
    "SKU_PRODUCT": "str",
    "GRUPO_KEY": "str",
    "GRUPO_ID": "str",
    "GRUPO_DSC": "str",
    "CATEGORIA_KEY": "str",
    "CAT_ID": "str",
    "CAT_DSC": "str",
    "LINEA_KEY": "str",
    "LIN_ID": "str",
    "LIN_DESC": "str",
    "SECCION_KEY": "str",
    "SEC_ID": "str",
    "SEC_DSC": "str",
    "NEGOCIO_KEY": "str",
    "NEG_ID": "str",
    "NEG_DSC": "str",
    "SKU_NM": "str",
    "PROCEDENCIA": "str",
    "BRAND_KEY": "str",
    "BRAND_ID": "str",
    "BRAND_DESC": "str",
    "UMB": "str",
    "UMP": "str",
    "UMCONT": "str",
    "DESC_TIPO_MATERIAL": "str",
    "DESC_CATEGORIA_MATERIAL": "str",
    "ENVASE": "str",
    "DESCRIPCION": "str",
    "PESO_NETO": "float",
    "PESO_BRUTO": "float",
    "SUPPLIER_KEY": "str",
    "SUPPLIER_ID": "str",
    "SUPPLIER_NM": "str",
    "SUPPLIER_TYPE": "str",
    "SUPPLIER_RETAIL": "str",
    "NUESTRO_100": "str",
    "MARCA_PROPIA": "str"
    }

    df_supp = pd.read_csv(ventas_sala_dw_object.get()["Body"], dtype=column_types)
    print(f"Number of records found: {len(df_supp.index)}")

    if len(df_supp.index) == 0:
        print("There are no new records to load. Task will exit as successfull.")
        return
    df_supp.info()

    df_lista8 = materiales_lista8()
    df_lista8.info()

    df_aux = pd.merge(df_lista8, df_supp, left_on='material', right_on='SKU_PRODUCT', how = 'left').drop('SKU_PRODUCT', axis=1)


    df_final = df_aux
    df_final.info()

    buffer = io.StringIO()
    df_final.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"master_skus_/{exec_date}/master_skus_{date_aux}.csv"
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

def master_sku_to_postgresq(ti):
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text

    filename = ti.xcom_pull(key="return_value", task_ids=["master_sku_to_s3"])[0]

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
    df.columns = ['material',
                'sku_key',
                'grupo_key',
                'grupo_id', 
                'grupo_sap', 
                'categoria_key', 
                'categoria_id', 
                'categoria_sap', 
                'linea_key', 
                'linea_id', 
                'linea_sap', 
                'seccion_key', 
                'seccion_id', 
                'seccion_sap', 
                'negocio_key', 
                'negocio_id', 
                'negocio_sap', 
                'sku_name', 
                'procedencia', 
                'marca_key', 
                'marca_id', 
                'marca', 
                'umb', 
                'ump', 
                'um_count', 
                'tipo_material', 
                'categoria_material', 
                'envase_id', 
                'envase', 
                'peso_neto', 
                'peso_bruto', 
                'key_proveedor', 
                'id_proveedor', 
                'nombre_proveedor', 
                'tipo_proveedor', 
                'proveedor_retail', 
                'nuestro_100', 
                'marca_propia']

    df.info()

    df['proveedor_retail'] = pd.to_numeric(df['proveedor_retail'], errors='coerce').astype('Int64')
    df['nuestro_100'] = pd.to_numeric(df['nuestro_100'], errors='coerce').astype('Int64')
    df['marca_propia'] = df['marca_propia'].fillna(False)
    df['marca_propia'] = df['marca_propia'].astype(int)
    df['material'] = df['material'].apply(lambda x: str(x).zfill(18))

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    with engine.begin() as conn:
        conn.execute("TRUNCATE ecommdata.maestra_sku_proveedor")
        df.to_sql(name="maestra_sku_proveedor",
                    con=conn,         
                    schema="ecommdata",         
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
    'etl_skus_master_table',
    default_args=default_args,
    description="cargar maestra skus",
    schedule_interval= "0 5 * * 1",
    start_date=pendulum.datetime(2023, 6, 14, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "postgres", "ecommdata", "maestra_skus", "proveedores", "PATRICIO"],
) as dag:
    

    dag.doc_md = """
    generar dataframe a partir de vistas en DW, guardarlos en s3 y cargar la vista del dia en postgesql. \n
    Truncate and load diario.
    """ 

    t0 = PythonOperator(
        task_id = "extract_data_from_dw",
        python_callable = load_custom_query_to_s3,
        op_kwargs = {
            "query": """
                SELECT H.SKU_KEY,H.SKU_PRODUCT, H.GRUPO_KEY, H.GRUPO_ID, H.GRUPO_DSC, H.CATEGORIA_KEY, H.CAT_ID, H.CAT_DSC,
                H.LINEA_KEY, H.LIN_ID, H.LIN_DESC, H.SECCION_KEY, H.SEC_ID, H.SEC_DSC, H.NEGOCIO_KEY, H.NEG_ID, H.NEG_DSC,
                H.SKU_NM, H.PROCEDENCIA,H.BRAND_KEY , H.BRAND_ID, H.BRAND_DESC , H.UMB, H.UMP, H.UMCONT, H.DESC_TIPO_MATERIAL,
                H.DESC_CATEGORIA_MATERIAL, H.ENVASE, E.DESCRIPCION, H.PESO_NETO, H.PESO_BRUTO, S.SUPPLIER_KEY, S.SUPPLIER_ID, S.SUPPLIER_NM,
                S.SUPPLIER_TYPE, S.SUPPLIER_RETAIL, S.NUESTRO_100, H.MARCA_PROPIA
                FROM DWC_SMU.SMU.VW_DIM_SKU_HIERARCHY AS H
                LEFT JOIN DWC_SMU.SMU.VW_DIM_SUPPLIER AS S on H.PROVEEDOR_PPAL_KEY = S.SUPPLIER_KEY
                LEFT JOIN DWC_SMU.SMU.VW_DIM_ENVASE AS E on E.CODIGO = H.ENVASE
            """,
            "query_name": "HIERARCHYxSUPPLIER_query"
        },
        retries = 2,
        retry_delay = timedelta(minutes=1),
        execution_timeout = timedelta(minutes=60),
        pool = "backfill_pool"
    )

    t1 = PythonOperator(
        task_id = "master_sku_to_s3",
        python_callable = master_sku_to_s3,
    )

    t2 = PythonOperator(
        task_id = "master_sku_to_postgresq",
        python_callable = master_sku_to_postgresq,
    )

    t0 >> t1 >> t2