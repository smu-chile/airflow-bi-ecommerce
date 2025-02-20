from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

from utils.netezza_utils import load_custom_query_to_s3

from datetime import datetime

import pendulum

def _incremental_load_sales_table_unimarc(ti, ds):
    import numpy as np
    import pandas as pd
    
    sales_file = ti.xcom_pull(key="return_value", task_ids=["load_custom_query_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+sales_file)
    if not s3_hook.check_for_key(sales_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % sales_file)

    sales_object = s3_hook.get_key(sales_file, bucket_name=s3_bucket)

    df = pd.read_csv(sales_object.get()["Body"])
    
    print(f"Number of records extracted: {len(df.index)}")

    # Select only relevant columns:
    df = df[["DATE_VALUE",
            "STORE_ID",
            "ORG_IP_ID",
            "ORG_IP",
            "SKU_PRODUCT",
            "UOM_COD_COM",
            "VENTA_BRUTA",
            "VENTA_NETA",
            "VENTA_UMV",
            "GRUPO_DSC",
            "CAT_DSC",
            "LIN_DESC",
            "SEC_DSC",
            "NEG_DSC",
            "SKU_NM",
            "PROCEDENCIA",
            "BRAND_DESC",
            "MARCA_PROPIA",
            "DESC_TIPO_MATERIAL",
            "DESC_CATEGORIA_MATERIAL"
            ]]

    # Rename columns to match workspace schema:
    columns_rename = {
            "DATE_VALUE" : "fecha",
            "STORE_ID" : "id_tienda",
            "ORG_IP_ID" : "id_org",
            "ORG_IP" : "organizacion",
            "SKU_PRODUCT" : "material",
            "UOM_COD_COM" : "umv",
            "VENTA_BRUTA" : "venta_bruta",
            "VENTA_NETA" : "venta_neta",
            "VENTA_UMV" : "venta_umv",
            "GRUPO_DSC" : "grupo",
            "CAT_DSC" : "categoria",
            "LIN_DESC" : "linea",
            "SEC_DSC" : "seccion",
            "NEG_DSC" : "negocio",
            "SKU_NM" : "descripcion",
            "PROCEDENCIA" : "procedencia",
            "BRAND_DESC" : "marca",
            "MARCA_PROPIA" : "marca_propia",
            "DESC_TIPO_MATERIAL" : "tipo_material",
            "DESC_CATEGORIA_MATERIAL" : "categoria_material"
    }
    df = df.rename(columns=columns_rename)


    # Cast numeric values to int

    df = df.astype({
        "fecha": "string",
        "id_tienda": "string",
        "id_org": "string",
        "marca_propia": "bool",
    }, errors="ignore")

    columns = [
            "id_tienda",
            "id_org",
            "organizacion",
            "material",
            "umv",
            "venta_bruta",
            "venta_neta",
            "venta_umv",
            "grupo",
            "categoria",
            "linea",
            "seccion",
            "negocio",
            "descripcion",
            "procedencia",
            "marca",
            "marca_propia",
            "tipo_material",
            "categoria_material"
    ]

    df = df[["fecha",
            "id_tienda",
            "id_org",
            "organizacion",
            "material",
            "umv",
            "venta_bruta",
            "venta_neta",
            "venta_umv",
            "grupo",
            "categoria",
            "linea",
            "seccion",
            "negocio",
            "descripcion",
            "procedencia",
            "marca",
            "marca_propia",
            "tipo_material",
            "categoria_material"]]
    
    print(df)
    df = df[df["id_org"] == "1"]
    print(df)
    df["id_tienda"] = df["id_tienda"].astype("string").str.pad(4, "left", '0')
    df["material"] = df["id_tienda"].astype("string").str.pad(18, "left", '0')

    columns_query = ",".join(columns)
    excluded_query = ",".join(["EXCLUDED."+column for column in columns])
    values_query = "%s,"+",".join(["%s" for column in columns])
    df = df.fillna("NULL")
    records = list(df.to_records(index=False))
    
    # Change data types to native python types
    fixed_records = []
    for record in records:
        fixed_record = []
        for value in record:
            if isinstance(value, np.generic):
                fixed_record.append(value.item())
            elif value == "NULL":
                fixed_record.append(None)
            else:
                fixed_record.append(value)
        fixed_records.append(tuple(fixed_record))
    print(f"Number of records to load: {str(len(fixed_records))}")
    query = """
        INSERT INTO ecommdata.venta_sku_tienda (fecha,"""+columns_query+""") 
        VALUES ("""+values_query+""")
    """
    print(query)
    delete_query = f"""
        DELETE FROM ecommdata.venta_sku_tienda
        where fecha < '{ds}'::date - interval '75 days'
    """
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.executemany(query, fixed_records)
    cursor.execute(delete_query)
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    print("Data loaded to Postgres")

    return

def _incremental_load_sales_table_alvi(ti, ds):
    import numpy as np
    import pandas as pd
    
    sales_file = ti.xcom_pull(key="return_value", task_ids=["load_custom_query_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+sales_file)
    if not s3_hook.check_for_key(sales_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % sales_file)

    sales_object = s3_hook.get_key(sales_file, bucket_name=s3_bucket)

    df = pd.read_csv(sales_object.get()["Body"])
    
    print(f"Number of records extracted: {len(df.index)}")

    # Select only relevant columns:
    df = df[["DATE_VALUE",
            "STORE_ID",
            "ORG_IP_ID",
            "ORG_IP",
            "SKU_PRODUCT",
            "UOM_COD_COM",
            "VENTA_BRUTA",
            "VENTA_NETA",
            "VENTA_UMV",
            "GRUPO_DSC",
            "CAT_DSC",
            "LIN_DESC",
            "SEC_DSC",
            "NEG_DSC",
            "SKU_NM",
            "PROCEDENCIA",
            "BRAND_DESC",
            "MARCA_PROPIA",
            "DESC_TIPO_MATERIAL",
            "DESC_CATEGORIA_MATERIAL"
            ]]

    # Rename columns to match workspace schema:
    columns_rename = {
            "DATE_VALUE" : "fecha",
            "STORE_ID" : "id_tienda",
            "ORG_IP_ID" : "id_org",
            "ORG_IP" : "organizacion",
            "SKU_PRODUCT" : "material",
            "UOM_COD_COM" : "umv",
            "VENTA_BRUTA" : "venta_bruta",
            "VENTA_NETA" : "venta_neta",
            "VENTA_UMV" : "venta_umv",
            "GRUPO_DSC" : "grupo",
            "CAT_DSC" : "categoria",
            "LIN_DESC" : "linea",
            "SEC_DSC" : "seccion",
            "NEG_DSC" : "negocio",
            "SKU_NM" : "descripcion",
            "PROCEDENCIA" : "procedencia",
            "BRAND_DESC" : "marca",
            "MARCA_PROPIA" : "marca_propia",
            "DESC_TIPO_MATERIAL" : "tipo_material",
            "DESC_CATEGORIA_MATERIAL" : "categoria_material"
    }
    df = df.rename(columns=columns_rename)


    # Cast numeric values to int

    df = df.astype({
        "fecha": "string",
        "id_tienda": "string",
        "id_org": "string",
        "marca_propia": "bool",
    }, errors="ignore")

    columns = [
            "id_tienda",
            "id_org",
            "organizacion",
            "material",
            "umv",
            "venta_bruta",
            "venta_neta",
            "venta_umv",
            "grupo",
            "categoria",
            "linea",
            "seccion",
            "negocio",
            "descripcion",
            "procedencia",
            "marca",
            "marca_propia",
            "tipo_material",
            "categoria_material"
    ]

    df = df[["fecha",
            "id_tienda",
            "id_org",
            "organizacion",
            "material",
            "umv",
            "venta_bruta",
            "venta_neta",
            "venta_umv",
            "grupo",
            "categoria",
            "linea",
            "seccion",
            "negocio",
            "descripcion",
            "procedencia",
            "marca",
            "marca_propia",
            "tipo_material",
            "categoria_material"]]
    
    df = df[df["id_org"] == "8"]

    columns_query = ",".join(columns)
    excluded_query = ",".join(["EXCLUDED."+column for column in columns])
    values_query = "%s,"+",".join(["%s" for column in columns])
    df = df.fillna("NULL")
    records = list(df.to_records(index=False))
    
    # Change data types to native python types
    fixed_records = []
    for record in records:
        fixed_record = []
        for value in record:
            if isinstance(value, np.generic):
                fixed_record.append(value.item())
            elif value == "NULL":
                fixed_record.append(None)
            else:
                fixed_record.append(value)
        fixed_records.append(tuple(fixed_record))
    print(f"Number of records to load: {str(len(fixed_records))}")
    query = """
        INSERT INTO ecommdata_alvi.venta_sku_tienda (fecha,"""+columns_query+""") 
        VALUES ("""+values_query+""")
    """
    print(query)
    delete_query = f"""
        DELETE FROM ecommdata_alvi.venta_sku_tienda
        where fecha < '{ds}'::date - interval '29 days'
    """
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.executemany(query, fixed_records)
    cursor.execute(delete_query)
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
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
    'etl_venta_sku_tienda_dw_load_and_truncate',
    default_args=default_args,
    description="Extracción y carga de vistas de venta por sku desde DW hasta Workspace.",
    schedule_interval="30 8 * * *",
    start_date=pendulum.datetime(2023, 8, 15, tz="America/Santiago"),
    catchup=True,
    max_active_runs = 1,
    tags=["DATA", "DW", "ecommdata", "ventas", "MATIAS"],
) as dag:

    dag.doc_md = """
    Extracción y carga de vistas de venta por sku desde DW hasta Workspace.
    """ 
    
    t0 = PythonOperator(
        task_id = "load_custom_query_to_s3",
        python_callable = load_custom_query_to_s3,
        op_kwargs = {
            "query": """SELECT VENTAC.DATE_VALUE
            , VENTAC.STORE_ID
            , STORE_H.ORG_IP_ID
            , STORE_H.ORG_IP
            , S.SKU_PRODUCT
            , U.UOM_COD_COM
            , VENTAC.VENTA_BRUTA
            , VENTAC.VENTA_NETA
            , VENTAC.VENTA_UMV
            , SH.GRUPO_DSC
            , SH.CAT_DSC
            , SH.LIN_DESC
            , SH.SEC_DSC
            , SH.NEG_DSC
            , SH.SKU_NM
            , SH.PROCEDENCIA
            , SH.BRAND_DESC
            , SH.MARCA_PROPIA
            , SH.DESC_TIPO_MATERIAL
            , SH.DESC_CATEGORIA_MATERIAL
            FROM DWC_SMU.SMU.VW_FACT_REGISTRO_VENTA_CONTABLE VENTAC
            LEFT JOIN DWC_SMU.SMU.VW_DIM_STORE_HIERARCHY STORE_H ON STORE_H.STORE_KEY = VENTAC.STORE_KEY
            LEFT JOIN DWC_SMU.SMU.VW_DIM_SKU_ATTR S ON VENTAC.SKU_KEY = S.SKU_KEY
            LEFT JOIN DWC_SMU.SMU.VW_DIM_PRODUCT P ON VENTAC.PRODUCT_KEY = P.PRODUCT_KEY
            LEFT JOIN DWC_SMU.SMU.VW_DIM_UOM U ON P.UOM_VTA_KEY = U.UOM_KEY
            LEFT JOIN DWC_SMU.SMU.VW_DIM_SKU_HIERARCHY SH ON VENTAC.SKU_KEY = SH.SKU_KEY
            WHERE VENTAC.DATE_VALUE = '{{ds}}' AND STORE_H.ORG_IP_ID IN ('01', '06' , '08');
            """,
            "query_name": "venta_sku_tienda_dw",
        }
    )

    t1 = PostgresOperator(
        task_id = "clean_day_unimarc",
        postgres_conn_id="postgresql_conn",
        sql="""
        delete from ecommdata.venta_sku_tienda
        where fecha = '{{ds}}'
        """
    )

    t2 = PostgresOperator(
        task_id = "clean_day_alvi",
        postgres_conn_id="postgresql_conn",
        sql="""
        delete from ecommdata_alvi.venta_sku_tienda
        where fecha = '{{ds}}'
        """
    )

    t3 = PythonOperator(
        task_id = "_incremental_load_sales_table_unimarc",
        python_callable = _incremental_load_sales_table_unimarc
    )

    t4 = PythonOperator(
        task_id = "_incremental_load_sales_table_alvi",
        python_callable = _incremental_load_sales_table_alvi
    )

    t0 >> t1 >> t3
    t0 >> t2 >> t4
