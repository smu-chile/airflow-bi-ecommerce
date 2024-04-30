from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.netezza_utils import load_custom_query_to_s3

from datetime import datetime, timedelta
import pendulum

def _load_to_postgres(ti):
    import pandas as pd
    import numpy as np

    costos_dw_file = ti.xcom_pull(key="return_value", task_ids=["extract_data_from_dw"])[0]
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+ costos_dw_file)
    if not s3_hook.check_for_key(costos_dw_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % costos_dw_file)

    costos_dw_object = s3_hook.get_key(costos_dw_file, bucket_name=s3_bucket)

    column_types = {
        "SKU_PRODUCT": "str",
        "SKU_NM": "str",
        "FECHA": "str",
        "FORMATO": "str",
        "ID_TIENDA": "str",
        "CANAL_DE_VENTA": "str",
        "VENTA_NETA": "str",
        "COSTO_NETO_CALCULADO": "str"
    }

    df = pd.read_csv(costos_dw_object.get()["Body"], dtype=column_types)
    print(f"Number of records found: {len(df.index)}")

    if len(df.index) == 0:
        print("There are no new records to load. Task will exit as successfull.")
        return
    
    column_names = {
        "SKU_PRODUCT": "sku",
        "SKU_NM": "descripcion",
        "FECHA": "fecha",
        "FORMATO": "formato",
        "ID_TIENDA": "id_tienda",
        "CANAL_DE_VENTA": "canal_de_venta",
        "VENTA_NETA": "venta_neta",
        "COSTO_NETO_CALCULADO": "costo_neto_calculado"
    }

    df = df.rename(columns=column_names)

    df = df[["sku" , "fecha", "id_tienda", "descripcion", "formato", "canal_de_venta", "venta_neta", "costo_neto_calculado"]]

    columns = [
        "descripcion",
        "formato",
        "canal_de_venta",
        "venta_neta",
        "costo_neto_calculado"
    ]

    columns_query = ",".join(columns)
    excluded_query = ",".join(["EXCLUDED."+column for column in columns])
    values_query = "%s, %s, %s,"+",".join(["%s" for column in columns])
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
    incremental_query = """
        INSERT INTO ecommdata.costos_dw (sku, fecha, id_tienda, """+columns_query+""") 
        VALUES ("""+values_query+""")
        ON CONFLICT (sku, fecha, id_tienda)
        DO UPDATE SET ("""+columns_query+""") = ("""+excluded_query+""") ;
    """
    print(incremental_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.executemany(incremental_query, fixed_records)
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    print("Data loaded to Postgres. ecommdata.costos_dw")

    return

    

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'etl_costos_dw',
    default_args=default_args,
    description="Extracción de costos por sku de dw",
    schedule_interval="15 9 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Santiago"),
    catchup=True,
    max_active_runs = 1,
    tags=["DATA", "DW", "S3", "costos", "MATIAS"],
) as dag:

    dag.doc_md = """
    Extracción costos por sku de dw.
    """ 
    t0 = PythonOperator(
        task_id = "extract_data_from_dw",
        python_callable = load_custom_query_to_s3,
        op_kwargs = {
            "query": """
                SELECT
                    DSH.SKU_PRODUCT ,
                    DSH.SKU_NM ,
                    CAST(DD.DATE_VALUE AS TIMESTAMP) AS FECHA,
                    SH.ORG_IP AS FORMATO,
                    SH.STORE_ID AS ID_TIENDA,
                    VEC.CANAL_VENTA AS CANAL_DE_VENTA,
                    SUM(VEC.VENTA_NETA) AS VENTA_NETA,
                    SUM(CASE
                        WHEN VC.VENTA_UMB = 0 THEN 0
                        ELSE CAST(VC.COSTO_NETO AS DOUBLE PRECISION) / VC.VENTA_UMB
                    END * ((CAST(DP.CONT_CONV_UMB AS DOUBLE PRECISION) / DP.DENOM_UMB) * VEC.VENTA_UMV)) AS COSTO_NETO_CALCULADO
                FROM
                    DWC_SMU.SMU.VW_DIM_DATE DD
                        INNER JOIN DWC_SMU.SMU.VW_FACT_VENTA_E_COMMERCE VEC
                ON DD.DATE_KEY = VEC.DATE_KEY
                INNER JOIN DWC_SMU.SMU.VW_DIM_PRODUCT DP
                            ON DP.EAN = VEC.PTR_CODPROD
                        INNER JOIN DWC_SMU.SMU.VW_DIM_SKU_HIERARCHY DSH ON DP.SKU_KEY = DSH.SKU_KEY
                INNER JOIN DWC_SMU.SMU.VW_DIM_STORE_HIERARCHY SH
                            ON SH.STORE_KEY = VEC.STORE_KEY
                LEFT OUTER JOIN
                (
                SELECT
                VW_FACT_CONT_DIA_SKU.DATE_KEY AS DATE_KEY,
                VW_FACT_CONT_DIA_SKU.STORE_KEY AS STORE_KEY,
                MIN(VW_FACT_CONT_DIA_SKU.VENTA_UMB) AS VENTA_UMB,
                MIN(VW_FACT_CONT_DIA_SKU.COSTO_NETO) AS COSTO_NETO,
                VW_FACT_CONT_DIA_SKU.PRODUCT_KEY AS PRODUCT_KEY
                FROM
                (
                SELECT
                VW_FACT_CONT_DIA_SKU.SKU_KEY AS SKU_KEY,
                VW_FACT_CONT_DIA_SKU.DATE_KEY AS DATE_KEY,
                VW_FACT_CONT_DIA_SKU.STORE_KEY AS STORE_KEY,
                VW_FACT_CONT_DIA_SKU.STORE_ID AS STORE_ID,
                VW_FACT_CONT_DIA_SKU.DATE_VALUE AS DATE_VALUE,
                VW_FACT_CONT_DIA_SKU.VENTA_BRUTA AS VENTA_BRUTA,
                VW_FACT_CONT_DIA_SKU.VENTA_NETA AS VENTA_NETA,
                VW_FACT_CONT_DIA_SKU.IMPUESTOS_VENTA_CONTABILIZADA AS IMPUESTOS_VENTA_CONTABILIZADA,
                VW_FACT_CONT_DIA_SKU.VENTA_UMV AS VENTA_UMV,
                VW_FACT_CONT_DIA_SKU.VENTA_NETA_CON_ILA AS VENTA_NETA_CON_ILA,
                VW_FACT_CONT_DIA_SKU.IMPUESTO_ESPECIFICO AS IMPUESTO_ESPECIFICO,
                VW_FACT_CONT_DIA_SKU.VENTA___M2 AS VENTA___M2,
                VW_FACT_CONT_DIA_SKU.COSTO_ILA AS COSTO_ILA,
                VW_FACT_CONT_DIA_SKU.MARGEN_TEORICO AS MARGEN_TEORICO,
                VW_FACT_CONT_DIA_SKU.VENTA_UMB AS VENTA_UMB,
                VW_FACT_CONT_DIA_SKU.PRECIO_VENTA_UNITARIO AS PRECIO_VENTA_UNITARIO,
                VW_FACT_CONT_DIA_SKU.VENTA_UM_CONTENIDO_ACTUAL AS VENTA_UM_CONTENIDO_ACTUAL,
                VW_FACT_CONT_DIA_SKU.COSTO_NETO AS COSTO_NETO,
                VW_FACT_CONT_DIA_SKU.UM_BASE_KEY AS UM_BASE_KEY,
                VW_FACT_CONT_DIA_SKU.UM_VENTA_KEY AS UM_VENTA_KEY,
                VW_FACT_CONT_DIA_SKU.COSTO_REPOSICION AS COSTO_REPOSICION,
                VW_FACT_CONT_DIA_SKU.PRODUCT_KEY AS PRODUCT_KEY,
                VW_FACT_CONT_DIA_SKU.OU_SKU_KEY AS OU_SKU_KEY,
                VW_DIM_UOM.UOM_COD_COM AS UMV_COD_COM
                FROM
                DWC_SMU.SMU.VW_FACT_CONT_DIA_SKU VW_FACT_CONT_DIA_SKU
                INNER JOIN DWC_SMU.SMU.VW_DIM_UOM VW_DIM_UOM
                ON VW_DIM_UOM.UOM_KEY = VW_FACT_CONT_DIA_SKU.UM_VENTA_KEY
                WHERE
                PRODUCT_KEY <> 0
                ) VW_FACT_CONT_DIA_SKU
                GROUP BY
                VW_FACT_CONT_DIA_SKU.DATE_KEY,
                VW_FACT_CONT_DIA_SKU.STORE_KEY,
                VW_FACT_CONT_DIA_SKU.PRODUCT_KEY
                )	VC
                                    ON
                                        VC.STORE_KEY = VEC.STORE_KEY AND VC.DATE_KEY = VEC.DATE_KEY AND VC.PRODUCT_KEY = VEC.PRODUCT_KEY
                WHERE
                    NOT ( SH.STORE_ID LIKE '6%' ) AND
                    NOT ( SH.STORE_ID LIKE '7%' ) AND
                    NOT ( SH.ORG_IP_ID IS NULL ) AND
                    NOT ( SH.ORG_IP_ID IN ('00', '03', '07','02','04', '09') ) AND
                CAST(DD.DATE_VALUE AS TIMESTAMP) = '{{ds}}' AND
                    VEC.CANAL_VENTA IN ( 'E-COMMERCE' )
                GROUP BY 1,2,3,4,5,6
            """,
            "query_name": "venta_locales_pbi"
        },
        retries = 2,
        retry_delay = timedelta(minutes=1),
        execution_timeout = timedelta(minutes=60),
        pool = "backfill_pool"
    )

    t1= PythonOperator(
        task_id = "load_to_postgres",
        python_callable = _load_to_postgres
    )

    

    t0 >> t1
    