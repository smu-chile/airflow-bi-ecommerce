from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from uitls.bigquery_utils import load_custom_bq_query_to_s3

from datetime import datetime, timedelta
import pendulum

def _load_to_postgres(ti):
    import pandas as pd
    import numpy as np

    ventas_sala_dw_file = ti.xcom_pull(key="return_value", task_ids=["extract_data_from_dw"])[0]
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+ventas_sala_dw_file)
    if not s3_hook.check_for_key(ventas_sala_dw_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % ventas_sala_dw_file)

    ventas_sala_dw_object = s3_hook.get_key(ventas_sala_dw_file, bucket_name=s3_bucket)

    column_types = {
        "DATE_KEY": "str",
        "STORE_ID": "str",
        "ORG_IP": "str",
        "VENTA_NETA": "int"
    }

    df = pd.read_csv(ventas_sala_dw_object.get()["Body"], dtype=column_types)
    print(f"Number of records found: {len(df.index)}")

    if len(df.index) == 0:
        print("There are no new records to load. Task will exit as successfull.")
        return
    
    column_names = {
        "DATE_VALUE": "fecha",
        "STORE_ID": "id_tienda",
        "ORG_IP": "organizacion",
        "VENTA_NETA": "venta_neta"
    }

    df = df.rename(columns=column_names)

    df = df[["id_tienda","fecha","organizacion","venta_neta"]]

    columns = [
        "organizacion",
        "venta_neta"
    ]

    columns_query = ",".join(columns)
    excluded_query = ",".join(["EXCLUDED."+column for column in columns])
    values_query = "%s, %s,"+",".join(["%s" for column in columns])
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
        INSERT INTO ecommdata.venta_locales_pbi (id_tienda, fecha,"""+columns_query+""") 
        VALUES ("""+values_query+""")
        ON CONFLICT (id_tienda, fecha)
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
    print("Data loaded to Postgres. ecommdata.venta_locales_pbi")

    return

    

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'etl_venta_locales_pbi',
    default_args=default_args,
    description="Extracción de venta de sala de dw",
    schedule_interval="15 7 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="America/Santiago"),
    catchup=True,
    max_active_runs = 1,
    tags=["DATA", "DW", "S3", "venta", "sala", "MATIAS"],
) as dag:

    dag.doc_md = """
    Extracción de venta de sala de dw.
    """ 
    t0 = PythonOperator(
        task_id = "extract_data_from_dw",
        python_callable = load_custom_bq_query_to_s3,
        op_kwargs = {
            "query": """
                SELECT FECHA.DATE_VALUE,
                S.STORE_ID,
                STORE_H.ORG_IP,
                ROUND(SUM(VENTAC.VENTA_NETA), 0) AS VENTA_NETA
                FROM (((`cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_REGISTRO_VENTA_CONTABLE` VENTAC
                JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_DATE` FECHA ON ((FECHA.DATE_KEY = VENTAC.DATE_KEY))) 
                JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_STORE` S ON ((S.STORE_KEY = VENTAC.STORE_KEY))) 
                JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_STORE_HIERARCHY` STORE_H ON ((STORE_H.STORE_KEY = VENTAC.STORE_KEY))) 
                WHERE FECHA.DATE_VALUE >= DATE_SUB(DATE('{{ds}}'), INTERVAL 2 WEEK)
                AND VENTAC.DATE_VALUE >= DATE_SUB(DATE('{{ds}}'), INTERVAL 2 WEEK)
                GROUP BY 1,2,3;
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
    