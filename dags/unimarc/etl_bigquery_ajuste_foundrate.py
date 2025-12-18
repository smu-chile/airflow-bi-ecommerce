from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable

from utils.postgres_utils import is_empty_table
from utils.bigquery_utils import bq_query_to_df
from utils.slack_utils import dag_success_slack, dag_failure_slack

from google.cloud import bigquery
from psycopg2.extras import execute_values
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pendulum
from datetime import datetime


# ---------------------------
# Branch: full vs incremental
# ---------------------------
def _evaluate_load_method(schema, table_name, ti):
    if is_empty_table(schema, table_name):
        ti.xcom_push(key="load_method", value="full")
        return "run_full_load"
    else:
        ti.xcom_push(key="load_method", value="incremental")
        return "run_incremental_load"


# ---------------------------
# Query BQ (usando parámetros)
# ---------------------------
BASE_QUERY = """
        SELECT 
        DATE(a.DATE_VALUE) AS fecha,
        a.OU_ID,
        a.SKU_PRODUCT,
        SUM(a.AJUSTE_CLP) AS AJUSTE_CLP,
        SUM(a.AJUSTE_UMB) AS AJUSTE_UMB
        FROM (
        SELECT
            DATE(aa.DATE_VALUE) AS DATE_VALUE,
            cc.OU_ID,
            dd.SKU_PRODUCT,
            SUM(aa.GOODS_VALUE_AMMOUNT) AS AJUSTE_CLP,
            SUM(aa.NUMBER_OF_ITEMS) AS AJUSTE_UMB
        FROM `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_LOGISTIC_ACTIVITY` aa
        LEFT JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_CONCEPTO_CLASE_MOV` bb
            ON aa.CONCEPTO_CLASE_MOV_KEY = bb.CONCEPTO_CLASE_MOV_KEY
        LEFT JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_ORGANIZATION_UNIT` cc
            ON cc.OU_KEY = aa.ORGANIZATION_UNIT_KEY
        LEFT JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_SKU_HIERARCHY` dd
            ON dd.SKU_KEY = aa.SKU_KEY
        WHERE bb.CONCEPTO_CLASE_MOV_DSC = 'Ajustes Manuales'
            AND DATE(aa.DATE_VALUE) BETWEEN DATE_SUB(@ds, INTERVAL @days DAY)
                                    AND DATE_SUB(@ds, INTERVAL 1 DAY)
        GROUP BY 
            DATE(aa.DATE_VALUE),
            cc.OU_ID,
            dd.SKU_PRODUCT
        ) a
        GROUP BY fecha, OU_ID, SKU_PRODUCT
"""


def query_bq(ds, days):
    params = [
        bigquery.ScalarQueryParameter("ds", "DATE", ds),
        bigquery.ScalarQueryParameter("days", "INT64", days),
    ]
    return bq_query_to_df(BASE_QUERY, query_parameters=params)


# ---------------------------
# UPSERT a Postgres
# ---------------------------
def upsert_to_pg(df):
    if df.empty:
        print("⚠️ No hay datos para cargar.")
        return

    pg = PostgresHook(postgres_conn_id="postgresql_conn")
    conn = pg.get_conn()
    cur = conn.cursor()

    records = df.to_records(index=False).tolist()

    insert_query = """
        INSERT INTO ecommdata.ajuste_foundrate (
            fecha,
            ou_id,
            sku_product,
            ajuste_clp,
            ajuste_umb
        )
        VALUES %s
        ON CONFLICT (fecha, ou_id, sku_product)
        DO UPDATE SET
          ajuste_clp = EXCLUDED.ajuste_clp,
          ajuste_umb = EXCLUDED.ajuste_umb;
    """

    execute_values(cur, insert_query, records)
    conn.commit()
    cur.close()
    conn.close()


# ---------------------------
# Tasks de carga
# ---------------------------
def run_full_load(ds):
    df = query_bq(ds, 60)   # 60 días
    upsert_to_pg(df)


def run_incremental_load(ds):
    df = query_bq(ds, 7)    # 7 días
    upsert_to_pg(df)


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_bigquery_ajuste_foundrate',
    default_args=default_args,
    description="Extracción y carga del ajuste de foundrate de productos desde BigQuery a Postgres.",
    schedule_interval= "0 9 * * *", # Diario a las 09:00 AM
    start_date=pendulum.datetime(2023, 6, 14, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "postgres", "ecommdata", "ajuste_foundrate", "BigQuery", "KEVIN"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:
    

    dag.doc_md = """
    Extracción y carga del ajuste de foundrate de productos desde BigQuery a Postgres.
    Carga diaria.
    """ 

   
    t0 = BranchPythonOperator(
        task_id="decide_load_method",
        python_callable=_evaluate_load_method,
        op_kwargs={"schema": "ecommdata", "table_name": "ajuste_foundrate"},
    )

    t1 = PythonOperator(
        task_id="run_full_load",
        python_callable=run_full_load,
        op_kwargs={"ds": "{{ ds }}"},
    )

    t2 = PythonOperator(
        task_id="run_incremental_load",
        python_callable=run_incremental_load,
        op_kwargs={"ds": "{{ ds }}"},
    )

    t0 >> [t1, t2]
