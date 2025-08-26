from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import ExternalTaskSensor

from utils.bigquery_utils import bigquery_full_table_load_to_s3

from datetime import datetime, timedelta

import pendulum



def _get_store_list_alvi():
    query = "SELECT id FROM ecommdata_alvi.tiendas"
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    print(results)
    cursor.close()
    pg_connection.close()
    return results

def _get_ou_key_list_alvi(ti, ts):
    import pandas as pd
    store_ids = ti.xcom_pull(key="return_value", task_ids=["get_store_id_list_from_workspace_alvi"])[0]
    store_ids = [store_id[0] for store_id in store_ids]

    execution_datetime = ts[:10].replace("-", "/")
    prefix = "data_warehouse/`cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_STORE`/"+execution_datetime+"/"
    print("Searching prefix: "+prefix)
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    store_object_list = s3_hook.list_keys(bucket_name=s3_bucket, prefix=prefix)
    print("Store object list: "+str(store_object_list))
    if len(store_object_list) == 0:
        print("There are no objects on the given prefix. Upstream tasks will be mark as Failed.")
    store_object_key = store_object_list[0]
    store_object = s3_hook.get_key(store_object_key, bucket_name=s3_bucket)
    df_stores = pd.read_csv(store_object.get()["Body"])

    df_stores = df_stores[df_stores["STORE_ID"].isin(store_ids)]
    ou_key_list = df_stores["OU_HEX"].to_list()
    ou_key_list_string = "(" + ",".join([f"'{str(ou_key)}'" for ou_key in ou_key_list]) + ")"
    ti.xcom_push(key="ou_key_list_alvi", value=ou_key_list_string)

    store_ou_key_list = list(zip(df_stores["OU_HEX"], df_stores["STORE_ID"]))

    return store_ou_key_list

def _create_final_costs_table_alvi(ti, ts):
    import pandas as pd
    import sqlalchemy

    execution_datetime = ts[:10].replace("-", "/")
    dw_sku_attr_file = f"data_warehouse/`cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_SKU_ATTR`/{execution_datetime}/"

    print("Searching prefix: " + dw_sku_attr_file)

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    dw_sku_attr_list = s3_hook.list_keys(bucket_name=s3_bucket, prefix=dw_sku_attr_file)

    print("SKU ATTR Object List: " + str(dw_sku_attr_list))
    if not dw_sku_attr_list:
        raise ValueError("No se encontraron archivos para SKU ATTR")

    # Obtener el primer archivo de la lista
    dw_sku_attr_key = dw_sku_attr_list[0]

    # Buscar los archivos generados en el día para las tablas respectivas
    dw_fact_ou_logt_file = ti.xcom_pull(key="return_value", task_ids=["netezza_vm_fact_ou_logt_smy_filtered_load_alvi"])[0]
    store_ou_key_list = ti.xcom_pull(key="return_value", task_ids=["get_ou_key_list_from_datawarehouse_alvi"])[0]
    df_store_ou_key = pd.DataFrame(store_ou_key_list, columns=["OU_HEX", "STORE_ID"])

    # Verifica si los archivos existen en S3
    if not s3_hook.check_for_key(dw_fact_ou_logt_file, bucket_name=s3_bucket):
        raise Exception(f"Key {dw_fact_ou_logt_file} does not exist.")
    if not s3_hook.check_for_key(dw_sku_attr_key, bucket_name=s3_bucket):
        raise Exception(f"Key {dw_sku_attr_key} does not exist in S3.")

    # Leer los archivos CSV desde S3
    dw_fact_ou_object = s3_hook.get_key(dw_fact_ou_logt_file, bucket_name=s3_bucket)
    df_dw_fact_ou = pd.read_csv(dw_fact_ou_object.get()["Body"])
    df_dw_fact_ou_store = pd.merge(df_dw_fact_ou, df_store_ou_key, on="OU_HEX", how="left")
    df_dw_fact_ou_store = df_dw_fact_ou_store[["DATE_VALUE", "ACTIVO", "CATALOGADO", "NBR_ITM_SOLD", "COGS", "SKU_KEY", "STORE_ID"]]

    dw_sku_attr_object = s3_hook.get_key(dw_sku_attr_key, bucket_name=s3_bucket)
    df_dw_sku_attr = pd.read_csv(dw_sku_attr_object.get()["Body"], dtype={"SKU_PRODUCT": "str"})
    df_dw_sku_attr = df_dw_sku_attr[["SKU_PRODUCT", "NM", "SKU_KEY"]]

    # Unir las tablas
    df = pd.merge(df_dw_fact_ou_store, df_dw_sku_attr, on="SKU_KEY", how="left")
    df = df.drop(columns=["SKU_KEY"])
    df = df.rename(columns={
        "DATE_VALUE": "fecha",
        "STORE_ID": "id_tienda",
        "SKU_PRODUCT": "material",
        "NM": "descripcion_material",
        "ACTIVO": "activo",
        "CATALOGADO": "catalogado",
        "NBR_ITM_SOLD": "unidades_vendidas",
        "COGS": "cogs"
    })

    print(df.dtypes)
    print(df.head(1))

    # Conexión a PostgreSQL
    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")

    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    # Guardar en PostgreSQL
    df.to_sql(
        name="costos",
        con=engine,
        schema="ecommdata_alvi",
        if_exists='append',
        index=False,
        chunksize=20000,
        method='multi'
    )

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
    'etl_costs_table_incremental_load_alvi',
    default_args=default_args,
    description="Extraction and transformation of costs data.",
    schedule_interval="30 8 * * *",
    start_date=pendulum.datetime(2025, 4, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["DATA", "DW", "S3", "workspace", "costos", "ALVI", "KEVIN"],
) as dag:

    dag.doc_md = """
    Extract costs data from Datawarehouse to consolidate
    a single costs table on Postgres workspace for ALVI.
    """ 
    t0 = ExternalTaskSensor(
        task_id="wait_costos_unimarc",
        external_dag_id='etl_costs_table_incremental_load',
        external_task_id=None,
        allowed_states=['success'],
        failed_states=['failed']
    )
    t1 = PythonOperator(
        task_id = "get_store_id_list_from_workspace_alvi",
        python_callable = _get_store_list_alvi
    )

    t2 = PythonOperator(
        task_id = "get_ou_key_list_from_datawarehouse_alvi",
        python_callable = _get_ou_key_list_alvi
    )

    t3 = PythonOperator(
        task_id = "netezza_vm_fact_ou_logt_smy_filtered_load_alvi",
        python_callable = bigquery_full_table_load_to_s3,
        op_kwargs = {"table_name": "`cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_OU_LOGT_SMY`",
                    "where": """ (NBR_ITM_SOLD > 0 OR COGS > 0)
                                AND OU_HEX IN {{ti.xcom_pull(key="ou_key_list_alvi", task_ids=["get_ou_key_list_from_datawarehouse_alvi"][0])}}
                                AND DATE_VALUE = DATE('{{execution_date.strftime('%Y-%m-%d')}}')  
                            """ ,
                    "extra_prefix": "ALVI"
                    },
        retries = 2,
        retry_delay = timedelta(minutes=1),
        execution_timeout = timedelta(minutes=60)
    )

    t4 = PythonOperator(
        task_id = "create_final_costs_table_alvi",
        python_callable = _create_final_costs_table_alvi
    )

    t0 >> t1 >> t2 >> t3 >> t4
