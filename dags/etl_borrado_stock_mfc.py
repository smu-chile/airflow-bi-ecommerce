from airflow import DAG
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import pendulum


from datetime import datetime, timedelta

def ubicaciones_mfc():
    import pandas as pd
    ubi_mfc_query = """select CONCAT(LPAD(sap_code, 18, '0'), '-', measurement_unit) as ref_id,
                    mfc_is_item_side
                    from ecommdata.ubicacion_mfc
                    where mfc_is_item_side = 'FLO'"""
    print(ubi_mfc_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(ubi_mfc_query)
    results = cursor.fetchall()
    results=pd.DataFrame(results)
    print(results)
    results.columns = ["ref_id","side"]
    cursor.close()
    pg_connection.close()
    return results

def lista8_0917():
    import pandas as pd
    lista8_0917_query = """select material ||'-' ||umv, id_tienda
                    from ecommdata.lista8_lite
                    where id_tienda = '0917'"""
    print(lista8_0917_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(lista8_0917_query)
    results = cursor.fetchall()
    results=pd.DataFrame(results)
    print(results)
    results.columns = ["ref_id","id_tienda"]
    cursor.close()
    pg_connection.close()
    return results




def load_to_s3():
    #xd
    return

def load_to_janis():

    return


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0
}

with DAG(
    'etl_borrado_stock_janis_alvi',
    default_args=default_args,
    description="Borrado de stock janis alvi en base a productos removidos de lista8.",
    schedule_interval="0 10 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["DATA", "SAP", "ecommdata_alvi", "lista8", "stock", "janis", "alvi"],
) as dag:

    dag.doc_md = """
    Borrado de stock janis alvi en base a productos removidos de lista8."
    """ 
    t0 = PythonOperator(
        task_id = "load_to_s3",
        python_callable = load_to_s3
    )
    t1 = PythonOperator(
        task_id = "load_to_janis",
        python_callable = load_to_janis
    )
    t0 >> t1