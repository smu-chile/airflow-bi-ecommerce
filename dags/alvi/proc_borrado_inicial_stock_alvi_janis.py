from airflow import DAG
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import pendulum


from datetime import datetime, timedelta

def no_lista8():
    import pandas as pd
    ubi_mfc_query = """select LEFT(pt.ref_id, 18) as material, pt.id_tienda
                    from ecommdata_alvi.productos_tienda pt
                    left join ecommdata_alvi.lista8 l 
                    on pt.ref_id = l.material ||'-'||l.umv  and pt.id_tienda = l.id_tienda 
                    where l.material is null"""
    print(ubi_mfc_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(ubi_mfc_query)
    results = cursor.fetchall()
    results=pd.DataFrame(results)
    print(results)
    results.columns = ["material","id_tienda"]
    cursor.close()
    pg_connection.close()
    return results

def _send_stock_0_to_janis_alvi():
    import requests
    import pandas as pd
    
    df = no_lista8()
    print("se han cargado los productos\n")

    base_url = Variable.get("JANIS_API_URL")

    url = f"{base_url}stock"

    JANIS_ALVI_API_KEY = Variable.get("JANIS_ALVI_API_KEY")
    JANIS_ALVI_API_SECRET = Variable.get("JANIS_ALVI_API_SECRET")
    JANIS_ALVI_CLIENT = Variable.get("JANIS_ALVI_CLIENT")

    headers = {
    "janis-api-key" : JANIS_ALVI_API_KEY,
    "janis-api-secret" : JANIS_ALVI_API_SECRET,
    "janis-client" : JANIS_ALVI_CLIENT,
    "Connection" : "keep-alive"
    }

    payload=[]
    for i in range(len(df.index)):
        print(i)
        material = df.material
        store = df.id_tienda
        row = {"IdSku": material, "Quantity": 0, "Store": store}
        print(row)
        payload.append(row)    
        if i % 499 == 0:
            payload = str(payload).replace("'", '"')
            response = requests.request("POST", url, headers=headers, data=payload)
            print(response.text)
            payload = []
    payload = str(payload).replace("'", '"')
    response = requests.request("POST", url, headers=headers, data=payload)
    print(response.text)


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0
}

with DAG(
    'proc_borrado_stock_janis_alvi_init',
    default_args=default_args,
    description="Borrado de stock janis alvi inicial.",
    schedule_interval="0 10 * * *",
    start_date=pendulum.datetime(2023, 7, 4, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["DATA", "SAP", "ecommdata_alvi", "lista8", "stock", "janis", "alvi"],
) as dag:

    dag.doc_md = """
    Borrado de stock janis alvi inicial, borra todo el stock de janis alvi para los productos que no se encuentren en lista8 alvi"
    """ 
    t0 = PythonOperator(
        task_id = "_send_stock_0_to_janis_alvi",
        python_callable = _send_stock_0_to_janis_alvi
    )

    t0
