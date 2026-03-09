from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
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
                    where l.material is null
                    and pt.id_tienda is not null"""
    print(ubi_mfc_query)
    pg_hook = PostgresHook(conn_id="postgresql_prod_conn")
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
    
    list_ref_id = ['000000000000651352-UN','000000000638605003-UN','000000000000647623-UN','000000000606949005-UN','000000000000181371-UN','000000000942367002-UN','000000000000197623-UN','000000000000569005-UN','000000000000651952-UN','000000000000614885-UN','000000000000646866-UN','000000000648540003-UN','000000000000890480-UN','000000000000621539-UN','000000000699230005-UN','000000000401676002-UN','000000000000615249-UN','000000000000647010-UN','000000000638340001-UN','000000000648540002-UN','000000000638588004-UN','000000000000639472-UN','000000000802977004-UN','000000000640169001-UN','000000000649114002-UN','000000000000357188-UN','000000000000651668-UN','000000000000357189-UN','000000000554622003-UN','000000000000622751-UN','000000000000662010-UN','000000000554622005-UN','000000000650103001-UN','000000000000647343-UN','000000000000649856-UN','000000000000651330-UN','000000000635113001-UN','000000000000338573-UN','000000000400217009-UN','000000000000118792-UN','000000000000216752-UN','000000000000725009-UN','000000000000850700-UN','000000000000651307-UN','000000000401676003-UN','000000000000053855-UN','000000000000630237-UN','000000000000631671-UN','000000000401805002-UN','000000000630621001-UN','000000000648275001-UN','000000000606945002-UN','000000000000008195-UN','000000000000139922-UN','000000000000849698-UN','000000000000907878-UN','000000000651653001-UN','000000000000137817-UN','000000000400159033-UN','000000000622768004-UN','000000000652332002-UN','000000000000197573-UN','000000000000632816-UN','000000000000651954-UN','000000000000654767-UN','000000000000776757-UN','000000000000939716-UN','000000000661457002-UN','000000000000007292-UN','000000000000171123-UN','000000000000776756-UN','000000000634627002-UN','000000000645890001-UN','000000000651963002-UN','000000000000605639-UN','000000000000650033-UN','000000000000687437-UN','000000000000776758-UN','000000000401793002-UN','000000000650812003-UN','000000000652546001-UN','000000000655121002-UN','000000000826973006-UN','000000000000133153-UN','000000000000337277-UN','000000000000560816-UN','000000000000563112-UN','000000000000653183-UN','000000000000658539-UN','000000000000980750-UN','000000000655121001-UN','000000000000010102-UN','000000000000339630-UN','000000000000652174-UN','000000000000653979-UN','000000000000662856-UN','000000000650026001-UN','000000000000177660-UN','000000000000614462-UN','000000000000629775-UN','000000000000640185-UN','000000000000649120-UN','000000000000650031-UN','000000000000655777-UN','000000000000661486-UN','000000000000662438-UN','000000000651988001-UN','000000000000646695-UN','000000000000655776-UN','000000000000638251-UN','000000000000639960-UN','000000000000195857-UN','000000000000209774-UN','000000000000638500-UN','000000000000639961-UN','000000000000651305-UN','000000000000651598-UN','000000000651153002-UN','000000000000138542-UN','000000000000625296-UN','000000000000630625-UN','000000000000631189-UN','000000000000649115-UN','000000000000652438-UN','000000000000035730-UN','000000000000613371-UN','000000000000653588-UN','000000000000776752-UN','000000000638588005-UN','000000000000333639-UN','000000000000789521-UN','000000000557923002-UN']
    df = pd.DataFrame(list_ref_id)
    df.columns = ["ref_id"]
    print(df)
    print("se han cargado los productos\n")

    base_url = Variable.get("JANIS_API_URL")

    url = f"{base_url}stock"

    JANIS_API_KEY = Variable.get("JANIS_API_KEY")
    JANIS_API_SECRET = Variable.get("JANIS_API_SECRET")
    JANIS_CLIENT = Variable.get("JANIS_CLIENT")

    headers = {
    "janis-api-key" : JANIS_API_KEY,
    "janis-api-secret" : JANIS_API_SECRET,
    "janis-client" : JANIS_CLIENT,
    "Connection" : "keep-alive"
    }

    payload=[]
    for i in range(len(df.index)):
        print(i)
        material = df.ref_id[i]
        id_tienda = "1917"
        row = {"IdSku": material, "Quantity": 0, "Store": id_tienda}
        print(row)
        payload.append(row)    
        if i % 499 == 0:
            payload = str(payload).replace("'", '"')
            print(payload)
            response = requests.request("POST", url, headers=headers, data=payload)
            print(response.text)
            payload = []
    print(payload)
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
    'proc_borrado_stock_MFC_skus_en_especificos',
    default_args=default_args,
    description="Borrado de stock janis alvi inicial.",
    schedule=None,
    start_date=pendulum.datetime(2023, 7, 12, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["DATA","stock", "janis", "MFC"],
) as dag:

    dag.doc_md = """
    Borrado de stock janis MFC, borra todo el stock de 132 skus del MFC"
    """ 
    t0 = PythonOperator(
        task_id = "_send_stock_0_to_janis_alvi",
        python_callable = _send_stock_0_to_janis_alvi
    )

    t0
