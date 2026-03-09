from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.janis_utils import incremental_unixtime_load_table_s3
from utils.postgres_utils import get_max_updated_at_value
from utils.slack_utils import dag_success_slack, dag_failure_slack

import pendulum

def _proc_mfc_sustitucion(ts,ds):
        import requests
        


        query = f"""
            select orden, ref_id, unidades_pickeadas
            from operaciones_unimarc.found_rate_productos frp
            where id_tienda = '1917'
            and pickeador <> 'USUARIO  ORQUESTADOR MFC' and fecha_facturacion <= '{ts.split("+")[0]}'::date and fecha_facturacion > '{ts.split("+")[0]}'::date - interval '1 day' and unidades_pickeadas > 0
        """
        print(query)

        pg_hook = PostgresHook(conn_id="postgresql_conn")
        pg_connection = pg_hook.get_conn()
        cursor = pg_connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()

        print(f"Number of rows found: {len(results[0])}")
        if len(results[0]) == 0:
            print("No records found. Exit.")
            return
        
        MFC_API_SUST_URL = Variable.get("MFC_API_SUST_URL")
        MFC_API_SUST_USER = Variable.get("MFC_API_SUST_USER")
        MFC_API_SUST_PASS = Variable.get("MFC_API_SUST_PASS")

        headers = {
             "Content-Type": "application/json"
        }                

        for row in results:
            order = row[0]
            ref_id = row[1]
            quantity = row[2]
            unit_m = ref_id.split("-")[1]
            print(unit_m)
            if (unit_m == 'KG') or (unit_m == 'KGV'):
                quantity = quantity * 1000
                payload = {
                    "movimientoInventario": {
                        "quantityafter": 0,            
                        "quantitybefore": 0,            
                        "operation": "inc",             
                        "userid": "JN_mseguraa@smu.cl",    
                        "takeoffitemid": ref_id,   
                        "reason": "XX",                
                        "datetime": ts,      
                        "movementid": "",            
                        "quantity": int(quantity),                 
                        "referencedoc": str(order),     
                        "mfcid": "1917"
                    }
                    }
            else:
                    payload = {
                    "movimientoInventario": {
                        "quantityafter": 0,            
                        "quantitybefore": 0,            
                        "operation": "inc",             
                        "userid": "mseguraa@smu.cl",    
                        "takeoffitemid": ref_id,   
                        "reason": "JN",                
                        "datetime": ts,      
                        "movementid": "",            
                        "quantity": int(quantity),                 
                        "referencedoc": str(order),     
                        "mfcid": "1917"
                    }
                    }
            print(payload)
            response = requests.post(MFC_API_SUST_URL, json=payload, headers=headers, auth=(MFC_API_SUST_USER, MFC_API_SUST_PASS))
            print(response)
            print(response.text)

        return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'proc_mfc_sustitucion',
    default_args=default_args,
    description="Declaración de sustitución de productos de orden MFC realizada en Sala a través de API",
    schedule="0 1 * * *",
    start_date= pendulum.datetime(2023, 6, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["MFC", "API", "sustitucion", "foundrate", "Unimarc"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Declaración de sustitución de productos de orden MFC realizada en Sala a través de API
    """ 


    t0 = PythonOperator(
        task_id = "proc_mfc_sustitucion",
        python_callable = _proc_mfc_sustitucion
    )

    t0
