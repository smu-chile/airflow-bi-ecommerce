from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

import pendulum

def query_to_df(query):
    import pandas as pd
    print(query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn_prod")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query)
    column_names = [desc[0] for desc in cursor.description]
    results = cursor.fetchall()
    results = pd.DataFrame(results, columns=column_names)
    print(results.head(20))
    cursor.close()
    pg_connection.close()
    return results

def get_api_data(url, headers=None):
    import requests
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Lanza una excepción para códigos de error HTTP
        return response.json()  # Retorna los datos en formato JSON
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")  # Muestra el error HTTP
    except Exception as err:
        print(f"Other error occurred: {err}")  # Muestra cualquier otro error
    return None

def carga_tiendas_to_s3(ds):
    import pandas as pd
    import io
    from io import StringIO
    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"carga_tiendas_vtex/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    query_tiendas_producto = f"""select distinct concat(l.material,'-', l.umv) as ref_id, l.id_tienda, t.canal_venta_vtex , p.vtex_id 
                    from ecommdata.lista8 l 
                    left join ecommdata.productos p on p.ref_id = concat(l.material,'-', l.umv)
                    left join ecommdata.skus s on s.ref_id = concat(l.material,'-', l.umv)
                    left join ecommdata.tiendas t on t.id = l.id_tienda 
                    left join catalogo.productos_excluidos pe on l.material = pe.material and l.umv = pe.umv 
                    where t.status = 1
                    and p.ref_id is not null
                    and s.ref_id is not null
                    and pe.material is null
                    union
                    select pc.ref_id, pc.id_tienda, t.canal_venta_vtex , p.vtex_id 
                    from ecommdata.publicacion_catalogo pc
                    left join ecommdata.productos p on p.ref_id = pc.ref_id
                    left join ecommdata.tiendas t on t.id = pc.id_tienda 
                    where pc.fecha_hora = (select max(fecha_hora) from ecommdata.publicacion_catalogo pc2 where fecha_hora >= current_date)
                    and pc.id_tienda = '1917'
                    and pc.mfc is true
                    and pc.stock_janis > 0"""
    
    df = query_to_df(query_tiendas_producto)
    
    lista_ref_ids = df['vtex_id'].unique()

    for aux in lista_ref_ids:
        X_VTEX_API_AppKey = Variable.get("X_VTEX_API_AppKey")
        X_VTEX_API_AppToken = Variable.get("X_VTEX_API_AppToken")
        account_name = Variable.get("VTEX_ACCOUNT_NAME") 
        env = Variable.get("VTEX_ENV")

        header = {"X-VTEX-API-AppKey" : X_VTEX_API_AppKey, "X-VTEX-API-AppToken" : X_VTEX_API_AppToken}
        url = f"https://{account_name}.{env}.com.br/api/catalog/pvt/product/{str(int(aux))}/salespolicy"
        data = get_api_data(url, header)
        print(data)
        
    buffer = io.StringIO()
    df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    buffer.seek(0)

    filename = f"carga_tiendas_vtex/{exec_date}/carga_tiendas_{date_aux}.csv"

    print(f"con fecha {ds} y nombre de filename como {filename}")
    s3_hook.load_string(buffer.getvalue(),
                key=filename,
                bucket_name=s3_bucket,
                replace=True,
                encrypt=False)


    print("se logro transformar los dataframes a archivos .csv")
    print(f"File load on S3: {prefix}")

    return filename

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_carga_tiendas_vtex',
    default_args=default_args,
    description="Carga y elimina tradePolicy de tiendas a los productos en vte",
    schedule_interval="0 10 * * *",
    start_date=pendulum.datetime(2024, 7, 30, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "tiendas", "Productos", "ecommdata", "VTEX", "unimarc", "PATRICIO"],
) as dag:
    

    dag.doc_md = """
    Carga y elimina tradePolicy de tiendas a los productos en vtex\n
    guardar en S3.
    """ 

    t0 = PythonOperator(
        task_id = 'carga_tiendas_to_s3',
        python_callable=carga_tiendas_to_s3,
    )

    t0