from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable

from utils.slack_utils import dag_success_slack, dag_failure_slack
from utils.postgres_utils import query_to_df

import pendulum

def _join_stock_and_promo_prices_from_s3(ds, ti):
    import json
    import pandas as pd

    rappi_store_ids = ['3512','3552','3540','3227','3580',
                       '3546','3547','3564','3579','3570',
                       '3036','3164','3506','3508','3541',
                       '3503','3501','3502','3530','3517',
                       '3538','3515','3544','3509','3545',
                       '3504','3548','3040','3520','3535',
                       '3554'] #cambiar por lista
    print(rappi_store_ids)

    exec_date = ds.replace("-", "/")

    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    for store_id in rappi_store_ids:
        print(f"Store id: {store_id}")
        
        join_file_name = f"integraciones/last_millers/stock/out_m10/rappi/{exec_date}/{store_id}.json"
        if s3_hook.check_for_key(join_file_name, bucket_name=s3_bucket):
            print(f"File {join_file_name} already exists on bucket: {s3_bucket}. Skipping...")
            continue

        peya_stock_query = f"""
            select lspp.id_tienda as store_id
                , case 
                    when (lspp.multiplicador_unidad > 1 and lspp.unidad_de_medida not IN ('KG', 'KGV')) then (lspp.material::int)::varchar || '_' || lspp.multiplicador_unidad
                    else (lspp.material::int)::varchar 
                end as id
                , case 
                    when lspp.unidad_de_medida IN ('KG', 'KGV') then lspp.stock_unitario::int
                    else (lspp.stock_unitario/lspp.multiplicador_unidad)::int
                end as stock
                , lspp.nombre as "name"
                , lspp.ean as ean 
                , lspp.precio as price 
                , least(lspp.precio_promocional, lspp.precio) as discount_price
                , lspp.marca as trademark 
                , case 
                    when lspp.unidad_de_medida in ('KG', 'KGV') then 'WW'
                else 'U'
                    end as sale_type
                from integraciones.lm_stock_precio_promo_10 lspp
                where lspp.id_tienda = '{store_id}';
        """
        df = query_to_df(peya_stock_query)
        if len(df) == 0:
            print(f"No records found for Store Id: {store_id}. Skipping...")
            continue
        print(f"Number of records found on stock: {len(df.index)}")

        df.columns = map(str.lower, df.columns)
        df["is_available"] = True

        dict_body = df.to_dict(orient="records")
        json_body = json.dumps(dict_body)

        s3_hook.load_string(json_body,
                    key=join_file_name,
                    bucket_name=s3_bucket,
                    replace=True,
                    encrypt=False)
        print(f"Tienda: {store_id} lista y se guarda en {join_file_name}")

    return

def _send_joined_data_to_api(ds):
    import json
    import requests

    exec_date = ds.replace("-", "/")
    prefix = f"integraciones/last_millers/stock/out_m10/rappi/{exec_date}/"

    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    s3_file_list = s3_hook.list_keys(s3_bucket, prefix=prefix)

    print(f"Number of files found: {len(s3_file_list)}")

    responses_prefix = f"rappi/api/stock_m10/post/full/responses/{exec_date}/"

    for stock_file in s3_file_list:
        print(stock_file)

        json_body_object = s3_hook.get_key(stock_file, bucket_name=s3_bucket)
        json_body_string = json_body_object.get()["Body"].read()
        json_body = json.loads(json_body_string)
        payload = {
            "records": json_body
        }

        print(f"Number of records found: {len(payload['records'])}")
        rappi_endpoint = Variable.get("RAPPI_ENDPOINT_M10")

        headres = {
            "api_key": Variable.get("RAPPI_API_KEY_M10"),
            "Content-Type": "application/json"
        }
        response = requests.post(url=rappi_endpoint, json=payload, headers=headres)
        print(response.status_code)
        try:
            response_json = response.json()
            response_string = json.dumps(response_json)
            s3_hook.load_string(response_string,
                  key=responses_prefix+stock_file.split("/")[-1],
                  bucket_name=s3_bucket,
                  replace=True,
                  encrypt=False)
            print("Response body saved to S3.")
        except Exception as e:
            print(e)
            print("Error on response.")
            break
    return

default_args = {
    "owner": "ecommerce_ops",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    "proc_rappi_stock_integration_m10",
    default_args=default_args,
    description="Cruce de stock, precios y precios promocionales simples para integracion Rappi x M10",
    schedule=None, 
    start_date=pendulum.datetime(2024, 6, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,

    tags=["OPS", "last_millers", "dw", "stock", "precios", "NICOLAS","PATRICIO","M10"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Cruce de stock, precios y precios promocionales simples para integración con Last Millers M10: **Rappi**. \n
    * Se obtiene listado de tiendas activas para la integración Rappi m10 (`registros con id_rappi NOT NULL de la tabla integraciones.tiendas_last_millers`). \n
    * A partir de esta lista, se obtiene listado de archivos CSV de stock + precio para cada una de las tiendas activas en **Rappi**. \n
    * Desde **S3** se extrae archivo CSV de precios modales. \n
    * Para cada tienda activa, se cruzan los archivos de stock + precio promo y el de precios modales, se les da formato correspondiente para luego
    ser almacenados en **S3**. 
    * En este caso, el formato de integración de los archivos es JSON con la siguiente estructura: \n
    ```
        {
            "store_id": "0469",
            "id": "28611",
            "stock": 2,
            "name": "Punta ganso vacuno Nacional al vacío 1.5 Kg",
            "ean": "2528611000007",
            "price": 16199,
            "discount_price": 16199,
            "trademark": "NACIONAL",
            "sale_type": "WW",
            "is_available": true
        }
    ```
    * Finalmente, se itera sobre los archivos generados, enviándolos en el body de una POST request al endpoint: **https://services.grability.rappi.com/api/cpgs-integration/datasets**.
    Este DAG depende del DAG: [ **proc_stock_last_millers** ].
    """ 

    t0 = PythonOperator(
        task_id = "join_stock_and_promo_prices_from_s3",
        python_callable = _join_stock_and_promo_prices_from_s3
    )

    t1 = PythonOperator(
        task_id = "send_joined_data_to_api",
        python_callable = _send_joined_data_to_api
    )

    t0 >> t1
