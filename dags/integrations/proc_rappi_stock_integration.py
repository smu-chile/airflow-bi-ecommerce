from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

import pendulum

def _get_rappi_active_stores():
    peya_stores_query = """
        SELECT id
        FROM integraciones.tiendas_last_millers
        WHERE id_rappi is not NULL;
    """
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(peya_stores_query)
    results = cursor.fetchall()
    cursor.close()
    pg_connection.close()
    return results

def _join_stock_and_promo_prices_from_s3(ds, ti):
    import json
    import pandas as pd

    rappi_stores = ti.xcom_pull(key="return_value", task_ids=["get_rappi_active_stores"])[0]
    rappi_store_ids = [rappi_store_id[0] for rappi_store_id in rappi_stores]
    print(rappi_store_ids)

    exec_date = ds.replace("-", "/")

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()

    for store_id in rappi_store_ids:
        print(f"Store id: {store_id}")
        
        join_file_name = f"integraciones/last_millers/stock/out/rappi/{exec_date}/{store_id}.json"
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
                end as sale_type,
                s.multiplicador_unidad_medida as quantity,
                case
	                when lspp.nombre LIKE '%Kg%' OR lspp.nombre LIKE '%g%' or lspp.nombre LIKE '%KG%' or lspp.nombre LIKE '%GR%' THEN 'g'
                	ELSE 'ml'
                end as unit_type,
               concat('https://unimarc.vteximg.com.br', is2.imagen) as imagen
            from integraciones.lm_stock_precio_promo lspp
            left join ecommdata.skus s on s.ref_id  = concat(lspp.material,'-',lspp.unidad_de_medida)
            left join ecommdata.imagenes_sku is2 on is2.ref_id  = concat(lspp.material,'-',lspp.unidad_de_medida)
            where lspp.id_tienda = '{store_id}';
        ;
        """

        cursor.execute(peya_stock_query)
        results = cursor.fetchall()
        columns = [i[0] for i in cursor.description]

        if len(results) == 0:
            print(f"No records found for Store Id: {store_id}. Skipping...")
            continue

        df = pd.DataFrame(results, columns=columns)
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

    cursor.close()
    pg_connection.close()
    return

def _send_joined_data_to_api(ds):
    import json
    import requests

    exec_date = ds.replace("-", "/")
    prefix = f"integraciones/last_millers/stock/out/rappi/{exec_date}/"

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    s3_file_list = s3_hook.list_keys(s3_bucket, prefix=prefix)

    print(f"Number of files found: {len(s3_file_list)}")

    responses_prefix = f"rappi/api/stock/post/full/responses/{exec_date}/"

    for stock_file in s3_file_list:
        print(stock_file)

        json_body_object = s3_hook.get_key(stock_file, bucket_name=s3_bucket)
        json_body_string = json_body_object.get()["Body"].read()
        json_body = json.loads(json_body_string)
        payload = {
            "records": json_body
        }

        print(f"Number of records found: {len(payload['records'])}")
        rappi_endpoint = "https://services.grability.rappi.com/api/cpgs-integration/datasets"

        headres = {
            "api_key": Variable.get("RAPPI_API_KEY"),
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
    "proc_rappi_stock_integration",
    default_args=default_args,
    description="Cruce de stock, precios y precios promocionales simples para integracion Rappi",
    schedule_interval=None, 
    start_date=pendulum.datetime(2023, 2, 21, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    concurrency=2,
    tags=["OPS", "last_millers", "dw", "stock", "precios", "NICOLAS"],
) as dag:

    dag.doc_md = """
    Cruce de stock, precios y precios promocionales simples para integración con Last Millers: **Rappi**. \n
    * Se obtiene listado de tiendas activas para la integración Rappi (`registros con id_rappi NOT NULL de la tabla integraciones.tiendas_last_millers`). \n
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
        task_id = "get_rappi_active_stores",
        python_callable = _get_rappi_active_stores
    )

    t1 = PythonOperator(
        task_id = "join_stock_and_promo_prices_from_s3",
        python_callable = _join_stock_and_promo_prices_from_s3
    )

    t2 = PythonOperator(
        task_id = "send_joined_data_to_api",
        python_callable = _send_joined_data_to_api
    )

    t0 >> t1 >> t2
