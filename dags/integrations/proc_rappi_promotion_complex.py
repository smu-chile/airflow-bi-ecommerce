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
        
        join_file_name = f"integraciones/last_millers/stock/out/rappi/Complex/{exec_date}/{store_id}.json"
        if s3_hook.check_for_key(join_file_name, bucket_name=s3_bucket):
            print(f"File {join_file_name} already exists on bucket: {s3_bucket}. Skipping...")
            continue

        peya_stock_query = f"""
            SELECT DISTINCT  
                CAST(s.ou_id AS VARCHAR) AS store_id,
                TO_CHAR(WP.fecha_inicio_de_promocion, 'YYYY/MM/DD') AS start_date,
                TO_CHAR(WP.fecha_fin_de_promocion, 'YYYY/MM/DD') AS end_date,
                CASE
                    WHEN wp.desc_promocion IN ('COMBINACION NXM') THEN 
                        CONCAT('llevas ', CAST(wp.cantidad_n AS VARCHAR), ',Pague ', CAST(cantidad_m AS VARCHAR))
                    WHEN wp.desc_promocion IN ('COMBINACION NX$') THEN 
                        CONCAT('llevas ', CAST(wp.cantidad_n AS VARCHAR), 'x por ', CAST(ROUND(wp.precio_total_promocional, 0) AS VARCHAR))
                END AS description,
                CASE
                    WHEN wp.desc_promocion IN ('COMBINACION NXM') THEN 
                        CONCAT('L', CAST(wp.cantidad_n AS VARCHAR), '_P', CAST(cantidad_m AS VARCHAR))
                    WHEN wp.desc_promocion IN ('COMBINACION NX$') THEN 
                        CONCAT('T', CAST(FLOOR(((lspp.precio - (wp.precio_total_promocional / wp.cantidad_n)) / lspp.precio) * 100) AS VARCHAR), '_U', CAST(wp.cantidad_n AS VARCHAR))
                END AS type_format,
                wp.descripcion_material AS name,
                CAST((wp.material::int) AS VARCHAR) AS id
            FROM 
                ecommdata.workflow_promociones wp 
            LEFT JOIN 
                integraciones.stock s ON s.sku_product = wp.material 
            LEFT JOIN 
                integraciones.tiendas_last_millers tlm ON tlm.id = s.ou_id 
            LEFT JOIN 
                integraciones.productos p ON s.sku_key = p.sku_key AND p.ean = wp.ean
            LEFT JOIN 
                integraciones.lm_stock_precio_promo lspp ON lspp.ean = wp.ean AND lspp.id_tienda = s.ou_id
            WHERE 
                wp.fecha_inicio_de_promocion <= CURRENT_DATE
                AND wp.fecha_fin_de_promocion >= CURRENT_DATE
                AND tlm.id_rappi IS NOT NULL
                AND wp.tipo_promocion IN (2, 7)
                AND wp.registro_valido = TRUE
                AND wp.organizacion_ventas = '1000'
                AND wp.canal_distribucion = '10'
                AND wp.id_mecanica NOT IN (25, 27, 36, 37, 50, 51, 53, 67, 72, 77, 93, 99, 123, 124)
                AND wp.nombre_promocion::text !~~ '%MFC%'::text
                AND wp.nombre_promocion::text !~~ '%BANCO%'::text 
                AND wp.nombre_promocion::text !~~ '%UNIPAY%'::text
                AND wp.nombre_promocion::text !~~ '%TERCERA%'::text 
                AND wp.nombre_promocion::text !~~ '%917%'::text
                AND wp.nombre_promocion::text !~~ '%ESTADO%'::text
                AND wp.nombre_promocion::text !~~ '% LOC%'::text
                AND wp.nombre_promocion::text !~~ '%LIQ%'::text
                AND lspp.ean IS NOT null
                and wp.n_promocion  not in  ('5552392024','1120012024',
                '1120022024',
                '1120032024',
                '1120042024',
                '1120052024',
                '1120062024',
                '1120082024',
                '1120092024',
                '1120102024',
                '1120112024',
                '1120122024',
                '4000512024')
                and lspp.id_tienda = '{store_id}'
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
    prefix = f"integraciones/last_millers/stock/out/rappi/Complex/{exec_date}/"

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
    "proc_rappi_promotion_complex",
    default_args=default_args,
    description="Cruce de stock, precios y precios promocionales complejas para integracion Rappi",
    schedule_interval=None, 
    start_date=pendulum.datetime(2023, 2, 21, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    concurrency=2,
    tags=["OPS", "last_millers", "dw", "stock", "precios", "NICOLAS","PROMOTIONS"],
) as dag:

    dag.doc_md = """
        Dag creado para envio de promociones complejas a rappi.

        Este es un dag de testeo, en el cual se vera la integracion.
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
