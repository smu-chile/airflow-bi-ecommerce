from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

import pendulum

def _get_uber_active_stores():
    uber_stores_query = """
        SELECT id
        FROM integraciones.tiendas_last_millers
        WHERE id_uber is not NULL;
    """
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(uber_stores_query)
    results = cursor.fetchall()
    cursor.close()
    pg_connection.close()
    return results

def _join_stock_and_promo_prices_from_s3(ds, ti):
    import json
    import pandas as pd

    uber_stores = ti.xcom_pull(key="return_value", task_ids=["get_uber_active_stores"])[0]
    uber_store_ids = [uber_store_id[0] for uber_store_id in uber_stores]
    print(uber_store_ids)

    exec_date = ds.replace("-", "/")

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()

    for store_id in uber_store_ids:
        print(f"Store id: {store_id}")
        
        join_file_name = f"integraciones/last_millers/stock/out/uber/{exec_date}/{store_id}.json"
        if s3_hook.check_for_key(join_file_name, bucket_name=s3_bucket):
            print(f"File {join_file_name} already exists on bucket: {s3_bucket}. Skipping...")
            continue

        uber_stock_query = f"""
            SELECT
                lspp.ean as Sku,
                lspp.id_tienda as id_de_tienda,
                'Descuento' as tipo_de_promoción,
                CASE
                    WHEN  lspp.unidad_de_medida NOT IN ('KG', 'KGV') THEN round(LEAST(lspp.precio, lspp.precio_promocional))
                    ELSE round(LEAST(lspp.precio, lspp.precio_promocional) * s.multiplicador_unidad_medida)
                END AS precio_venta ,
                null as fecha_inicio_venta,
                null as fecha_final_venta,
                null as cantidad_compra,
                CASE
                    WHEN  lspp.unidad_de_medida NOT IN ('KG', 'KGV') THEN (lspp.precio - round(LEAST(lspp.precio, lspp.precio_promocional)))
                    else (lspp.precio - round(LEAST(lspp.precio, lspp.precio_promocional) * s.multiplicador_unidad_medida))
                END as cantidad_descuento
            FROM integraciones.lm_stock_precio_promo lspp
            INNER JOIN ecommdata.skus s ON s.ref_id = CONCAT(lspp.material, '-', lspp.unidad_de_medida)
            WHERE lspp.id_tienda = '{store_id}'
        """

        cursor.execute(uber_stock_query)
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

def _send_joined_data_to_sftp(ds):
    import os
    import pysftp

    ftp_host = Variable.get("UBER_SFTP_HOST")
    ftp_port = 22
    ftp_user = Variable.get("UBER_SFTP_USER")
    ftp_password = Variable.get("UBER_SFTP_PASSWORD")


    exec_date = ds.replace("-", "/")
    prefix = f"integraciones/last_millers/stock/out/uber/{exec_date}/"

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    s3_file_list = s3_hook.list_keys(s3_bucket, prefix=prefix)

    print(f"Number of files found: {len(s3_file_list)}")
    

    for stock_file in s3_file_list:
        print(stock_file)

        stock_object = s3_hook.get_key(stock_file, bucket_name=s3_bucket)
        stock_object_body = stock_object.get()["Body"]

        output_stock_file = stock_file.split("/")[-1]
        print(f"File to load to SFTP Server: {output_stock_file}")
        '''
        with pysftp.Connection(host=ftp_host, 
                                username=ftp_user, 
                                port=ftp_port, 
                                password=ftp_password) as sftp:
            localFile = stock_object_body
            remotePath = f"/data/{output_stock_file}"
            sftp.putfo(localFile, remotePath)
        
        print("File loaded.")
        '''
    return

default_args = {
    "owner": "ecommerce_ops",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    "proc_uber_stock_integration",
    default_args=default_args,
    description="Cruce de stock, precios y precios promocionales simples para integracion Uber",
    schedule_interval=None, 
    start_date=pendulum.datetime(2023, 2, 21, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    concurrency=2,
    tags=["OPS", "last_millers", "dw", "stock", "precios"],
) as dag:

    dag.doc_md = """
    Cruce de stock, precios y precios promocionales simples para integración con Last Millers: **Uber**. \n
    * Se obtiene listado de tiendas activas para la integración PEYA (`registros con id_uber NOT NULL de la tabla integraciones.tiendas_last_millers`). \n
    * A partir de esta lista, se obtiene listado de archivos CSV de stock + precio para cada una de las tiendas activas en **PEYA**. \n
    * Desde **S3** se extrae archivo CSV de precios modales. \n
    * Para cada tienda activa, se cruzan los archivos de stock + precio promo y el de precios modales, se les da formato correspondiente para luego
    ser almacenados en **S3**. 
    * En este caso, el formato de integración de los archivos es CSV con las columnas [**SKU**, **PRECIO**, **STOCK**], donde **SKU** corresponde al ean interno
    del producto, **PRECIO** es el menor valor entre precio modal y precio promocional y **STOCK** es un valor binario, donde 0 se asigna a aquellos
    productos con stock menor a 7 unidades, y 1 a aquellos productos con 7 o más unidades. \n
    * Finalmente, se itera sobre los archivos generados, dejando cada uno de estos en el servidor SFTP de Uber.
    Este DAG depende del DAG: [ **proc_stock_last_millers** ].
    """ 

    t0 = PythonOperator(
        task_id = "get_uber_active_stores",
        python_callable = _get_uber_active_stores
    )

    t1 = PythonOperator(
        task_id = "join_stock_and_promo_prices_from_s3",
        python_callable = _join_stock_and_promo_prices_from_s3
    )

    t2 = PythonOperator(
        task_id = "send_joined_data",
        python_callable = _send_joined_data_to_sftp
    )

    t0 >> t1
    t1 >> t2
