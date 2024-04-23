from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

import pendulum

def _get_peya_active_stores():
    peya_stores_query = """
        SELECT id, id_peya
        FROM integraciones.tiendas_last_millers
        WHERE id_peya is not NULL;
    """
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(peya_stores_query)
    results = cursor.fetchall()
    cursor.close()
    pg_connection.close()
    return results

def _get_peya_botilleria_active_stores():
    peya_stores_query = """
        SELECT id, id_peya_botilleria
        FROM integraciones.tiendas_last_millers
        WHERE id_peya_botilleria is not NULL;
    """
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(peya_stores_query)
    results = cursor.fetchall()
    cursor.close()
    pg_connection.close()
    return results

def _get_peya_market_active_stores():
    peya_stores_query = """
        SELECT id, peya_market
        FROM integraciones.tiendas_last_millers
        WHERE peya_market is not NULL;
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
    import io
    import pandas as pd

    exec_date = ds.replace("-", "/")

    peya_stores = ti.xcom_pull(key="return_value", task_ids=["get_peya_active_stores"])[0]
    peya_store_ids = dict([(peya_store_id[0], peya_store_id[1]) for peya_store_id in peya_stores])
    print(peya_store_ids)

    peya_botilleria_stores = ti.xcom_pull(key="return_value", task_ids=["get_peya_botilleria_active_stores"])[0]
    peya_botilleria_store_ids = dict([(peya_store_id[0], peya_store_id[1]) for peya_store_id in peya_botilleria_stores])
    print(f"Botilleria: {peya_botilleria_store_ids}")
    
    peya_market_stores = ti.xcom_pull(key="return_value", task_ids=["get_peya_market_active_stores"])[0]
    peya_market_store_ids = dict([(peya_store_id[0], peya_store_id[1]) for peya_store_id in peya_market_stores])
    print(f"Market: {peya_market_store_ids}")

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()

    for store_id in peya_store_ids.keys():
        print(f"PEYA id: {peya_store_ids[store_id]}")
        join_file_name = f"integraciones/last_millers/stock/out/peya/{exec_date}/{peya_store_ids[store_id]}.csv"
        if s3_hook.check_for_key(join_file_name, bucket_name=s3_bucket):
            print(f"File {join_file_name} already exists on bucket: {s3_bucket}. Skipping...")
            continue
        
        peya_stock_query = f"""
        SELECT	
            NULL AS barcode,
            lspp.ean AS sku,
            CASE
                WHEN lspp.unidad_de_medida NOT IN ('KG', 'KGV') THEN ROUND(lspp.precio)
                when lspp.unidad_de_medida in ('KG','KGV') and s.multiplicador_unidad_medida = '0.1' then ROUND((lspp.precio) * 0.25)
                ELSE ROUND((lspp.precio) * s.multiplicador_unidad_medida)
            END AS price,
            CASE
                WHEN (lspp.unidad_de_medida NOT IN ('KG', 'KGV') AND (lspp.stock_unitario / lspp.multiplicador_unidad) >= COALESCE(ssp.stock_seguridad, 2)) THEN 1
                WHEN (lspp.unidad_de_medida IN ('KG', 'KGV') AND lspp.stock_unitario >= COALESCE(ssp.stock_seguridad, 2)) THEN 1
                ELSE 0
            END AS active
            FROM integraciones.lm_stock_precio_promo lspp
            INNER JOIN integraciones.tiendas_last_millers tlm ON lspp.id_tienda = tlm.id
            INNER JOIN ecommdata.skus s ON s.ref_id = CONCAT(lspp.material, '-', lspp.unidad_de_medida)
            LEFT JOIN integraciones.stock_seguridad_peya ssp ON ssp.ref_id  = CONCAT(lspp.material, '-', lspp.unidad_de_medida) AND lspp.id_tienda = ssp.id_tienda
            WHERE lspp.id_tienda = '{store_id}'
        """
         #AND lspp.id_tienda = '0755' 
        #AND lspp.id_tienda = '{store_id}'

        cursor.execute(peya_stock_query)
        results = cursor.fetchall()
        columns = [i[0] for i in cursor.description]
        print(columns)
        
        if len(results) == 0:
            print(f"No records found for Store Id: {store_id}")
            continue

        df = pd.DataFrame(results, columns=columns)
        print(f"Number of records found on stock: {len(df.index)}")

        df.columns = map(str.upper, df.columns)
        #df["SKU"] = df["SKU"].astype("int64")
        
        prev_exec_date = macros.ds_add(ds, -1).replace("-","/")
        prev_join_file_name = f"integraciones/last_millers/stock/out/peya/{prev_exec_date}/{peya_store_ids[store_id]}.csv"
        print(f"Checking for previous executions on {prev_join_file_name}.")
        if s3_hook.check_for_key(prev_join_file_name, bucket_name=s3_bucket):
            print(f"Looking for missing products from previous execution on file {prev_join_file_name}.")

            prev_stock_file = s3_hook.get_key(prev_join_file_name, bucket_name=s3_bucket)
            df_prev = pd.read_csv(prev_stock_file.get()["Body"])

            df_prev = df_prev[~df_prev["SKU"].isin(df["SKU"])]
            df_prev = df_prev[df_prev["SKU"]==1]
            df_prev["SKU"] = 0

            print(f"Adding {len(df_prev.index)} missing products as inactive: STOCK = 0.")

            df = pd.concat([df, df_prev])
            
        print(f"Total number of records: {len(df.index)}.")

        buffer = io.StringIO()
        df.to_csv(buffer, header=True, index=False, encoding="utf-8")
        buffer.seek(0)

        s3_hook.load_string(buffer.getvalue(),
                    key=join_file_name,
                    bucket_name=s3_bucket,
                    replace=True,
                    encrypt=False)
        print(f"File load on S3: {join_file_name}")
        
        if peya_botilleria_store_ids.get(store_id, False):
            join_file_name = f"integraciones/last_millers/stock/out/peya/{exec_date}/{peya_botilleria_store_ids[store_id]}.csv"
            if s3_hook.check_for_key(join_file_name, bucket_name=s3_bucket):
                print(f"File {join_file_name} already exists on bucket: {s3_bucket}. Skipping...")
                continue
            s3_hook.load_string(buffer.getvalue(),
                    key=join_file_name,
                    bucket_name=s3_bucket,
                    replace=True,
                    encrypt=False)
            print(f"File load on S3: {join_file_name}")
            
        if peya_market_store_ids.get(store_id, False):
            join_file_name = f"integraciones/last_millers/stock/out/peya/{exec_date}/{peya_market_store_ids[store_id]}.csv"
            if s3_hook.check_for_key(join_file_name, bucket_name=s3_bucket):
                print(f"File {join_file_name} already exists on bucket: {s3_bucket}. Skipping...")
                continue
            s3_hook.load_string(buffer.getvalue(),
                    key=join_file_name,
                    bucket_name=s3_bucket,
                    replace=True,
                    encrypt=False)
            print(f"File load on S3: {join_file_name}")
        #Aqui va la nueva logica
        join_file_name = f"integraciones/last_millers/promotions/out/peya/{exec_date}/{peya_store_ids[store_id]}.csv"
        if s3_hook.check_for_key(join_file_name, bucket_name=s3_bucket):
            print(f"File {join_file_name} already exists on bucket: {s3_bucket}. Skipping...")
            continue
        
        peya_stock_query = f"""
             SELECT DISTINCT
                null AS barcode,
                lspp.ean AS sku,
                'Promociones' AS campaign_name,
                'PedidosYa' AS reason,
                concat(current_date ,' 10:00:00-03:00') AS start_date,
                concat(current_date + 1,' 11:00:00-03:00') AS end_date,
                CASE
    				WHEN lspp.unidad_de_medida NOT IN ('KG', 'KGV') THEN ROUND(lspp.precio_promocional)
                    when lspp.unidad_de_medida in ('KG','KGV') and s.multiplicador_unidad_medida = '0.1' then ROUND((lspp.precio_promocional) * 0.25)
    				ELSE ROUND(lspp.precio_promocional * (s.multiplicador_unidad_medida))
				END AS discounted_price,
                --s.multiplicador_unidad_medida,
                999 AS max_no_of_orders,
                1 AS campaign_status
                FROM integraciones.lm_stock_precio_promo lspp
                INNER JOIN ecommdata.skus s ON s.ref_id = CONCAT(lspp.material, '-', lspp.unidad_de_medida)
                WHERE (lspp.unidad_de_medida IN ('KG', 'KGV') OR
                    (lspp.unidad_de_medida NOT IN ('KG', 'KGV') AND (lspp.stock_unitario / lspp.multiplicador_unidad) >= 7))
                and lspp.precio_promocional  is not null
                AND lspp.id_tienda = '{store_id}'
                GROUP BY
                lspp.ean,
                lspp.nombre,
                lspp.precio_promocional ,
                s.multiplicador_unidad_medida,
                lspp.unidad_de_medida,
                CASE
                    WHEN lspp.unidad_de_medida NOT IN ('KG', 'KGV') THEN ROUND(LEAST(lspp.precio, lspp.precio_promocional))
                    ELSE ROUND(LEAST(lspp.precio, lspp.precio_promocional) * s.multiplicador_unidad_medida)
                end;
        """
        #AND lspp.id_tienda = '0755'
        #AND lspp.id_tienda = '{store_id}'
        cursor.execute(peya_stock_query)
        results = cursor.fetchall()
        columns = [i[0] for i in cursor.description]

        if len(results) == 0:
            print(f"No records found for Store Id: {store_id}")
            continue

        df = pd.DataFrame(results, columns=columns)
        print(f"Number of records found on stock: {len(df.index)}")

        df.columns = map(str.upper, df.columns)
        #df["SKU"] = df["SKU"].astype("int64")
        
        prev_exec_date = macros.ds_add(ds, -1).replace("-","/")
        prev_join_file_name = f"integraciones/last_millers/promotions/out/peya/{prev_exec_date}/{peya_store_ids[store_id]}.csv"
        print(f"Checking for previous executions on {prev_join_file_name}.")
            
        print(f"Total number of records: {len(df.index)}.")

        buffer = io.StringIO()
        df.to_csv(buffer, header=True, index=False, encoding="utf-8")
        buffer.seek(0)

        s3_hook.load_string(buffer.getvalue(),
                    key=join_file_name,
                    bucket_name=s3_bucket,
                    replace=True,
                    encrypt=False)
        print(f"File load on S3: {join_file_name}")
        
        if peya_botilleria_store_ids.get(store_id, False):
            join_file_name = f"integraciones/last_millers/promotions/out/peya/{exec_date}/{peya_botilleria_store_ids[store_id]}.csv"
            if s3_hook.check_for_key(join_file_name, bucket_name=s3_bucket):
                print(f"File {join_file_name} already exists on bucket: {s3_bucket}. Skipping...")
                continue
            s3_hook.load_string(buffer.getvalue(),
                    key=join_file_name,
                    bucket_name=s3_bucket,
                    replace=True,
                    encrypt=False)
            print(f"File load on S3: {join_file_name}")
            
        if peya_market_store_ids.get(store_id, False):
            join_file_name = f"integraciones/last_millers/promotions/out/peya/{exec_date}/{peya_market_store_ids[store_id]}.csv"
            if s3_hook.check_for_key(join_file_name, bucket_name=s3_bucket):
                print(f"File {join_file_name} already exists on bucket: {s3_bucket}. Skipping...")
                continue
            s3_hook.load_string(buffer.getvalue(),
                    key=join_file_name,
                    bucket_name=s3_bucket,
                    replace=True,
                    encrypt=False)
            print(f"File load on S3: {join_file_name}")
        #en el for agregar en la parte que comienzo a sacar la nueva query por tienda y lo guarda en nuestro s3
        #
        #guardarla en promociones out peya 
    
    cursor.close()
    pg_connection.close()
    return
#a

def _send_joined_data_to_stfp(ds):
    import os
    import pysftp

    ftp_host = Variable.get("NEW_PEYA_SFTP_HOST")
    ftp_port = 22
    ftp_user = Variable.get("NEW_PEYA_SFTP_USER")
    ftp_rsa_key = Variable.get("NEW_PEYA_SFTP_PASSWORD")

    exec_date = ds.replace("-", "/")
    prefix = f"integraciones/last_millers/stock/out/peya/{exec_date}/"
     #Crear un prefix para promo
    prefix2 = f"integraciones/last_millers/promotions/out/peya/{exec_date}/"
    
   
    
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    s3_file_list = s3_hook.list_keys(s3_bucket, prefix=prefix)
    s3_file_list2 = s3_hook.list_keys(s3_bucket, prefix=prefix2)
    

    print(f"Number of files found: {len(s3_file_list)}")

    for stock_file in s3_file_list:
        print(stock_file)

        stock_object = s3_hook.get_key(stock_file, bucket_name=s3_bucket)
        stock_object_body = stock_object.get()["Body"]

        output_stock_file = stock_file.split("/")[-1]
        print(f"File to load to SFTP Server: {output_stock_file}")

        with pysftp.Connection(host=ftp_host, 
                                username=ftp_user, 
                                port=ftp_port, 
                                password=ftp_rsa_key) as sftp:
            localFile = stock_object_body
            remotePath = f"/vendor-automation-sftp-storage-live-us-1/home/PY_CL_1fff4594-d35e-44ad-af7e-1f7d663d60de/catalog/SMUCatalog_{output_stock_file}"
            sftp.putfo(localFile, remotePath)
        
        print("File loaded.")

    #Crear for para promo
    for promo_file in s3_file_list2:
        print(promo_file)

        stock_object = s3_hook.get_key(promo_file, bucket_name=s3_bucket)
        stock_object_body = stock_object.get()["Body"]

        output_promo_file = promo_file.split("/")[-1]
        print(f"File to load to SFTP Server: {output_promo_file}")

        with pysftp.Connection(host=ftp_host, 
                                username=ftp_user, 
                                port=ftp_port, 
                                password=ftp_rsa_key) as sftp:
            localFile = stock_object_body
            remotePath = f"/vendor-automation-sftp-storage-live-us-1/home/PY_CL_1fff4594-d35e-44ad-af7e-1f7d663d60de/promotions/SMU_{output_promo_file}"
            sftp.putfo(localFile, remotePath)
        
        print("File loaded.")

    return

default_args = {
    "owner": "ecommerce_ops",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    "proc_peya_stock_integration",
    default_args=default_args,
    description="Cruce de stock, precios y precios promocionales simples para integracion Pedidos Ya",
    schedule_interval=None, 
    start_date=pendulum.datetime(2023, 2, 21, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    concurrency=2,
    tags=["OPS", "last_millers", "dw", "stock", "precios", "NICOLAS"],
) as dag:

    dag.doc_md = """
    Cruce de stock, precios y precios promocionales simples para integración con Last Millers: **Pedidos Ya**. \n
    * Se obtiene listado de tiendas activas para la integración PEYA (`registros con id_peya NOT NULL de la tabla integraciones.tiendas_last_millers`). \n
    * A partir de esta lista, se obtiene listado de archivos CSV de stock + precio para cada una de las tiendas activas en **PEYA**. \n
    * Desde **S3** se extrae archivo CSV de precios modales. \n
    * Para cada tienda activa, se cruzan los archivos de stock + precio promo y el de precios modales, se les da formato correspondiente para luego
    ser almacenados en **S3**. 
    * En este caso, el formato de integración de los archivos es CSV con las columnas [****, **PRECIO**, **STOCK**], donde **** corresponde al ean interno
    del producto, **PRECIO** es el menor valor entre precio modal y precio promocional y **STOCK** es un valor binario, donde 0 se asigna a aquellos
    productos con stock menor a 7 unidades, y 1 a aquellos productos con 7 o más unidades. \n
    * Finalmente, se itera sobre los archivos generados, dejando cada uno de estos en el servidor SFTP de Pedidos Ya.
    Este DAG depende del DAG: [ **proc_stock_last_millers** ].
    """ 

    t0 = PythonOperator(
        task_id = "get_peya_active_stores",
        python_callable = _get_peya_active_stores
    )

    t1 = PythonOperator(
        task_id = "get_peya_botilleria_active_stores",
        python_callable = _get_peya_botilleria_active_stores
    )
    t2 = PythonOperator(
        task_id = "get_peya_market_active_stores",
        python_callable = _get_peya_market_active_stores
    )

    t3 = PythonOperator(
        task_id = "join_stock_and_promo_prices_from_s3",
        python_callable = _join_stock_and_promo_prices_from_s3
    )

    t4 = PythonOperator(
        task_id = "send_joined_data_to_stfp",
        python_callable = _send_joined_data_to_stfp
    )

    t0 >> t3
    t1 >> t3
    t2 >> t3
    t3 >> t4