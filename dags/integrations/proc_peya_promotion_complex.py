from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

import pendulum
    ######################################################################################################################
    #                               Carga de las tiendas de pediddos Ya                                                  #
    ######################################################################################################################
    
def _get_peya_active_stores():
    peya_stores_query = """
        SELECT id, id_peya
        FROM integraciones.tiendas_last_millers
        WHERE id_peya = '512089';
    """
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(peya_stores_query)
    results = cursor.fetchall()
    cursor.close()
    pg_connection.close()
    return results

    #################################################################################################################
    #                                   Carga de promociones Complejas NxM                                          #
    #################################################################################################################

def _join_promo_prices_from_s3(ds, ti):
    import io
    import pandas as pd

    exec_date = ds.replace("-", "/")

    peya_stores = ti.xcom_pull(key="return_value", task_ids=["get_peya_active_stores"])[0]
    peya_store_ids = dict([(peya_store_id[0], peya_store_id[1]) for peya_store_id in peya_stores])
    print(peya_store_ids)

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    for n in range(2, 11):  # Iterar desde 2 hasta 10
        print(f"Iterracion numero:{n} ")
        for store_id in peya_store_ids.keys():
            print(f"PEYA id: {peya_store_ids[store_id]}")
            join_file_name = f"integraciones/last_millers/promotions/out/peya/Complex/NXM/{exec_date}/{peya_store_ids[store_id]}_{n}.csv"

            if s3_hook.check_for_key(join_file_name, bucket_name=s3_bucket):
                print(f"File {join_file_name} already exists on bucket: {s3_bucket}. Skipping...")
                continue
            
            peya_promotion_nxm_query = f"""
                    select distinct 
                        null as barcode,
                        lspp.ean as SKU,
                        'Promociones Unimarc' as campaign_name,
                        'Promociones Complejas' as reason,
                        concat(current_date ,' 10:00:00-03:00') AS start_date,
                        concat(current_date + 1 ,' 10:00:00-03:00') AS end_date,
                        1 as campaign_status,
                        'same_item_bundle' as promotion_type,
                        'free_item' as promotion_sub_type,
                        null as discount_usage_limit,
                        case
                            when WP.desc_promocion = 'COMBINACION NXM' then Concat('B',Wp.cantidad_n - 1,'G',cantidad_n - cantidad_m)
                        end as bundle_details,
                        null as bundle_discount
                    from integraciones.lm_stock_precio_promo lspp 
                    left join ecommdata.workflow_promociones wp on concat(wp.material, '-', CASE WHEN wp.umv = 'ST' THEN 'UN' ELSE wp.umv END) = concat(lspp.material, '-', lspp.unidad_de_medida) 
                    where  wp.fecha_inicio_de_promocion <= CURRENT_DATE 
                    AND wp.fecha_fin_de_promocion >= CURRENT_DATE 
                    AND lspp.id_tienda = '0053'
                    AND wp.tipo_promocion IN (2, 7)
                    and Wp.cantidad_n = '{n}'
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
                    and WP.desc_promocion = 'COMBINACION NXM'
                    and wp.n_promocion  not in  ('5552392024',
                    '1120012024',
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
                    AND lspp.id_tienda = '{store_id}'
                """
                #AND lspp.id_tienda = '0755' 
                #AND lspp.id_tienda = '{store_id}'

            cursor.execute(peya_promotion_nxm_query)
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
            prev_join_file_name = f"integraciones/last_millers/promotions/out/peya/Complex/NXM/{prev_exec_date}/{peya_store_ids[store_id]}_{n}.csv"
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
                #Aqui va la nueva logica
                join_file_name = f"integraciones/last_millers/promotions/out/peya/Complex/NXM/{exec_date}/{peya_store_ids[store_id]}_{n}.csv"
                if s3_hook.check_for_key(join_file_name, bucket_name=s3_bucket):
                    print(f"File {join_file_name} already exists on bucket: {s3_bucket}. Skipping...")
                continue
        ####################################################################################################################
        #                   Promociones Complejas Nx$ o BUY X and Buy X get 1 item from it for a Y% discount               # 
        ####################################################################################################################
    for n in range(2, 11):  # Iterar desde 2 hasta 10    
        for store_id in peya_store_ids.keys():
            print(f"PEYA id: {peya_store_ids[store_id]}")
            join_file_name = f"integraciones/last_millers/promotions/out/peya/Complex/NXS/{exec_date}/{peya_store_ids[store_id]}_{n}.csv"
            if s3_hook.check_for_key(join_file_name, bucket_name=s3_bucket):
                print(f"File {join_file_name} already exists on bucket: {s3_bucket}. Skipping...")
                continue

            peya_promotion_nxs_query = f"""
                    SELECT DISTINCT
                    NULL AS barcode,
                    lspp.ean AS SKU,
                    'Promociones Unimarc' AS campaign_name,
                    'Promociones Complejas' AS reason,
                    concat(current_date ,' 10:00:00-03:00') AS start_date,
                    concat(current_date + 1 ,' 10:00:00-03:00') AS end_date,
                    1 AS campaign_status,
                    'same_item_bundle' AS promotion_type,
                    'percentage_value_off' AS promotion_sub_type,
                    NULL AS discount_usage_limit,
                    CASE
                        WHEN WP.desc_promocion = 'COMBINACION NX$' THEN CONCAT('B', Wp.cantidad_n, 'G', 1)
                    END AS bundle_details,
                    CASE
                        WHEN wp.precio_modal IS NOT NULL AND wp.cantidad_n > 0 THEN
                        FLOOR(((wp.precio_modal * wp.cantidad_n - (wp.precio_total_promocional - wp.precio_modal))/ wp.precio_modal )*100)-100
                    ELSE 
                        NULL
                    END AS bundle_discount
                FROM integraciones.lm_stock_precio_promo lspp 
                LEFT JOIN ecommdata.workflow_promociones wp 
                    ON CONCAT(wp.material, '-', CASE WHEN wp.umv = 'ST' THEN 'UN' ELSE wp.umv END) = CONCAT(lspp.material, '-', lspp.unidad_de_medida) 
                WHERE wp.fecha_inicio_de_promocion <= CURRENT_DATE 
                AND wp.fecha_fin_de_promocion >= CURRENT_DATE 
                AND wp.tipo_promocion IN (2, 7)
                AND lspp.id_tienda = '0053'
                AND Wp.cantidad_n = '{n}'  -- Número de la iteración actual
                AND wp.registro_valido = TRUE
                AND wp.organizacion_ventas = '1000'
                AND wp.canal_distribucion = '10'
                AND wp.id_mecanica NOT IN (25, 27, 36, 37, 50, 51, 53, 67, 72, 77, 93, 99, 123, 124)
                AND wp.nombre_promocion::text NOT LIKE '%MFC%'
                AND wp.nombre_promocion::text NOT LIKE '%BANCO%'
                AND wp.nombre_promocion::text NOT LIKE '%UNIPAY%'
                AND wp.nombre_promocion::text NOT LIKE '%TERCERA%'
                AND wp.nombre_promocion::text NOT LIKE '%917%'
                AND wp.nombre_promocion::text NOT LIKE '%ESTADO%'
                AND wp.nombre_promocion::text NOT LIKE '% LOC%'
                AND wp.nombre_promocion::text NOT LIKE '%LIQ%'
                AND lspp.ean IS NOT NULL
                AND WP.desc_promocion = 'COMBINACION NX$'
                --AND lspp.material in ('000000000000345768' ,'000000000000753782','000000000000990546')
                AND wp.n_promocion NOT IN (
                '5552392024', '1120012024', '1120022024', '1120032024', '1120042024', 
                '1120052024', '1120062024', '1120082024', '1120092024', '1120102024', 
                '1120112024', '1120122024', '4000512024'
                );
                AND lspp.id_tienda = '{store_id}'
            """
        
            # Ejecutar la consulta
            cursor.execute(peya_promotion_nxs_query)
            results = cursor.fetchall()
            columns = [i[0] for i in cursor.description]

            if len(results) == 0:
                print(f"No records found for Store Id: {store_id} with cantidad_n = {n}")
                continue
            ##No me esta tomando esta parte
            df = pd.DataFrame(results, columns=columns)
            print(f"Number of records found on stock for cantidad_n = {n}: {len(df.index)}")

            df.columns = map(str.upper, df.columns)

            prev_exec_date = macros.ds_add(ds, -1).replace("-", "/")
            prev_join_file_name = f"integraciones/last_millers/promotions/out/peya/Complex/NXS/{prev_exec_date}/{peya_store_ids[store_id]}_{n}.csv"
            print(f"Checking for previous executions on {prev_join_file_name}.")

            print(f"Total number of records for cantidad_n = {n}: {len(df.index)}.")

            buffer = io.StringIO()
            df.to_csv(buffer, header=True, index=False, encoding="utf-8")
            buffer.seek(0)

            s3_hook.load_string(buffer.getvalue(),
                            key=join_file_name,
                            bucket_name=s3_bucket,
                            replace=True,
                            encrypt=False)
            print(f"File load on S3 for cantidad_n = {n}: {join_file_name}")

    cursor.close()
    pg_connection.close()
    return
        ##################################################################################
        #               Envio de promociones Complejas a Pedidos Ya                      #
        ##################################################################################

def _send_joined_data_to_stfp(ds):
    import os
    import pysftp

    ftp_host = Variable.get("NEW_PEYA_SFTP_HOST")
    ftp_port = 22
    ftp_user = Variable.get("NEW_PEYA_SFTP_USER")
    ftp_rsa_key = Variable.get("NEW_PEYA_SFTP_PASSWORD")

    exec_date = ds.replace("-", "/")
    #Prefix para NXM
    prefix = f"integraciones/last_millers/promotions/out/peya/Complex/NXM/{exec_date}/"
    #Prefix para promociones NxS
    prefix2 = f"integraciones/last_millers/promotions/out/peya/Complex/NXS/{exec_date}/"
        
    
        
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
                remotePath = f"/vendor-automation-sftp-storage-live-us-1/home/PY_CL_1fff4594-d35e-44ad-af7e-1f7d663d60de/promotions/SMUPromoNXM_{output_stock_file}"
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
                remotePath = f"/vendor-automation-sftp-storage-live-us-1/home/PY_CL_1fff4594-d35e-44ad-af7e-1f7d663d60de/promotions/SMUPromoNXS_{output_promo_file}"
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
    "proc_peya_promotion_complex",
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
    * A partir de esta lista, se obtiene listado de archivos CSV de Promos tanto NXM o NXS + precio para cada una de las tiendas activas en **PEYA**. \n
    """ 

    t0 = PythonOperator(
        task_id = "get_peya_active_stores",
        python_callable = _get_peya_active_stores
    )

    t1 = PythonOperator(
        task_id = "join_promo_prices_from_s3",
        python_callable = _join_promo_prices_from_s3
    )

    t2 = PythonOperator(
        task_id = "send_joined_data_to_stfp",
        python_callable = _send_joined_data_to_stfp
    )

    t0 >> t1
    t1 >> t2