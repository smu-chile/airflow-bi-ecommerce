from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

from utils.slack_utils import dag_success_slack, dag_failure_slack

import pendulum
    ######################################################################################################################
    #                               Carga de las tiendas de pediddos Ya                                                  #
    ######################################################################################################################
    
def _get_peya_active_stores():
    peya_stores_query = """
        SELECT id, id_peya
        FROM integraciones.tiendas_last_millers
        WHERE id_peya in ('512089','277730');
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

    all_dfs = []

    # 1. Promociones Complejas NxM
    for n in range(2, 11):  # Iterar desde 2 hasta 10
        print(f"Procesando NxM para cantidad_n = {n}...")
        for store_id in peya_store_ids.keys():
            peya_promotion_nxm_query = f"""
                    SELECT DISTINCT 
                        'all' AS vendors,
                        '' AS barcode,
                        s.ref_id AS sku,
                        'Promociones Unimarc {n}' AS campaign_name,
                        'Promociones Complejas NXM' AS reason,
                        concat(current_date ,' 09:00:00') AS start_date,
                        concat(wp.fecha_fin_de_promocion ,' 09:00:00') AS end_date,
                        1 AS campaign_status,
                        'same_item_bundle' AS promotion_type,
                        'free_item' AS promotion_sub_type,
                        NULL AS discount_usage_limit,
                        CASE
                            WHEN WP.desc_promocion = 'COMBINACION NXM' THEN Concat('B', Wp.cantidad_n - 1, 'G', cantidad_n - cantidad_m)
                        END AS bundle_details,
                        NULL AS bundle_discount,
                        NULL AS discounted_price,
                        NULL AS max_no_of_orders,
                        COALESCE(lspp.precio, 0) AS regular_price,
                        COALESCE(wp.precio_total_promocional, 0) AS total_promo_price,
                        COALESCE(wp.cantidad_n, 0) AS qty_n,
                        COALESCE(wp.cantidad_m, 0) AS qty_m
                    FROM integraciones.lm_stock_precio_promo lspp 
                    INNER JOIN ecommdata.skus s ON s.ref_id = CONCAT(lspp.material, '-', lspp.unidad_de_medida)
                    LEFT JOIN ecommdata.workflow_promociones wp ON concat(wp.material, '-', CASE WHEN wp.umv = 'ST' THEN 'UN' ELSE wp.umv END) = concat(lspp.material, '-', lspp.unidad_de_medida) 
                    LEFT JOIN ecommdata.lista8 l ON l.material = lspp.material AND l.umv = lspp.unidad_de_medida AND l.id_tienda = lspp.id_tienda
                    LEFT JOIN ecommdata.productos p ON s.ref_id = p.ref_id
                    LEFT JOIN ecommdata.categorias ec ON p.id_categoria = ec.id
                    WHERE wp.fecha_inicio_de_promocion <= CURRENT_DATE 
                      AND wp.fecha_fin_de_promocion >= CURRENT_DATE 
                      AND lspp.id_tienda = '{store_id}'
                      AND wp.tipo_promocion IN (2, 7)
                      AND Wp.cantidad_n = '{n}'
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
                      AND wp.nombre_promocion::text !~~ '%CYBER%'::text
                      AND wp.nombre_promocion::text !~~ '%REGIO%'::text
                      AND lspp.ean IS NOT NULL
                      AND WP.desc_promocion = 'COMBINACION NXM'
                      AND wp.n_promocion NOT IN ('5552392024',
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
                      '4000512024','5552792024','5552852024')
                      AND l.excluido IS NOT TRUE
                      AND (ec.n1 NOT IN ('No Trabajar', 'Inactivos', 'Integración') OR ec.n1 IS NULL)
            """
            cursor.execute(peya_promotion_nxm_query)
            results = cursor.fetchall()
            if results:
                columns = [i[0] for i in cursor.description]
                df = pd.DataFrame(results, columns=columns)
                df.columns = map(str.lower, df.columns)
                all_dfs.append(df)

    # 2. Promociones Complejas NxS
    for n in range(2, 11):  # Iterar desde 2 hasta 10    
        print(f"Procesando NxS para cantidad_n = {n}...")
        for store_id in peya_store_ids.keys():
            peya_promotion_nxs_query = f"""
                    SELECT DISTINCT
                    'all' as vendors,
                    '' AS barcode,
                    s.ref_id AS sku,
                    'Promociones UnimarcNXS{n}' AS campaign_name,
                    'Promociones Complejas NX$' AS reason,
                    concat(current_date ,' 09:00:00') AS start_date,
                    concat(wp.fecha_fin_de_promocion ,' 09:00:00') AS end_date,
                    1 AS campaign_status,
                    'same_item_bundle' AS promotion_type,
                    'percentage_value_off' AS promotion_sub_type,
                    NULL AS discount_usage_limit,
                    CASE
                        WHEN WP.desc_promocion = 'COMBINACION NX$' THEN 
                            CONCAT('B', wp.cantidad_n , 'G1')
                    END AS bundle_details,
                    CASE
                        WHEN COALESCE(lspp.precio, 0) = 0 THEN 0
                        ELSE 
                            TRUNC(
                                (((1.0 * COALESCE((lspp.precio * wp.cantidad_n) - wp.precio_total_promocional, wp.ahorro_total, 0)) / 
                                NULLIF(lspp.precio, 0)) * 100)::numeric
                            )
                    END AS bundle_discount,
                    NULL AS discounted_price,
                    NULL AS max_no_of_orders,
                    COALESCE(lspp.precio, 0) AS regular_price,
                    COALESCE(wp.precio_total_promocional, 0) AS total_promo_price,
                    COALESCE(wp.cantidad_n, 0) AS qty_n,
                    COALESCE(wp.cantidad_m, 0) AS qty_m
                FROM integraciones.lm_stock_precio_promo lspp 
                INNER JOIN ecommdata.skus s ON s.ref_id = CONCAT(lspp.material, '-', lspp.unidad_de_medida)
                LEFT JOIN ecommdata.workflow_promociones wp ON CONCAT(wp.material, '-', CASE WHEN wp.umv = 'ST' THEN 'UN' ELSE wp.umv END) = CONCAT(lspp.material, '-', lspp.unidad_de_medida) 
                LEFT JOIN ecommdata.lista8 l ON l.material = lspp.material AND l.umv = lspp.unidad_de_medida AND l.id_tienda = lspp.id_tienda
                LEFT JOIN ecommdata.productos p ON s.ref_id = p.ref_id
                LEFT JOIN ecommdata.categorias ec ON p.id_categoria = ec.id
                WHERE wp.fecha_inicio_de_promocion <= CURRENT_DATE 
                AND wp.fecha_fin_de_promocion >= CURRENT_DATE 
                AND wp.tipo_promocion IN (2, 7)
                AND lspp.id_tienda = '{store_id}'
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
                AND wp.nombre_promocion::text NOT LIKE '%CYBER%'
                AND wp.nombre_promocion::text NOT LIKE '%REGIO%'
                AND lspp.ean IS NOT NULL
                AND wp.ahorro_total IS NOT NULL
                AND WP.desc_promocion = 'COMBINACION NX$'
                AND wp.n_promocion NOT IN (
                '5552392024', '1120012024', '1120022024', '1120032024', '1120042024', 
                '1120052024', '1120062024', '1120082024', '1120092024', '1120102024', 
                '1120112024', '1120122024', '4000512024','5552792024','5552852024'
                )
               AND lspp.unidad_de_medida NOT IN ('KG', 'KGV')
                AND l.excluido IS NOT TRUE
                AND (ec.n1 NOT IN ('No Trabajar', 'Inactivos', 'Integración') OR ec.n1 IS NULL)
                AND COALESCE(lspp.precio, 0) > 0
                AND (((1.0 * COALESCE((lspp.precio * wp.cantidad_n) - wp.precio_total_promocional, wp.ahorro_total, 0)) / NULLIF(lspp.precio, 0)) * 100) < 99
            """
            cursor.execute(peya_promotion_nxs_query)
            results = cursor.fetchall()
            if results:
                columns = [i[0] for i in cursor.description]
                df = pd.DataFrame(results, columns=columns)
                df.columns = map(str.lower, df.columns)
                all_dfs.append(df)


    # 3. Promociones Simples
    print("Procesando Promociones Simples...")
    for store_id in peya_store_ids.keys():
        peya_promotion_query = f"""
                SELECT DISTINCT
                    'all' as vendors,
                    '' AS barcode,
                    s.ref_id AS sku,
                    'Promociones' AS campaign_name,
                    'Promociones Simples' AS reason,
                    concat(current_date ,' 09:00:00') AS start_date,
                    concat(wp.fecha_fin_de_promocion ,' 09:00:00') AS end_date,
                    CASE
                        WHEN lspp.unidad_de_medida NOT IN ('KG', 'KGV') THEN ROUND(lspp.precio_promocional)
                        WHEN lspp.unidad_de_medida in ('KG','KGV') then ROUND(lspp.precio_promocional * (s.multiplicador_unidad_medida)) 
                    END AS discounted_price,
                    999 AS max_no_of_orders,
                    1 AS campaign_status,
                    'strikethrough' AS promotion_type,
                    NULL AS promotion_sub_type,
                    NULL AS bundle_details,
                    NULL AS bundle_discount,
                    COALESCE(lspp.precio, 0) AS regular_price,
                    0 AS total_promo_price,
                    0 AS qty_n,
                    0 AS qty_m
                FROM integraciones.lm_stock_precio_promo lspp
                INNER JOIN ecommdata.skus s ON s.ref_id = CONCAT(lspp.material, '-', lspp.unidad_de_medida)
                LEFT JOIN ecommdata.workflow_promociones wp ON CONCAT(wp.material, '-', CASE WHEN wp.umv = 'ST' THEN 'UN' ELSE wp.umv END) = CONCAT(lspp.material, '-', lspp.unidad_de_medida) 
                LEFT JOIN ecommdata.lista8 l ON l.material = lspp.material AND l.umv = lspp.unidad_de_medida AND l.id_tienda = lspp.id_tienda
                LEFT JOIN ecommdata.productos p ON s.ref_id = p.ref_id
                LEFT JOIN ecommdata.categorias ec ON p.id_categoria = ec.id
                where lspp.precio_promocional  is not null
                AND lspp.id_tienda = '{store_id}'
                AND l.excluido IS NOT TRUE
                AND (ec.n1 NOT IN ('No Trabajar', 'Inactivos', 'Integración') OR ec.n1 IS NULL)
                AND wp.fecha_inicio_de_promocion <= CURRENT_DATE 
                AND wp.fecha_fin_de_promocion >= CURRENT_DATE
                AND wp.tipo_promocion IN (1, 4)
                AND wp.registro_valido = TRUE
                AND wp.organizacion_ventas = '1000'
                AND wp.canal_distribucion = '10'
                AND wp.id_mecanica NOT IN (25, 27, 36, 37, 50, 51, 53, 67, 72, 77, 93, 99, 123, 124)
                AND wp.nombre_promocion::text !~~ '%ZONA%'::text
                AND wp.nombre_promocion::text !~~ '%MFC%'::text
                AND wp.nombre_promocion::text !~~ '%BANCO%'::text 
                AND wp.nombre_promocion::text !~~ '%UNIPAY%'::text
                AND wp.nombre_promocion::text !~~ '%TERCERA%'::text 
                AND wp.nombre_promocion::text !~~ '%917%'::text
                AND wp.nombre_promocion::text !~~ '%ESTADO%'::text
                AND wp.nombre_promocion::text !~~ '% LOC%'::text
                AND wp.nombre_promocion::text !~~ '%LIQ%'::text
                AND wp.nombre_promocion::text !~~ '%CYBER%'::text
                AND wp.nombre_promocion::text !~~ '%REGIO%'::text
                AND wp.n_promocion NOT IN ('5552392024',
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
                  '4000512024','5552792024','5552852024')
            """
        cursor.execute(peya_promotion_query)
        results = cursor.fetchall()
        if results:
            columns = [i[0] for i in cursor.description]
            df = pd.DataFrame(results, columns=columns)
            df.columns = map(str.lower, df.columns)
            all_dfs.append(df)

    cursor.close()
    pg_connection.close()

    if all_dfs:
        merged_df = pd.concat(all_dfs, ignore_index=True)

        def calculate_effective_unit_price(row):
            promo_type = str(row.get("promotion_type", ""))
            sub_type = str(row.get("promotion_sub_type", ""))
            
            try:
                reg_price = float(row.get("regular_price") or 0)
            except (ValueError, TypeError):
                reg_price = 0.0

            # 1. Promoción Simple (Descuento directo / strikethrough)
            if promo_type == "strikethrough":
                try:
                    disc_price = float(row.get("discounted_price") or 0)
                    if disc_price > 0:
                        return disc_price
                except (ValueError, TypeError):
                    pass
                return reg_price if reg_price > 0 else 9999999.0

            # 2. Promoción Compleja Nx$ (percentage_value_off)
            elif promo_type == "same_item_bundle" and sub_type == "percentage_value_off":
                try:
                    tot_promo = float(row.get("total_promo_price") or 0)
                    qty_n = float(row.get("qty_n") or 0)
                    if qty_n > 0 and tot_promo > 0:
                        return tot_promo / qty_n
                except (ValueError, TypeError):
                    pass

            # 3. Promoción Compleja NxM (free_item)
            elif promo_type == "same_item_bundle" and sub_type == "free_item":
                try:
                    qty_n = float(row.get("qty_n") or 0)
                    qty_m = float(row.get("qty_m") or 0)
                    if qty_n > 0 and reg_price > 0:
                        return (qty_m * reg_price) / qty_n
                except (ValueError, TypeError):
                    pass

            return reg_price if reg_price > 0 else 9999999.0

        merged_df["unit_effective_price"] = merged_df.apply(calculate_effective_unit_price, axis=1)

        # Ordenar por SKU y por menor precio unitario efectivo (el más barato primero)
        merged_df.sort_values(by=["sku", "unit_effective_price"], ascending=[True, True], inplace=True)

        # Eliminar duplicados reteniendo SOLO la mejor promoción por SKU
        merged_df.drop_duplicates(subset=["sku"], keep="first", inplace=True)
        print(f"Total registros consolidados con la mejor promoción por SKU: {len(merged_df.index)}")

        merged_df.columns = map(str.lower, merged_df.columns)
        expected_cols = [
            "barcode",
            "sku",
            "campaign_name",
            "reason",
            "start_date",
            "end_date",
            "promotion_type",
            "promotion_sub_type",
            "discount_usage_limit",
            "bundle_details",
            "bundle_discount",
            "discounted_price",
            "max_no_of_orders",
            "campaign_status",
            "vendors",
            "exclude"
        ]
        for col in expected_cols:
            if col not in merged_df.columns:
                merged_df[col] = None
        merged_df = merged_df[expected_cols]

        # S3 Path consolidado
        join_file_name = f"integraciones/last_millers/promotions/out/peya/Complex/Merged/{exec_date}/SMUPromotionsCombined.csv"

        buffer = io.StringIO()
        merged_df.to_csv(buffer, header=True, index=False, encoding="utf-8")
        buffer.seek(0)

        s3_hook.load_string(buffer.getvalue(),
                        key=join_file_name,
                        bucket_name=s3_bucket,
                        replace=True,
                        encrypt=False)
        print(f"File load on S3: {join_file_name}")
    else:
        print("No se encontraron promociones de ningún tipo.")

def _send_joined_data_to_stfp(ds):
    import os
    import pysftp

    ftp_host = Variable.get("NEW_PEYA_SFTP_HOST")
    ftp_port = 22
    ftp_user = Variable.get("NEW_PEYA_SFTP_USER")
    ftp_rsa_key = Variable.get("NEW_PEYA_SFTP_PASSWORD")

    exec_date = ds.replace("-", "/")
    prefix = f"integraciones/last_millers/promotions/out/peya/Complex/Merged/{exec_date}/"
        
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    s3_file_list = s3_hook.list_keys(s3_bucket, prefix=prefix)
        
    print(f"Number of consolidated promotion files found: {len(s3_file_list)}")
        
    for promo_file in s3_file_list:
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
            remotePath = f"/vendor-automation-sftp-storage-live-us-1/home/PY_CL_1fff4594-d35e-44ad-af7e-1f7d663d60de/promotions/{output_promo_file}"
            sftp.putfo(localFile, remotePath)
        
        print("Combined file loaded successfully to SFTP.")

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
    description="Cruce de stock, precios y precios promocionales simples y complejas para integracion Pedidos Ya",
    schedule_interval="30 7 * * *", 
    start_date=pendulum.datetime(2023, 2, 21, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    concurrency=2,
    tags=["OPS", "last_millers", "dw", "stock", "precios", "NICOLAS", "RODRIGO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
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