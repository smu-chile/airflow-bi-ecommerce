from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

from utils.slack_utils import dag_success_slack, dag_failure_slack

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
#################################################################################################################
# #                                   Carga de promociones Complejas                                            #
#################################################################################################################
def _join_stock_and_promo_prices_from_s3(ds, ti):
    import io
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
        
        join_file_name = f"integraciones/last_millers/stock/out/rappi/Complex/{exec_date}/store_id_{store_id}_COMBO_UNIMA.csv"
        if s3_hook.check_for_key(join_file_name, bucket_name=s3_bucket):
            print(f"File {join_file_name} already exists on bucket: {s3_bucket}. Skipping...")
            continue

        peya_stock_query = f"""
            SELECT DISTINCT  
                (s.ou_id::int) AS store_id,
                WP.fecha_inicio_de_promocion AS start_date,
                WP.fecha_fin_de_promocion AS end_date,
                CASE
                    WHEN wp.desc_promocion IN ('COMBINACION NXM') THEN 
                        CONCAT('llevas ', CAST(wp.cantidad_n AS VARCHAR), 'Pague ', CAST(cantidad_m AS VARCHAR))
                    WHEN wp.desc_promocion IN ('COMBINACION NX$') THEN 
                        CONCAT('llevas ', CAST(wp.cantidad_n AS VARCHAR), 'x por ', CAST(ROUND(wp.precio_total_promocional, 0) AS VARCHAR))
                END AS description,
                CASE
                    WHEN wp.desc_promocion IN ('COMBINACION NXM') THEN 
                        CONCAT('L', CAST(wp.cantidad_n AS VARCHAR), '_P', CAST(cantidad_m AS VARCHAR))
                    WHEN wp.desc_promocion IN ('COMBINACION NX$') THEN 
                        CONCAT('T', CAST(FLOOR(((lspp.precio - (wp.precio_total_promocional / wp.cantidad_n)) / lspp.precio) * 100) AS VARCHAR), '_U', CAST(wp.cantidad_n AS VARCHAR))
                END AS type_format,
                case
                	when wp.descripcion_material like '%,%' then REPLACE(wp.descripcion_material, ',', '')
                	else wp.descripcion_material
                end AS name,
                (wp.material::int) AS id
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
                '4000512024','1120012025','1120022025','1120032025','1120042025')
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

        buffer = io.StringIO()
        df.to_csv(buffer, header=True, index=False, encoding="utf-8")
        buffer.seek(0)

        s3_hook.load_string(buffer.getvalue(),
                            key=join_file_name,
                            bucket_name=s3_bucket,
                            replace=True,
                            encrypt=False)
        

    cursor.close()
    pg_connection.close()
    return


def _send_joined_data_to_stfp(ds):
    import os
    import pysftp

    ftp_host = Variable.get("SFTP_RAPPI_HOST")
    ftp_port = 22
    ftp_user = Variable.get("SFTP_RAPPI_USER")
    ftp_rsa_key = Variable.get("SFTP_RAPPI_PASSWORD")

    exec_date = ds.replace("-", "/")
    prefix = f"integraciones/last_millers/stock/out/rappi/Complex/{exec_date}/"

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    s3_file_list = s3_hook.list_keys(s3_bucket, prefix=prefix)

    print(f"Number of files found: {len(s3_file_list)}")

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
            remotePath = f"/sftp-allies/sftppruebas_co/store_id-{output_promo_file}_COMBO_UNIMARC"
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
    "proc_rappi_promotion_complex",
    default_args=default_args,
    description="Cruce de stock, precios y precios promocionales complejas para integracion Rappi",
    schedule_interval=None, 
    start_date=pendulum.datetime(2023, 2, 21, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    concurrency=2,
    tags=["OPS", "last_millers", "dw", "stock", "precios", "NICOLAS","PROMOTIONS"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
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
        task_id = "_send_joined_data_to_stfp",
        python_callable = _send_joined_data_to_stfp
    )

    t0 >> t1 >> t2
