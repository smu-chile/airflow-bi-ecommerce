from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

import pendulum

def _get_rappi_active_stores():
    peya_stores_query = """
        select t.id_tienda 
        from ecommdata_m10.tiendas t ;
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
        
        join_file_name = f"integraciones/last_millers/stock/out/rappi/M10/Complex/{exec_date}/store_id_{store_id}_COMBO_RAPPI.csv"
        if s3_hook.check_for_key(join_file_name, bucket_name=s3_bucket):
            print(f"File {join_file_name} already exists on bucket: {s3_bucket}. Skipping...")
            continue

        peya_stock_query = f"""
            select 
                lspp.id_tienda AS store_id,
                w.fecha_inicio_de_promocion as start_date,
                W.fecha_fin_de_promocion AS end_date,
                CASE
                    WHEN w.desc_promocion IN ('COMBINACION NXM') THEN 
                    CONCAT('llevas ', CAST(w.cantidad_n AS VARCHAR), 'Pague ', CAST(cantidad_m AS VARCHAR))
                    WHEN w.desc_promocion IN ('COMBINACION NX$') THEN 
                    CONCAT('llevas ', CAST(w.cantidad_n AS VARCHAR), 'x por ', CAST(ROUND(w.precio_total_promocional, 0) AS VARCHAR))
                END AS description,
                CASE
                    WHEN w.desc_promocion IN ('COMBINACION NXM') THEN 
                    CONCAT('L', CAST(w.cantidad_n AS VARCHAR), '_P', CAST(cantidad_m AS VARCHAR))
                    WHEN w.desc_promocion IN ('COMBINACION NX$') THEN 
                    CONCAT('T', CAST(FLOOR(((lspp.precio - (w.precio_total_promocional / w.cantidad_n)) / lspp.precio) * 100) AS VARCHAR), '_U', CAST(w.cantidad_n AS VARCHAR))
                END AS type_format,
                case
                    when w.desc_material  like '%,%' then REPLACE(w.desc_material, ',', '')
                    else w.desc_material
                end AS name,
                (w.material::int) AS id
                from ecommdata_m10.workflow w 
                left join integraciones.lm_stock_precio_promo_10 lspp on lspp.ean = w.ean 
                where  w.fecha_inicio_de_promocion <= CURRENT_DATE
                AND w.fecha_fin_de_promocion >= CURRENT_DATE
                AND w.tipo_promocion IN (2, 7)
                AND w.organizacion_ventas = '1000'
                AND w.canal_distribucion = '10'
                AND w.id_mecanica NOT IN (25, 27, 36, 37, 50, 51, 53, 67, 72, 77, 93, 99, 123, 124)
                AND lspp.ean IS NOT null
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
    prefix = f"integraciones/last_millers/stock/out/rappi/M10/Complex/{exec_date}/"

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
            remotePath = f"/sftp-allies/sftppruebas_co/store_id-{output_promo_file}_COMBO_RAPPI"
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
    "proc_rappi_M10_promotion_complex",
    default_args=default_args,
    description="Cruce de stock, precios y precios promocionales complejas para integracion Rappi",
    schedule_interval=None, 
    start_date=pendulum.datetime(2023, 2, 21, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    concurrency=2,
    tags=["OPS", "last_millers", "dw", "stock", "precios", "NICOLAS","PROMOTIONS","M10"],
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
