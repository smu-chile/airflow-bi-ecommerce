from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

import pendulum

def _get_uber_active_stores():
    uber_stores_query = """
           
    select id
    from integraciones.tiendas_last_millers tlm 
    where tlm.id_uber is not null 
    """
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(uber_stores_query)
    results = cursor.fetchall()
    cursor.close()
    pg_connection.close()
    return results

def _join_promo_prices_from_s3(ds, ti):
    import json
    import pandas as pd
    import io

    uber_stores = ti.xcom_pull(key="return_value", task_ids=["get_uber_active_stores"])[0]
    uber_store_ids = [uber_store_id[0] for uber_store_id in uber_stores]
    print(uber_store_ids)

    exec_date = ds.replace("-", "/")

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
    
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()

    aux_list = []

    for store_id in uber_store_ids:
        join_file_name = f"integraciones/last_millers/promotions/out/uber/{exec_date}/{store_id}.csv"
        if s3_hook.check_for_key(join_file_name, bucket_name=s3_bucket):
            print(f"File {join_file_name} already exists on bucket: {s3_bucket}. Skipping...")
            continue

        uber_promotions_query = f"""
            WITH RankedPromotions AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY wp.material, wp.precio_total_promocional) AS id,
        wp.descripcion_material AS "Name",
        wp.fecha_inicio_de_promocion::timestamp AS "Start Date",
        wp.fecha_fin_de_promocion::timestamp AS "End Date",
        CASE
            WHEN wp.cantidad_n = 0 THEN 1
            ELSE wp.cantidad_n
        END AS "Required Quantity",
        CASE
            WHEN wp.cantidad_n = 0 THEN 1
            ELSE wp.cantidad_n
        END AS "Entitled Quantity",
        TRIM(LEADING '0' FROM wp.material) AS "Sku",
            '0917-0926-0956-0962-0581-0717-0008-0009-0017-0357-0011-0626-0332-0336-0030-0903-0602-0953-0914-0325-0476-0347-0954-0005-0326-0333-0469-0644-0982-0028-0324-0477-0445-0939-0763-0915-0345-0474-0758-0755-0601' AS "Branch",
        wp.precio_total_promocional AS "Price",
        NULL AS "Percentage Discount",
        ROW_NUMBER() OVER (PARTITION BY wp.material ORDER BY wp.precio_total_promocional) AS RowNum
        FROM
            ecommdata.workflow_promociones wp
        WHERE
            wp.fecha_inicio_de_promocion <= current_date + 1
            AND wp.fecha_fin_de_promocion >= current_date + 1
            AND wp.id_mecanica <> ALL (ARRAY[36, 67, 72, 99, 84, 12, 37, 51, 93, 53, 96, 77, 59])
            AND wp.nombre_promocion::text !~~ '%MFC%'::text
            AND wp.nombre_promocion::text !~~ '%UNIPAY%'::text
            AND wp.nombre_promocion::text !~~ '%917%'::text
            AND wp.nombre_promocion::text !~~ '%0743%'::text
    )
    SELECT
        "id",
        "Name",
        "Start Date",
        "End Date",
        "Required Quantity",
        "Entitled Quantity",
        "Sku",
        "Branch",
        "Price",
        "Percentage Discount"
    FROM
        RankedPromotions
    WHERE
        RowNum = 1;

            """
        cursor.execute(uber_promotions_query)
        results = cursor.fetchall()
        columns = [i[0] for i in cursor.description]

        if len(results) == 0:
            print(f"No records found for Store Id: {store_id}. Skipping...")
            continue

        df = pd.DataFrame(results, columns=columns)
        print(f"Number of records found on stock: {len(df.index)}")

        aux_list.append(df)
    if aux_list:
        final_df = pd.concat(aux_list, ignore_index=True)
        
        buffer = io.StringIO()
        final_df.to_csv(buffer, header=True, index=False, encoding="utf-8")
        buffer.seek(0)

        s3_hook.load_string(buffer.getvalue(),
                    key=join_file_name,
                    bucket_name=s3_bucket,
                    replace=True,
                    encrypt=False)
        print(f"File load on S3: {join_file_name}")
    else:
        print("No data collected in aux_list.")
        cursor.close()
        pg_connection.close()
    return

def _send_joined_data_to_sftp(ds):
    import os
    import pysftp
    from airflow.models import Variable

    ftp_host = Variable.get("Old_Uber_Host")
    ftp_port = 22
    ftp_user = Variable.get("Old_Uber_User")
    ftp_rsa_key = Variable.get("Old_Uber_password")

    prefix = f"integraciones/last_millers/promotions/out/uber/{exec_date}/"

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    s3_file_list = s3_hook.list_keys(s3_bucket, prefix=prefix)

    print(f"Number of files found: {len(s3_file_list)}")
    
    for promotions_file in s3_file_list:
        print(promotions_file)

        promotions_object = s3_hook.get_key(promotions_file, bucket_name=s3_bucket)
        promotions_object_body = promotions_object.get()["Body"]

        output_promotions_file = promotions_file.split("/")[-1]
        print(output_promotions_file)
        print(f"File to load to SFTP Server: {output_promotions_file}")
        
        with pysftp.Connection(host=ftp_host, 
                                username=ftp_user, 
                                port=ftp_port,
                                private_key=ftp_rsa_key) as sftp:
            localFile = promotions_object_body
            remotePath = f"/test/synchronize/{output_promotions_file}"
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
    "proc_uber_promotions_integration",
    default_args=default_args,
    description="Cruce de precios y precios promocionales simples para integracion Uber",
    schedule_interval="15 9 * * *", 
    start_date=pendulum.datetime(2023, 2, 21, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    concurrency=2,
    tags=["OPS", "last_millers", "dw", "promotions", "precios"],
) as dag:

    dag.doc_md = """
    Cruce de precios y precios promocionales simples para integración con Last Millers: **Uber**. \n
    * Se obtiene listado de tiendas activas para la integración UBER (`registros con id_uber NOT NULL de la tabla integraciones.tiendas_last_millers`). \n
    * A partir de esta lista, se obtiene listado de archivos CSV de precio para cada una de las tiendas activas en **UBER**. \n
    * Desde **S3** se extrae archivo CSV de precios modales. \n
    * Para cada tienda activa, se cruzan los archivos de precio promo y el de precios modales, se les da formato correspondiente para luego
    ser almacenados en **S3**. 
    * Finalmente, se itera sobre los archivos generados, dejando cada uno de estos en el servidor SFTP de Uber.
    Este DAG depende del DAG: [ **proc_stock_last_millers** ].
    """ 

    t0 = PythonOperator(
        task_id = "get_uber_active_stores",
        python_callable = _get_uber_active_stores
    )

    t1 = PythonOperator(
        task_id = "join_promo_prices_from_s3",
        python_callable = _join_promo_prices_from_s3
    )

    t2 = PythonOperator(
        task_id = "send_joined_data",
        python_callable = _send_joined_data_to_sftp
    )

    t0 >> t1
    t1 >> t2
