from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from datetime import datetime

from utils.slack_utils import dag_success_slack, dag_failure_slack

import pendulum

   #####################################################################################################
   #                                 QUERY PRODUCTOS                                                   #
   #####################################################################################################

def _join_Catalog_from_s3(ds, ti):
    import pandas as pd
    import io

    # Obtener la fecha actual
    fecha_actual = datetime.strptime(ds, "%Y-%m-%d")
    # Obtener el día de la semana como un número
    numero_dia_semana = fecha_actual.weekday()

    exec_date = ds.replace("-", "/")

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
    
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()

    aux_list = []

    # Obtén la fecha de ejecución en formato YYYYMMDD
    exec_date_formatted = datetime.now().strftime("%Y%m%d")

    join_file_name = f"integraciones/last_millers/stock/out/uber/Catalog/{exec_date}/{exec_date_formatted}.csv"
    if s3_hook.check_for_key(join_file_name, bucket_name=s3_bucket):
            print(f"File {join_file_name} already exists on bucket: {s3_bucket}. Skipping...")

    results = []

    

    uber_catalog_query = f"""
    WITH RankedUberCatalog AS (
    SELECT
        l.material AS SKU,
        s.unidad_de_venta AS Unidad_de_unidad_venta,
        s.ean_primario::varchar AS "código de barras",
        p.nombre AS descripcion,
        m.nombre AS Marca,
        CASE 
            WHEN img.imagen IS NOT NULL AND img.imagen <> '' 
                THEN CONCAT('https://unimarc.vteximg.com.br', img.imagen)
            ELSE NULL
        END AS main_image_url,
        c.n2 AS Category_level_1,
        c.n3 AS Category_level_2,
        ROW_NUMBER() OVER (
            PARTITION BY s.ref_id 
            ORDER BY s.ref_id ASC
        ) AS rn
    FROM ecommdata.lista8 l
    INNER JOIN ecommdata.skus s
        ON l.material || '-' || l.umv = s.ref_id
    INNER JOIN ecommdata.productos p
        ON s.ref_id = p.ref_id
    LEFT JOIN ecommdata.marcas m
        ON m.id = p.id_marca
    LEFT JOIN ecommdata.categorias c
        ON p.id_categoria = c.id
    LEFT JOIN ecommdata.imagenes_sku img
        ON img.ref_id = s.ref_id
        AND img.orden = 1
    WHERE l.excluido IS NOT TRUE
      AND (c.n1 NOT IN ('No Trabajar', 'Inactivos', 'Integración') OR c.n1 IS NULL)
      AND c.n2 IS NOT NULL
      AND c.n3 IS NOT NULL
      AND s.ean_primario IS NOT NULL
      AND img.imagen IS NOT NULL
)
SELECT
    SKU,
    Unidad_de_unidad_venta,
    "código de barras",
    descripcion,
    Marca,
    main_image_url,
    Category_level_1,
    Category_level_2
FROM RankedUberCatalog
WHERE rn = 1
ORDER BY SKU ASC;
                    """
    cursor.execute(uber_catalog_query)
    results = cursor.fetchall()
    columns = [i[0] for i in cursor.description]

    if len(results) == 0:
        print(f"No records found. Skipping...")
        cursor.close()
        pg_connection.close()
        return

    df = pd.DataFrame(results, columns=columns)
    print(f"Number of records found on stock: {len(df.index)}")


    aux_list.append(df)


    print(df['código de barras'])

    print(df['código de barras'].dtypes)


    buffer = io.StringIO()
    df['sku'] = df['sku'].apply(lambda x: int(x) if pd.notnull(x) else x)
    df['código de barras'] = df['código de barras'].apply(lambda x: str(x) if pd.notnull(x) else x)
    df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    buffer.seek(0)

    print(df['código de barras'])

    print(df['código de barras'].dtypes)
    
    s3_hook.load_string(buffer.getvalue(),
                key=join_file_name,
                bucket_name=s3_bucket,
                replace=True,
                encrypt=False)
    print(f"File load on S3: {join_file_name}")
    cursor.close()
    pg_connection.close()
    return

   #####################################################################################################
   #                                 QUERY Stock diario                                                #
   #####################################################################################################
def _join_stock_from_s3(ds, ti):
    import json
    import pandas as pd
    import io

    exec_date = ds.replace("-", "/")

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
    
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()

    # Obtén la fecha de ejecución en formato YYYYMMDD
    exec_date_formatted = datetime.now().strftime("%Y%m%d")

    join_file_name = f"integraciones/last_millers/stock/out/uber/stock/{exec_date}/{exec_date_formatted}.csv"
    if s3_hook.check_for_key(join_file_name, bucket_name=s3_bucket):
            print(f"File {join_file_name} already exists on bucket: {s3_bucket}. Skipping...")

    uber_catalog_query = f"""
       SELECT 
             CAST(CAST(lspp.material AS DECIMAL) AS VARCHAR) AS "SKU",
             lspp.ean::varchar AS "EAN",
             lspp.id_tienda AS "BRANCH",
             lspp.unidad_de_medida AS "UM VTA",
              CASE 
                  WHEN lspp.unidad_de_medida = 'KGV' THEN 1
                  ELSE lspp.multiplicador_unidad
              END AS "UXV",
        		CASE 
          		WHEN lspp.unidad_de_medida = 'KGV' THEN GREATEST(lspp.stock_unitario::numeric(13,3), 0)
          		WHEN lspp.unidad_de_medida != 'KG' THEN GREATEST(((lspp.stock_unitario * lspp.multiplicador_unidad))::numeric(13,0), 0)
          		WHEN lspp.unidad_de_medida = 'KG' THEN GREATEST(((lspp.stock_unitario * lspp.multiplicador_unidad))::numeric(13,3), 0)
     		 	 END AS "STOCK X UMV",
          	lspp.precio AS "PRICE"
         FROM integraciones.lm_stock_precio_promo lspp
         INNER JOIN integraciones.tiendas_last_millers tlm ON tlm.id = lspp.id_tienda AND tlm.id_uber IS NOT NULL
         --left join integraciones.stock_seguridad_uber ssu on ssu.ref_id = (lspp.material || '-' || lspp.unidad_de_medida);
        """
    cursor.execute(uber_catalog_query)
    results = cursor.fetchall()
    columns = [i[0] for i in cursor.description]

    if len(results) == 0:
        print(f"No records found. Skipping...")
        cursor.close()
        pg_connection.close()
        return
    

    df = pd.DataFrame(results, columns=columns)
    print(f"Number of records found on stock: {len(df.index)}")
    
    df['PRICE'] = df['PRICE'].apply(lambda x: int(x) if pd.notnull(x) else x)

    buffer = io.StringIO()
    df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    buffer.seek(0)
        
    s3_hook.load_string(buffer.getvalue(),
                key=join_file_name,
                bucket_name=s3_bucket,
                replace=True,
                encrypt=False)
    print(f"File load on S3: {join_file_name}")
    
    cursor.close()
    pg_connection.close()
    return
   
#####################################################################################################
#                          ENVIO DE LOS ARCHIVOS AL SFTP UBER                                       #
#####################################################################################################

def _send_joined_data_to_sftp(ds):
    import os
    import paramiko
    import pandas as pd
    from airflow.models import Variable
    import io
    from datetime import datetime, timedelta

    # Obtener la fecha actual
    fecha_actual = datetime.strptime(ds, "%Y-%m-%d")
    # Obtener el día de la semana como un número
    numero_dia_semana = fecha_actual.weekday()
    
    #Variable de los datos

    ftp_host = Variable.get("UBER_SFTP_HOST")
    ftp_port = 2222
    ftp_user = Variable.get("UBER_SFTP_USER")
    ftp_rsa_key = Variable.get("UBER_SFTP_SECRET_RSA_KEY")

    #Datos de los envios

    exec_date = ds.replace("-", "/")
    prefix_Catalog = f"integraciones/last_millers/stock/out/uber/Catalog/{exec_date}/" #Prefis para el catologo enviado a uber
    prefix_Stock = f"integraciones/last_millers/stock/out/uber/stock/{exec_date}/" #Prefix para actualizacion de stock

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    # Establecer una única conexión SSH para todos los envíos
    key_buffer = io.StringIO(ftp_rsa_key)
    p_key = paramiko.RSAKey.from_private_key(key_buffer)
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(ftp_host, username=ftp_user, port=ftp_port, pkey=p_key)
    sftp = ssh.open_sftp()

    try:
        # Envio de productos
        s3_file_list = s3_hook.list_keys(s3_bucket, prefix=prefix_Catalog)
        print(f"Number of files found: {len(s3_file_list)}")

        for products_file in s3_file_list:
            print(products_file)

            products_object = s3_hook.get_key(products_file, bucket_name=s3_bucket)
            # Asegurar que "codigo de barra" sea tratado como texto
            products_object_body = pd.read_csv(products_object.get()["Body"], dtype={"código de barras": str})

            output_products_file = products_file.split("/")[-1]
            print(output_products_file)
            print(f"File to load to SFTP Server: {output_products_file}")

            remotePath = f"/prod/Archivo_productos_semana_{output_products_file}"
            with sftp.open(remotePath, 'w') as f:
                f.write(products_object_body.to_csv(index=False, sep=';'))

        # Envio de stock diario
        s3_file_list_stock = s3_hook.list_keys(s3_bucket, prefix=prefix_Stock)
        print(f"Number of files found: {len(s3_file_list_stock)}")

        for stock_file in s3_file_list_stock:
            print(stock_file)

            stock_object = s3_hook.get_key(stock_file, bucket_name=s3_bucket)
            stock_object_body = pd.read_csv(stock_object.get()["Body"], dtype={"EAN": str})

            output_stock_file = stock_file.split("/")[-1]
            print(output_stock_file)
            print(f"File to load to SFTP Server: {output_stock_file}")

            remotePath = f"/prod/CS-UNI-STOCK-PRICES-{output_stock_file}"
            with sftp.open(remotePath, 'w') as f:
                f.write(stock_object_body.to_csv(index=False, sep=';'))

        print("Todo Cargadito")

    finally:
        ssh.close()

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
    schedule_interval=None, 
    start_date=pendulum.datetime(2023, 2, 21, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    concurrency=2,
    tags=["OPS", "last_millers", "dw", "promotions", "precios","NICOLAS","UBER","RODRIGO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Cruce de datos de catalogo y promociones simples para lastmiller Uber.
    Se enviara el dia lunes el envio del catalogo actualizado hasta la fecha y diariamente se hara entrega del
    stock de uber en el archivo de stock tiendas Uber.
    * Finalmente, se itera sobre los archivos generados, dejando cada uno de estos en el servidor SFTP de Uber.
    Este DAG depende del DAG: [ **proc_stock_last_millers** ].
    """ 

    t0 = PythonOperator(
        task_id = "join_Catalog_prices_from_s3",
        python_callable = _join_Catalog_from_s3
    )

    t1 = PythonOperator(
        task_id = "join_stock_prices_from_s3",
        python_callable = _join_stock_from_s3
    )

    t2 = PythonOperator(
        task_id = "send_joined_data",
        python_callable = _send_joined_data_to_sftp
    )

    t0 >> t1
    t1 >> t2