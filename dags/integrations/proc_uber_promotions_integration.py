from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from datetime import datetime

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

    if numero_dia_semana == 0 :

        uber_catalog_query = f"""
                select distinct  
                    p.material as SKU,
                    se.umv as Unidad_de_unidad_venta,
                    se.ean as "código de barras",
                    p.nombre as descripcion,
                    m.nombre as Marca,
                    concat('https://unimarc.vteximg.com.br', is2.imagen) as main_image_url,
                    c.n2 as Category_level_1,
                    c.n3 as Category_level_2
                from ecommdata.productos p
                left join ecommdata.sku_ean se on se.ref_id = p.ref_id 
                left join ecommdata.marcas m on m.id  = p.id_marca 
                LEFT JOIN ecommdata.imagenes_sku is2 ON is2.ref_id = p.ref_id
                LEFT JOIN ecommdata.categorias c ON p.id_categoria = c.id
                LEFT JOIN ecommdata.lista8 l ON l.material = p.material
                WHERE is2.orden = '1'
                and is2.ref_id = p.ref_id
                AND l.material IS NOT NULL
                AND c.n2 IS NOT NULL
                AND c.n3 IS NOT NULL;
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


    buffer = io.StringIO()
    df['SKU'] = df['SKU'].apply(lambda x: int(x) if pd.notnull(x) else x)
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
   #                                 QUERY PROMOCIONES                                                 #
   #####################################################################################################

def _join_promo_prices_from_s3(ds, ti):
    import json
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


        # Obtén la fecha de ejecución en formato YYYYMMDD
    exec_date_formatted = datetime.now().strftime("%Y%m%d")

    join_file_name = f"integraciones/last_millers/promotions/out/uber/{exec_date}/{exec_date_formatted}.csv"
    if s3_hook.check_for_key(join_file_name, bucket_name=s3_bucket):
        print(f"File {join_file_name} already exists on bucket: {s3_bucket}. Skipping...")
        

    uber_promotions_query = None

    if numero_dia_semana == 0 :
            uber_promotions_query = f"""
                SELECT DISTINCT 
                    lspp.material AS Sku,
                    lspp.unidad_de_medida as unidad_de_medida_venta, 
                    lspp.id_tienda AS id_de_tienda,
                    current_date AS fecha_inicio_venta ,
                    current_date + 3 AS fecha_final_venta,
                    'Descuento' AS tipo_de_promoción,
                    LEAST(lspp.precio_promocional, lspp.precio) AS precio_venta
                FROM integraciones.lm_stock_precio_promo lspp;
                """
    if numero_dia_semana == 3 :
            uber_promotions_query = f"""
                SELECT DISTINCT 
                    lspp.material AS Sku,
                    lspp.unidad_de_medida as unidad_de_medida_venta, 
                    lspp.id_tienda AS id_de_tienda,
                    current_date AS fecha_inicio_venta ,
                    current_date + 4 AS fecha_final_venta,
                    'Descuento' AS tipo_de_promoción,
                    LEAST(lspp.precio_promocional, lspp.precio) AS precio_venta
                FROM integraciones.lm_stock_precio_promo lspp;
                """
    if uber_promotions_query is not None:
        cursor.execute(uber_promotions_query)
        results = cursor.fetchall()
        columns = [i[0] for i in cursor.description]
    
    else:
         results = []

    if len(results) == 0:
        print(f"No records found. Skipping...")
        return
    

    df = pd.DataFrame(results, columns=columns)
    print(f"Number of records found on stock: {len(df.index)}")
        
    df['precio_venta'] = df['precio_venta'].apply(lambda x: int(x) if pd.notnull(x) else x)

    buffer = io.StringIO()
    df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    buffer.seek(0)

    
    
    s3_hook.load_string(buffer.getvalue(),
                key=join_file_name,
                bucket_name=s3_bucket,
                replace=True,
                encrypt=False)
    print(f"File load on S3: {join_file_name}")
    
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
            lspp.multiplicador_unidad  AS "UXV",
        case 
            when lspp.unidad_de_medida != 'KG' then (lspp.stock_unitario * lspp.multiplicador_unidad)::numeric(13,0)
            when lspp.unidad_de_medida = 'KG' then (lspp.stock_unitario * lspp.multiplicador_unidad)::numeric(13,3)
        end AS "STOCK X UMV",
        lspp.precio AS "PRICE"
        FROM integraciones.lm_stock_precio_promo lspp
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
    prefix_Promotions = f"integraciones/last_millers/promotions/out/uber/{exec_date}/"
    prefix_Catalog = f"integraciones/last_millers/stock/out/uber/Catalog/{exec_date}/"
    prefix_Stock = f"integraciones/last_millers/stock/out/uber/stock/{exec_date}/"

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    #Envio de promociones solo los dias lunes y jueves

    if numero_dia_semana == 0 or numero_dia_semana == 3 :
        s3_file_list = s3_hook.list_keys(s3_bucket, prefix=prefix_Promotions)

        print(f"Number of files found: {len(s3_file_list)}")
    
        for promotions_file in s3_file_list:
            print(promotions_file)

            promotions_object = s3_hook.get_key(promotions_file, bucket_name=s3_bucket)
            promotions_object_body = pd.read_csv(promotions_object.get()["Body"])

            output_promotions_file = promotions_file.split("/")[-1]
            print(output_promotions_file)
            print(f"File to load to SFTP Server: {output_promotions_file}")

            key_buffer = io.StringIO(ftp_rsa_key)
            p_key = paramiko.RSAKey.from_private_key(key_buffer)
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(ftp_host, username = ftp_user, port = ftp_port, pkey = p_key)
            sftp = ssh.open_sftp()

            exec_date = datetime.strptime(ds, "%Y-%m-%d")

            if numero_dia_semana == 0 :
                fecha_limite = exec_date + timedelta(days=3)
                remotePath = f"/test/delta/Archivo_promociones{output_promotions_file}_al_{fecha_limite}"
            if numero_dia_semana == 3 :
                 fecha_limite = exec_date + timedelta(days=4)
                 remotePath = f"/test/delta/Archivo_promociones{output_promotions_file}_al_{fecha_limite}"
            with sftp.open(remotePath, 'w') as f:
                 f.write(promotions_object_body.to_csv(index=False, sep=';'))
        
            ssh.close()

    #Envio de productos 
        if numero_dia_semana == 0:
         s3_file_list = s3_hook.list_keys(s3_bucket, prefix=prefix_Catalog)

        print(f"Number of files found: {len(s3_file_list)}")
    
        for products_file in s3_file_list:
            print(products_file)

            products_object = s3_hook.get_key(products_file, bucket_name=s3_bucket)
            products_object_body = pd.read_csv(products_object.get()["Body"])

            output_products_file = products_file.split("/")[-1]
            print(output_products_file)
            print(f"File to load to SFTP Server: {output_products_file}")

            key_buffer = io.StringIO(ftp_rsa_key)
            p_key = paramiko.RSAKey.from_private_key(key_buffer)
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(ftp_host, username = ftp_user, port = ftp_port, pkey = p_key)
            sftp = ssh.open_sftp()

            
            remotePath = f"/test/delta/Archivo_productos_semana_{output_products_file}"
            with sftp.open(remotePath, 'w') as f:
                 f.write(products_object_body.to_csv(index=False, sep=';'))
        
            ssh.close()
        
        #Envio de stock diario
        s3_file_list = s3_hook.list_keys(s3_bucket, prefix=prefix_Stock)

        print(f"Number of files found: {len(s3_file_list)}")
     
        for stock_file in s3_file_list:
            print(stock_file)

            stock_object = s3_hook.get_key(stock_file, bucket_name=s3_bucket)
            stock_object_body = pd.read_csv(stock_object.get()["Body"])

            output_stock_file = stock_file.split("/")[-1]
            print(output_stock_file)
            print(f"File to load to SFTP Server: {output_stock_file}")

            key_buffer = io.StringIO(ftp_rsa_key)
            p_key = paramiko.RSAKey.from_private_key(key_buffer)
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(ftp_host, username = ftp_user, port = ftp_port, pkey = p_key)
            sftp = ssh.open_sftp()

            remotePath = f"/test/delta/CS-UNI-STOCK-PRICES-{output_stock_file}"

            with sftp.open(remotePath, 'w') as f:
                 f.write(stock_object_body.to_csv(index=False, sep=';'))
        
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
    schedule_interval=None, 
    start_date=pendulum.datetime(2023, 2, 21, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    concurrency=2,
    tags=["OPS", "last_millers", "dw", "promotions", "precios","NICOLAS"],
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
        task_id = "join_Catalog_prices_from_s3",
        python_callable = _join_Catalog_from_s3
    )

    t1 = PythonOperator(
        task_id = "join_promo_prices_from_s3",
        python_callable = _join_promo_prices_from_s3
    )

    t2 = PythonOperator(
        task_id = "join_stock_prices_from_s3",
        python_callable = _join_stock_from_s3
    )

    t3 = PythonOperator(
        task_id = "send_joined_data",
        python_callable = _send_joined_data_to_sftp
    )

    t0 >> t1
    t1 >> t2
    t2 >> t3
