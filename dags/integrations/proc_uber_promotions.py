from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from datetime import datetime
import pendulum

 #####################################################################################################
 #                                 QUERY Promociones complejas                                       #
 #####################################################################################################
 
def _join_promo_prices_test_from_s3(ds, ti):
    import json
    import pandas as pd
    import io
    # Obtener la fecha actual
    fecha_actual = datetime.strptime(ds, "%Y-%m-%d")

    exec_date = ds.replace("-", "/")

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
    
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()


    # Obtén la fecha de ejecución en formato YYYYMMDD
    exec_date_formatted = datetime.now().strftime("%Y%m%d")

    join_file_name = f"integraciones/last_millers/promotions/out/uber/Test_{exec_date}/{exec_date_formatted}.csv"

    uber_promotions_query = None

    uber_promotions_query = f""" 
    WITH promociones_filtradas AS (
    SELECT DISTINCT 
        p.ean AS ean,
        tlm.id AS id_de_tienda,
        wp.material AS sku,
        wp.precio_modal AS price,
        CASE
            WHEN wp.umv = 'ST' THEN 'UN'
            ELSE wp.umv
        END AS unidad_de_medida_venta,
        CASE
            WHEN wp.desc_promocion IN ('PRECIO FIJO', '% DE DESCUENTO') THEN 'descuento'
            ELSE 'pack'
        END AS tipo_de_promoción,
        CASE
            WHEN wp.desc_promocion IN ('COMBINACION NXM') THEN CONCAT(wp.cantidad_n, 'x', wp.cantidad_m)
            WHEN wp.desc_promocion IN ('COMBINACION NX$') THEN CONCAT(wp.cantidad_n, 'x')
            ELSE 'null'
        END AS combinacion,
        wp.precio_promocional AS precio_venta_individual,
        wp.precio_total_promocional AS precio_venta_total,
        CURRENT_DATE AS fecha_inicio_venta,
        wp.fecha_fin_de_promocion AS fecha_final_venta,
        ROW_NUMBER() OVER (PARTITION BY wp.material, tlm.id ORDER BY wp.precio_promocional ASC) AS rn
    FROM ecommdata.workflow_promociones wp
    LEFT JOIN integraciones.stock s ON s.sku_product = wp.material
    LEFT JOIN integraciones.tiendas_last_millers tlm ON tlm.id_uber IS NOT NULL
    LEFT JOIN integraciones.productos p ON s.sku_key = p.sku_key AND p.ean = wp.ean
    WHERE wp.fecha_inicio_de_promocion <= CURRENT_DATE 
      AND wp.fecha_fin_de_promocion >= CURRENT_DATE
      AND wp.tipo_promocion IN (1, 2, 4, 7)
      AND wp.registro_valido = TRUE
      AND wp.organizacion_ventas = '1000'
      AND wp.canal_distribucion = '10'
      AND wp.id_mecanica NOT IN (25, 27, 36, 37, 50, 51, 53, 67, 72, 77, 84, 93, 99, 123, 124)
      AND wp.nombre_promocion::TEXT !~~ '%ZONA%'::TEXT
      AND wp.nombre_promocion::TEXT !~~ '%MFC%'::TEXT
      AND wp.nombre_promocion::TEXT !~~ '%BANCO%'::TEXT
      AND wp.nombre_promocion::TEXT !~~ '%UNIPAY%'::TEXT
      AND wp.nombre_promocion::TEXT !~~ '%TERCERA%'::TEXT
      AND wp.nombre_promocion::TEXT !~~ '%917%'::TEXT
      AND wp.nombre_promocion::TEXT !~~ '%ESTADO%'::TEXT
      AND wp.nombre_promocion::TEXT !~~ '% LOC%'::TEXT
      AND wp.nombre_promocion::TEXT !~~ '%LIQ%'::text
      AND wp.n_promocion NOT IN ('5552392024', '1120012024', '1120022024', '1120032024', '1120042024', '1120052024',
                                 '1120062024', '1120082024', '1120092024', '1120102024', '1120112024', '1120122024', 
                                 '4000512024','1120012025','1120022025','1120032025','1120042025')
)
SELECT ean,
       id_de_tienda,
       sku,
       price,
       unidad_de_medida_venta,
       tipo_de_promoción,
       combinacion,
       precio_venta_individual,
       precio_venta_total,
       fecha_inicio_venta,
       fecha_final_venta
FROM promociones_filtradas
WHERE rn = 1;
    """
    cursor.execute(uber_promotions_query)  # Ejecuta la consulta directamente
    results = cursor.fetchall()  # Obtiene los resultados
    columns = [i[0] for i in cursor.description]  # Extrae los nombres de las columnas

    if not results:  # Verifica si los resultados están vacíos
        print(f"No records found. Skipping...")
        return
    
    df = pd.DataFrame(results, columns=columns)
    print(f"Number of records found on stock: {len(df.index)}")
        
    df['precio_venta_individual'] = df['precio_venta_individual'].apply(lambda x: int(x) if pd.notnull(x) else x)

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

     #Variable de los datos

    ftp_host = Variable.get("UBER_SFTP_HOST")
    ftp_port = 2222
    ftp_user = Variable.get("UBER_SFTP_USER")
    ftp_rsa_key = Variable.get("UBER_SFTP_SECRET_RSA_KEY")

    #Datos de los envios

    exec_date = ds.replace("-", "/")

    prefix_test = f"integraciones/last_millers/promotions/out/uber/Test_{exec_date}/"  #Prefix para promociones complejas

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    s3_file_list = s3_hook.list_keys(s3_bucket, prefix=prefix_test)

    print(f"Number of files found: {len(s3_file_list)}")
    
    for promotions_file_test in s3_file_list:
        print(promotions_file_test)

        promotions_object = s3_hook.get_key(promotions_file_test, bucket_name=s3_bucket)
        promotions_object_body = pd.read_csv(promotions_object.get()["Body"])

        output_promotions_file = promotions_file_test.split("/")[-1]
        print(output_promotions_file)
        print(f"File to load to SFTP Server: {output_promotions_file}")

        key_buffer = io.StringIO(ftp_rsa_key)
        p_key = paramiko.RSAKey.from_private_key(key_buffer)
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(ftp_host, username = ftp_user, port = ftp_port, pkey = p_key)
        sftp = ssh.open_sftp()

        exec_date = datetime.strptime(ds, "%Y-%m-%d")

        remotePath = f"/prod/Archivo_promociones_{output_promotions_file}"

        with sftp.open(remotePath, 'w') as f:
                 f.write(promotions_object_body.to_csv(index=False, sep=';'))
        
        ssh.close()
    print("Todo Cargado y subido al S3 de Uber")

    return

default_args = {
    "owner": "ecommerce_ops",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    "proc_uber_promotions_night_send",
    default_args=default_args,
    description="Cruce de precios y precios promocionales simples para integracion Uber",
    schedule_interval="0 0 * * *", #para que cargue a las 12:00 de la noche 
    start_date=pendulum.datetime(2023, 2, 21, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    concurrency=2,
    tags=["OPS", "last_millers", "dw", "promotions", "precios","NICOLAS","UBER"],
) as dag:

    dag.doc_md = """
    Cruce de promociones complejas, para el envio de las noche
        -Se enviara siempre la mejor promocion cargada hasta la fecha.
        -El envio sera siempre las 12:00 de las noche y se demora 6 horas en procesar, por lo cual correciones de precio x
        promociones no es posible sin perder el dia de venta.
    """ 

    t0 = PythonOperator(
        task_id = "_join_promo_prices_test_from_s3",
        python_callable = _join_promo_prices_test_from_s3
    )

    t1 = PythonOperator(
        task_id = "send_joined_data",
        python_callable = _send_joined_data_to_sftp
    )

    t0 >> t1