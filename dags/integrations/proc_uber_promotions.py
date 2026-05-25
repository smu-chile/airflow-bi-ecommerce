from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from datetime import datetime
import pendulum

from utils.slack_utils import dag_success_slack, dag_failure_slack

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
    WITH promociones_base AS (
        SELECT DISTINCT
            wp.ean,
            wp.material,
            wp.precio_modal,
            wp.umv,
            wp.desc_promocion,
            wp.cantidad_n,
            wp.cantidad_m,
            wp.precio_promocional,
            wp.precio_total_promocional,
            wp.fecha_fin_de_promocion
        FROM ecommdata.workflow_promociones wp
        LEFT JOIN ecommdata.productos ep ON ep.material = wp.material
        LEFT JOIN ecommdata.categorias ec ON ep.id_categoria = ec.id
        WHERE wp.fecha_inicio_de_promocion <= CURRENT_DATE
          AND wp.fecha_fin_de_promocion >= CURRENT_DATE
          AND (ec.n1 NOT IN ('No Trabajar', 'Inactivos','Integración') OR ec.n1 IS NULL)
          AND wp.tipo_promocion IN (1, 2, 4, 7)
          AND wp.registro_valido = TRUE
          AND wp.cantidad_n < 10
          AND wp.organizacion_ventas = '1000'
          AND wp.canal_distribucion = '10'
          AND wp.id_mecanica NOT IN (25, 27, 36, 37, 50, 51, 53, 67, 72, 77, 84, 93, 99, 123, 124)
          AND wp.nombre_promocion NOT ILIKE '%ZONA%'
          AND wp.nombre_promocion NOT ILIKE '%MFC%'
          AND wp.nombre_promocion NOT ILIKE '%BANCO%'
          AND wp.nombre_promocion NOT ILIKE '%UNIPAY%'
          AND wp.nombre_promocion NOT ILIKE '%TERCERA%'
          AND wp.nombre_promocion NOT ILIKE '%917%'
          AND wp.nombre_promocion NOT ILIKE '%ESTADO%'
          AND wp.nombre_promocion NOT ILIKE '% LOC%'
          AND wp.nombre_promocion NOT ILIKE '%CYBER%'
          AND wp.nombre_promocion NOT ILIKE '%LIQ%'
          AND wp.n_promocion NOT IN ('5552392024', '1120012024', '1120022024', '1120032024', '1120042024', '1120052024',
                                     '1120062024', '1120082024', '1120092024', '1120102024', '1120112024', '1120122024', 
                                     '4000512024','1120012025','1120022025','1120032025','1120042025','1120212025')
    ),
    promociones_filtradas AS (
        SELECT
            pb.ean                                              AS ean,
            tlm.id                                              AS id_de_tienda,
            pb.material                                         AS sku,
            pb.precio_modal                                     AS price,
            CASE WHEN pb.umv = 'ST' THEN 'UN' ELSE pb.umv END  AS unidad_de_medida_venta,
            CASE
                WHEN pb.desc_promocion IN ('PRECIO FIJO', '% DE DESCUENTO') THEN 'descuento'
                ELSE 'pack'
            END                                                 AS tipo_de_promoción,
            CASE
                WHEN pb.desc_promocion = 'COMBINACION NXM' THEN CONCAT(pb.cantidad_n, 'x', pb.cantidad_m)
                WHEN pb.desc_promocion = 'COMBINACION NX$' THEN CONCAT(pb.cantidad_n, 'x')
                ELSE 'null'
            END                                                 AS combinacion,
            pb.precio_promocional                               AS precio_venta_individual,
            pb.precio_total_promocional                         AS precio_venta_total,
            CURRENT_DATE                                        AS fecha_inicio_venta,
            pb.fecha_fin_de_promocion                           AS fecha_final_venta,
            ROW_NUMBER() OVER (
                PARTITION BY pb.material, tlm.id
                ORDER BY pb.precio_promocional ASC
            )                                                   AS rn
        FROM promociones_base pb
        LEFT JOIN integraciones.stock s ON s.sku_product = pb.material
        LEFT JOIN integraciones.tiendas_last_millers tlm ON tlm.id_uber IS NOT NULL
        INNER JOIN integraciones.productos p ON p.ean = pb.ean
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
        promotions_object_body = pd.read_csv(promotions_object.get()["Body"], dtype={"ean": str})

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
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
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