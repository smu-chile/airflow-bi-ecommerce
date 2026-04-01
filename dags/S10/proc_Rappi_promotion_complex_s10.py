from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

from utils.slack_utils import dag_success_slack, dag_failure_slack

import pendulum
import io
import pandas as pd
import pysftp

def _get_rappi_active_stores_s10():
    """
    Obtiene dinámicamente las tiendas de S10 habilitadas para Rappi.
    """
    query = "SELECT id_tienda FROM ecommdata_s10.tiendas WHERE last_millers_rappi = TRUE;"
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    pg_connection.close()
    return [r[0] for r in results]

def _generate_complex_promos_csv_s10(ds, ti):
    """
    Genera el archivo CSV local de promociones complejas (NxM, Nx$) para S10.
    """
    store_ids = ti.xcom_pull(task_ids='get_active_stores')
    if not store_ids:
        print("No hay tiendas activas para procesar.")
        return

    exec_date = ds.replace("-", "/")
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()

    for store_id in store_ids:
        print(f"Generando Combos para Tienda S10: {store_id}")
        
        # Ruta dedicada S10 para Complex
        s3_key = f"integraciones/last_millers/stock/out_s10/rappi/Complex/{exec_date}/store_id_{store_id}_COMBO_RAPPI.csv"
        
        # Lógica de Combos (NxM / Nx$)
        # IMPORTANTE: Corregido a Organizacion_Ventas = '3000' para S10
        complex_query = f"""
            SELECT 
                lspp.id_tienda AS store_id,
                w.fecha_inicio_de_promocion as start_date,
                w.fecha_fin_de_promocion AS end_date,
                CASE
                    WHEN w.desc_promocion IN ('COMBINACION NXM') THEN 
                    CONCAT('llevas ', CAST(w.cantidad_n::int AS VARCHAR), ' Pague ', CAST(w.cantidad_m::int AS VARCHAR))
                    WHEN w.desc_promocion IN ('COMBINACION NX$') THEN 
                    CONCAT('llevas ', CAST(w.cantidad_n::int AS VARCHAR), 'x por ', CAST(ROUND(w.precio_total_promocional, 0)::int AS VARCHAR))
                END AS description,
                CASE
                    WHEN w.desc_promocion IN ('COMBINACION NXM') THEN 
                    CONCAT('L', CAST(w.cantidad_n::int AS VARCHAR), '_P', CAST(w.cantidad_m::int AS VARCHAR))
                    WHEN w.desc_promocion IN ('COMBINACION NX$') THEN 
                    CONCAT('T', CAST(FLOOR(((lspp.precio - (w.precio_total_promocional / w.cantidad_n::int)) / lspp.precio) * 100)::int AS VARCHAR), '_U', CAST(w.cantidad_n::int AS VARCHAR))
                END AS type_format,
                CASE
                    WHEN w.desc_material LIKE '%,%' THEN REPLACE(w.desc_material, ',', '')
                    ELSE w.desc_material
                END AS name,
                (w.material::numeric::int) AS id
            FROM ecommdata_s10.workflow w 
            LEFT JOIN ecommdata_s10.tmp_stock_prices_promos_last_millers_s10 lspp ON lspp.ean = w.ean::varchar 
            WHERE w.fecha_inicio_de_promocion <= CURRENT_DATE
              AND w.fecha_fin_de_promocion >= CURRENT_DATE
              AND w.tipo_promocion::int IN (2, 7)
              AND w.organizacion_ventas = '3000'
              AND w.canal_distribucion = '10'
              AND w.id_mecanica::int NOT IN (25, 27, 36, 37, 50, 51, 53, 67, 72, 77, 93, 99, 123, 124)
              AND lspp.ean IS NOT null
              AND lspp.id_tienda = '{store_id}';
        """
        
        cursor.execute(complex_query)
        results = cursor.fetchall()
        if not results:
            print(f"Tienda {store_id} no tiene combos hoy. Saltando...")
            continue
            
        columns = [i[0] for i in cursor.description]
        df = pd.DataFrame(results, columns=columns)
        
        buffer = io.StringIO()
        df.to_csv(buffer, header=True, index=False, encoding="utf-8")
        buffer.seek(0)

        s3_hook.load_string(
            buffer.getvalue(),
            key=s3_key,
            bucket_name=s3_bucket,
            replace=True
        )
        print(f"CSV de Combos generado en S3: {s3_key}")

    cursor.close()
    pg_connection.close()

def _push_complex_promos_to_sftp_s10(ds):
    """
    Toma los CSV de S3 y los sube al servidor SFTP de Rappi.
    """
    exec_date = ds.replace("-", "/")
    prefix = f"integraciones/last_millers/stock/out_s10/rappi/Complex/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    s3_file_list = s3_hook.list_keys(s3_bucket, prefix=prefix)
    if not s3_file_list:
        print("No se encontraron CSV de combos para subir.")
        return

    # Credenciales SFTP (Mismas que M10 segun usuario)
    ftp_host = Variable.get("SFTP_RAPPI_HOST")
    ftp_user = Variable.get("SFTP_RAPPI_USER")
    ftp_pass = Variable.get("SFTP_RAPPI_PASSWORD")

    print(f"Subiendo {len(s3_file_list)} archivos al SFTP de Rappi...")

    with pysftp.Connection(host=ftp_host, username=ftp_user, password=ftp_pass) as sftp:
        for s3_file in s3_file_list:
            obj = s3_hook.get_key(s3_file, bucket_name=s3_bucket)
            body = obj.get()["Body"]
            
            output_filename = s3_file.split("/")[-1]
            remote_path = f"/sftp-allies/sftppruebas_co/{output_filename}"
            
            sftp.putfo(body, remote_path)
            print(f"Archivo {output_filename} subido a SFTP Rappi en {remote_path}")

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
}

with DAG(
    "proc_Rappi_promotion_complex_s10",
    default_args=default_args,
    description="Dispatcher S10: Combos NxM y Nx$ vía SFTP (Dedicado S10)",
    schedule_interval=None,
    start_date=pendulum.datetime(2026, 3, 30, tz="America/Santiago"),
    catchup=False,
    tags=["S10", "integraciones", "rappi", "sftp", "combos"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    t0 = PythonOperator(
        task_id="get_active_stores",
        python_callable=_get_rappi_active_stores_s10
    )

    t1 = PythonOperator(
        task_id="generate_complex_csv",
        python_callable=_generate_complex_promos_csv_s10
    )

    t2 = PythonOperator(
        task_id="upload_to_rappi_sftp",
        python_callable=_push_complex_promos_to_sftp_s10
    )

    # ACTIVACION OFICIAL: Go-Live
    t0 >> t1 >> t2
