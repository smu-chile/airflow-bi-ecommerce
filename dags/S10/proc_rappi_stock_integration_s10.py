from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

from utils.slack_utils import dag_success_slack, dag_failure_slack
from utils.postgres_utils import query_to_df

import pendulum
import json
import requests

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

def _join_stock_and_promo_prices_from_db_s10(ds, ti):
    """
    Toma los datos masticados de la tabla de integración S10 y genera los JSON en S3.
    """
    store_ids = ti.xcom_pull(task_ids='get_active_stores')
    if not store_ids:
        print("No hay tiendas activas para procesar.")
        return

    exec_date = ds.replace("-", "/")
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    for store_id in store_ids:
        print(f"Procesando Tienda S10: {store_id}")
        
        # Ruta S10 dedicada en S3
        s3_key = f"integraciones/last_millers/stock/out_s10/rappi/{exec_date}/{store_id}.json"
        
        # Query de transformación exacta (Espejo M10 pero apuntando a tablas S10)
        # 1. Ajusta ID material (con multiplicador si aplica)
        # 2. Ajusta Stock (considerando divisor de pack)
        # 3. LEAST entre base y promo para el precio final de descuento
        query = f"""
            SELECT 
                lspp.id_tienda as store_id,
                CASE 
                    WHEN (lspp.multiplicador_unidad > 1 AND lspp.unidad_de_medida NOT IN ('KG', 'KGV')) 
                    THEN (lspp.material::numeric::int)::varchar || '_' || lspp.multiplicador_unidad
                    ELSE (lspp.material::numeric::int)::varchar 
                END as id,
                CASE 
                    WHEN lspp.unidad_de_medida IN ('KG', 'KGV') THEN lspp.stock_unitario::int
                    ELSE (lspp.stock_unitario/lspp.multiplicador_unidad)::int
                END as stock,
                lspp.nombre as "name",
                lspp.ean as ean,
                lspp.precio::int as price,
                LEAST(lspp.precio_promocional, lspp.precio)::int as discount_price,
                lspp.marca as trademark,
                CASE 
                    WHEN lspp.unidad_de_medida IN ('KG', 'KGV') THEN 'WW'
                    ELSE 'U'
                END as sale_type
            FROM ecommdata_s10.tmp_stock_prices_promos_last_millers_s10 lspp
            WHERE lspp.id_tienda = '{store_id}';
        """
        
        df = query_to_df(query)
        if df.empty:
            print(f"Tienda {store_id} no tiene registros en la tabla de integración. Saltando...")
            continue

        df["is_available"] = True
        
        # S10/Rappi CLP no usa decimales. Forzamos a int para evitar el ".0" en el JSON
        # Usamos fillna(0) por seguridad antes de cast a int para evitar el error con NaNs
        df["price"] = df["price"].fillna(0).astype(int)
        df["discount_price"] = df["discount_price"].fillna(df["price"]).astype(int)
        
        # Generación de JSON con formato Rappi (usamos to_json para manejar Decimals)
        json_body = df.to_json(orient="records")

        s3_hook.load_string(
            json_body,
            key=s3_key,
            bucket_name=s3_bucket,
            replace=True,
            encrypt=False
        )
        print(f"JSON generado y subido a S3: {s3_key}")

def _send_joined_data_to_api_s10(ds):
    """
    Consume los JSON de S3 y los dispara al endpoint de Rappi.
    """
    exec_date = ds.replace("-", "/")
    prefix = f"integraciones/last_millers/stock/out_s10/rappi/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    s3_file_list = s3_hook.list_keys(s3_bucket, prefix=prefix)
    if not s3_file_list:
        print("No se encontraron archivos en S3 para enviar.")
        return

    print(f"Enviando {len(s3_file_list)} archivos a la API de Rappi...")
    
    # S10 usa las mismas credenciales que M10 según instrucción del usuario
    rappi_endpoint = Variable.get("RAPPI_ENDPOINT_M10")
    headers = {
        "api_key": Variable.get("RAPPI_API_KEY_M10"),
        "Content-Type": "application/json"
    }

    responses_prefix = f"rappi/api/stock_s10/post/full/responses/{exec_date}/"

    for stock_file in s3_file_list:
        print(f"Enviando archivo: {stock_file}")
        
        obj = s3_hook.get_key(stock_file, bucket_name=s3_bucket)
        json_data = json.loads(obj.get()["Body"].read())
        
        payload = {"records": json_data}
        
        try:
            response = requests.post(url=rappi_endpoint, json=payload, headers=headers)
            print(f"Status: {response.status_code}")
            
            # Guardar respuesta para auditoría
            resp_body = json.dumps(response.json())
            s3_hook.load_string(
                resp_body,
                key=responses_prefix + stock_file.split("/")[-1],
                bucket_name=s3_bucket,
                replace=True
            )
        except Exception as e:
            print(f"Error al enviar {stock_file}: {e}")

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
}

with DAG(
    "proc_rappi_stock_integration_s10",
    default_args=default_args,
    description="Dispatcher final S10: Stock y Precios para Rappi (Dedicado S10)",
    schedule_interval=None, # Gatillado manualmente o por el integrador
    start_date=pendulum.datetime(2024, 6, 1, tz="America/Santiago"),
    catchup=False,
    tags=["S10", "integraciones", "rappi", "api", "last-millers", "MAURICIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    t0 = PythonOperator(
        task_id="get_active_stores",
        python_callable=_get_rappi_active_stores_s10
    )

    t1 = PythonOperator(
        task_id="generate_json_to_s3",
        python_callable=_join_stock_and_promo_prices_from_db_s10
    )

    t2 = PythonOperator(
        task_id="push_to_rappi_api",
        python_callable=_send_joined_data_to_api_s10
    )

    # ACTIVACION OFICIAL: Go-Live
    t0 >> t1 >> t2
