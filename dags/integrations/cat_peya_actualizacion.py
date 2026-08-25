from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

from utils.slack_utils import dag_success_slack, dag_failure_slack

import pendulum
import pandas as pd
import io
import mysql.connector

def _process_catalog(ds=None, **kwargs):
    print("Iniciando proceso de actualización del catálogo Peya...")
    
    # 1. Ejecutar query de catalogo peya postgres (sin volumen)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    
    # Leer query del archivo SQL
    sql_path = "/opt/airflow/dags/integrations/sql/actualizacion_catalogo_peya.sql"
    print(f"Leyendo query desde {sql_path}...")
    with open(sql_path, "r") as f:
        query = f.read()
        
    print("Ejecutando consulta del catálogo en PostgreSQL...")
    df_catalog = pg_hook.get_pandas_df(query)
    print(f"Número de registros obtenidos del catálogo: {len(df_catalog)}")
    
    if df_catalog.empty:
        print("El catálogo está vacío. Deteniendo ejecución.")
        return
    
    # Asegurar que SKU sea string para el cruce
    df_catalog['SKU'] = df_catalog['SKU'].astype(str).str.strip()
    
    # 2. Descargar archivo Excel desde S3
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME", default_var="s3-bi-ecommerce-prod")
    if not s3_bucket:
        s3_bucket = "s3-bi-ecommerce-prod"
        
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
    file_key = "actualizacion_catalogo/products_peya.xlsx"
    
    print(f"Verificando si existe el archivo {file_key} en el bucket {s3_bucket}...")
    if not s3_hook.check_for_key(file_key, bucket_name=s3_bucket):
        raise FileNotFoundError(f"El archivo {file_key} no existe en el bucket {s3_bucket}.")
        
    print("Descargando archivo Excel de S3...")
    s3_object = s3_hook.get_key(file_key, bucket_name=s3_bucket)
    excel_bytes = s3_object.get()["Body"].read()
    
    print("Cargando archivo Excel en Pandas...")
    df_excel = pd.read_excel(io.BytesIO(excel_bytes), engine="openpyxl")
    print(f"Número de filas cargadas del Excel: {len(df_excel)}")
    
    # Normalizar nombres de columnas a minúsculas
    df_excel.columns = [c.lower().strip() for c in df_excel.columns]
    if 'sku' not in df_excel.columns:
        raise ValueError(f"La columna 'sku' no fue encontrada en el archivo Excel. Columnas: {df_excel.columns.tolist()}")
        
    # Limpiar y extraer skus del S3
    s3_skus = df_excel['sku'].dropna().astype(str).str.strip().unique()
    print(f"Número de SKUs únicos en el archivo Excel de S3: {len(s3_skus)}")
    
    # 3. Separar productos existentes de productos nuevos
    # Conjunto 1: Productos de la query que existen en S3
    df_existentes = df_catalog[df_catalog['SKU'].isin(s3_skus)].copy()
    print(f"Productos existentes (cruzados con S3): {len(df_existentes)}")
    
    # Conjunto 2: Productos de la query que NO existen en S3
    df_nuevos = df_catalog[~df_catalog['SKU'].isin(s3_skus)].copy()
    print(f"Productos nuevos (no están en S3): {len(df_nuevos)}")
    
    # Conectarse a MariaDB solo si hay productos nuevos para cruzar
    if not df_nuevos.empty:
        print("Conectándose a MariaDB para obtener dimensiones físicas de los SKUs nuevos...")
        conn = mysql.connector.connect(
            user=Variable.get("JANIS_MARIADB_USER"),
            password=Variable.get("JANIS_MARIADB_PASSWORD"),
            host=Variable.get("JANIS_MARIADB_HOST"),
            port=3306,
            database=Variable.get("JANIS_MARIADB_DATABASE")
        )
        mariadb_query = """
        SELECT 
            s.ref_id, 
            s.freight_height AS alto, 
            s.freight_length AS largo,
            s.freight_width AS ancho, 
            s.freight_weight AS peso 
        FROM skus s
        """
        cur = conn.cursor()
        cur.execute(mariadb_query)
        results = cur.fetchall()
        columns = [i[0] for i in cur.description]
        cur.close()
        conn.close()
        
        df_mariadb = pd.DataFrame(results, columns=columns)
        df_mariadb['ref_id'] = df_mariadb['ref_id'].astype(str).str.strip()
        print(f"Número de SKUs extraídos desde MariaDB: {len(df_mariadb)}")
        
        # Realizar el cruce con MariaDB
        df_nuevos_dimensiones = df_nuevos.merge(df_mariadb, left_on='SKU', right_on='ref_id', how='left')
        if 'ref_id' in df_nuevos_dimensiones.columns:
            df_nuevos_dimensiones.drop(columns=['ref_id'], inplace=True)
    else:
        print("No hay productos nuevos para cruzar con MariaDB.")
        # Generar dataframe vacío con las columnas esperadas más dimensiones
        df_nuevos_dimensiones = df_nuevos.copy()
        for col in ['alto', 'largo', 'ancho', 'peso']:
            df_nuevos_dimensiones[col] = None

    # 4. Guardar resultados en S3
    fecha_hoy = pendulum.now("America/Santiago").strftime("%Y-%m-%d")
    output_prefix = f"actualizacion_catalogo/output/{fecha_hoy}/"
    print(f"Preparando archivos de salida en el prefijo S3: {output_prefix}...")
    
    # Archivo 1: Productos existentes
    file_existentes_key = f"{output_prefix}productos_existentes.xlsx"
    buf_existentes = io.BytesIO()
    with pd.ExcelWriter(buf_existentes, engine='openpyxl') as writer:
        df_existentes.to_excel(writer, index=False)
    buf_existentes.seek(0)
    
    print(f"Subiendo {file_existentes_key} a S3...")
    s3_hook.load_bytes(
        buf_existentes.getvalue(),
        key=file_existentes_key,
        bucket_name=s3_bucket,
        replace=True
    )
    
    # Archivo 2: Productos nuevos con dimensiones
    file_nuevos_key = f"{output_prefix}productos_nuevos_dimensiones.xlsx"
    buf_nuevos = io.BytesIO()
    with pd.ExcelWriter(buf_nuevos, engine='openpyxl') as writer:
        df_nuevos_dimensiones.to_excel(writer, index=False)
    buf_nuevos.seek(0)
    
    print(f"Subiendo {file_nuevos_key} a S3...")
    s3_hook.load_bytes(
        buf_nuevos.getvalue(),
        key=file_nuevos_key,
        bucket_name=s3_bucket,
        replace=True
    )
    
    print("Proceso finalizado con éxito.")

default_args = {
    "owner": "ecommerce_ops",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    "cat_peya_actualizacion",
    default_args=default_args,
    description="Cruce de catálogo Postgres con excel en S3 y dimensiones físicas de MariaDB para Pedidos Ya",
    schedule_interval=None,
    start_date=pendulum.datetime(2023, 2, 21, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=["OPS", "last_millers", "dw", "catalog", "peya", "RODRIGO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:
    
    process_task = PythonOperator(
        task_id="procesar_actualizacion_catalogo",
        python_callable=_process_catalog
    )
