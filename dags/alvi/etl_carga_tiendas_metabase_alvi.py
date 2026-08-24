from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator

import pendulum
import pandas as pd
import io
import sqlalchemy
import requests
import json
import time

from utils.postgres_utils import query_to_df
from utils.slack_utils import upload_bytes_to_slack, dag_success_slack, dag_failure_slack

def lista8(ds):
    promociones_query = """select distinct concat(material,'-',umv) as ref_id, id_tienda, excluido, umv
                from ecommdata_alvi.lista8 l"""
    results = query_to_df(promociones_query)
    results.columns = ["ref_id","id_tienda","excluido","umv"]
    return results

def productos():
    productos_query = """select ref_id, nombre 
                    from ecommdata_alvi.productos"""
    results = query_to_df(productos_query)
    results.columns = ["ref_id","nombre_producto"]
    return results

def tiendas():
    tiendas_query = """select id, status, nombre_tienda_janis
                    from ecommdata_alvi.tiendas t 
                    where status = 1"""
    results = query_to_df(tiendas_query)
    results.columns = ["id_tienda","status","nombre_tienda_janis"]
    return results

def skus():
    skus_query = """select ref_id, nombre_sku
                    from ecommdata_alvi.skus"""
    results = query_to_df(skus_query)
    results.columns = ["ref_id","nombre_sku"]
    return results

def producto_tienda_janis():
    # Leer el catálogo real desde nuestra nueva réplica productos_janis_api
    productos_tienda_query = """select ref_id, id_tienda, activo
                        from ecommdata_alvi.productos_janis_api
                        where activo is true or show_without_stock is true"""
    results = query_to_df(productos_tienda_query)
    results.columns = ["ref_id","id_tienda","activo"]
    results = results[["ref_id","id_tienda"]]
    return results

def load_tables_to_s3(ts, ds, ti):
    exec_date = ds.replace("-", "/")
    date_aux = ts.replace("-", "_")
    prefix = f"carga_tiendas_alvi/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    # 1. Obtener datos de origen
    df_producto_tienda_janis = producto_tienda_janis()
    df_lista_8 = lista8(ds)
    df_productos = productos()
    df_skus = skus()
    df_tiendas = tiendas()
    
    # 2. Identificar SKUs huérfanos/inválidos en base de datos para filtro de seguridad
    df_productos_sin_skus = df_productos.merge(df_lista_8, on=["ref_id"], how='left')
    df_skus_sin_producto = df_productos_sin_skus.merge(df_skus, on=["ref_id"], how='left')
    df_skus_sin_producto = df_skus_sin_producto[
        (df_skus_sin_producto["id_tienda"].notna()) &
        (df_skus_sin_producto["nombre_sku"].isna())
    ].drop_duplicates(subset=['ref_id']).reset_index(drop=True)
    lista_skus_sin_producto = df_skus_sin_producto["ref_id"].to_list()

    # 3. Filtrar tiendas activas en la lista8
    series_active_stores = df_tiendas['id_tienda'].unique()
    df_lista_8 = df_lista_8[df_lista_8['id_tienda'].isin(series_active_stores)]
    
    # 4. Separar lista8 en permitidos y excluidos (manejando nulos como False)
    df_lista_8['excluido'] = df_lista_8['excluido'].fillna(False)
    df_lista8_active = df_lista_8[df_lista_8['excluido'] == False].copy()
    df_lista8_excluded = df_lista_8[(df_lista_8['excluido'] == True) & (df_lista_8['umv'].isin(['UN', 'KG', 'KGV']))].copy()
    
    df_producto_tienda_janis = df_producto_tienda_janis[
        df_producto_tienda_janis['id_tienda'].isin(series_active_stores) |
        (df_producto_tienda_janis['id_tienda'] == '3188')
    ]
    df_producto_tienda_janis_sorted = df_producto_tienda_janis.sort_values(by=['ref_id', 'id_tienda'])
    df_janis_grouped = df_producto_tienda_janis_sorted.groupby('ref_id')['id_tienda'].apply(
        lambda x: ','.join(sorted(x.dropna().unique()))
    ).reset_index(name='stores_janis')
    
    # Obtener el show_without_stock de la tabla de API de Janis
    df_janis_full = query_to_df("select ref_id, show_without_stock from ecommdata_alvi.productos_janis_api")
    df_janis_full = df_janis_full.drop_duplicates(subset=['ref_id'])
    
    # 6. Agrupar tiendas permitidas hoy en la lista8
    df_lista8_active_sorted = df_lista8_active.sort_values(by=['ref_id', 'id_tienda'])
    df_active_grouped = df_lista8_active_sorted.groupby('ref_id')['id_tienda'].apply(
        lambda x: ','.join(sorted(x.dropna().unique()))
    ).reset_index(name='stores_target')
    
    # 7. Calcular Altas y Cambios de Tienda (Caso A)
    target_show_unavailable = int(Variable.get("ALVI_SHOW_UNAVAILABLE_ACTIVE", default_var="1"))
    target_show_without_stock_bool = bool(target_show_unavailable == 1)

    df_merge = df_active_grouped.merge(df_janis_grouped, on='ref_id', how='left')
    df_merge = df_merge.merge(df_janis_full, on='ref_id', how='left')
    df_merge = df_merge[df_merge['ref_id'].isin(df_janis_full['ref_id'])]
    
    df_to_update = df_merge[
        (df_merge['stores_janis'].isna()) | 
        (df_merge['stores_janis'] != df_merge['stores_target']) |
        (df_merge['show_without_stock'].fillna(not target_show_without_stock_bool).astype(bool) != target_show_without_stock_bool)
    ]
    
    df_changes_final = pd.DataFrame()
    if not df_to_update.empty:
        df_changes_final['refId'] = df_to_update['ref_id']
        df_changes_final['stores'] = df_to_update['stores_target']
        df_changes_final['publish'] = 1
        df_changes_final['updatePending'] = 1
        df_changes_final['visible'] = 1
        df_changes_final['active'] = 1
        df_changes_final['showUnavailable'] = target_show_unavailable
    else:
        df_changes_final['refId'] = pd.Series(dtype='str')
        df_changes_final['stores'] = pd.Series(dtype='str')
        df_changes_final['publish'] = pd.Series(dtype='int')
        df_changes_final['updatePending'] = pd.Series(dtype='int')
        df_changes_final['visible'] = pd.Series(dtype='int')
        df_changes_final['active'] = pd.Series(dtype='int')
        df_changes_final['showUnavailable'] = pd.Series(dtype='int')

    # 8. Calcular Bajas / Desactivaciones (Caso B & C)
    # Productos activos en Janis que ya no tienen tiendas permitidas en la lista8
    df_to_deactivate = df_janis_grouped[~df_janis_grouped['ref_id'].isin(df_active_grouped['ref_id'])].copy()
    
    df_desactivados_productos = pd.DataFrame()
    df_desactivados_sku = pd.DataFrame()
    
    if not df_to_deactivate.empty:
        df_desactivados_productos['refId'] = df_to_deactivate['ref_id']
        df_desactivados_productos['stores'] = "3188"
        df_desactivados_productos['publish'] = 1
        df_desactivados_productos['updatePending'] = 1
        df_desactivados_productos['visible'] = 0
        df_desactivados_productos['active'] = 0
        df_desactivados_productos['showUnavailable'] = 0
        
        df_desactivados_sku['refId'] = df_to_deactivate['ref_id']
        df_desactivados_sku['publish'] = 1
        df_desactivados_sku['updatePending'] = 1
        df_desactivados_sku['active'] = 0
    else:
        df_desactivados_productos['refId'] = pd.Series(dtype='str')
        df_desactivados_productos['stores'] = pd.Series(dtype='str')
        df_desactivados_productos['publish'] = pd.Series(dtype='int')
        df_desactivados_productos['updatePending'] = pd.Series(dtype='int')
        df_desactivados_productos['visible'] = pd.Series(dtype='int')
        df_desactivados_productos['active'] = pd.Series(dtype='int')
        df_desactivados_productos['showUnavailable'] = pd.Series(dtype='int')
        
        df_desactivados_sku['refId'] = pd.Series(dtype='str')
        df_desactivados_sku['publish'] = pd.Series(dtype='int')
        df_desactivados_sku['updatePending'] = pd.Series(dtype='int')
        df_desactivados_sku['active'] = pd.Series(dtype='int')

    # 9. Consolidar Dataframes Finales para CSV
    df_final_productos = pd.concat([df_changes_final, df_desactivados_productos], axis=0).reset_index(drop=True)
    
    df_final_skus_active = df_changes_final[["refId", "publish", "updatePending", "active"]].copy()
    df_final_skus = pd.concat([df_final_skus_active, df_desactivados_sku], axis=0).reset_index(drop=True)
    
    # Aplicar filtros de seguridad
    df_final_skus = df_final_skus[~df_final_skus['refId'].isin(lista_skus_sin_producto)]
    df_final_productos = df_final_productos[~df_final_productos['refId'].isin(lista_skus_sin_producto)]

    # 10. Generar y Subir Archivos a S3
    buffer_1 = io.StringIO()
    df_final_productos.to_csv(buffer_1, header=True, index=False, encoding="utf-8")
    buffer_1.seek(0)
    
    buffer_2 = io.StringIO()
    df_final_skus.to_csv(buffer_2, header=True, index=False, encoding="utf-8")
    buffer_2.seek(0)

    filename_productos = f"carga_tiendas_alvi/{exec_date}/productos_{date_aux}.csv"
    filename_skus = f"carga_tiendas_alvi/{exec_date}/skus_{date_aux}.csv"

    print(f"Subiendo a S3: {filename_productos}")
    s3_hook.load_string(buffer_1.getvalue(), key=filename_productos, bucket_name=s3_bucket, replace=True)
    
    print(f"Subiendo a S3: {filename_skus}")
    s3_hook.load_string(buffer_2.getvalue(), key=filename_skus, bucket_name=s3_bucket, replace=True)

    # 11. Generar payload de stock 0 para productos excluidos (Caso B)
    stock_payload = []
    for _, row in df_lista8_excluded.iterrows():
        sku_code = row['ref_id']
        material = str(sku_code.split('-')[0]).zfill(18)
        id_tienda = str(row['id_tienda']).zfill(4)
        stock_payload.append({
            "IdSku": material,
            "Quantity": 0,
            "Store": id_tienda,
            "Type": 1
        })
        
    print(f"Total stock 0 payloads a inyectar por exclusión: {len(stock_payload)}")
    ti.xcom_push(key="stock_0_payload", value=stock_payload)

    return filename_productos, filename_skus

def load_tables_to_postgres(ti):
    filename_productos, filename_skus = ti.xcom_pull(key="return_value", task_ids=["load_tables_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    # Descargar productos
    if not s3_hook.check_for_key(filename_productos, bucket_name=s3_bucket):
        raise Exception("Key %s products does not exist." % filename_productos)
    obj_prod = s3_hook.get_key(filename_productos, bucket_name=s3_bucket)
    df_productos = pd.read_csv(obj_prod.get()["Body"])

    # Descargar SKUs
    if not s3_hook.check_for_key(filename_skus, bucket_name=s3_bucket):
        raise Exception("Key %s skus does not exist." % filename_skus)
    obj_skus = s3_hook.get_key(filename_skus, bucket_name=s3_bucket)
    df_skus = pd.read_csv(obj_skus.get()["Body"])

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    # Asegurar que existe la columna showUnavailable en la tabla Postgres
    print("Verificando/Alterando tabla carga_productos en Postgres...")
    with engine.begin() as conn:
        conn.execute(sqlalchemy.text('ALTER TABLE ecommdata_alvi.carga_productos ADD COLUMN IF NOT EXISTS "showUnavailable" integer DEFAULT 0;'))

    df_lista = [df_productos, df_skus]
    names = ["carga_productos", "carga_skus"]

    for i in [0, 1]:
        with engine.begin() as conn:
            conn.execute(sqlalchemy.text(f"TRUNCATE ecommdata_alvi.{names[i]}"))
            df_lista[i].to_sql(
                name=names[i],
                con=conn,         
                schema="ecommdata_alvi",         
                if_exists='append',         
                index=False,         
                chunksize=20000,         
                method='multi'
            )

    print("Datos cargados exitosamente a Postgres.")

def get_and_send_cargas_csv():
    pg_hook   = PostgresHook(postgres_conn_id="postgresql_conn")
    engine    = pg_hook.get_sqlalchemy_engine()
    fecha_str = str(pendulum.now("America/Santiago").date())

    SQL_PRODUCTOS = """
        select "refId", stores, publish, "updatePending", visible, active, "showUnavailable"
        from ecommdata_alvi.carga_productos
    """
    SQL_SKUS = """
        select "refId", publish, "updatePending", active
        from ecommdata_alvi.carga_skus
    """

    df_prod = pd.read_sql(SQL_PRODUCTOS, engine)
    df_skus = pd.read_sql(SQL_SKUS, engine)

    buf_prod = io.StringIO()
    buf_skus = io.StringIO()
    df_prod.to_csv(buf_prod, sep=';', index=False)
    df_skus.to_csv(buf_skus, sep=';', index=False)

    bytes_prod = buf_prod.getvalue().encode("utf-8")
    bytes_skus = buf_skus.getvalue().encode("utf-8")

    file_prod = f"carga_productos_{fecha_str}.csv"
    file_skus = f"carga_skus_{fecha_str}.csv"
    
    comment = "📎<!channel> [Alvi] Ya se puede cargar {name}! :cat0:"

    upload_bytes_to_slack(
        file_name=file_prod,
        data_bytes=bytes_prod,
        channel_var_name="token_slack_carga_tiendas",
        initial_comment=comment.format(name=file_prod),
    )

    upload_bytes_to_slack(
        file_name=file_skus,
        data_bytes=bytes_skus,
        channel_var_name="token_slack_carga_tiendas",
        initial_comment=comment.format(name=file_skus),
    )

    print(f"✅ CSVs enviados a Slack: {file_prod}, {file_skus}")

def post_stock_chunk_carga_tiendas(session, url, headers, chunk, batch_index):
    payload_json = json.dumps(chunk)
    print(f"Enviando lote {batch_index} ({len(chunk)} registros)...")
    response = session.post(url, headers=headers, data=payload_json, timeout=60)
    if response.status_code != 200:
        print(f"⚠️ Error en lote {batch_index}. Código: {response.status_code} | Respuesta: {response.text}")
    else:
        print(f"Lote {batch_index} enviado exitosamente: {response.status_code}")
    return batch_index, response.status_code

def send_excluded_stock_0_to_janis(ti):
    # 1. Obtener credenciales de Janis Alvi
    janis_api_key = Variable.get("JANIS_ALVI_API_KEY")
    janis_api_secret = Variable.get("JANIS_ALVI_API_SECRET")
    janis_client = Variable.get("JANIS_ALVI_CLIENT")
    janis_base_url = Variable.get("JANIS_API_URL")
    
    if not all([janis_api_key, janis_api_secret, janis_client, janis_base_url]):
        raise Exception("Faltan variables de configuración de Janis (JANIS_API_URL) o exclusivas de Alvi (JANIS_ALVI_API_KEY, JANIS_ALVI_API_SECRET, JANIS_ALVI_CLIENT).")
        
    url = f"{janis_base_url.rstrip('/')}/stock"
    
    # 2. Obtener el payload de stock 0 de XCom
    stock_payload = ti.xcom_pull(key="stock_0_payload", task_ids=["load_tables_to_s3"])
    
    if not stock_payload:
        print("No hay stocks en 0 que inyectar para excluidos hoy.")
        return
        
    print(f"Iniciando envío multihilo de {len(stock_payload)} registros de stock 0 a Janis...")
    
    headers = {
        "janis-api-key": janis_api_key,
        "janis-api-secret": janis_api_secret,
        "janis-client": janis_client,
        "Content-Type": "application/json",
        "Connection": "keep-alive"
    }
    
    from requests.adapters import HTTPAdapter
    from urllib3.util import Retry
    from concurrent.futures import ThreadPoolExecutor, as_completed

    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=10))
    session.mount('http://', HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=10))

    chunk_size = 500
    chunks = [stock_payload[i:i + chunk_size] for i in range(0, len(stock_payload), chunk_size)]
    
    max_workers = 5
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(post_stock_chunk_carga_tiendas, session, url, headers, chunk, idx + 1)
            for idx, chunk in enumerate(chunks)
        ]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error enviando lote en paralelo: {e}")

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'etl_carga_tiendas_metabase_alvi',
    default_args=default_args,
    description="cargar tabla de productos y skus de carga tiendas",
    schedule_interval="0 7 * * *",
    start_date=pendulum.datetime(2023, 12, 6, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "tiendas", "ecommdata", "metabase", "alvi", "MAURICIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    t0 = ExternalTaskSensor(
        task_id="wait_lista8_alvi",
        external_dag_id='etl_lista8_alvi_datastage_truncate_and_load',
        external_task_id=None,
        allowed_states=['success'],
        failed_states=['failed']
    )
    
    # Sensor adicional para esperar que termine la sincronización del catálogo por API
    t_wait_sync = ExternalTaskSensor(
        task_id="wait_janis_api_sync",
        external_dag_id='etl_sync_catalogo_janis_alvi',
        external_task_id='sync_catalogo_api',
        allowed_states=['success'],
        failed_states=['failed'],
        execution_delta=pendulum.duration(hours=2, minutes=30),
        timeout=1800,
        poke_interval=60,
        mode='reschedule'
    )

    t1 = PythonOperator(
        task_id = 'load_tables_to_s3',
        python_callable=load_tables_to_s3,
    )

    t2 = PythonOperator(
        task_id = "load_tables_to_postgres",
        python_callable = load_tables_to_postgres,
    )

    t3 = PythonOperator(
        task_id = "get_and_send_cargas_csv",
        python_callable = get_and_send_cargas_csv,
    )    

    t4 = PythonOperator(
        task_id = "send_excluded_stock_0_to_janis",
        python_callable = send_excluded_stock_0_to_janis,
    )

    [t0, t_wait_sync] >> t1
    t1 >> t2 >> t3
    t1 >> t4