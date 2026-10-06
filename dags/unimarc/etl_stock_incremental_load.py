from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from utils.janis_utils import load_full_table_to_s3
from utils.slack_utils import dag_success_slack, dag_failure_slack

from datetime import datetime

import pendulum

def _get_table_stock_janis_from_S3(ts, ti):
    import pandas as pd

    stock_file = ti.xcom_pull(key="return_value", task_ids=["load_full_table_to_s3"])[0]
    print(stock_file)
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+stock_file)
    if not s3_hook.check_for_key(stock_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % stock_file)

    orders_object = s3_hook.get_key(stock_file, bucket_name=s3_bucket)

    df = pd.read_csv(orders_object.get()["Body"])
    print(f"Number of records found: {len(df.index)}")

    return df

def _save_table_stock_janis(ts, ti):
    import pandas as pd
    import sqlalchemy
    from io import StringIO
    import csv

    df = _get_table_stock_janis_from_S3(ts, ti)
    df = df[['id', 'item_id', 'store_id','warehouse_id', 'stock', 'min_stock', 'infinite_stock', 'date_published', 'date_modified', 'operation_type']]
    df = df.loc[df['stock'] >= 0]
    df["date_published"] = pd.to_datetime(df["date_published"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["date_modified"] = pd.to_datetime(df["date_modified"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")

    int_cols = ['id', 'item_id', 'stock', 'min_stock', 'infinite_stock']
    for c in int_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce').astype('Int64')

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, na_rep='\\N', quoting=csv.QUOTE_MINIMAL)
    buffer.seek(0)

    conn = engine.raw_connection()
    try:
        cursor = conn.cursor()
        cursor.copy_expert("COPY staging.stock_unimarc (id, item_id, store_id, warehouse_id, stock, min_stock, infinite_stock, date_published, date_modified, operation_type) FROM STDIN WITH CSV NULL '\\N'", buffer)
        conn.commit()
        cursor.close()
    except Exception as e:
        print(f"Error executing COPY for stock_unimarc: {e}")
        conn.rollback()
        raise e
    finally:
        conn.close()
    
    return

def load_full_table_from_staging_to_s3(table_name, df, ts):
    from io import StringIO
    import boto3
    
    curr_datetime = ts[:16].replace("-", "/").replace("T", "/").replace(":", "")
    prefix = "staging/"+table_name+"/"+curr_datetime
    file_name = prefix+table_name+".csv"

    buffer = StringIO()

    df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    buffer.seek(0)

    access_key = Variable.get("AWS_ACCESS_KEY")
    secret_key = Variable.get("AWS_SECRET_KEY")
    bucket_name = Variable.get("AWS_S3_BUCKET_NAME")
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name = "us-east-1"
    )
    response = s3_client.put_object(
        Bucket=bucket_name, Key=file_name, Body=buffer.getvalue()
    )

    return file_name

def fetch_vtex_stock(url, session, X_VTEX_API_AppKey, X_VTEX_API_AppToken, max_retries=4):
    import time
    import random
    headers = {
        "X-VTEX-API-AppKey": X_VTEX_API_AppKey,
        "X-VTEX-API-AppToken": X_VTEX_API_AppToken,
        "Accept": "application/json"
    }
    for attempt in range(max_retries):
        try:
            r = session.get(url, headers=headers, timeout=10)
            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")
                sleep_time = float(retry_after) if retry_after else (0.5 * (attempt + 1) + random.uniform(0.1, 0.3))
                time.sleep(sleep_time)
                continue
            r.raise_for_status()
            return {"status": "ok", "json": r.json(), "url": url}
        except Exception as e:
            if 'r' in locals() and r is not None and r.status_code == 429:
                sleep_time = 0.5 * (attempt + 1) + random.uniform(0.1, 0.3)
                time.sleep(sleep_time)
                continue
            time.sleep(0.3)
    return {"status": "error", "url": url}

def _load_vtex_id_list():
    query = """
        select s.vtex_id
        from ( select CONCAT(l.material, '-', l.umv) as ref_id, l.material, l.umv
            from ecommdata.lista8 l) _t
        inner join ecommdata.skus s on _t.ref_id = s.ref_id
        where s.vtex_id is not null
        UNION
        select distinct s.vtex_id
        from staging.stock_unimarc sa
        inner join ecommdata.skus s on s.id = sa.item_id
        where sa.stock > 0 and s.vtex_id is not null;
        """
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    pg_connection.close()
    return results

def _load_final_responses_to_postgres(final_responses, ts, file_name):
    import pandas as pd
    import sqlalchemy

    df = pd.DataFrame(final_responses)
    
    df = df[[
        "skuId",
        "warehouseId",
        "totalQuantity",
        "reservedQuantity",
        "hasUnlimitedQuantity",
    ]]

    df["skuId"] = df["skuId"].astype("str")
    df["warehouseId"] = df["warehouseId"].astype("str")
    df["totalQuantity"] = df["totalQuantity"].astype("int")
    df["reservedQuantity"] = df["reservedQuantity"].astype("int")
    df["hasUnlimitedQuantity"] = df["hasUnlimitedQuantity"].astype("bool")

    columns_rename = {
        "skuId": "vtex_id",
        "warehouseId": "id_warehouse",
        "totalQuantity": "cantidad_total",
        "reservedQuantity": "cantidad_reservada",
        "hasUnlimitedQuantity": "cantidad_ilimitada"
    }

    df = df.rename(columns=columns_rename)


    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    from io import StringIO
    import csv

    # Escribir el DataFrame en memoria como CSV
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, na_rep='\\N', quoting=csv.QUOTE_MINIMAL)
    buffer.seek(0)

    # Inyectar a Postgres usando COPY para máximo rendimiento
    conn = engine.raw_connection()
    try:
        cursor = conn.cursor()
        cursor.copy_expert("COPY staging.stock_vtex_unimarc (vtex_id, id_warehouse, cantidad_total, cantidad_reservada, cantidad_ilimitada) FROM STDIN WITH CSV NULL '\\N'", buffer)
        conn.commit()
        cursor.close()
    except Exception as e:
        print(f"Error executing COPY: {e}")
        conn.rollback()
        raise e
    finally:
        conn.close()

    load_full_table_from_staging_to_s3(file_name, df, ts)

    return

def _save_vtex_stock_in_ecommdata(ti, ts):
    import requests
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import pandas as pd
    import sqlalchemy
    
    l_vtex_id = _load_vtex_id_list()

    if len(l_vtex_id) == 0:
        print('the list of vtex id was empty')
        return

    accountName = Variable.get("VTEX_ACCOUNT_NAME")
    env = Variable.get("VTEX_ENV")
    url_list = [f"https://{accountName}.{env}.com.br/api/logistics/pvt/inventory/skus/{i[0]}" for i in l_vtex_id]
    
    max_workers = 20
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=max_workers, pool_maxsize=max_workers)
    session.mount('https://', adapter)

    responses = []
    exception_cases = []

    X_VTEX_API_AppKey = Variable.get("X_VTEX_API_AppKey")
    X_VTEX_API_AppToken = Variable.get("X_VTEX_API_AppToken")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {
            executor.submit(fetch_vtex_stock, url, session, X_VTEX_API_AppKey, X_VTEX_API_AppToken): url
            for url in url_list
        }
        for future in as_completed(future_to_url):
            res = future.result()
            if res["status"] == "ok":
                responses.append({'json': res['json'], 'url': res['url']})
            else:
                exception_cases.append(res['url'])
    
    session.close()

    final_responses = []

    for response in responses:
        try:
            for balance in response['json']['balance']:
                aux = balance.copy()
                aux['skuId'] = response['json']['skuId']
                final_responses.append(aux)
        except KeyError as e:
            print(e)
            print(response)
            exception_cases.append(response['url'])
    
    _load_final_responses_to_postgres(final_responses, ts, 'stock_vtex')

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    date_path = ts[:10].replace("-","/")
    s3_path = f"vtex/api/get_stock_url_retries/{date_path}/"
    retries = s3_path+"retries"

    s3_hook.load_string(str(exception_cases),retries,bucket_name=s3_bucket,replace=True)
    ti.xcom_push(key = 'vtex_retries', value = retries)

    return

def _vtex_get_stock_retries(ti, ts):
    import requests
    from concurrent.futures import ThreadPoolExecutor, as_completed

    retries_file = ti.xcom_pull(key="vtex_retries", task_ids=["save_vtex_stock_in_ecommdata"])[0]
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+retries_file)
    if not s3_hook.check_for_key(retries_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % retries_file)

    retries_object = s3_hook.get_key(retries_file, bucket_name=s3_bucket)
    retries_string = retries_object.get()["Body"].read().decode('utf-8')[1:-1]
    retries_string = retries_string.replace("'","").strip()
    retries = [x.strip() for x in retries_string.split(",") if x.strip() != ""]

    if len(retries) == 0:
        print("No retries to process.")
        return
    
    print(f"Retrying {len(retries)} URLs in parallel...")
    max_workers = min(10, len(retries))
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=max_workers, pool_maxsize=max_workers)
    session.mount('https://', adapter)

    responses = []
    exception_cases = []

    X_VTEX_API_AppKey = Variable.get("X_VTEX_API_AppKey")
    X_VTEX_API_AppToken = Variable.get("X_VTEX_API_AppToken")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {
            executor.submit(fetch_vtex_stock, url, session, X_VTEX_API_AppKey, X_VTEX_API_AppToken, 5): url
            for url in retries
        }
        for future in as_completed(future_to_url):
            res = future.result()
            if res["status"] == "ok":
                responses.append({'json': res['json'], 'url': res['url']})
            else:
                exception_cases.append(res['url'])

    session.close()

    final_responses = []

    for response in responses:
        try:
            for balance in response['json']['balance']:
                aux = balance.copy()
                aux['skuId'] = response['json']['skuId']
                final_responses.append(aux)
        except KeyError as e:
            print(e)
            print(response)
            exception_cases.append(response['url'])

    if len(exception_cases) > 0:
        raise Exception(f'{len(exception_cases)} exception cases found during retry.')
    
def _save_stock_final_batched(ds, ts, **kwargs):
    import time

    print(f"Iniciando carga dosificada y parcializada de stock final. Fecha: {ds}, TS: {ts}")

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    conn = pg_hook.get_conn()
    conn.autocommit = False
    cursor = conn.cursor()

    try:
        # Configurar límites estrictos de memoria para no competir con el resto del RDS
        cursor.execute("SET work_mem = '32MB';")
        cursor.execute("SET max_parallel_workers_per_gather = 0;")

        # 1. Obtener lista de tiendas activas con inventario en staging
        query_tiendas = """
        SELECT DISTINCT t.id
        FROM staging.stock_vtex_unimarc svu
        JOIN ecommdata.bodegas b ON svu.id_warehouse = b.id AND b.dock_activo IS TRUE
        JOIN ecommdata.tiendas t ON b.id_tienda = t.id AND t.status = 1
        WHERE NOT (
            (t.id = '0018' AND b.id = '9051') OR
            (t.id = '0069' AND b.id = '0576') OR
            (t.id = '0088' AND b.id = '0324')
        )
        ORDER BY t.id;
        """
        cursor.execute(query_tiendas)
        tiendas = [row[0] for row in cursor.fetchall()]
        total_tiendas = len(tiendas)
        print(f"Total de tiendas a procesar tienda por tienda: {total_tiendas}")

        if total_tiendas == 0:
            print("No se encontraron tiendas activas para procesar.")
            return

        delete_query = """
        DELETE FROM ecommdata.stock
        WHERE fecha = %(ds)s::date
          AND id_tienda = %(id_tienda)s;
        """

        insert_query = """
        INSERT INTO ecommdata.stock (
            fecha,
            id_tienda,
            glosa_tienda,
            id_bodega,
            nombre_bodega,
            ref_id,
            material,
            descripcion,
            c1,
            c2,
            c3,
            multiplicador_unidad_medida,
            unidades_pack,
            stock_janis,
            stock_seguridad_janis,
            stock_infinito_janis,
            tipo_operacion_janis,
            stock_vtex,
            stock_reservado_vtex,
            stock_disponible_vtex,
            stock_infinito_vtex,
            fecha_publicacion_janis,
            fecha_modificacion_janis,
            ultima_actualizacion,
            surtido_ecommerce,
            infaltable
        )
        SELECT 
            %(ds)s::date as fecha,
            t.id as id_tienda,
            t.glosa as glosa_tienda,
            b.id as id_bodega,
            b.nombre as nombre_bodega,
            s.ref_id,
            p.material,
            s.nombre_sku as descripcion,
            c.n1 as c1,
            c.n2 as c2,
            c.n3 as c3,
            s.multiplicador_unidad_medida,
            s.unidades_pack,
            su.stock as stock_janis,
            su.min_stock as stock_seguridad_janis,
            su.infinite_stock::int::bool as stock_infinito_janis,
            su.operation_type as tipo_operacion_janis,
            svu.cantidad_total as stock_vtex,
            svu.cantidad_reservada as stock_reservado_vtex,
            (svu.cantidad_total - svu.cantidad_reservada) as stock_disponible_vtex,
            svu.cantidad_ilimitada as stock_infinito_vtex,
            su.date_published as fecha_publicacion_janis,
            su.date_modified as fecha_modificacion_janis,
            %(ts)s::timestamptz at time zone 'America/Santiago' + interval '4 hours' as ultima_actualizacion,
            (sa.ref_id IS NOT NULL) as surtido_ecommerce,
            (li.material IS NOT NULL) as infaltable
        FROM staging.stock_vtex_unimarc svu
        JOIN ecommdata.bodegas b 
          ON svu.id_warehouse = b.id 
         AND b.dock_activo IS TRUE
        JOIN ecommdata.tiendas t 
          ON b.id_tienda = t.id 
         AND t.status = 1
        LEFT JOIN ecommdata.skus s 
          ON svu.vtex_id = s.vtex_id
        LEFT JOIN staging.stock_unimarc su 
          ON s.id = su.item_id 
         AND t.id_janis = su.store_id 
         AND b.id_janis = su.warehouse_id
        LEFT JOIN ecommdata.productos p 
          ON s.ref_id = p.ref_id
        LEFT JOIN ecommdata.categorias c 
          ON p.id_categoria = c.id
        LEFT JOIN staging.surtido_activo_unimarc sa 
          ON sa.id_tienda = t.id 
         AND sa.ref_id = s.ref_id
        LEFT JOIN ecommdata.lista_infaltables li 
          ON p.material = li.material
        WHERE t.id = %(id_tienda)s
          AND NOT (
            (t.id = '0018' AND b.id = '9051') OR
            (t.id = '0069' AND b.id = '0576') OR
            (t.id = '0088' AND b.id = '0324')
        );
        """

        total_inserted = 0
        failed_tiendas = []
        start_time_total = time.time()

        for idx, tienda_id in enumerate(tiendas, 1):
            t_start = time.time()
            max_retries = 2
            success = False

            for attempt in range(1, max_retries + 1):
                try:
                    params = {
                        "ds": ds,
                        "ts": ts,
                        "id_tienda": tienda_id
                    }
                    cursor.execute(delete_query, params)
                    cursor.execute(insert_query, params)
                    inserted_rows = cursor.rowcount
                    conn.commit()
                    total_inserted += inserted_rows
                    t_elapsed = time.time() - t_start
                    print(f"[{idx}/{total_tiendas}] Tienda {tienda_id}: {inserted_rows} filas insertadas ({t_elapsed:.2f}s)")
                    success = True
                    break
                except Exception as e:
                    conn.rollback()
                    print(f"Intento {attempt}/{max_retries} fallido para tienda {tienda_id}: {e}")
                    if attempt < max_retries:
                        time.sleep(1)

            if not success:
                failed_tiendas.append(tienda_id)
                print(f"ERROR: No se pudo procesar tienda {tienda_id} tras {max_retries} intentos.")

            # Throttling dosificado: 50ms de pausa entre tiendas para dar respiro al CPU y conexiones de Postgres
            time.sleep(0.05)

        total_time = time.time() - start_time_total
        print(f"Proceso finalizado en {total_time:.1f}s. Total filas insertadas: {total_inserted}")

        if failed_tiendas:
            raise Exception(f"Carga incompleta. Fallaron {len(failed_tiendas)} tiendas: {failed_tiendas}")

    finally:
        cursor.close()
        conn.close()


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'etl_stock_incremental_load',
    default_args=default_args,
    description="Extracción y carga de tabla stock desde Vtex y Janis.",
    schedule_interval="0 1,4/4 * * *",
    start_date=pendulum.datetime(2022, 7, 11, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["DATA", "vtex", "janis", "staging", "unimarc", "vtex_stock", "janis_stock", "stock", "MATIAS"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla stock desde Vtex y Janis.
    """ 

    t0 = PostgresOperator(
        task_id = "truncate_janis_staging_table",
        postgres_conn_id="postgresql_conn",
        sql="""
        TRUNCATE staging.stock_unimarc
        """,
    )

    t1 = PostgresOperator(
        task_id = "truncate_vtex_staging_table",
        postgres_conn_id="postgresql_conn",
        sql="""
        TRUNCATE staging.stock_vtex_unimarc
        """,
    )

    t2 = PythonOperator(
        task_id = "load_full_table_to_s3",
        python_callable = load_full_table_to_s3,
        op_kwargs = {"table_name": "stock"}
    )

    t3 = PythonOperator(
        task_id = "save_table_stock",
        python_callable = _save_table_stock_janis,
    )

    t4 = PythonOperator(
        task_id = "save_vtex_stock_in_ecommdata",
        python_callable = _save_vtex_stock_in_ecommdata
    )

    t5 = PythonOperator(
        task_id = "vtex_get_stock_retries",
        python_callable = _vtex_get_stock_retries
    )

    t_prepare_surtido = PostgresOperator(
        task_id = "prepare_surtido_staging",
        postgres_conn_id = "postgresql_conn",
        sql = "sql/prepare_surtido_activo.sql"
    )

    t6 = PythonOperator(
        task_id = "save_stock_final",
        python_callable = _save_stock_final_batched,
    )

    t7 = PostgresOperator(
        task_id = "delete_old_stock",
        postgres_conn_id = "postgresql_conn",
        sql = """DELETE
            FROM ecommdata.stock
            WHERE fecha <= '{{ds}}'::date - interval '21 days' """
    )


[t0, t2] >> t3
[t1, t3] >> t4 >> t5 >> t_prepare_surtido >> t6 >> t7

