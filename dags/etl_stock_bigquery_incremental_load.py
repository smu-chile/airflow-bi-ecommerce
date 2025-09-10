from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator

import pendulum

def extract_bq_to_s3(ti, ds, ts):
    """
    BQ → CSV local (por chunks) → S3.
    Carpeta única por día: ecommdata/stock_bq_dw/YYYY/MM/DD/stock_{HHmmss}.csv
    """
    import os
    import pandas as pd
    from time import perf_counter
    from google.oauth2 import service_account
    from google.cloud import bigquery

    print("=" * 100)
    print(f"[extract] START | ds={ds} | ts={ts}")

    # --- SQL ---
    BQ_STOCK_QUERY = f"""
    SELECT
        LPAD(CAST(SA.SKU_PRODUCT AS STRING), 18, '0') AS material,
        S.NBR_ITM                                     AS stock,
        LPAD(CAST(OU.OU_ID AS STRING), 4, '0')        AS id_tienda,
        SA.NM                                         AS nombre,
        S.DATE_VALUE                                  AS fecha,
        S.SKU_HEX                                     AS sku_key, -- (hex=key)
        LPAD(CAST(ST.ORG_COMPRAS AS STRING), 4, '0')  AS org_compras,
        O.ORG_IP_ID                                   AS org_ip_id
    FROM cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_STOCK S
        LEFT JOIN cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_SKU_ATTR SA
            ON SA.SKU_KEY = S.SKU_KEY
        LEFT JOIN cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_ORGANIZATION_UNIT OU
            ON OU.OU_KEY = S.OU_KEY
        LEFT JOIN cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_ORGANIZATION O
            ON OU.ORG_KEY = O.ORGANIZATION_KEY
        LEFT JOIN cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_STORE ST 
            ON O.ORGANIZATION_KEY = ST.ORG_KEY
        LEFT JOIN cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_ALMACEN A
            ON A.ALMACEN_KEY = S.ALMACEN_KEY
        LEFT JOIN cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_PARTICULARIDAD PART
            ON PART.PARTICULARIDAD_KEY = S.PARTICULARIDAD_KEY
        WHERE
            A.ALMACEN_COD = '0001'
            AND S.APLICA_STOCK = 'S'
            AND S.TIPO_STOCK_KEY = MD5('TIPOSTOCK^CL^SMC^')
            AND PART.PARTICULARIDAD_COD = 'A'
            AND S.DATE_VALUE = '{ds}'
    """
    print("[extract] SQL >>>")
    print("\n".join("  " + ln for ln in BQ_STOCK_QUERY.strip().splitlines()))

    # --- Credenciales y clientes ---
    sa_info = Variable.get("BIGQUERY_CREDENTIALS", deserialize_json=True)
    creds = service_account.Credentials.from_service_account_info(
        sa_info, scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    bq_client = bigquery.Client(project=sa_info["project_id"], credentials=creds, location="US")
    print(f"[extract] BQ client listo | project={sa_info['project_id']} | location=US")

    try:
        from google.cloud import bigquery_storage
        try:
            bqstorage_client = bigquery_storage.BigQueryReadClient(credentials=creds)
            print("[extract] BigQuery Storage API: ON ✅")
        except Exception as e:
            print(f"[extract][warn] BigQuery Storage no disponible → REST. Detalle: {e}")
            bqstorage_client = None
    except Exception as e:
        print(f"[extract][warn] bigquery_storage no instalado → REST. Detalle: {e}")
        bqstorage_client = None

    # --- Paths S3 / tmp ---
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3 = S3Hook(aws_conn_id="aws_s3_connection")

    exec_date = ds.replace("-", "/")
    ts_local = pendulum.parse(ts).in_timezone("America/Santiago")
    time_aux = ts_local.format("HHmmss")

    prefix = f"ecommdata/stock_bq_dw/{exec_date}/"
    key = f"{prefix}stock_{time_aux}.csv"

    tmp_path = f"/tmp/stock_{time_aux}.csv"
    if os.path.exists(tmp_path):
        os.remove(tmp_path)

    print(f"[extract] SNAPSHOT | tmp={tmp_path} | s3://{s3_bucket}/{key}")

    # --- Query & streaming por chunks ---
    CHUNK_ROWS = int(Variable.get("BQ_CHUNK_ROWS", default_var="50000"))
    print(f"[extract] CHUNK_ROWS={CHUNK_ROWS}")

    t0 = perf_counter()
    job = bq_client.query(BQ_STOCK_QUERY)
    print(f"[extract] Job enviado | job_id={job.job_id}")

    row_it = job.result(page_size=CHUNK_ROWS)

    # Stats opcionales
    try:
        stats = getattr(job, "_properties", {}).get("statistics", {}).get("query", {})
        bytes_proc = int(stats.get("totalBytesProcessed", 0))
        slot_ms = int(stats.get("totalSlotMs", 0))
        cache_hit = bool(stats.get("cacheHit", False))
        print(f"[extract] BQ stats | bytes_processed={bytes_proc:,} | slot_ms={slot_ms:,} | cache_hit={cache_hit}")
    except Exception:
        pass

    print(f"[extract] BQ total_rows reportado: {row_it.total_rows}")

    # NORMALIZA A 'nbr_item' PARA MATCHEAR TABLA
    want_cols = ["sku_product", "nbr_item", "id_tienda", "nombre", "fecha", "sku_key", "org_compras", "org_ip_id"]

    first = True
    total = 0
    chunk_i = 0
    showed_head = False

    for chunk in row_it.to_dataframe_iterable(bqstorage_client=bqstorage_client):
        chunk_i += 1
        c0 = perf_counter()

        # rename + tipos
        chunk = chunk.rename(columns={
            "material": "sku_product",
            "stock": "nbr_item",
        })

        chunk["sku_product"] = chunk["sku_product"].astype(str).str.zfill(18)
        chunk["id_tienda"] = chunk["id_tienda"].astype(str).str.zfill(4)

        chunk["nbr_item"] = pd.to_numeric(chunk["nbr_item"], errors="coerce").astype("float64")
        chunk["fecha"] = pd.to_datetime(chunk["fecha"], errors="coerce").dt.date
        chunk = chunk[want_cols]

        # Mostrar head del primer chunk procesado
        if not showed_head:
            print("[extract] HEAD (primer chunk):")
            try:
                print(chunk.head(10).to_string(index=False))
            except Exception as e:
                print(f"[extract][warn] no se pudo imprimir head: {e}")
            showed_head = True

        # append (no cargamos todo a RAM)
        chunk.to_csv(tmp_path, index=False, mode="a", header=first, line_terminator="\n")
        first = False
        total += len(chunk)
        c1 = perf_counter()
        print(f"[extract] chunk#{chunk_i:02d} | rows={len(chunk):,} | cum={total:,} | {c1 - c0:.3f}s")

    if first:
        # sin filas -> deja header
        import pandas as pd  # por si quedó fuera de scope
        pd.DataFrame(columns=want_cols).to_csv(tmp_path, index=False)
        print("[extract] no rows → CSV con solo header")

    # --- subir a S3 y limpiar ---
    s3_t0 = perf_counter()
    s3.load_file(filename=tmp_path, key=key, bucket_name=s3_bucket, replace=True)
    s3_t1 = perf_counter()
    try:
        fsize = __import__("os").path.getsize(tmp_path)
    except Exception:
        fsize = -1
    try:
        __import__("os").remove(tmp_path)
    except Exception:
        pass
    print(f"[extract] uploaded s3://{s3_bucket}/{key} | file_size~{fsize if fsize!=-1 else 'n/a'} bytes | {s3_t1 - s3_t0:.3f}s")

    # XComs
    ti.xcom_push(key="snapshot_key", value=key)
    ti.xcom_push(key="rows_count", value=int(total))
    t1 = perf_counter()
    print(f"[extract] DONE | rows_total={total:,} | elapsed={t1 - t0:.3f}s")
    print("=" * 100)


def upsert_stock_postgres(ti):
    import io
    import os
    import sqlalchemy as sa
    from sqlalchemy import text
    from time import perf_counter

    print("=" * 100)
    print("[upsert] START (COPY + single upsert)")

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    key = ti.xcom_pull(key="snapshot_key")
    print(f"[upsert] snapshot: s3://{s3_bucket}/{key}")

    # --- Conexión PG (psycopg2 bajo SQLAlchemy) ---
    host     = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    engine   = sa.create_engine(
        f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    )
    print(f"[upsert] PG OK | host={host} db={database}")

    # --- Abrimos stream S3 (sin descargar a disco) ---
    s3 = S3Hook(aws_conn_id="aws_s3_connection")
    obj = s3.get_key(key, bucket_name=s3_bucket)
    body = obj.get()["Body"]  # botocore.response.StreamingBody
    # COPY requiere texto; envolvemos bytes → texto sin cargar todo en RAM
    stream = io.TextIOWrapper(body, encoding="utf-8", newline="")

    t0 = perf_counter()
    with engine.begin() as conn:
        # 1) staging temporal
        conn.execute(text("""
            CREATE TEMP TABLE IF NOT EXISTS tmp_stock_dw_bq (
                sku_product text,
                nbr_item    double precision,
                id_tienda   text,
                nombre      text,
                fecha       date,
                sku_key     text,
                org_compras text,
                org_ip_id   text
            ) ON COMMIT DROP;
            TRUNCATE tmp_stock_dw_bq;
        """))
        print("[upsert] temp table ready")

        raw_conn = conn.connection 
        with raw_conn.cursor() as cur:
            cur.copy_expert(
                """
                COPY tmp_stock_dw_bq (sku_product, nbr_item, id_tienda, nombre, fecha, sku_key, org_compras, org_ip_id)
                FROM STDIN WITH (FORMAT csv, HEADER true)
                """,
                stream
            )
        print("[upsert] COPY OK")

        # UPSERT
        conn.execute(text("""
            INSERT INTO ecommdata.stock_dw_bq
                (sku_product, nbr_item, id_tienda, nombre, fecha, sku_key, org_compras, org_ip_id)
            SELECT
                NULLIF(sku_product,'')::text,
                nbr_item::double precision,
                NULLIF(id_tienda,'')::text,
                NULLIF(nombre,'')::text,
                fecha::date,
                NULLIF(sku_key,'')::text,
                NULLIF(org_compras,'')::text,
                NULLIF(org_ip_id,'')::text
            FROM tmp_stock_dw_bq
            ON CONFLICT (sku_product, id_tienda, fecha, nombre, sku_key,org_compras, org_ip_id)
            DO UPDATE SET
                nbr_item   = EXCLUDED.nbr_item,
                updated_at = NOW();
        """))
        print("[upsert] single UPSERT OK")

    t1 = perf_counter()
    print(f"[upsert] DONE | elapsed={t1 - t0:.3f}s")
    print("=" * 100)


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    dag_id="etl_stock_disponible_bq_incremental_load",
    description="BQ → S3 snapshots → Postgres UPSERT.",
    schedule_interval="0 6 * * *", # daily at 06:00 AM
    start_date=pendulum.datetime(2025, 8, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["stock", "BQ", "DW", "incremental", "postgres", "FRANCISCO"],
) as dag:

    dag.doc_md = """
    **Flow:** BigQuery → DataFrame (chunked) → S3 → Postgres (chunked).
    - Snapshot: `ecommdata/stock_bq_dw/YYYY/MM/DD/stock_{HHmmss}.csv`.
    - UPSERT en `ecommdata.stock_dw_bq`
    - Solo se actualiza `nbr_item` (y `updated_at` si existe).
    - Tamaños configurables vía Variables: `BQ_CHUNK_ROWS`, `PG_UPSERT_CHUNK_ROWS`.
    """

    t0 = PythonOperator(
        task_id="extract_bq_to_s3",
        python_callable=extract_bq_to_s3,
    )

    t1 = PythonOperator(
        task_id="upsert_stock_postgres",
        python_callable=upsert_stock_postgres,
    )

    t2 = PostgresOperator(
        task_id = "delete_old_data",
        postgres_conn_id="postgresql_conn",
        sql="""
        delete from ecommdata.stock_dw_bq
        where fecha < current_date::date - interval '3 days';
        """,
    )

    t0 >> t1 >> t2
