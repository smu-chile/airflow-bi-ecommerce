from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.slack_utils import dag_success_slack, dag_failure_slack

import pendulum


# ─────────────────────────────────────────────────────────────────────────────
# TASK 1: Llama a la API de VTEX, filtra cupones vigentes con UTM (sin
#         "reclamo") y consulta el contador de usos por cada cupón.
#         Guarda el resultado como CSV en S3.
# ─────────────────────────────────────────────────────────────────────────────
def extract_cupones_to_s3(ts, ds):
    import requests
    import pandas as pd
    from io import StringIO
    from datetime import datetime, timezone

    accountName         = Variable.get("VTEX_ACCOUNT_NAME",  default_var="unimarc")
    env                 = Variable.get("VTEX_ENV",           default_var="vtexcommercestable")
    X_VTEX_API_AppKey   = Variable.get("X_VTEX_API_AppKey",  default_var="")
    X_VTEX_API_AppToken = Variable.get("X_VTEX_API_AppToken",default_var="")

    headers = {
        "Accept":              "application/json",
        "X-VTEX-API-AppKey":   X_VTEX_API_AppKey,
        "X-VTEX-API-AppToken": X_VTEX_API_AppToken,
    }
    base_url = f"https://{accountName}.{env}.com.br"

    # ── 1. Obtener lista completa de calculator configurations ────────────────
    url  = f"{base_url}/api/rnb/pvt/benefits/calculatorconfiguration"
    resp = requests.get(url, headers=headers, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    items = data.get("items", data) if isinstance(data, dict) else data
    now   = datetime.now(tz=timezone.utc)

    # ── 2. Filtros (mismos que cupones-vigentes.py) ───────────────────────────
    def utm_val(p):
        return (p.get("utmCampain") or p.get("utmCampaign") or "").strip()

    def parse_date(s):
        if not s:
            return None
        try:
            from datetime import datetime, timezone
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            return None

    vigentes = [
        p for p in items
        if utm_val(p) and utm_val(p).lower() != "null"
        and p.get("isActive") is True
        and p.get("isArchived") is not True
        and (not p.get("status") or p.get("status") == "active")
        and (not p.get("beginDate") or parse_date(p["beginDate"]) <= now)
        and (not p.get("endDate")   or parse_date(p["endDate"])   >= now)
        and "reclamo" not in (p.get("name") or "").lower()
        and "reclamo" not in utm_val(p).lower()
    ]
    print(f"Cupones vigentes con UTM (sin 'reclamo'): {len(vigentes)}")

    def es_vigente(p):
        begin = p.get("beginDateUtc") or p.get("beginDate")
        end   = p.get("endDateUtc")   or p.get("endDate")
        return (
            p.get("isActive") is True
            and p.get("isArchived") is not True
            and (not begin or (parse_date(begin) or now) <= now)
            and (not end   or (parse_date(end)   or now) >= now)
        )

    # ── 2b. Campaign Audiences (campaignConfiguration) ───────────────────────
    # Estructura real: [{id, name, beginDateUtc, endDateUtc, isActive, isArchived,
    #                    targetConfigurations: [{id, name, coupon: [], ...}]}]
    def extract_campaigns():
        r = requests.get(
            f"{base_url}/api/rnb/pvt/campaignConfiguration",
            headers=headers,
            timeout=120,
        )
        r.raise_for_status()
        raw = r.json()
        camp_items = raw.get("items", raw) if isinstance(raw, dict) else raw

        def extraer_utms(obj):
            """Busca UTMs en los posibles campos de un dict (campaña o targetConfiguration)."""
            encontrados = []
            for k in ("utmCampaign", "utmCampain"):
                v = obj.get(k)
                if isinstance(v, str):
                    encontrados.append(v)
            for k in ("utmCampaigns", "coupon"):
                for v in obj.get(k) or []:
                    if isinstance(v, str):
                        encontrados.append(v)
                    elif isinstance(v, dict):
                        encontrados.append(v.get("utmCampaign") or v.get("code") or v.get("name") or "")
            return encontrados

        resultado = []
        for c in camp_items or []:
            if not es_vigente(c):
                continue

            tcs = [t for t in (c.get("targetConfigurations") or []) if isinstance(t, dict)]
            utms_campania = extraer_utms(c)

            print(f"  · Campaña vigente: {c.get('name')} ({c.get('id')}) "
                  f"- audiencias: {[t.get('name') for t in tcs]}")

            for t in tcs or [{}]:
                nombre = t.get("name") or c.get("name") or ""
                utms = extraer_utms(t) + utms_campania
                utms = [u.strip() for u in utms if u and u.strip() and u.strip().lower() != "null"]
                print(f"      audiencia '{nombre}' -> UTMs: {utms}")

                for utm in dict.fromkeys(utms):  # únicos, conserva orden
                    if "reclamo" in nombre.lower() or "reclamo" in utm.lower():
                        continue
                    resultado.append({
                        "id":           c.get("id"),
                        "nombre":       nombre,
                        "utm_source":   "",
                        "utm_campaign": utm,
                        "fecha_inicio": c.get("beginDateUtc") or c.get("beginDate"),
                        "fecha_fin":    c.get("endDateUtc")   or c.get("endDate"),
                        "status":       "active",
                        "id_promo":     ",".join(promos_por_campania.get(c.get("id"), [])),
                    })
        return resultado

    # Mapa campaña -> promos asociadas. Cada promo de calculatorconfiguration
    # trae "Campaigns": [{id, name}] con las Campaign Audiences que la activan.
    promos_por_campania = {}
    for p in items:
        if p.get("isArchived") is True:
            continue
        for camp in (p.get("Campaigns") or p.get("campaigns") or []):
            if isinstance(camp, dict) and camp.get("id"):
                promos_por_campania.setdefault(camp["id"], []).append(p.get("idCalculatorConfiguration"))

    campaigns = extract_campaigns()
    print(f"Cupones vigentes vía Campaign Audience: {len(campaigns)}")

    # ── 3. Consultar usage-count por cada cupón ───────────────────────────────
    def get_usage_count(utm):
        try:
            r = requests.get(
                f"{base_url}/api/rnb/v2/pvt/coupon/usage-count/{utm}",
                headers=headers,
                timeout=30,
            )
            if r.status_code != 200:
                return None
            raw = r.json()
            if isinstance(raw, (int, float)):
                return int(raw)
            return raw.get("count") or raw.get("usageCount") or 0
        except Exception as e:
            print(f"  ⚠️  Error usage-count para {utm}: {e}")
            return None

    # ── 3b. Merge + dedup (prioridad: calculatorconfiguration) ───────────────
    candidatos = []
    for p in vigentes:
        candidatos.append({
            "id":           p.get("idCalculatorConfiguration"),
            "nombre":       p.get("name"),
            "utm_source":   p.get("utmSource", ""),
            "utm_campaign": utm_val(p),
            "fecha_inicio": p.get("beginDate"),
            "fecha_fin":    p.get("endDate"),
            "status":       p.get("status"),
            "fuente":       "calculatorconfiguration",
            "id_promo":     p.get("idCalculatorConfiguration"),
        })
    for c in campaigns:
        candidatos.append({**c, "fuente": "campaignConfiguration"})

    records, utms_vistos, claves_vistas = [], set(), set()
    for rec in candidatos:
        utm_key = rec["utm_campaign"].upper()
        clave   = (rec["id"], rec["utm_campaign"])
        if utm_key in utms_vistos or clave in claves_vistas:
            continue
        utms_vistos.add(utm_key)
        claves_vistas.add(clave)
        # Los usos de Campaign Audience se calculan en Postgres (task de carga)
        rec["usos"]        = (get_usage_count(rec["utm_campaign"])
                              if rec["fuente"] == "calculatorconfiguration" else None)
        rec["fecha_carga"]   = ds
        # Timestamp UTC de la lectura (para campañas se reemplaza en la carga)
        rec["fecha_lectura"] = datetime.now(tz=timezone.utc).isoformat()
        records.append(rec)
    print(f"Total cupones tras merge/dedup: {len(records)}")

    # ── 4. Guardar CSV en S3 ──────────────────────────────────────────────────
    df = pd.DataFrame(records)
    print(df)

    exec_date = ds.replace("-", "/")
    file_name = f"vtex/cupones_vigentes/{exec_date}/cupones_vigentes.csv"

    buffer = StringIO()
    df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    buffer.seek(0)

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook   = S3Hook(aws_conn_id="aws_s3_connection")
    s3_hook.load_string(
        buffer.getvalue(),
        key=file_name,
        bucket_name=s3_bucket,
        replace=True,
        encrypt=False,
    )
    print(f"Archivo guardado en S3: {file_name}")
    return file_name


# ─────────────────────────────────────────────────────────────────────────────
# TASK 2: Lee el CSV desde S3 y carga el snapshot diario en
#         catalogo.cupones_vigentes_vtex (DELETE del día + INSERT)
# ─────────────────────────────────────────────────────────────────────────────
def load_cupones_to_postgres(ti):
    import pandas as pd
    import numpy as np
    import sqlalchemy
    from sqlalchemy import text

    file_name = ti.xcom_pull(key="return_value", task_ids=["extract_cupones_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook   = S3Hook(aws_conn_id="aws_s3_connection")

    print(f"Buscando archivo en S3: {file_name}")
    if not s3_hook.check_for_key(file_name, bucket_name=s3_bucket):
        raise Exception(f"Key {file_name} no existe en S3.")

    s3_obj = s3_hook.get_key(file_name, bucket_name=s3_bucket)
    df     = pd.read_csv(s3_obj.get()["Body"])

    if df.empty:
        print("No hay cupones vigentes. Task finaliza como exitosa.")
        return

    print(f"Registros a cargar: {len(df)}")
    print(df.dtypes)

    # Castear tipos
    df["usos"]        = pd.to_numeric(df["usos"],  errors="coerce").astype("Int64")
    df["fecha_carga"] = pd.to_datetime(df["fecha_carga"], errors="coerce")
    if "fecha_lectura" in df.columns:
        df["fecha_lectura"] = pd.to_datetime(df["fecha_lectura"], errors="coerce", utc=True)
    else:
        df["fecha_lectura"] = pd.Timestamp.now(tz="UTC")
    df = df.where(pd.notnull(df), None)

    host     = Variable.get("POSTGRESQL_HOST",     default_var="")
    database = Variable.get("POSTGRESQL_DB",       default_var="postgres")
    username = Variable.get("POSTGRESQL_USER",     default_var="")
    password = Variable.get("POSTGRESQL_PASSWORD", default_var="")

    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine   = sqlalchemy.create_engine(conn_url)

    # ── Usos de cupones Campaign Audience: órdenes distintas que aplicaron
    #    alguna de las promos asociadas a la campaña ──
    query_usos = text("""
        SELECT COUNT(DISTINCT op.id_orden)
        FROM ecommdata.orden_producto_promociones opp
        JOIN ecommdata.orden_productos op ON op.id = opp.orden_producto
        WHERE opp.ref_id = ANY(:ref_ids)
    """)
    if "id_promo" in df.columns:
        mask = (df["fuente"] == "campaignConfiguration") & df["id_promo"].notna()
        with engine.connect() as conn:
            for idx in df[mask].index:
                ref_ids = [x.strip() for x in str(df.at[idx, "id_promo"]).split(",") if x.strip()]
                if not ref_ids:
                    continue
                usos = conn.execute(query_usos, {"ref_ids": ref_ids}).scalar()
                df.at[idx, "usos"]          = int(usos or 0)
                df.at[idx, "fecha_lectura"] = pd.Timestamp.now(tz="UTC")
                print(f"  Usos {df.at[idx, 'utm_campaign']} (promos {ref_ids}): {usos}")

    with engine.begin() as conn:
        # CREATE TABLE si no existe
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS catalogo.cupones_vigentes_vtex (
                id            TEXT,
                nombre        TEXT,
                utm_source    TEXT,
                utm_campaign  TEXT,
                fecha_inicio  TEXT,
                fecha_fin     TEXT,
                status        TEXT,
                usos          INTEGER,
                fecha_carga   DATE,
                fuente        TEXT,
                id_promo      TEXT,
                fecha_lectura TIMESTAMPTZ
            );
        """))
        # Tablas existentes: agregar columnas nuevas si faltan
        conn.execute(text("""
            ALTER TABLE catalogo.cupones_vigentes_vtex
            ADD COLUMN IF NOT EXISTS fuente TEXT;
        """))
        conn.execute(text("""
            ALTER TABLE catalogo.cupones_vigentes_vtex
            ADD COLUMN IF NOT EXISTS id_promo TEXT;
        """))
        conn.execute(text("""
            ALTER TABLE catalogo.cupones_vigentes_vtex
            ADD COLUMN IF NOT EXISTS fecha_lectura TIMESTAMPTZ;
        """))
        # Snapshot diario: se conserva el histórico y solo se reemplaza la
        # foto del mismo día (re-ejecuciones no duplican filas)
        fecha_snapshot = df["fecha_carga"].dropna().iloc[0].date()
        conn.execute(
            text("DELETE FROM catalogo.cupones_vigentes_vtex WHERE fecha_carga = :f;"),
            {"f": fecha_snapshot},
        )

    df.to_sql(
        name="cupones_vigentes_vtex",
        con=engine,
        schema="catalogo",
        if_exists="append",
        index=False,
        chunksize=5000,
        method="multi",
    )
    print("Datos cargados en catalogo.cupones_vigentes_vtex")


# ─────────────────────────────────────────────────────────────────────────────
# DAG definition
# ─────────────────────────────────────────────────────────────────────────────
default_args = {
    "owner":            "ecommerce_data",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          1,
}

with DAG(
    "etl_cupones_vigentes_vtex",
    default_args=default_args,
    description=(
        "Extrae cupones VTEX activos con UTM desde la API calculatorconfiguration, "
        "consulta el contador de usos por cupón y carga el resultado diario en "
        "catalogo.cupones_vigentes_vtex (TRUNCATE + INSERT)."
    ),
    schedule_interval="0 13 * * *",   # 10:00 AM Chile (UTC-3) = 13:00 UTC
    start_date=pendulum.datetime(2026, 10, 20, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "ecommdata", "VTEX", "cupones", "Unimarc", "SERGIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    ## ETL Cupones Vigentes VTEX

    Ejecuta diariamente a las **10:00 AM hora Chile**.

    ### Pasos:
    1. **extract_cupones_to_s3** — Llama a `GET /api/rnb/pvt/benefits/calculatorconfiguration`,
       aplica filtros (isActive, no archivado, fechas vigentes, UTM no vacío, sin "reclamo")
       y consulta `GET /api/rnb/v2/pvt/coupon/usage-count/{utm}` por cada cupón.
       Guarda el resultado en `s3://bucket/vtex/cupones_vigentes/{ds}/cupones_vigentes.csv`.

    2. **load_cupones_to_postgres** — Lee el CSV desde S3, hace TRUNCATE e inserta en
       `catalogo.cupones_vigentes_vtex`.

    ### Tabla destino:
    ```
    catalogo.cupones_vigentes_vtex (
        id, nombre, utm_source, utm_campaign,
        fecha_inicio, fecha_fin, status, usos, fecha_carga
    )
    ```
    """

    t0 = PythonOperator(
        task_id="extract_cupones_to_s3",
        python_callable=extract_cupones_to_s3,
    )

    t1 = PythonOperator(
        task_id="load_cupones_to_postgres",
        python_callable=load_cupones_to_postgres,
    )

    t0 >> t1
