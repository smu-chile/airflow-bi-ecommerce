from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.exceptions import AirflowFailException

from utils.slack_utils import dag_success_slack, dag_failure_slack

from datetime import timedelta

import pendulum

TZ = "America/Santiago"
S3_BASE_PREFIX = "datastage/instock_unimarc"
TARGET_TABLE = "ecommdata.instock"
TIENDAS_EXCLUIDAS = ("0486", "0053", "0398", "0054")  # Excluidas de reportes y stock esten o no con status 1
DEADLINE_HOURS = 2  # Horas de espera desde las 08:00 antes de fallar por tiendas faltantes

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    fecha             DATE        NOT NULL,
    tienda            VARCHAR(4)  NOT NULL,
    material          VARCHAR(18) NOT NULL,
    catalogado        BOOLEAN,
    instock           SMALLINT,
    clase_abc         VARCHAR(2),
    car_plan          VARCHAR(4),
    estado_operativo  BOOLEAN,
    fecha_carga       TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    CONSTRAINT pk_instock PRIMARY KEY (tienda, material)
);
CREATE INDEX IF NOT EXISTS idx_instock_material ON {TARGET_TABLE} (material);
"""

# Columnas origen (CSV) -> destino (tabla)
COLUMN_NAMES = {
    "Fecha": "fecha",
    "Centro": "tienda",
    "PLU_SAP60": "material",
    "Catalogado": "catalogado",
    "InStock": "instock",
    "ClaseABC": "clase_abc",
    "CarPLan": "car_plan",
    "EstadoOperativo": "estado_operativo",
}
TARGET_COLUMNS = list(COLUMN_NAMES.values())


def _get_target_date(data_interval_end, dag_run):
    """
    Fecha de la carpeta a procesar en S3.
    El InStock llega a las 8:00 AM con fecha de ayer (hoy usamos el de ayer).
    Permite override manual con conf {"fecha": "YYYY-MM-DD"}.
    """
    conf = (dag_run.conf or {}) if dag_run else {}
    if conf.get("fecha"):
        return pendulum.parse(conf["fecha"], tz=TZ)
    return data_interval_end.in_timezone(TZ).subtract(days=1)


def _get_prefix(target_date):
    return f"{S3_BASE_PREFIX}/{target_date.strftime('%Y/%m/%d')}/"


def _list_store_files(prefix):
    """Retorna {tienda: [keys csv]} a partir de la estructura .../{tienda}/archivo.csv"""
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
    keys = s3_hook.list_keys(s3_bucket, prefix=prefix) or []

    files_by_store = {}
    for key in keys:
        if not key.lower().endswith(".csv"):
            continue
        parts = key[len(prefix):].split("/")
        if len(parts) < 2:
            continue
        tienda = parts[0].zfill(4)
        if tienda in TIENDAS_EXCLUIDAS:
            continue
        files_by_store.setdefault(tienda, []).append(key)
    return files_by_store


def _get_active_stores():
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    records = pg_hook.get_records(
        "SELECT id FROM ecommdata.tiendas WHERE status = 1 AND id NOT IN %s;",
        parameters=(TIENDAS_EXCLUIDAS,),
    )
    return {str(r[0]).zfill(4) for r in records}


def _check_all_stores(data_interval_end, dag_run, **_):
    """
    Sensor: no existe archivo bandera, por lo que se valida que estén los CSV de TODAS
    las tiendas activas. Si pasado el deadline siguen faltando tiendas, falla sin reintentos.
    """
    target_date = _get_target_date(data_interval_end, dag_run)
    prefix = _get_prefix(target_date)

    active_stores = _get_active_stores()
    found_stores = set(_list_store_files(prefix).keys())
    missing = sorted(active_stores - found_stores)

    print(f"Prefix: {prefix}")
    print(f"Tiendas activas: {len(active_stores)} | Encontradas: {len(found_stores)} | Faltantes: {len(missing)}")

    if not missing:
        return True

    print(f"Tiendas faltantes: {missing}")
    # El deadline de espera (10:00 AM) se evalúa sobre el día de ejecución del DAG (hoy)
    run_date = data_interval_end.in_timezone(TZ)
    deadline = run_date.replace(hour=8, minute=0, second=0, microsecond=0).add(hours=DEADLINE_HOURS)
    if pendulum.now(TZ) > deadline:
        raise AirflowFailException(
            f"No se encontraron archivos InStock para las tiendas: {missing} (prefix {prefix}). "
            f"Deadline {deadline.to_datetime_string()} superado, no se carga la tabla."
        )
    return False


def _transform_instock(df):
    """Estandariza el dataframe crudo (todo str) al formato de la tabla destino."""
    import pandas as pd

    df.columns = [c.strip() for c in df.columns]
    missing_cols = set(COLUMN_NAMES) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Columnas faltantes en archivo InStock: {missing_cols}")

    df = df[list(COLUMN_NAMES)].rename(columns=COLUMN_NAMES)
    df = df.apply(lambda s: s.str.strip())

    df["tienda"] = df["tienda"].str.zfill(4)
    df["material"] = df["material"].str.zfill(18)

    bool_map = {"1": True, "0": False}
    df["catalogado"] = df["catalogado"].map(bool_map)
    df["estado_operativo"] = df["estado_operativo"].map(bool_map)

    # InStock se conserva en formato 1/0
    df["instock"] = pd.to_numeric(df["instock"], errors="coerce").astype("Int64")

    # Conserva valor original (A, B, C, N o #)
    df["clase_abc"] = df["clase_abc"].replace({"": None})
    df["car_plan"] = df["car_plan"].replace({"": None})

    # Excluir tiendas no operadas en ecommerce esten o no con status 1
    df = df[~df["tienda"].isin(TIENDAS_EXCLUIDAS)]
    df = df.dropna(subset=["tienda", "material"])
    df = df.drop_duplicates(subset=["tienda", "material"], keep="last")
    return df[TARGET_COLUMNS]


def _load_instock(data_interval_end, dag_run, **_):
    import io
    import pandas as pd
    from concurrent.futures import ThreadPoolExecutor

    target_date = _get_target_date(data_interval_end, dag_run)
    prefix = _get_prefix(target_date)
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    files = [k for keys in _list_store_files(prefix).values() for k in keys]
    if not files:
        raise AirflowFailException(f"No hay archivos en {prefix}")
    print(f"Iniciando carga paralela de {len(files)} archivos desde {prefix}...")

    def read_file(key):
        local_hook = S3Hook(aws_conn_id="aws_s3_connection")
        body = local_hook.get_key(key, bucket_name=s3_bucket).get()["Body"]
        # dtype=str: preserva ceros a la izquierda y evita inferencia de tipos
        return pd.read_csv(body, sep=";", dtype=str, keep_default_na=False)

    with ThreadPoolExecutor(max_workers=10) as executor:
        df_list = list(executor.map(read_file, files))

    df = _transform_instock(pd.concat(df_list, ignore_index=True))
    del df_list

    if df.empty:
        raise AirflowFailException("El dataframe InStock quedó vacío tras la transformación.")

    fechas = df["fecha"].unique().tolist()
    print(f"Registros a cargar: {len(df)} | Tiendas: {df['tienda'].nunique()} | Fechas en archivos: {fechas}")

    # Buffer en memoria ANTES de tocar la BD para no dejar la tabla bloqueada/vacía ante un fallo
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False, sep="\t", na_rep="\\N")
    buffer.seek(0)

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    conn = pg_hook.get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute(CREATE_TABLE_SQL)
            cursor.execute(f"TRUNCATE {TARGET_TABLE};")
            cursor.copy_expert(
                f"COPY {TARGET_TABLE} ({','.join(TARGET_COLUMNS)}) FROM STDIN WITH CSV DELIMITER E'\\t' NULL '\\N'",
                buffer,
            )
        conn.commit()  # TRUNCATE + COPY atómico
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    # VACUUM ANALYZE fuera de la transacción para refrescar mapa de visibilidad y estadísticas del planner
    pg_hook.run(f"VACUUM ANALYZE {TARGET_TABLE};", autocommit=True)
    print(f"Data saved to PostgreSQL. Table: {TARGET_TABLE}")


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    "etl_instock_unimarc_truncate_and_load",
    default_args=default_args,
    description="Carga diaria de InStock Unimarc desde S3 (datastage/instock_unimarc) a ecommdata.instock.",
    schedule_interval="0 8 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz=TZ),
    catchup=False,
    max_active_runs=1,
    tags=["DATA", "SAP", "ecommdata", "instock", "MAURICIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Extracción de archivos InStock por tienda desde S3
    (`datastage/instock_unimarc/YYYY/MM/DD/{tienda}/instock_{tienda}_YYYYMMDD.csv`) y carga en `ecommdata.instock`. \n
    No existe archivo bandera: el sensor espera (reschedule cada 5 min) a que estén los CSV de todas las tiendas
    activas (excluye 0486 y 0053). Si a las 10:00 siguen faltando tiendas, el DAG falla y NO se carga la tabla. \n
    Carga truncate & load atómica (TRUNCATE + COPY en una sola transacción). \n
    Transformaciones: Centro -> tienda (4 dígitos), PLU_SAP60 -> material (18 dígitos, sin UMV),
    InStock se conserva 1/0, ClaseABC conserva valor original (incluyendo #). \n
    Reproceso manual de otra fecha: trigger con conf `{"fecha": "YYYY-MM-DD"}`.
    """

    t0 = PythonSensor(
        task_id="wait_for_all_stores",
        python_callable=_check_all_stores,
        mode="reschedule",
        poke_interval=5 * 60,
        timeout=3 * 60 * 60,  # Safety net; el deadline real se controla dentro del callable
    )

    t1 = PythonOperator(
        task_id="load_instock",
        python_callable=_load_instock,
    )

    t0 >> t1
