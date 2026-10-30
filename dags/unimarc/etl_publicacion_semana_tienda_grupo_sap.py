from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.slack_utils import dag_success_slack, dag_failure_slack

import csv
import io
import time
import pendulum

TZ = "America/Santiago"
TARGET_TABLE = "catalogo.publicacion_semana_tienda_grupo_sap"
S3_ARCHIVE_PREFIX = "catalogo/publicacion_semana_tienda_grupo_sap"
RETENTION_DAYS = 365  # 1 año de historia activa en PostgreSQL
TIENDAS_EXCLUIDAS = ("0486", "0053", "0398", "0054")
CATEGORIAS_C1_EXCLUIDAS = (
    "Inactivos",
    "No Trabajar",
    "Sustitución",
    "Tabaquería",
    "Tabaqueria",
)

UPSERT_WEEK_SQL = f"""
INSERT INTO {TARGET_TABLE} (
    semana_ano_texto,
    id_tienda,
    negocio_sap,
    seccion_sap,
    linea_sap,
    categoria_sap,
    grupo_id,
    grupo_sap,
    inweb,
    veces_disponible_web,
    veces_medido,
    instock,
    veces_instock,
    productos_total,
    productos_surtido_ecommerce,
    productos_no_surtido_ecommerce
)
SELECT
    %(semana_ano_texto)s AS semana_ano_texto,
    pc.id_tienda,
    MAX(m.negocio_sap)   AS negocio_sap,
    MAX(m.seccion_sap)   AS seccion_sap,
    MAX(m.linea_sap)     AS linea_sap,
    MAX(m.categoria_sap) AS categoria_sap,
    m.grupo_id,
    MAX(m.grupo_sap)     AS grupo_sap,
    AVG(CASE WHEN pc.surtido_ecommerce IS TRUE THEN pc.disponible_web::int END)::numeric(11, 10) AS inweb,
    COALESCE(SUM(CASE WHEN pc.surtido_ecommerce IS TRUE THEN pc.disponible_web::int END), 0) AS veces_disponible_web,
    COUNT(CASE WHEN pc.surtido_ecommerce IS TRUE THEN pc.fecha_hora END) AS veces_medido,
    AVG(CASE WHEN pc.surtido_ecommerce IS TRUE THEN pc.instock END)::numeric(11, 10) AS instock,
    SUM(CASE WHEN pc.surtido_ecommerce IS TRUE THEN pc.instock END) AS veces_instock,
    COUNT(DISTINCT pc.ref_id) AS productos_total,
    COUNT(DISTINCT CASE WHEN pc.surtido_ecommerce IS TRUE THEN pc.ref_id END) AS productos_surtido_ecommerce,
    COUNT(DISTINCT CASE WHEN pc.surtido_ecommerce IS FALSE THEN pc.ref_id END) AS productos_no_surtido_ecommerce
FROM ecommdata.publicacion_catalogo pc
JOIN ecommdata.tiendas t
    ON t.id = pc.id_tienda
   AND t.status = 1
   AND t.id NOT IN %(tiendas_excluidas)s
LEFT JOIN tmp_lista8_bloqueados li
    ON li.id_tienda = pc.id_tienda
   AND li.ref_id = pc.ref_id
LEFT JOIN ecommdata.maestra_sku_proveedor m
    ON LPAD(SPLIT_PART(pc.ref_id, '-', 1), 18, '0') = m.material
WHERE pc.fecha_hora >= %(fecha_inicio)s::date
  AND pc.fecha_hora <  (%(fecha_fin)s::date + INTERVAL '1 day')
  AND (
      pc.c1 NOT IN %(categorias_excluidas)s
      OR pc.c1 IS NULL
  )
  AND m.grupo_id IS NOT NULL
  AND li.ref_id IS NULL
  AND (%(tiendas_filtro)s::text[] IS NULL OR pc.id_tienda = ANY(%(tiendas_filtro)s::text[]))
GROUP BY
    pc.id_tienda,
    m.grupo_id
ON CONFLICT ON CONSTRAINT uq_publicacion_semana_tienda_grupo_sap
DO UPDATE SET
    negocio_sap                    = EXCLUDED.negocio_sap,
    seccion_sap                    = EXCLUDED.seccion_sap,
    linea_sap                      = EXCLUDED.linea_sap,
    categoria_sap                  = EXCLUDED.categoria_sap,
    grupo_sap                      = EXCLUDED.grupo_sap,
    inweb                          = EXCLUDED.inweb,
    veces_disponible_web           = EXCLUDED.veces_disponible_web,
    veces_medido                   = EXCLUDED.veces_medido,
    instock                        = EXCLUDED.instock,
    veces_instock                  = EXCLUDED.veces_instock,
    productos_total                = EXCLUDED.productos_total,
    productos_surtido_ecommerce    = EXCLUDED.productos_surtido_ecommerce,
    productos_no_surtido_ecommerce = EXCLUDED.productos_no_surtido_ecommerce,
    fecha_actualizacion            = NOW();
"""


def _resolve_weeks_to_process(cursor, data_interval_end, dag_run):
    """
    Determina las semanas completas (semana_ano_texto, fecha_inicio, fecha_fin) a procesar
    usando power_bi.calendario para nunca pisar una semana con un rango parcial de días.

    Soporta override manual en dag_run.conf:
      - {"semana": "2026W43"} o {"semanas": ["2026W42", "2026W43"]}
      - {"init": "2026-10-19", "end": "2026-10-25"}
      - {"fecha": "2026-10-25"}
    Por defecto evalúa la semana correspondiente al día anterior (ayer en hora Chile):
      - De martes a domingo actualiza el acumulado de la semana en curso (WTD).
      - El lunes cierra y consolida la semana anterior completa (lunes a domingo).
    """
    conf = (dag_run.conf or {}) if dag_run else {}

    semanas_conf = conf.get("semanas") or ([conf["semana"]] if conf.get("semana") else None)
    if semanas_conf:
        cursor.execute(
            """
            SELECT
                semana_ano_texto,
                MIN(fecha)::date AS fecha_inicio,
                MAX(fecha)::date AS fecha_fin
            FROM power_bi.calendario
            WHERE semana_ano_texto IN %s
            GROUP BY semana_ano_texto
            ORDER BY MIN(fecha);
            """,
            (tuple(semanas_conf),),
        )
        return cursor.fetchall()

    if conf.get("init") and conf.get("end"):
        init_date = pendulum.parse(str(conf["init"]), tz=TZ).to_date_string()
        end_date = pendulum.parse(str(conf["end"]), tz=TZ).to_date_string()
    elif conf.get("fecha"):
        init_date = pendulum.parse(str(conf["fecha"]), tz=TZ).to_date_string()
        end_date = init_date
    else:
        eval_date = data_interval_end.in_timezone(TZ).subtract(days=1).to_date_string()
        init_date = eval_date
        end_date = eval_date

    cursor.execute(
        """
        SELECT
            c.semana_ano_texto,
            MIN(c.fecha)::date AS fecha_inicio,
            MAX(c.fecha)::date AS fecha_fin
        FROM power_bi.calendario c
        WHERE c.semana_ano_texto IN (
            SELECT DISTINCT semana_ano_texto
            FROM power_bi.calendario
            WHERE fecha >= %s::date
              AND fecha <= %s::date
              AND semana_ano_texto IS NOT NULL
        )
        GROUP BY c.semana_ano_texto
        ORDER BY MIN(c.fecha);
        """,
        (init_date, end_date),
    )
    return cursor.fetchall()


def _resolve_target_stores(dag_run):
    """
    Obtiene el filtro opcional de tiendas si se especificó en dag_run.conf:
      {"tiendas": ["0581", "0018"]} o {"id_tienda": "0581"}.
    Retorna una lista de tiendas o None si no hay filtro puntual (aplica a todas las tiendas activas).
    """
    conf = (dag_run.conf or {}) if dag_run else {}
    tiendas_conf = conf.get("tiendas") or ([conf["id_tienda"]] if conf.get("id_tienda") else None)

    if tiendas_conf:
        return sorted(
            {
                str(t).strip().zfill(4)
                for t in tiendas_conf
                if str(t).strip().zfill(4) not in TIENDAS_EXCLUIDAS
            }
        )
    return None


def _load_publicacion_semana_tienda_grupo_sap(data_interval_end, dag_run, **_):
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    try:
        # 1. Configurar memoria de trabajo de la sesión para soportar hash joins y agregación masiva
        cursor.execute("SET work_mem = '256MB';")
        conn.commit()

        # 2. Resolver semanas y tiendas a procesar
        semanas = _resolve_weeks_to_process(cursor, data_interval_end, dag_run)
        if not semanas:
            raise AirflowFailException("No se encontraron semanas válidas en power_bi.calendario para procesar.")

        tiendas_filtro = _resolve_target_stores(dag_run)
        print(f"Semanas a procesar ({len(semanas)}): {[(s[0], str(s[1]), str(s[2])) for s in semanas]}")
        print(
            f"Filtro puntual de tiendas: "
            f"{tiendas_filtro if tiendas_filtro else 'Todas las tiendas activas (excluyendo TIENDAS_EXCLUIDAS)'}"
        )

        # 3. Precalcular bloqueados de lista8 UNA sola vez en tabla temporal indexada de sesión
        t_bloq = time.time()
        cursor.execute(
            """
            DROP TABLE IF EXISTS tmp_lista8_bloqueados;
            CREATE TEMP TABLE tmp_lista8_bloqueados (
                id_tienda VARCHAR(10) NOT NULL,
                ref_id    VARCHAR(50) NOT NULL,
                PRIMARY KEY (id_tienda, ref_id)
            ) ON COMMIT PRESERVE ROWS;

            INSERT INTO tmp_lista8_bloqueados (id_tienda, ref_id)
            SELECT DISTINCT
                l.id_tienda,
                CONCAT(l.material, '-', l.umv) AS ref_id
            FROM ecommdata.lista8 l
            WHERE l.material IS NOT NULL
              AND l.umv IS NOT NULL
              AND (l.bloq_centro IS NOT NULL OR l.bloq_formato IS NOT NULL)
            ON CONFLICT DO NOTHING;

            ANALYZE tmp_lista8_bloqueados;
            """
        )
        conn.commit()
        cursor.execute("SELECT COUNT(1) FROM tmp_lista8_bloqueados;")
        total_bloq = cursor.fetchone()[0]
        print(f"Tabla temporal tmp_lista8_bloqueados lista con {total_bloq} combinaciones bloqueadas ({time.time() - t_bloq:.2f}s)")

        # 4. Procesar semana por semana en un único barrido de las particiones (Set-based SQL)
        total_upserted_global = 0
        start_global = time.time()

        for semana_ano_texto, fecha_inicio, fecha_fin in semanas:
            # Verificar que existan registros en publicacion_catalogo para no alterar históricos ya purgados (>28 días)
            cursor.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM ecommdata.publicacion_catalogo
                    WHERE fecha_hora >= %s::date
                      AND fecha_hora <  (%s::date + INTERVAL '1 day')
                    LIMIT 1
                );
                """,
                (fecha_inicio, fecha_fin),
            )
            has_data = cursor.fetchone()[0]
            if not has_data:
                print(
                    f"⚠️ Semana {semana_ano_texto} ({fecha_inicio} al {fecha_fin}) sin datos en "
                    f"ecommdata.publicacion_catalogo (posiblemente fuera de retención de 28 días). Se omite."
                )
                continue

            print(f"=== Iniciando carga masiva semana {semana_ano_texto} ({fecha_inicio} al {fecha_fin}) ===")
            start_week = time.time()
            params = {
                "semana_ano_texto": semana_ano_texto,
                "fecha_inicio": fecha_inicio,
                "fecha_fin": fecha_fin,
                "categorias_excluidas": CATEGORIAS_C1_EXCLUIDAS,
                "tiendas_excluidas": TIENDAS_EXCLUIDAS,
                "tiendas_filtro": tiendas_filtro,
            }
            cursor.execute(UPSERT_WEEK_SQL, params)
            affected = cursor.rowcount
            conn.commit()
            total_upserted_global += affected
            print(
                f"✅ Semana {semana_ano_texto} completada en {time.time() - start_week:.1f}s | "
                f"Filas agregadas/actualizadas: {affected}"
            )

        print(
            f"Proceso global finalizado en {time.time() - start_global:.1f}s | "
            f"Total filas actualizadas: {total_upserted_global}"
        )

    finally:
        cursor.close()
        conn.close()

    # Refrescar estadísticas tras la carga
    pg_hook.run(f"VACUUM ANALYZE {TARGET_TABLE};", autocommit=True)
    print(f"VACUUM ANALYZE ejecutado sobre {TARGET_TABLE}.")


def _archive_and_purge_old_weeks(data_interval_end, **_):
    """
    Retención de 1 año (365 días):
    1. Identifica semanas completas cuyo último día (MAX(fecha) en calendario) sea anterior
       a (fecha_referencia - 365 días).
    2. Por cada semana antigua, exporta sus filas a CSV, sube el archivo a S3 en
       s3://<bucket>/catalogo/publicacion_semana_tienda_grupo_sap/<año>/<semana_ano_texto>.csv,
       valida existencia, tamaño > 0 y cuadratura exacta de filas.
    3. Recién cuando el respaldo en S3 está confirmado, ejecuta el DELETE de esa semana en Postgres.
    """
    cutoff_date = data_interval_end.in_timezone(TZ).subtract(days=RETENTION_DAYS).to_date_string()
    print(f"Evaluando retención histórica (> {RETENTION_DAYS} días, semanas cerradas antes de {cutoff_date})...")

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    purged_any = False

    try:
        cursor.execute(
            f"""
            WITH semanas_calendario AS (
                SELECT
                    semana_ano_texto,
                    MIN(fecha)::date AS fecha_inicio,
                    MAX(fecha)::date AS fecha_fin
                FROM power_bi.calendario
                WHERE semana_ano_texto IS NOT NULL
                GROUP BY semana_ano_texto
            )
            SELECT
                t.semana_ano_texto,
                sc.fecha_inicio,
                sc.fecha_fin,
                COUNT(1) AS total_filas
            FROM {TARGET_TABLE} t
            INNER JOIN semanas_calendario sc
                ON sc.semana_ano_texto = t.semana_ano_texto
            WHERE sc.fecha_fin < %s::date
            GROUP BY t.semana_ano_texto, sc.fecha_inicio, sc.fecha_fin
            ORDER BY sc.fecha_inicio;
            """,
            (cutoff_date,),
        )
        expired_weeks = cursor.fetchall()

        if not expired_weeks:
            print("No existen semanas con antigüedad mayor a 1 año para respaldar y purgar.")
            return

        s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
        s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

        print(f"Se encontraron {len(expired_weeks)} semanas antiguas para respaldar en S3 antes de borrar.")

        for semana_ano_texto, fecha_inicio, fecha_fin, total_filas in expired_weeks:
            anio_semana = str(semana_ano_texto).split("W")[0]
            s3_key = f"{S3_ARCHIVE_PREFIX}/{anio_semana}/{semana_ano_texto}.csv"

            print(
                f"Exportando semana {semana_ano_texto} ({fecha_inicio} al {fecha_fin} | "
                f"{total_filas} filas) -> s3://{s3_bucket}/{s3_key}"
            )

            buffer = io.StringIO()
            copy_sql = cursor.mogrify(
                f"""
                COPY (
                    SELECT *
                    FROM {TARGET_TABLE}
                    WHERE semana_ano_texto = %s
                    ORDER BY id_tienda, grupo_id
                ) TO STDOUT WITH CSV HEADER
                """,
                (semana_ano_texto,),
            ).decode("utf-8")
            cursor.copy_expert(copy_sql, buffer)

            csv_content = buffer.getvalue()
            exported_rows = sum(1 for _ in csv.reader(io.StringIO(csv_content))) - 1  # Descontar header
            if exported_rows != total_filas or exported_rows <= 0:
                raise AirflowFailException(
                    f"Descuadre al exportar {semana_ano_texto}: esperadas={total_filas}, "
                    f"exportadas={exported_rows}. Se aborta sin borrar de Postgres."
                )

            # Subir respaldo a S3
            s3_hook.load_string(
                string_data=csv_content,
                key=s3_key,
                bucket_name=s3_bucket,
                replace=True,
                encrypt=False,
            )

            # Verificación estricta del respaldo en S3 antes de ejecutar DELETE
            if not s3_hook.check_for_key(s3_key, bucket_name=s3_bucket):
                raise AirflowFailException(
                    f"No se encontró el archivo en S3 tras la carga ({s3_key}). "
                    f"No se borrará la semana {semana_ano_texto} de Postgres."
                )

            s3_obj = s3_hook.get_key(s3_key, bucket_name=s3_bucket)
            s3_bytes = getattr(s3_obj, "content_length", 0) or 0
            if s3_bytes <= 0:
                raise AirflowFailException(
                    f"El archivo respaldado en S3 tiene 0 bytes ({s3_key}). "
                    f"No se borrará la semana {semana_ano_texto} de Postgres."
                )

            # Recién confirmado el respaldo íntegro en S3, se elimina la semana de Postgres
            cursor.execute(
                f"DELETE FROM {TARGET_TABLE} WHERE semana_ano_texto = %s;",
                (semana_ano_texto,),
            )
            deleted_rows = cursor.rowcount
            conn.commit()
            purged_any = True

            print(
                f"✅ Semana {semana_ano_texto} respaldada en s3://{s3_bucket}/{s3_key} "
                f"({s3_bytes:,} bytes, {exported_rows} filas) y eliminada de Postgres ({deleted_rows} filas)."
            )

    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

    if purged_any:
        pg_hook.run(f"VACUUM ANALYZE {TARGET_TABLE};", autocommit=True)
        print(f"VACUUM ANALYZE ejecutado sobre {TARGET_TABLE} tras purga histórica.")


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    "etl_publicacion_semana_tienda_grupo_sap",
    default_args=default_args,
    description=(
        "Carga incremental semanal (WTD diario y cierre semanal los lunes) de disponibilidad "
        "In-Web y conteo de SKUs por tienda y Grupo SAP, con retención de 1 año y respaldo previo en S3."
    ),
    schedule_interval="30 9 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz=TZ),
    catchup=False,
    max_active_runs=1,
    tags=["DATA", "SAP", "catalogo", "publicacion_catalogo", "unimarc", "MAURICIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    ### Carga de `catalogo.publicacion_semana_tienda_grupo_sap`
    Resume semanalmente `ecommdata.publicacion_catalogo` por `semana_ano_texto`, `id_tienda` y `grupo_id` (con su jerarquía SAP desde `ecommdata.maestra_sku_proveedor`), excluyendo SKUs bloqueados en `ecommdata.lista8` (`bloq_centro IS NOT NULL OR bloq_formato IS NOT NULL`).

    #### Planificación (`30 9 * * *` Chile)
    Corre después de `calendar_table_etl` (06:30), `etl_lista8_datastage_truncate_and_load` (07:40), `etl_publicacion_catalogo` (08:00) y `etl_skus_master_table` (lunes 09:00):
    - **Martes a domingo:** actualiza la semana en curso (*Week-To-Date*).
    - **Lunes:** cierra y consolida la semana completa anterior (lunes a domingo).

    #### Retención y Respaldo en S3 (`archive_and_purge_old_weeks`)
    - Mantiene **1 año (365 días)** de semanas activas en PostgreSQL.
    - Las semanas cuyo último día supera 1 año se exportan a `s3://<AWS_S3_BUCKET_NAME>/catalogo/publicacion_semana_tienda_grupo_sap/<año>/<semana_ano_texto>.csv`.
    - Solo tras validar cuadratura de filas y existencia/tamaño `> 0` en S3, se eliminan de PostgreSQL.

    #### Ejecución manual / Backfill (`Trigger DAG w/ config`)
    - Semana específica: `{"semana": "2026W43"}` o `{"semanas": ["2026W42", "2026W43"]}`
    - Rango de fechas (se expande automáticamente a semanas completas del calendario): `{"init": "2026-10-05", "end": "2026-10-25"}`
    - Filtrar tienda(s) puntual(es) para pruebas: `{"semana": "2026W43", "tiendas": ["0581"]}`
    """

    load_publicacion_semana_grupo_sap = PythonOperator(
        task_id="load_publicacion_semana_tienda_grupo_sap",
        python_callable=_load_publicacion_semana_tienda_grupo_sap,
    )

    archive_and_purge_old_weeks = PythonOperator(
        task_id="archive_and_purge_old_weeks",
        python_callable=_archive_and_purge_old_weeks,
    )

    load_publicacion_semana_grupo_sap >> archive_and_purge_old_weeks
