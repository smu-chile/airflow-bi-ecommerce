from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from datetime import datetime
import pendulum

from utils.slack_utils import dag_success_slack, dag_failure_slack

#####################################################################################################
#                         ETL – Peso Estimado basado en Found Rate                                  #
#####################################################################################################

QUERY_FR_SEMANAL = """
SELECT
    ms.id_tienda                                                                            AS tienda,
    semana,
    ms.peso,
    fecha_inicio,
    fecha_fin,
    COUNT(CASE WHEN frp.estado_foundrate = 3 THEN frp.ref_id END)                          AS productos_completos,
    COUNT(CASE WHEN frp.producto_substituto IS FALSE THEN frp.ref_id END)                  AS productos_solicitados,
    COUNT(CASE WHEN frp.estado_foundrate = 3 THEN frp.ref_id END)::decimal
        / NULLIF(COUNT(CASE WHEN frp.producto_substituto IS FALSE THEN frp.ref_id END), 0) AS fr
FROM catalogo.matriz_ss ms
JOIN operaciones_unimarc.found_rate_productos frp
    ON frp.id_tienda = ms.id_tienda
CROSS JOIN (
    VALUES
        ('Hace 1 semana',  (DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '7 days')::date,   (DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1 day')::date),
        ('Hace 2 semanas', (DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '14 days')::date,  (DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '8 days')::date),
        ('Hace 3 semanas', (DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '21 days')::date,  (DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '15 days')::date)
) AS semanas(semana, fecha_inicio, fecha_fin)
WHERE frp.fecha_facturacion BETWEEN fecha_inicio AND fecha_fin
GROUP BY ms.id_tienda, semana, ms.peso, fecha_inicio, fecha_fin
ORDER BY ms.id_tienda, fecha_inicio;
"""




def _calcular_peso_estimado(ds, **kwargs):
    """
    1. Extrae datos de FR por tienda y semana (últimas 3 semanas).
    2. Calcula FR % semanal y peso semanal.
    3. Calcula FR promedio móvil de 3 semanas (suma acumulada encontrados / solicitados).
    4. Calcula peso esperado y sube CSV a S3.
    """
    import pandas as pd
    import numpy as np
    import io
    from datetime import date, timedelta

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()

    # ── 1. Extraer datos de FR semanal ──────────────────────────────────────────
    cursor.execute(QUERY_FR_SEMANAL)
    results = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    if not results:
        print("No se encontraron registros de Found Rate. Finalizando.")
        cursor.close()
        pg_connection.close()
        return

    df = pd.DataFrame(results, columns=columns)
    print(f"Registros obtenidos de BD: {len(df)}")

    # Asegurar tipos
    df["productos_completos"] = pd.to_numeric(df["productos_completos"])
    df["productos_solicitados"] = pd.to_numeric(df["productos_solicitados"])
    # Renombrar 'fr' a 'fr_pct' y convertir a porcentaje (ya que la query retorna 0-1)
    df["fr_pct"] = pd.to_numeric(df["fr"]) * 100

    # Ordenar por tienda y fecha_inicio
    df = df.sort_values(["tienda", "fecha_inicio"]).reset_index(drop=True)

    # ── 2. Calcular peso estimado base (para hace 2 y 3 semanas) ───────────────
    df["peso_estimado_calc"] = 0.3 + ((98.6 - df["fr_pct"]) * 0.1)
    df["peso_estimado_calc"] = df["peso_estimado_calc"].clip(lower=0.3, upper=1.5)

    # ── 3. Asignar peso según la semana ─────────────────────────────────────────
    # Si es "Hace 1 semana", usa el peso de la BD (columna "peso").
    # Si es "Hace 2 semanas" o "Hace 3 semanas", usa el calculado.
    df["peso_final"] = np.where(
        df["semana"] == "Hace 1 semana", 
        df["peso"], 
        df["peso_estimado_calc"]
    )

    # ── 4. Calcular FR promedio móvil de las 3 semanas para la "Semana actual" ──
    df["encontrados_3s"] = (
        df.groupby("tienda")["productos_completos"]
        .rolling(window=3, min_periods=1)
        .sum()
        .reset_index(level=0, drop=True)
    )
    df["solicitados_3s"] = (
        df.groupby("tienda")["productos_solicitados"]
        .rolling(window=3, min_periods=1)
        .sum()
        .reset_index(level=0, drop=True)
    )
    
    df["fr_promedio_3s"] = (df["encontrados_3s"] / df["solicitados_3s"]) * 100
    df["peso_semana_actual"] = 0.3 + ((98 - df["fr_promedio_3s"]) * 0.1)
    df["peso_semana_actual"] = df["peso_semana_actual"].clip(lower=0.3, upper=1.5)

    # ── 5. Crear Dataframe de Salida ────────────────────────────────────────────
    # a. Parte histórica (Hace 1, 2, 3 semanas)
    df_historico = df[[
        "tienda", "semana", "fecha_inicio", "fecha_fin",
        "productos_completos", "productos_solicitados", "fr_pct", "peso_final"
    ]].copy()
    
    df_historico = df_historico.rename(columns={
        "tienda": "tiendas",
        "fecha_inicio": "fecha inicio",
        "fecha_fin": "fecha fin",
        "fr_pct": "fr",
        "peso_final": "peso"
    })

    # b. Construir fila para "Semana actual"
    # Tomamos la última fila disponible por tienda para sacar el peso_semana_actual
    inicio_semana_actual = date.today() - timedelta(days=date.today().weekday())
    fin_semana_actual = inicio_semana_actual + timedelta(days=6)

    df_actual = df.sort_values("fecha_inicio").groupby("tienda").tail(1).copy()
    
    df_actual["semana"] = "Semana actual"
    df_actual["fecha inicio"] = inicio_semana_actual
    df_actual["fecha fin"] = fin_semana_actual
    df_actual["productos_completos"] = None
    df_actual["productos_solicitados"] = None
    df_actual["fr"] = df_actual["fr_promedio_3s"] # FR usado como base para el calculo
    df_actual["peso"] = df_actual["peso_semana_actual"]
    df_actual["tiendas"] = df_actual["tienda"]

    df_actual = df_actual[[
        "tiendas", "semana", "fecha inicio", "fecha fin",
        "productos_completos", "productos_solicitados", "fr", "peso"
    ]]

    # Unir histórico y actual
    df_export = pd.concat([df_historico, df_actual], ignore_index=True)
    df_export = df_export.sort_values(["tiendas", "fecha inicio"]).reset_index(drop=True)

    # Redondear valores
    df_export["fr"] = df_export["fr"].round(2)
    df_export["peso"] = df_export["peso"].round(2)

    print(f"\n{'='*90}")
    print(f"  VISTA PREVIA DEL CSV (Mostrando 1 tienda como ejemplo)")
    print(f"{'='*90}")
    # Mostrar el bloque de una sola tienda de ejemplo en los logs
    if len(df_export) > 0:
        tienda_ejemplo = df_export['tiendas'].iloc[0]
        print(df_export[df_export['tiendas'] == tienda_ejemplo].to_string(index=False))

    # ── 6. Subir CSV a S3 ────────────────────────────────────────────────────────
    exec_date = ds.replace("-", "/")
    exec_date_formatted = datetime.now().strftime("%Y%m%d")
    join_file_name = f"Peso_estimado/out/{exec_date}/{exec_date_formatted}.csv"

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    buffer = io.StringIO()
    df_export.to_csv(buffer, header=True, index=False, encoding="utf-8")
    buffer.seek(0)

    s3_hook.load_string(
        buffer.getvalue(),
        key=join_file_name,
        bucket_name=s3_bucket,
        replace=True,
        encrypt=False,
    )
    print(f"\nArchivo CSV subido a S3: s3://{s3_bucket}/{join_file_name}")

    cursor.close()
    pg_connection.close()


#####################################################################################################
#                                        DAG Definition                                             #
#####################################################################################################

default_args = {
    "owner": "ecommerce_ops",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    "etl_peso_estimado_fr",
    default_args=default_args,
    description="Calcula el peso esperado por tienda basado en el Found Rate de las últimas 3 semanas",
    schedule_interval="0 6 * * 1",  # Todos los lunes a las 06:00 AM
    start_date=pendulum.datetime(2025, 5, 26, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=["OPS", "catalogo", "found_rate", "peso_estimado","Rodrigo"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    ### ETL Peso Estimado - Found Rate

    Calcula y actualiza el peso de cada tienda en `catalogo.matriz_ss` 
    basándose en el Found Rate de las últimas 3 semanas.

    **Lógica:**
    1. Obtiene productos_completos y productos_solicitados por tienda/semana.
    2. Calcula FR % semanal y un promedio móvil de 3 semanas.
    3. Aplica la fórmula: `peso = 0.3 + ((98 - FR%) * 0.1)`, limitado entre 0.3 y 1.5.
    4. Actualiza `catalogo.matriz_ss.peso` con el valor calculado.
    """

    t0 = PythonOperator(
        task_id="calcular_peso_estimado",
        python_callable=_calcular_peso_estimado,
    )

    t0
