from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

from utils.slack_utils import dag_success_slack, dag_failure_slack

import pendulum
from datetime import datetime, timedelta
import ast
import io
import logging
import pandas as pd
from shapely.geometry import Polygon, Point
from shapely.validation import make_valid
from shapely.prepared import prep
import sqlalchemy
from sqlalchemy import text


def _parse_polygon_coordinates(coord_raw):
    """
    Parsea las coordenadas almacenadas como texto o lista a una geometría Polygon de Shapely,
    asegurando cierre del anillo y validando geometrías complejas.
    """
    if not coord_raw or coord_raw is None:
        return None

    coords = None
    if isinstance(coord_raw, (list, tuple)):
        coords = coord_raw
    else:
        s = str(coord_raw).strip()
        if not s or s.lower() == "nan" or s.lower() == "none":
            return None
        try:
            coords = ast.literal_eval(s)
        except Exception:
            try:
                s_fixed = s.replace("(", "[").replace(")", "]")
                coords = ast.literal_eval(s_fixed)
            except Exception:
                return None

    if not coords or len(coords) < 3:
        return None

    try:
        coords_list = list(coords)
        if coords_list[0] != coords_list[-1]:
            coords_list.append(coords_list[0])

        poly = Polygon(coords_list)
        if not poly.is_valid:
            poly = make_valid(poly)
            if poly.geom_type == "MultiPolygon":
                poly = max(poly.geoms, key=lambda p: p.area)
        return poly
    except Exception as e:
        logging.warning(f"Error construyendo Polygon con coordenadas: {e}")
        return None


def _purgar_historico_antiguo(ds, table_name):
    """
    Elimina registros con fecha_facturacion anterior a 25 meses móviles.
    """
    try:
        host = Variable.get("POSTGRESQL_HOST")
        database = Variable.get("POSTGRESQL_DB")
        username = Variable.get("POSTGRESQL_USER")
        password = Variable.get("POSTGRESQL_PASSWORD")
        conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
        engine = sqlalchemy.create_engine(conn_url)
        with engine.begin() as conn:
            logging.info(f"Purgando registros más antiguos a 25 meses en {table_name}...")
            conn.execute(text(f"""
                DELETE FROM {table_name}
                WHERE fecha_facturacion < ('{ds}'::date - interval '25 month');
            """))
    except Exception as e:
        logging.warning(f"Error purgando histórico antiguo en {table_name}: {e}")


def coordenadas_poligonos(ds):
    """
    Extrae polígonos activos de Unimarc para delivery desde forecast_and_planning.poligonos,
    incluyendo el mapeo a la tienda vigente (id_tienda_actual).
    Usa la fecha ds o, en su defecto, la fecha más reciente disponible.
    """
    coordenadas_poligonos_query = f"""
        SELECT 
            p.id, 
            p.name AS transportadora, 
            p.polygon, 
            p.coordenadas,
            COALESCE(
                t.id_tienda, 
                SUBSTRING(p.polygon FROM '^([0-9]{{4}})'), 
                SUBSTRING(p.name FROM '^([0-9]{{4}})')
            ) AS id_tienda_actual
        FROM forecast_and_planning.poligonos p 
        LEFT JOIN ecommdata.transportadoras t 
            ON t.id::text = p.id::text
        WHERE p."isActive" = true
          AND p.coordenadas IS NOT NULL 
          AND p."deliveryChannel" = 'delivery'
          AND p.fecha = (
              SELECT CASE 
                  WHEN EXISTS (SELECT 1 FROM forecast_and_planning.poligonos WHERE fecha = '{ds}'::date)
                  THEN '{ds}'::date
                  ELSE (SELECT MAX(fecha) FROM forecast_and_planning.poligonos)
              END
          );
    """
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    logging.info(f"Consultando polígonos Unimarc con query:\n{coordenadas_poligonos_query}")
    df = pg_hook.get_pandas_df(coordenadas_poligonos_query)
    logging.info(f"Polígonos extraídos: {len(df.index)}")
    return df


def ordenes_janis(ds, **context):
    """
    Extrae órdenes de Janis Unimarc con coordenadas válidas de entrega.
    Por defecto corre en modo incremental (últimos 7 días).
    Si se pasa conf={"full_refresh": true}, consulta los últimos 25 meses.
    """
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    full_refresh = conf.get("full_refresh", False)

    if full_refresh:
        date_filter = f"oj.fecha_facturacion::date >= '{ds}'::date - interval '25 month'"
        logging.info("Modo FULL REFRESH activado: consultando 25 meses de órdenes Janis Unimarc.")
    else:
        days_back = int(conf.get("days_back", 7))
        date_filter = f"oj.fecha_facturacion::date >= '{ds}'::date - interval '{days_back} day'"
        logging.info(f"Modo INCREMENTAL activado: consultando {days_back} días de órdenes Janis Unimarc.")

    ordenes_janis_query = f"""
        SELECT DISTINCT 
            oj.id AS id_orden, 
            oj.venta_creada_neta AS venta_creada, 
            oj.venta_facturada_neta AS venta_facturada, 
            d.lat, 
            d.lng, 
            d.id_transportadora, 
            t.id_tienda AS id_tienda_origen, 
            oj.fecha_facturacion
        FROM ecommdata.ordenes_janis oj
        LEFT JOIN ecommdata.despachos d 
            ON d.id_orden = oj.id
        LEFT JOIN ecommdata.transportadoras t 
            ON t.id = d.id_transportadora
        WHERE d.lat IS NOT NULL 
          AND d.lng IS NOT NULL
          AND d.lat BETWEEN -56.0 AND -17.0
          AND d.lng BETWEEN -76.0 AND -66.0
          AND d.tipo_despacho = 'delivery'
          AND {date_filter};
    """
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    logging.info(f"Consultando órdenes Unimarc con query:\n{ordenes_janis_query}")
    df = pg_hook.get_pandas_df(ordenes_janis_query)
    logging.info(f"Órdenes extraídas de Janis Unimarc: {len(df.index)}")
    return df


def poligonos_ordenes_to_s3(ds, **context):
    """
    Cruza órdenes y polígonos de Unimarc mediante prefiltrado Bounding Box (NumPy) y validación exacta (Shapely prep)
    y sube el archivo procesado a Amazon S3.
    """
    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"ordenes_poligonos/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    poligonos = coordenadas_poligonos(ds)
    if poligonos.empty:
        logging.warning("No se encontraron polígonos activos en forecast_and_planning.poligonos.")
        return "empty"

    ordenes = ordenes_janis(ds, **context)
    if ordenes.empty:
        logging.warning("No se encontraron órdenes para procesar en la ventana solicitada.")
        return "empty"

    # Construir geometrías válidas de polígonos
    poligonos["geometry"] = poligonos["coordenadas"].apply(_parse_polygon_coordinates)
    poligonos = poligonos.dropna(subset=["geometry"]).reset_index(drop=True)
    if poligonos.empty:
        logging.warning("Ningún polígono cuenta con coordenadas válidas para construir geometrías.")
        return "empty"

    # Cruce espacial Point-in-Polygon de alta velocidad sin requerir rtree/pygeos
    # (Pre-filtrado por Bounding Box con NumPy + validación con Shapely prep en C/GEOS)
    order_lngs = ordenes["lng"].values
    order_lats = ordenes["lat"].values
    matched_dfs = []

    for _, poly_row in poligonos.iterrows():
        poly_geom = poly_row["geometry"]
        poly_name = poly_row["polygon"]
        cur_tienda = poly_row.get("id_tienda_actual")

        minx, miny, maxx, maxy = poly_geom.bounds
        mask = (order_lngs >= minx) & (order_lngs <= maxx) & (order_lats >= miny) & (order_lats <= maxy)
        if not mask.any():
            continue

        candidates = ordenes[mask].copy()
        prep_poly = prep(poly_geom)
        is_inside = [
            prep_poly.contains(Point(x, y))
            for x, y in zip(candidates["lng"].values, candidates["lat"].values)
        ]

        if any(is_inside):
            matched = candidates[is_inside].copy()
            matched["nombre_poligono_actual"] = poly_name
            matched["id_tienda_actual"] = cur_tienda
            matched_dfs.append(matched)

    if not matched_dfs:
        logging.warning("No hubo coincidencias espaciales entre órdenes y polígonos de Unimarc.")
        return "empty"

    joined = pd.concat(matched_dfs, ignore_index=True)

    target_columns = [
        "id_orden",
        "nombre_poligono_actual",
        "id_tienda_origen",
        "id_tienda_actual",
        "id_transportadora",
        "fecha_facturacion",
        "venta_creada",
        "venta_facturada",
        "lat",
        "lng"
    ]
    df_final = joined[target_columns].drop_duplicates(subset=["id_orden", "nombre_poligono_actual"])
    logging.info(f"Total registros orden-polígono generados: {len(df_final)}")

    buffer = io.StringIO()
    df_final.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"ordenes_poligonos/{exec_date}/ordenes_poligonos_{date_aux}.csv"
    buffer.seek(0)
    s3_hook.load_string(
        buffer.getvalue(),
        key=filename,
        bucket_name=s3_bucket,
        replace=True,
        encrypt=False
    )
    logging.info(f"Archivo subido exitosamente a S3: s3://{s3_bucket}/{filename}")
    return filename


def poligonos_ordenes_to_postgres(ti, ds, **context):
    """
    Descarga archivo desde S3 y aplica carga incremental, sincronización de tienda actual
    y purga de 25 meses en PostgreSQL dentro de una transacción atómica.
    """
    filename = ti.xcom_pull(key="return_value", task_ids=["poligonos_ordenes_to_s3"])[0]

    if not filename or filename == "empty":
        logging.info("No hay registros nuevos para cargar. Purgando registros obsoletos si aplica.")
        _purgar_historico_antiguo(ds, "forecast_and_planning.ordenes_poligonos")
        return

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    if not s3_hook.check_for_key(filename, bucket_name=s3_bucket):
        raise Exception(f"El archivo {filename} no existe en S3 {s3_bucket}.")

    s_stock_object = s3_hook.get_key(filename, bucket_name=s3_bucket)
    df = pd.read_csv(s_stock_object.get()["Body"])
    if df.empty:
        logging.info("El DataFrame desde S3 está vacío. Finalizando.")
        _purgar_historico_antiguo(ds, "forecast_and_planning.ordenes_poligonos")
        return

    logging.info(f"Registros extraídos desde S3: {len(df.index)}")

    column_types = {
        "id_orden": "string",
        "nombre_poligono_actual": "string",
        "id_tienda_origen": "string",
        "id_tienda_actual": "string",
        "id_transportadora": "string",
        "fecha_facturacion": "string",
        "venta_creada": "float",
        "venta_facturada": "float",
        "lat": "float",
        "lng": "float"
    }
    df = df.astype(column_types, errors="ignore")
    df = df.drop_duplicates(subset=["id_orden", "nombre_poligono_actual"])

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")

    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    full_refresh = conf.get("full_refresh", False)

    with engine.begin() as conn:
        if full_refresh:
            logging.info("Modo FULL REFRESH: Truncando forecast_and_planning.ordenes_poligonos...")
            conn.execute(text("TRUNCATE forecast_and_planning.ordenes_poligonos"))
        else:
            days_back = int(conf.get("days_back", 7))
            logging.info(f"Modo INCREMENTAL: Borrando registros de los últimos {days_back} días para evitar duplicados...")
            conn.execute(text(f"""
                DELETE FROM forecast_and_planning.ordenes_poligonos
                WHERE fecha_facturacion >= ('{ds}'::date - interval '{days_back} day');
            """))

        logging.info(f"Insertando {len(df)} registros en forecast_and_planning.ordenes_poligonos...")
        df.to_sql(
            name="ordenes_poligonos",
            con=conn,
            schema="forecast_and_planning",
            if_exists="append",
            index=False,
            chunksize=20000,
            method="multi"
        )

        logging.info("Sincronizando id_tienda_actual en todo el histórico de 25 meses...")
        conn.execute(text("""
            UPDATE forecast_and_planning.ordenes_poligonos op
            SET id_tienda_actual = pol.id_tienda_actual
            FROM (
                SELECT 
                    p.polygon,
                    COALESCE(
                        t.id_tienda, 
                        SUBSTRING(p.polygon FROM '^([0-9]{4})'), 
                        SUBSTRING(p.name FROM '^([0-9]{4})')
                    ) AS id_tienda_actual
                FROM forecast_and_planning.poligonos p
                LEFT JOIN ecommdata.transportadoras t ON t.id::text = p.id::text
                WHERE p."isActive" = true
                  AND p."deliveryChannel" = 'delivery'
                  AND p.fecha = (SELECT MAX(fecha) FROM forecast_and_planning.poligonos)
            ) pol
            WHERE op.nombre_poligono_actual = pol.polygon
              AND pol.id_tienda_actual IS NOT NULL
              AND op.id_tienda_actual IS DISTINCT FROM pol.id_tienda_actual;
        """))

        logging.info("Purgando registros con más de 25 meses de antigüedad (> 25 month)...")
        conn.execute(text(f"""
            DELETE FROM forecast_and_planning.ordenes_poligonos
            WHERE fecha_facturacion < ('{ds}'::date - interval '25 month');
        """))

    logging.info("Carga completada exitosamente en PostgreSQL (forecast_and_planning.ordenes_poligonos).")


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    'etl_poligonos_ordenes',
    default_args=default_args,
    description="Carga tabla ordenes_poligonos cruzando ordenes de Janis y polígonos VTEX Unimarc",
    schedule_interval="20 8 * * *",
    start_date=pendulum.datetime(2023, 12, 6, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["DATA", "ordenes", "forecast_and_planning", "polygons", "unimarc", "PATRICIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    ### ETL Órdenes y Polígonos Unimarc (Incremental 7 días + Purga 25 Meses)
    
    1. **Extracción**:
       - Extrae polígonos activos desde `forecast_and_planning.poligonos`.
       - Extrae órdenes desde `ecommdata.ordenes_janis` (por defecto últimos 7 días; 25 meses con `conf={"full_refresh": true}`).
    
    2. **Cruce Espacial**:
       - Point-in-Polygon vectorizado de alta velocidad mediante Bounding Box con NumPy + `shapely.prepared.prep` en C/GEOS.
    
    3. **Carga en Postgres**:
       - Borra los últimos 7 días para evitar duplicados.
       - Inserta el lote deduplicado en `forecast_and_planning.ordenes_poligonos`.
       - Purga automáticamente registros con más de 25 meses.
    """ 

    t0 = PythonOperator(
        task_id='poligonos_ordenes_to_s3',
        python_callable=poligonos_ordenes_to_s3,
    )

    t1 = PythonOperator(
        task_id="poligonos_ordenes_to_postgres",
        python_callable=poligonos_ordenes_to_postgres,
    )

    t0 >> t1