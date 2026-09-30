from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

from utils.slack_utils import dag_success_slack, dag_failure_slack

import pendulum
from datetime import datetime, timedelta
import ast
import json
import logging
import io

# Diccionario base de cuentas VTEX Alvi (como fallback o referencia rápida)
# Para nuevas tiendas, el DAG también consulta dinámicamente Variable.get(f"VTEX_ALVI{id_tienda}_ACCOUNT_NAME")
DEFAULT_VTEX_ACCOUNTS = {
    "3074": "VTEX_ALVI3074_ACCOUNT_NAME",
    "3089": "VTEX_ALVI3089_ACCOUNT_NAME",
    "3092": "VTEX_ALVI3092_ACCOUNT_NAME",
    "3093": "VTEX_ALVI3093_ACCOUNT_NAME",
    "3098": "VTEX_ALVI3098_ACCOUNT_NAME",
    "3172": "VTEX_ALVI3172_ACCOUNT_NAME",
    "3180": "VTEX_ALVI3180_ACCOUNT_NAME",
    "3181": "VTEX_ALVI3181_ACCOUNT_NAME",
    "3187": "VTEX_ALVI3187_ACCOUNT_NAME",
    "3188": "VTEX_ALVI3188_ACCOUNT_NAME",
    "3193": "VTEX_ALVI3193_ACCOUNT_NAME",
    "3088": "VTEX_ALVI3088_ACCOUNT_NAME",
    "3094": "VTEX_ALVI3094_ACCOUNT_NAME",
    "3086": "VTEX_ALVI3086_ACCOUNT_NAME",
    "3091": "VTEX_ALVI3091_ACCOUNT_NAME",
    "3206": "VTEX_ALVI3206_ACCOUNT_NAME",
    "3085": "VTEX_ALVI3085_ACCOUNT_NAME",
    "3212": "VTEX_ALVI3212_ACCOUNT_NAME",
    "3211": "VTEX_ALVI3211_ACCOUNT_NAME",
    "3223": "VTEX_ALVI3223_ACCOUNT_NAME",
}


def _get_store_vtex_account_map():
    """
    Obtiene dinámicamente el mapeo de id_tienda -> nombre de cuenta VTEX.
    Busca tanto en tiendas activas de Postgres como en las variables de Airflow.
    """
    import pandas as pd

    store_account_map = {}

    # 1. Cargar mapeos conocidos
    for store_id, var_name in DEFAULT_VTEX_ACCOUNTS.items():
        try:
            account_val = Variable.get(var_name, default_var=None)
            if account_val:
                store_account_map[str(store_id).strip()] = str(account_val).strip()
        except Exception as e:
            logging.warning(f"No se pudo obtener variable {var_name}: {e}")

    # 2. Consultar tiendas de Postgres para detectar si hay nuevas tiendas
    try:
        pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT id FROM ecommdata_alvi.tiendas WHERE id IS NOT NULL;")
        stores = cursor.fetchall()
        cursor.close()
        conn.close()

        for (st_id,) in stores:
            st_key = str(st_id).strip()
            if st_key not in store_account_map:
                var_name = f"VTEX_ALVI{st_key}_ACCOUNT_NAME"
                try:
                    account_val = Variable.get(var_name, default_var=None)
                    if account_val:
                        store_account_map[st_key] = str(account_val).strip()
                        logging.info(f"Tienda nueva auto-detectada: {st_key} -> {account_val}")
                except Exception:
                    pass
    except Exception as e:
        logging.warning(f"Error consultando ecommdata_alvi.tiendas para tiendas dinámicas: {e}")

    return store_account_map


def _parse_polygon_coordinates(coord_raw):
    """
    Parsea las coordenadas almacenadas como texto o lista a una geometría Polygon de Shapely.
    """
    from shapely.geometry import Polygon
    from shapely.validation import make_valid

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
        # Asegurar cierre del anillo
        coords_list = list(coords)
        if coords_list[0] != coords_list[-1]:
            coords_list.append(coords_list[0])

        poly = Polygon(coords_list)
        if not poly.is_valid:
            poly = make_valid(poly)
            # Si make_valid produce MultiPolygon, tomar el mayor polígono
            if poly.geom_type == 'MultiPolygon':
                poly = max(poly.geoms, key=lambda p: p.area)
        return poly
    except Exception as e:
        logging.warning(f"Error construyendo Polygon con coordenadas: {e}")
        return None


def coordenadas_poligonos_alvi(ds):
    """
    Extrae los polígonos activos de Alvi desde forecast_and_planning.poligonos_alvi.
    Usa la fecha de ejecución ds o, en su defecto, la fecha más reciente disponible.
    """
    import pandas as pd

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    query = f"""
        SELECT 
            p.id, 
            p.name AS transportadora, 
            p.polygon, 
            p.coordenadas, 
            p.vtex_account
        FROM forecast_and_planning.poligonos_alvi p
        WHERE p."isActive" = true
          AND p.coordenadas IS NOT NULL
          AND p."deliveryChannel" = 'delivery'
          AND p.fecha = (
              SELECT CASE 
                  WHEN EXISTS (SELECT 1 FROM forecast_and_planning.poligonos_alvi WHERE fecha = '{ds}'::date)
                  THEN '{ds}'::date
                  ELSE (SELECT MAX(fecha) FROM forecast_and_planning.poligonos_alvi)
              END
          );
    """
    logging.info(f"Consultando polígonos Alvi con query:\n{query}")
    df = pg_hook.get_pandas_df(query)
    logging.info(f"Polígonos extraídos: {len(df.index)}")
    return df


def ordenes_janis_alvi(ds):
    """
    Extrae las órdenes de Janis Alvi de los últimos 13 meses móviles.
    Deduplica múltiples despachos tomando el MAX(id) de ecommdata_alvi.despachos.
    Calcula comuna (con fallback a la tienda) y filtra coordenadas válidas en Chile.
    """
    import pandas as pd

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    query = f"""
        SELECT DISTINCT 
            oj.id AS id_orden, 
            oj.venta_creada_neta AS venta_creada, 
            oj.venta_facturada_neta AS venta_facturada, 
            d.lat, 
            d.lng, 
            d.id_transportadora,
            COALESCE(NULLIF(TRIM(d.comuna), ''), t.comuna, '') AS comuna,
            t.id AS id_tienda,
            oj.fecha_facturacion,
            COALESCE(d.tipo_despacho, 'delivery') AS tipo
        FROM ecommdata_alvi.ordenes_janis oj
        LEFT JOIN ecommdata_alvi.tiendas t 
            ON oj.id_tienda_janis = t.id_janis
        LEFT JOIN (
            SELECT MAX(d_sub.id) AS max_id, d_sub.id_orden
            FROM ecommdata_alvi.despachos d_sub
            GROUP BY d_sub.id_orden
        ) d_latest 
            ON oj.id = d_latest.id_orden
        LEFT JOIN ecommdata_alvi.despachos d 
            ON d_latest.max_id = d.id
        WHERE d.lat IS NOT NULL 
          AND d.lng IS NOT NULL
          AND d.lat BETWEEN -56.0 AND -17.0
          AND d.lng BETWEEN -76.0 AND -66.0
          AND oj.fecha_facturacion::date >= '{ds}'::date - interval '13 month'
    """
    logging.info(f"Consultando órdenes Alvi (ventana 13 meses):\n{query}")
    df = pg_hook.get_pandas_df(query)
    logging.info(f"Órdenes extraídas de Janis Alvi: {len(df.index)}")
    return df


def poligonos_ordenes_alvi_to_s3(ds):
    """
    Cruza órdenes y polígonos respetando la cuenta VTEX de cada tienda Alvi.
    Genera el archivo CSV final y lo almacena en Amazon S3.
    """
    import pandas as pd
    import geopandas as gpd
    from shapely.geometry import Point

    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"ordenes_poligonos_alvi/{exec_date}/"
    filename = f"ordenes_poligonos_alvi/{exec_date}/ordenes_poligonos_alvi_{date_aux}.csv"

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    # 1. Obtener polígonos y órdenes
    df_poligonos = coordenadas_poligonos_alvi(ds)
    if df_poligonos.empty:
        logging.warning("No se encontraron polígonos en forecast_and_planning.poligonos_alvi.")
        return "empty"

    df_ordenes = ordenes_janis_alvi(ds)
    if df_ordenes.empty:
        logging.warning("No se encontraron órdenes para la ventana de tiempo.")
        return "empty"

    # 2. Mapeo dinámico Tienda -> Cuenta VTEX
    store_account_map = _get_store_vtex_account_map()
    logging.info(f"Mapeo Tienda -> VTEX Account: {store_account_map}")

    df_ordenes["id_tienda_str"] = df_ordenes["id_tienda"].astype(str).str.strip()
    df_ordenes["vtex_account"] = df_ordenes["id_tienda_str"].map(store_account_map)

    # 3. Construir geometrías para los polígonos
    df_poligonos["geometry"] = df_poligonos["coordenadas"].apply(_parse_polygon_coordinates)
    df_poligonos = df_poligonos.dropna(subset=["geometry"]).reset_index(drop=True)

    if df_poligonos.empty:
        logging.warning("Ningún polígono cuenta con coordenadas válidas para construir geometrías.")
        return "empty"

    # 4. Cruce espacial particionado por cuenta VTEX (Evita colisiones entre tiendas Alvi)
    results = []
    unique_accounts = df_poligonos["vtex_account"].dropna().unique()

    for vtex_acc in unique_accounts:
        polys_acc = df_poligonos[df_poligonos["vtex_account"] == vtex_acc].copy()
        orders_acc = df_ordenes[df_ordenes["vtex_account"] == vtex_acc].copy()

        if orders_acc.empty or polys_acc.empty:
            continue

        logging.info(f"Procesando cuenta {vtex_acc}: {len(orders_acc)} órdenes contra {len(polys_acc)} polígonos.")

        try:
            # Spatial join con GeoPandas (indexado espacial STRtree - ultra rápido)
            gdf_orders = gpd.GeoDataFrame(
                orders_acc,
                geometry=gpd.points_from_xy(orders_acc["lng"], orders_acc["lat"]),
                crs="EPSG:4326"
            )
            gdf_polys = gpd.GeoDataFrame(
                polys_acc[["polygon", "geometry"]],
                geometry="geometry",
                crs="EPSG:4326"
            ).rename(columns={"polygon": "nombre_poligono"})

            joined = gpd.sjoin(gdf_orders, gdf_polys, predicate="within", how="inner")
            if not joined.empty:
                results.append(pd.DataFrame(joined.drop(columns=["geometry", "index_right"])))
        except Exception as e:
            logging.warning(f"Error en sjoin para cuenta {vtex_acc}, usando fallback: {e}")
            # Fallback iterativo seguro si sjoin tuviera alguna excepción
            for _, p_row in polys_acc.iterrows():
                poly_geom = p_row["geometry"]
                poly_name = p_row["polygon"]
                for _, o_row in orders_acc.iterrows():
                    pt = Point(float(o_row["lng"]), float(o_row["lat"]))
                    if poly_geom.contains(pt):
                        item = o_row.to_dict()
                        item["nombre_poligono"] = poly_name
                        results.append(pd.DataFrame([item]))

    if not results:
        logging.warning("No hubo coincidencias espaciales entre órdenes y polígonos de Alvi.")
        return "empty"

    df_final = pd.concat(results, ignore_index=True)
    logging.info(f"Total de registros orden-polígono generados: {len(df_final)}")

    # Deduplicar por si un punto intersecta el borde de dos polígonos de la misma cuenta
    df_final = df_final.drop_duplicates(subset=["id_orden", "nombre_poligono"])

    # 5. Formatear columnas según el esquema exacto de forecast_and_planning.venta_poligonos_alvi
    target_columns = [
        "id_orden",
        "nombre_poligono",
        "comuna",
        "venta_creada",
        "venta_facturada",
        "id_transportadora",
        "id_tienda",
        "fecha_facturacion",
        "tipo",
        "lat",
        "lng"
    ]

    df_final = df_final[target_columns]

    # 6. Subir archivo resultante a S3
    buffer = io.StringIO()
    df_final.to_csv(buffer, header=True, index=False, encoding="utf-8")
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


def poligonos_ordenes_alvi_to_postgres(ti):
    """
    Descarga el archivo procesado de S3 y realiza TRUNCATE + INSERT en Postgres.
    """
    import pandas as pd
    import sqlalchemy

    filename = ti.xcom_pull(key="return_value", task_ids=["poligonos_ordenes_alvi_to_s3"])[0]

    if not filename or filename == "empty":
        logging.info("No hay registros para cargar en Postgres. Tarea finalizada.")
        return

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    if not s3_hook.check_for_key(filename, bucket_name=s3_bucket):
        raise Exception(f"El archivo {filename} no existe en S3 {s3_bucket}.")

    s3_obj = s3_hook.get_key(filename, bucket_name=s3_bucket)
    df = pd.read_csv(s3_obj.get()["Body"])

    if df.empty:
        logging.info("El DataFrame desde S3 está vacío. Finalizando con éxito.")
        return

    logging.info(f"Cargando {len(df)} registros en forecast_and_planning.venta_poligonos_alvi...")

    column_types = {
        "id_orden": "string",
        "nombre_poligono": "string",
        "comuna": "string",
        "venta_creada": "float",
        "venta_facturada": "float",
        "id_transportadora": "string",
        "id_tienda": "string",
        "fecha_facturacion": "string",
        "tipo": "string",
        "lat": "float",
        "lng": "float"
    }
    df = df.astype(column_types, errors="ignore")

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")

    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    with engine.begin() as conn:
        logging.info("Truncando forecast_and_planning.venta_poligonos_alvi...")
        conn.execute("TRUNCATE forecast_and_planning.venta_poligonos_alvi")
        logging.info("Insertando datos procesados...")
        df.to_sql(
            name="venta_poligonos_alvi",
            con=conn,
            schema="forecast_and_planning",
            if_exists="append",
            index=False,
            chunksize=20000,
            method="multi"
        )

    logging.info("Carga completada exitosamente en PostgreSQL (forecast_and_planning.venta_poligonos_alvi).")


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "etl_ordenes_poligonos_alvi",
    default_args=default_args,
    description="Carga tabla venta_poligonos_alvi cruzando ordenes de Janis y polígonos multicuenta VTEX",
    schedule_interval="30 8 * * *",
    start_date=pendulum.datetime(2023, 12, 6, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["DATA", "Janis", "forecast_and_planning", "polygons", "alvi", "seller"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    ### ETL Órdenes y Ventas por Polígono Alvi
    
    1. **Extracción**:
       - Extrae polígonos activos desde `forecast_and_planning.poligonos_alvi` por cuenta VTEX.
       - Extrae órdenes de `ecommdata_alvi.ordenes_janis` con ventana móvil de **13 meses**.
       - Deduplica despachos en `ecommdata_alvi.despachos` mediante `MAX(id)`.
       - Mapea dinámicamente cada tienda a su cuenta VTEX (`VTEX_ALVI{id_tienda}_ACCOUNT_NAME`).
    
    2. **Transformación Geográfica**:
       - Cruce espacial (Point-in-Polygon) con GeoPandas particionado tienda a tienda para evitar colisiones multicuenta.
    
    3. **Carga**:
       - Sube snapshot a Amazon S3.
       - Trunca e inserta los 13 meses en `forecast_and_planning.venta_poligonos_alvi`.
    """

    t0 = PythonOperator(
        task_id="poligonos_ordenes_alvi_to_s3",
        python_callable=poligonos_ordenes_alvi_to_s3,
    )

    t1 = PythonOperator(
        task_id="poligonos_ordenes_alvi_to_postgres",
        python_callable=poligonos_ordenes_alvi_to_postgres,
    )

    t0 >> t1
