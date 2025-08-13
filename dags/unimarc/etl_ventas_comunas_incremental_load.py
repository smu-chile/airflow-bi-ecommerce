from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable

from datetime import datetime, timedelta
import pendulum

# -------------------
# 1) Define ETL functions
# -------------------
def extract_data(ti,ds):
    import pandas as pd
    import sqlalchemy as sa
    from sqlalchemy import MetaData, Table, cast, Date, literal, and_
    """
    Extract polygons and orders for the previous day from Postgres.
    """
    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sa.create_engine(conn_url)
    
    # ---------- POLÍGONOS ----------
    md_fp = MetaData(schema="forecast_and_planning")
    poligonos_tbl = Table("poligonos_comunas", md_fp, autoload_with=engine)

    poligonos_df = pd.read_sql(poligonos_tbl.select(), engine)

    # ---------- ÓRDENES ----------
    md_ec = MetaData(schema="ecommdata")
    ordenes_tbl       = Table("ordenes_janis",  md_ec, autoload_with=engine)
    despachos_tbl     = Table("despachos",      md_ec, autoload_with=engine)
    transport_tbl     = Table("transportadoras",md_ec, autoload_with=engine)
    tiendas_tbl       = Table("tiendas",        md_ec, autoload_with=engine)

    cols = [
        ordenes_tbl.c.id.label("id_orden"),
        ordenes_tbl.c.venta_creada_neta.label("venta_creada"),
        ordenes_tbl.c.venta_facturada_neta.label("venta_facturada"),
        despachos_tbl.c.lat,
        despachos_tbl.c.lng,
        despachos_tbl.c.id_transportadora,
        tiendas_tbl.c.id.label("id_tienda"),
        ordenes_tbl.c.fecha_facturacion,
    ]

    join_expr = (
        ordenes_tbl
        .join(despachos_tbl, despachos_tbl.c.id_orden == ordenes_tbl.c.id)
        .join(transport_tbl, transport_tbl.c.id == despachos_tbl.c.id_transportadora)
        .join(tiendas_tbl,   tiendas_tbl.c.id_janis == ordenes_tbl.c.id_tienda_janis)
    )
    
    ayer = pendulum.now("America/Santiago").subtract(days=1).date()

    ordenes_sel = (
        sa.select(cols)
        .select_from(join_expr)
        .where(
            and_(
                despachos_tbl.c.lat.isnot(None),
                cast(ordenes_tbl.c.fecha_facturacion, Date) == ayer,
                despachos_tbl.c.tipo_despacho == literal("delivery")
            )
        )
        .distinct()
    )
    ordenes_df = pd.read_sql(ordenes_sel, engine)

    # ---------- XCom ----------
    ti.xcom_push(key="poligonos", value=poligonos_df.to_json())
    ti.xcom_push(key="ordenes",   value=ordenes_df.to_json(date_format="iso",date_unit="s"))


def transform_data(ti, ds):
    import pandas as pd
    from shapely.geometry import Point, Polygon
    from shapely.strtree import STRtree
    import numpy as np

    """
    Perform spatial join: assign each order to its comuna.
    """
    raw_poligonos = ti.xcom_pull(key='poligonos')
    raw_ordenes = ti.xcom_pull(key='ordenes')
    poligonos = pd.read_json(raw_poligonos)
    ordenes = pd.read_json(raw_ordenes)
    # Parse coordinates and build geometry
    poligonos['coords'] = poligonos['coordenadas'].apply(eval)
    poligonos['geometry'] = poligonos['coords'].apply(Polygon)
    geoms = poligonos['geometry'].tolist()
    tree = STRtree(geoms)

    # Mapa robusto para recuperar índice desde una geometría retornada por query()
    id_map = {id(g): i for i, g in enumerate(geoms)}
    # Spatial join
    records = []
    for _, row in ordenes.iterrows():
        pt = Point(row['lng'], row['lat'])
        candidates = tree.query(pt)

        # Normaliza a índices
        if len(candidates) and isinstance(candidates[0], (int, np.integer)):
            idxs = candidates
        else:
            idxs = [id_map[id(g)] for g in candidates]
        
        for idx in idxs:
            geom = geoms[idx]                     # ahora geom es un Polygon real
            if geom.contains(pt):
                pol = poligonos.iloc[idx]
                records.append({
                    'id_orden': row['id_orden'],
                    'comuna':    pol['comuna'],
                    'provincia': pol['provincia'],
                    'region':    pol['region'],
                    'venta_creada':    row['venta_creada'],
                    'venta_facturada': row['venta_facturada'],
                    'id_transportadora': row['id_transportadora'],
                    'id_tienda':        row['id_tienda'],
                    'fecha_facturacion': row['fecha_facturacion'],
                    'lat': row['lat'],
                    'lng': row['lng']
                })
                break
    venta_comunas_df = pd.DataFrame(records)
    
    # --- Sube CSV a S3 ---
    csv_data = venta_comunas_df.to_csv(index=False)
    s3_hook  = S3Hook(aws_conn_id="aws_s3_connection")
    key      = f"forecast_and_planning/venta_comunas/{ds}/venta_comunas.csv"
    
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook.load_string(
        string_data=csv_data,
        key=key,
        bucket_name=s3_bucket,
        replace=True,
        encrypt=False
    )
    print(f"Data loaded to S3 at {key}.")

def upsert_venta_comunas(ti, ds):
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import MetaData, Table
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    # ---------- S3 → DataFrame ----------
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    key       = f"forecast_and_planning/venta_comunas/{ds}/venta_comunas.csv"
    s3_hook   = S3Hook(aws_conn_id="aws_s3_connection")
    obj       = s3_hook.get_key(key, bucket_name=s3_bucket)
    df        = pd.read_csv(obj.get()['Body'])

    # tipifica
    df = df.astype({
        "id_orden":          "string",
        "comuna":            "string",
        "provincia":         "string",
        "region":            "string",
        "venta_creada":      "float",
        "venta_facturada":   "float",
        "id_transportadora": "string",
        "id_tienda":         "string",
        "fecha_facturacion": "datetime64",
        "lat":               "float",
        "lng":               "float"
    }, errors="ignore")

    # conecta SQLAlchemy
    host     = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    engine   = sqlalchemy.create_engine(
        f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    )

    meta   = MetaData(schema="forecast_and_planning")
    ventas = Table("venta_comunas", meta, autoload_with=engine)

    # ---------- UPSERT ON CONFLICT ----------
    insert_stmt  = pg_insert(ventas).values(df.to_dict(orient="records"))
    update_stmt  = {
        c.name: insert_stmt.excluded[c.name]
        for c in ventas.c
        if c.name != "id_orden" 
    }
    upsert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=["id_orden"],
        set_=update_stmt
    )

    # ---------- ejecuta en batch ----------
    with engine.begin() as conn:
        conn.execute(upsert_stmt)


default_args = {
    'owner': 'ecommerce_data',
    'depends_on_past': False,
    "email_on_failure": False,
    "email_on_retry": False,
    'retries': 0
    }

with DAG(
    dag_id="etl_venta_por_comuna_incremental_load",
    description="Carga diaria de venta por comuna.",
    schedule_interval="0 5 * * *",
    start_date=pendulum.datetime(2025, 7, 28, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["venta", "comunas", "incremental", "Janis", "FRANCISCO", "postgres"]
) as dag:
    dag.doc_md = """
    Incremental load de ordenes por comuna, cruce entre polígonos de comunas y órdenes de venta.
    """
    # Define tasks
    t0 = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )
    t1 = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    t2 = PythonOperator(
        task_id='load_back_to_postgres',
        python_callable=upsert_venta_comunas,
    )

    t0 >> t1 >> t2
