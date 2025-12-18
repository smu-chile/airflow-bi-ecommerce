from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

from utils.slack_utils import dag_success_slack, dag_failure_slack

import pendulum

from datetime import datetime, timedelta

def coordenadas_poligonos(ds):
    import pandas as pd
    coordenadas_poligonos_query = f"""select id, name as transportadora,  polygon, coordenadas
                    from forecast_and_planning.poligonos p 
                    where p."isActive"  = true
                    and coordenadas is not null 
                    and  p."deliveryChannel" ='delivery'
                    and fecha = '{ds}'::date """
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    print(coordenadas_poligonos_query)
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(coordenadas_poligonos_query)
    results = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(results, columns=column_names)
    cursor.close()
    pg_connection.close()
    return df

def ordenes_janis(ds):
    import pandas as pd
    ordenes_janis_query = f"""select distinct oj.id as orden, oj.venta_creada_neta , oj.venta_facturada_neta , 
                d.lat , d.lng, d.id_transportadora, t.id_tienda, oj.fecha_facturacion
                from ecommdata.ordenes_janis oj
                left join ecommdata.despachos d 
                on d.id_orden =oj.id
                left join ecommdata.transportadoras t 
                on t.id = d.id_transportadora
                where d.lat is not null 
                and oj.fecha_facturacion::date >= '{ds}'::date - interval '13 month' 
                and  d.tipo_despacho ='delivery'"""
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    print(ordenes_janis_query)
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(ordenes_janis_query)
    results = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(results, columns=column_names)
    cursor.close()
    pg_connection.close()
    return df

def parse_coordinates(coord_str):
    return eval(coord_str)

def poligonos_ordenes_to_s3(ds):
    import pandas as pd
    import io
    from io import StringIO
    import geopandas as gpd
    from shapely.geometry import Polygon, Point

    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"ordenes_poligonos/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    poligonos = coordenadas_poligonos(ds) #lista de poligonos (DF)
    print("se han obtenido los polígonos")

    ordenes = ordenes_janis(ds) #Lista de ordenes (DF)
    print("Se han obtenido las coordenadas")

    # convierte lista de coordenadas en tipo geometry y cre un geodataframe
    geometrias = poligonos['coordenadas'].apply(parse_coordinates).apply(Polygon)
    pol = gpd.GeoDataFrame(geometry=geometrias)

    orden = []   #listas para ser agragadas como columnas en DF final
    nombre_poligono = []
    venta_facturada = []
    venta_creada =[]
    transportadora = []
    tienda = []
    fecha_facturacion = []
    latitud = []
    longitud = []


    for i in range(len(pol)): #recorre todos los polígonos
        poli=pol['geometry'][i]
        for j in range(len(ordenes)):    ## recorre todas las ordenes
            punto = Point( float(ordenes['lng'][j]), float(ordenes['lat'][j])) 
            if poli.contains(punto):    #si el punto está contenido en el polígonos, entonces lo agrega al DF
                orden.append(ordenes['orden'][j])
                nombre_poligono.append(poligonos['polygon'][i])
                venta_creada.append(ordenes['venta_creada_neta'][j])
                venta_facturada.append(ordenes['venta_facturada_neta'][j])
                transportadora.append(ordenes['id_transportadora'][j])
                tienda.append(ordenes['id_tienda'][j])
                fecha_facturacion.append(ordenes['fecha_facturacion'][j])
                latitud.append(ordenes['lat'][j])
                longitud.append(ordenes['lng'][j])
                #print("poligono ", poligonos['polygon'][i], " contiene a ", ordenes['orden'][j])
                
    df = {
        'id_orden' : orden,
        'nombre_poligono' : nombre_poligono,
        'venta_creada' : venta_creada,
        'venta_facturada' : venta_facturada,
        'id_transportadora' : transportadora,
        'id_tienda' : tienda,
        'fecha_facturacion' : fecha_facturacion,
        'lat' : latitud,
        'lng' : longitud
    }
    venta_poligono = pd.DataFrame(df)   

    buffer = io.StringIO()
    venta_poligono.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"ordenes_poligonos/{exec_date}/ordenes_poligonos_{date_aux}.csv"
    buffer.seek(0)
    print("se logro transformar el dataframe a un archivo .csv")
    print(f"con fecha {ds} y nombre de filename como {filename}")
    s3_hook.load_string(buffer.getvalue(),
                key=filename,
                bucket_name=s3_bucket,
                replace=True,
                encrypt=False)
    
    print(f"File load on S3: {prefix}")

    return filename

def poligonos_ordenes_to_postgres(ti):
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text

    filename = ti.xcom_pull(key="return_value", task_ids=["poligonos_ordenes_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+filename)
    if not s3_hook.check_for_key(filename, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % filename)

    s_stock_object = s3_hook.get_key(filename, bucket_name=s3_bucket)

    df = pd.read_csv(s_stock_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")

    column_types = {
    "id_orden": "string",
    "nombre_poligono" : "string",
    "venta_creada" : "float",
    "venta_facturada" : "float",
    "id_transportadora" : "string",
    "id_tienda" : "string",
    "fecha_facturacion" : "datetime64",
    "lat" : "float",
    "lng" : "float"
    }
    # verificación correcto datatypes:
    df = df.astype(column_types, errors="ignore")
    print(df.head())
    df.info()


    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    with engine.begin() as conn:
        conn.execute("TRUNCATE forecast_and_planning.ordenes_poligonos")
        df.to_sql(name="ordenes_poligonos",
                    con=conn,         
                    schema="forecast_and_planning",         
                    if_exists='append',         
                    index=False,         
                    chunksize=20000,         
                    method='multi')

    print("Data saved to PostgreSQL.")

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_poligonos_ordenes',
    default_args=default_args,
    description="cargar tabla poligonos ordenes",
    schedule_interval="20 8 * * *",
    start_date=pendulum.datetime(2023, 12, 6, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "ordenes", "forcast_and_plannig", "polygons", "unimarc", "PATRICIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:
    

    dag.doc_md = """
    Carga tabla poligonos ordenes\n
    guardar en S3.
    """ 
    t0 = PythonOperator(
        task_id='poligonos_ordenes_to_s3',
        python_callable=poligonos_ordenes_to_s3,
    )

    t1 = PythonOperator(
        task_id = "poligonos_ordenes_to_postgres",
        python_callable = poligonos_ordenes_to_postgres,
    )

    t0 >> t1