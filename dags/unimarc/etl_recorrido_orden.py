from airflow import DAG
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

from utils.janis_utils import load_custom_query_to_s3
from utils.postgres_utils import is_empty_table

from datetime import datetime, timedelta,time
import pendulum

def _calculate_routes():
    import pandas as pd
    import requests
    import sqlalchemy
    query = """select oj.id as id_orden, t.latitud as lat_tienda, t.longitud as lng_tienda,
                d2.lat as lat_cliente,d2.lng as lng_cliente, ocde.fecha_creacion as fecha_despacho
                from ecommdata.ordenes_janis oj
                left join ecommdata.tiendas t on oj.id_tienda_janis=t.id_janis
                LEFT JOIN ( SELECT max(d.id) AS id,
                            d.id_orden
                        FROM ecommdata.despachos d
                        GROUP BY d.id_orden) d1 ON oj.id = d1.id_orden
                left join ecommdata.despachos d2 on d1.id = d2.id 
                left join ecommdata.orden_cambios_de_estado ocde on ocde.id_orden = oj.janis_id
                where oj.fecha_facturacion  >= current_date-1
                and oj.fecha_facturacion  < current_date
                and ocde.estado_nuevo = 70
                and d2.tipo_despacho != 'pickup'"""
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn") #cambiar antes de pasar a prod
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    df_location=pd.DataFrame(results)
    df_location.columns = ["id_orden","lat_tienda","lng_tienda","lat_cliente","lng_cliente","fecha_despacho"]
    cursor.close()
    pg_connection.close()

    url_distance = Variable.get("ROUTES_API")
    key = Variable.get("ROUTES_API_KEY")

    aux_list = []

    for i in range(len(df_location)):
        start = str(df_location.iloc[i]['lat_tienda'])+" , "+str(df_location.iloc[i]['lng_tienda'])
        end = str(df_location.iloc[i]['lat_cliente'])+" , "+str(df_location.iloc[i]['lng_cliente'])
        tod = df_location.iloc[i]['fecha_despacho'] + timedelta(days=7)
        unixtime = str(int((tod - datetime(1970, 1, 1)).total_seconds()))
        r = requests.get(url_distance + "destinations=" + end + "&origins=" + start + "&departure_time="+ unixtime +"&traffic_model=best_guess"+ "&key=" + key)
        cliente_geo = r.json()["destination_addresses"]
        tiempo_estimado = r.json()["rows"][0]["elements"][0]["duration_in_traffic"]["text"]
        tiempo_estimado_seg = r.json()["rows"][0]["elements"][0]["duration_in_traffic"]["value"]
        distancia = r.json()["rows"][0]["elements"][0]["distance"]["text"]
        distancia_metros = r.json()["rows"][0]["elements"][0]["distance"]["value"]
        aux_list.append([df_location.iloc[i]['id_orden'],cliente_geo,tiempo_estimado,tiempo_estimado_seg,distancia,distancia_metros])
    df = pd.DataFrame(aux_list, columns = ['id_orden','cliente_geo','tiempo_estimado','tiempo_estimado_seg','distancia','distancia_metros'])

    column_types = {
        "id_orden": "int",
        "cliente_geo": "string",
        "tiempo_estimado": "string",
        "tiempo_estimado_seg": "int",
        "distancia": "string",
        "distancia_metros": "int"
    }

    # # Ensure correct datatypes:
    df = df.astype(column_types, errors="ignore")

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = "postgresql+psycopg2://"+username+":"+password+"@"+host+":5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    # Save to PostgreSQL:
    df.to_sql(name="recorrido_orden",
                con=engine,         
                schema="ecommdata",         
                if_exists='append',         
                index=False,         
                chunksize=20000,         
                method='multi')
    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_recorrido_orden_incremental_load',
    default_args=default_args,
    description="Extracción y calculo de tabla recorrido_orden.",
    schedule_interval="30 8 * * *",
    start_date=pendulum.datetime(2023, 8, 18, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "ecommdata", "recorrido_orden","km", "unimarc"],
) as dag:

    dag.doc_md = """
    calculo distancia y tiempo ordenes dia anterior
    """ 
    t0 = PythonOperator(
        task_id = "_calculate_routes",
        python_callable = _calculate_routes,
    )

    t0