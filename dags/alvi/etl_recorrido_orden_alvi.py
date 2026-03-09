from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator as PostgresOperator

from utils.janis_utils import load_custom_query_to_s3
from utils.postgres_utils import is_empty_table

from utils.slack_utils import dag_failure_slack, dag_success_slack

from datetime import datetime, timedelta,time
import pendulum

def _calculate_routes(ds):
    import pandas as pd
    import requests
    import sqlalchemy
    import io
    query = """SELECT
                oj.id AS id_orden,
                t.latitud AS lat_tienda,
                t.longitud AS lng_tienda,
                d2.lat AS lat_cliente,
                d2.lng AS lng_cliente,
                ocde.fecha_creacion AS fecha_despacho,
                CASE
                    WHEN EXTRACT(DOW FROM ocde.fecha_creacion AT TIME ZONE 'UTC') < EXTRACT(DOW FROM NOW() AT TIME ZONE 'UTC') THEN
                        (DATE_TRUNC('week', NOW() AT TIME ZONE 'UTC') + INTERVAL '1 day')::timestamp + (EXTRACT(HOUR FROM ocde.fecha_creacion) * INTERVAL '1 hour' + EXTRACT(MINUTE FROM ocde.fecha_creacion) * INTERVAL '1 minute')::interval
                    WHEN EXTRACT(DOW FROM ocde.fecha_creacion AT TIME ZONE 'UTC') = EXTRACT(DOW FROM NOW() AT TIME ZONE 'UTC') AND EXTRACT(HOUR FROM ocde.fecha_creacion) <= EXTRACT(HOUR FROM NOW() AT TIME ZONE 'UTC') THEN
                        (NOW() AT TIME ZONE 'UTC' + ((EXTRACT(HOUR FROM ocde.fecha_creacion) - EXTRACT(HOUR FROM NOW() AT TIME ZONE 'UTC')) * INTERVAL '1 hour' + (EXTRACT(MINUTE FROM ocde.fecha_creacion) - EXTRACT(MINUTE FROM NOW() AT TIME ZONE 'UTC')) * INTERVAL '1 minute'))::timestamp
                    ELSE
                        (DATE_TRUNC('week', NOW() AT TIME ZONE 'UTC') + (EXTRACT(DOW FROM ocde.fecha_creacion AT TIME ZONE 'UTC') * INTERVAL '1 day' + EXTRACT(HOUR FROM ocde.fecha_creacion) * INTERVAL '1 hour' + EXTRACT(MINUTE FROM ocde.fecha_creacion) * INTERVAL '1 minute'))::timestamp
                END AS next_timestamp
            FROM ecommdata_alvi.ordenes_janis oj
            LEFT JOIN ecommdata_alvi.tiendas t ON oj.id_tienda_janis = t.id_janis
            LEFT JOIN (
                SELECT MAX(d.id) AS id,
                    d.id_orden
                FROM ecommdata_alvi.despachos d
                GROUP BY d.id_orden
            ) d1 ON oj.id = d1.id_orden
            LEFT JOIN ecommdata_alvi.despachos d2 ON d1.id = d2.id
            LEFT JOIN ecommdata_alvi.orden_cambios_de_estado ocde ON ocde.id_orden = oj.janis_id
            WHERE oj.fecha_facturacion >= '"""+ds+"""'::date - 1
                AND oj.fecha_facturacion < '"""+ds+"""'::date
                AND ocde.estado_nuevo = 70
                AND d2.tipo_despacho != 'pickup';"""
    pg_hook = PostgresHook(conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    df_location=pd.DataFrame(results)
    df_location.columns = ["id_orden","lat_tienda","lng_tienda","lat_cliente","lng_cliente","fecha_despacho","next_timestamp"]
    cursor.close()
    df_location.info()
    pg_connection.close()

    url_distance = Variable.get("ROUTES_API")
    key = Variable.get("ROUTES_API_KEY")

    aux_list = []
    aux_list_s3 = []

    for i in range(len(df_location)):
        start = str(df_location.iloc[i]['lat_tienda'])+","+str(df_location.iloc[i]['lng_tienda'])
        print(start)
        end = str(df_location.iloc[i]['lat_cliente'])+","+str(df_location.iloc[i]['lng_cliente'])
        print(end)
        tod = df_location.iloc[i]['next_timestamp']
        unixtime = str(int((tod - datetime(1970, 1, 1)).total_seconds()) + 604800)
        r = requests.get(f"{url_distance}destinations={end}&origins={start}&departure_time={unixtime}&traffic_model=best_guess&key={key}")
        if r.status_code == 200:
            response_json = r.json()
            status = response_json.get("status")

            if status == "OK":
                elements = response_json.get("rows", [])[0].get("elements", [])[0]
                tiempo_estimado = elements.get("duration_in_traffic", {}).get("text", None)
                tiempo_estimado_seg = elements.get("duration_in_traffic", {}).get("value", None)
                distancia = elements.get("distance", {}).get("text", None)
                distancia_metros = elements.get("distance", {}).get("value", None)
                cliente_geo = response_json.get("destination_addresses", [])
                aux_list.append([df_location.iloc[i]['id_orden'], cliente_geo, tiempo_estimado, tiempo_estimado_seg, distancia, distancia_metros])
            else:
                print(f"Skipping order {df_location.iloc[i]['id_orden']} due to status: {status}")
        else:
            print(f"Skipping order {df_location.iloc[i]['id_orden']} due to status code: {r.status_code}")

        aux_list_s3.append([df_location.iloc[i]['id_orden'], r.json()])

    df = pd.DataFrame(aux_list, columns=['id_orden', 'cliente_geo', 'tiempo_estimado', 'tiempo_estimado_seg', 'distancia', 'distancia_metros'])
    df_s3 = pd.DataFrame(aux_list_s3, columns=['id_orden', 'response'])
    
    
    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"recorrido_orden/{exec_date}/"
    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

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
    key = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{key}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    # Save to PostgreSQL:
    df.to_sql(name="recorrido_orden",
                con=engine,         
                schema="ecommdata_alvi",         
                if_exists='append',         
                index=False,         
                chunksize=20000,         
                method='multi')
    
    #Save to S3
    buffer = io.StringIO()
    df_s3.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"recorrido_orden_alvi/{exec_date}/recorrido_orden_{date_aux}.csv"
    buffer.seek(0)
    print("se logro transformar el dataframe a un archivo .csv")
    print(f"con fecha {ds} y nombre de filename como {filename}")
    s3_hook.load_string(buffer.getvalue(),
                key=filename,
                bucket_name=s3_bucket,
                replace=True,
                encrypt=False)
    print(f"File load on S3: {prefix}")
    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_recorrido_orden_alvi_incremental_load',
    default_args=default_args,
    description="Extracción y calculo de tabla recorrido_orden para ALVI.",
    schedule="30 8 * * *",
    start_date=pendulum.datetime(2023, 7, 26, tz="America/Santiago"),
    catchup=True,
    max_active_runs = 1,
    tags=["DATA", "ecommdata", "recorrido_orden","km", "ALVI", "SERGIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    calculo distancia y tiempo ordenes dia anterior
    """ 
    t0 = PythonOperator(
        task_id = "_calculate_routes",
        python_callable = _calculate_routes,
    )

    t0