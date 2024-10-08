from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

import pendulum

def query_to_df(query):
    import pandas as pd
    print(query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query)
    column_names = [desc[0] for desc in cursor.description]
    results = cursor.fetchall()
    results = pd.DataFrame(results, columns=column_names)
    print(results.head(20))
    cursor.close()
    pg_connection.close()
    return results

def get_api_escenarios(url, exception_cases):
    import requests
    import pandas as pd

    api_key = Variable.get("API_KEY_DRIVIN")
    headers = {
        'X-API-Key': api_key
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Levanta una excepción para errores HTTP 4xx/5xx

        data = response.json()

        if 'response' in data and isinstance(data['response'], list):
            lista = [
                (
                    item.get('token'),
                    pd.to_datetime(item.get('deploy_date')).strftime('%Y-%m-%d'), 
                    item.get('description'),
                    item.get('status'),
                    item.get('schema_name'),
                    item.get('schema_code'),
                    pd.to_datetime(item.get('created_at')).strftime('%Y-%m-%d %H:%M:%S'),
                    item.get('children_scenarios', [])
                )
                for item in data['response']
            ]
        else:
            print(f"Formato inesperado en la respuesta de la API: {data}")
            exception_cases.append(url)
            lista = []

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        exception_cases.append(url)
        lista = []
    except requests.exceptions.RequestException as req_err:
        print(f"Request error occurred: {req_err}")
        exception_cases.append(url)
        lista = []
    except Exception as err:
        print(f"An error occurred: {err}")
        exception_cases.append(url)
        lista = []

    return lista

def get_api_ruta_escenario(url_list, exception_cases):
    import requests
    import pandas as pd

    api_key = Variable.get("API_KEY_DRIVIN")
    headers = {
        'X-API-Key': api_key
    }

    lista_final = []
    for url in url_list:
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()

            data = response.json()

            if 'response' in data and isinstance(data['response'], list):
                for item in data['response']:
                    for trip in item['trips']:
                        for result in trip['results']:
                            # Manejo de los time windows en stop
                            time_windows_start = result['stop']['time_windows'][0]['start'] if result['stop'].get('time_windows') else None
                            time_windows_end = result['stop']['time_windows'][0]['end'] if result['stop'].get('time_windows') else None
                            
                            # Obtención de datos de orders
                            order_idx = result['orders'][0]['idx'] if result.get('orders') else None
                            order_code = result['orders'][0]['code'] if result.get('orders') else None

                            fila = (
                                item.get('vehicle'),
                                item.get('scenario_description'),
                                item.get('deploy_date'),
                                item.get('scenario_token'),
                                item.get('fleet_sequence'),
                                item.get('peoneta'),
                                item.get('driver', {}).get('full_name'),
                                item.get('summary', {}).get('total_orders'),
                                item.get('summary', {}).get('total_addresses'),
                                item.get('summary', {}).get('total_distance'),
                                item.get('summary', {}).get('total_time'),
                                trip.get('approved_at'),
                                trip.get('started_at'),
                                trip.get('ended_at'),
                                trip.get('summary', {}).get('total_time'),
                                trip.get('summary', {}).get('original_total_distance'),
                                result.get('position'),
                                result.get('eta'),
                                result.get('real_distance'),
                                result.get('planned_distance'),
                                result.get('stop', {}).get('lat'),
                                result.get('stop', {}).get('lng'),
                                result.get('stop', {}).get('city'),
                                result.get('stop', {}).get('country'),
                                result.get('stop', {}).get('reference'),
                                time_windows_start,
                                time_windows_end,
                                order_idx,
                                order_code
                            )
                            lista_final.append(fila)
            else:
                print(f"Formato inesperado en la respuesta de la API: {data}")
                exception_cases.append(url)

        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
            exception_cases.append(url)
        except requests.exceptions.RequestException as req_err:
            print(f"Request error occurred: {req_err}")
            exception_cases.append(url)
        except Exception as err:
            print(f"An error occurred: {err}")
            exception_cases.append(url)

    return lista_final

def drivin_escenarios_to_s3(ts,ds):
    import pandas as pd
    import requests
    import io
    from io import StringIO

    exec_date = ds.replace("-", "/")
    date_aux = ts.replace("-", "_")
    prefix = f"forecast_and_planning/drivin/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    url = f"https://external.driv.in/api/external/v2/scenarios?date={str(ds)}"

    exception_cases = []

    lista_escenarios = get_api_escenarios(url,exception_cases)

    columns = ["token",
               "deploy_date",
               "description",
               "status",
               "schema_name",
               "schema_code",
               "created_at",
               "children_scenarios"]

    df = pd.DataFrame(lista_escenarios,columns=columns)

    buffer = io.StringIO()
    df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    buffer.seek(0)

    filename = f"forecast_and_planning/drivin/{exec_date}/escenarios/escenarios_{date_aux}.csv"

    print(f"con fecha {ds} y nombre de filename como {filename}")
    s3_hook.load_string(buffer.getvalue(),
                key=filename,
                bucket_name=s3_bucket,
                replace=True,
                encrypt=False)

    print("se logro transformar los dataframes a archivos .csv")
    print(f"File load on S3: {prefix}")

    return filename

def drivin_escenarios_to_postgres(ti,ts):
    import pandas as pd
    import sqlalchemy
    import numpy as np
    
    filename = ti.xcom_pull(key="return_value", task_ids=["drivin_escenarios_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+filename)
    if not s3_hook.check_for_key(filename, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % filename)

    hook_object = s3_hook.get_key(filename, bucket_name=s3_bucket)

    df = pd.read_csv(hook_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")

    df["fecha_hora"] = ts

    columns = ["deploy_date",
               "description",
               "status",
               "schema_name",
               "schema_code",
               "created_at",
               "children_scenarios",
               "fecha_hora"]

    columns_query = ",".join(columns)
    excluded_query = ",".join(["EXCLUDED."+column for column in columns])
    values_query = "%s,"+",".join(["%s" for column in columns])
    df = df.fillna("NULL")
    records = list(df.to_records(index=False))
    
    # Change data types to native python types
    fixed_records = []
    for record in records:
        fixed_record = []
        for value in record:
            if isinstance(value, np.generic):
                fixed_record.append(value.item())
            elif value == "NULL":
                fixed_record.append(None)
            else:
                fixed_record.append(value)
        fixed_records.append(tuple(fixed_record))
    print(f"Number of records to load: {str(len(fixed_records))}")
    incremental_query = """
        INSERT INTO ecommdata.drivin_escenarios (token,"""+columns_query+""") 
        VALUES ("""+values_query+""")
        ON CONFLICT (token)
        DO UPDATE SET ("""+columns_query+""") = ("""+excluded_query+""") 
    """
    print(incremental_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.executemany(incremental_query, fixed_records)
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    print("Data loaded to Postgres: ecommdata.drivin_escenarios")
    return

def drivin_rutas_escenario_to_s3(ds,ts):
    import pandas as pd
    import requests
    import io
    from io import StringIO

    exec_date = ds.replace("-", "/")
    date_aux = ts.replace("-", "_")
    prefix = f"forecast_and_planning/drivin/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    query_escenario_token = f"""select "token"
                        from ecommdata.drivin_escenarios
                        where deploy_date = '{ds}'::date """

    df = query_to_df(query_escenario_token)
    
    lista_tokens = df['token'].unique()

    url_list = []
    for token in lista_tokens:
        url = f"https://external.driv.in/api/external/v2/results?token={token}"
        url_list.append(url)

    exception_cases = []

    lista_rutas_escenario = get_api_ruta_escenario(url_list,exception_cases)

    columns = [
        "vehicle", "scenario_description", "deploy_date", "scenario_token", 
        "fleet_sequence", "peoneta", "driver_name", "total_orders", 
        "total_addresses", "total_distance", "total_time_response", 
        "approved_at", "started_at", "ended_at", "trip_total_time", 
        "original_total_distance", "position", "eta", "real_distance", 
        "planned_distance", "stop_lat", "stop_lng", "stop_city", 
        "stop_country", "stop_reference", "time_window_start", 
        "time_window_end", "order_idx", "order_code"
    ]

    df_rutas_escenarios = pd.DataFrame(lista_rutas_escenario,columns=columns)

    buffer = io.StringIO()
    df_rutas_escenarios.to_csv(buffer, header=True, index=False, encoding="utf-8")
    buffer.seek(0)

    filename = f"forecast_and_planning/drivin/{exec_date}/rutas_escenarios/rutas_escenario_{date_aux}.csv"

    print(f"con fecha {ds} y nombre de filename como {filename}")
    s3_hook.load_string(buffer.getvalue(),
                key=filename,
                bucket_name=s3_bucket,
                replace=True,
                encrypt=False)

    print("se logro transformar los dataframes a archivos .csv")
    print(f"File load on S3: {prefix}")

    return filename

def drivin_rutas_escenario_to_postgres(ti, ts):
    import pandas as pd
    import sqlalchemy
    import numpy as np
    from datetime import datetime


    filename = ti.xcom_pull(key="return_value", task_ids=["drivin_rutas_escenario_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: " + filename)
    if not s3_hook.check_for_key(filename, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % filename)

    hook_object = s3_hook.get_key(filename, bucket_name=s3_bucket)

    df = pd.read_csv(hook_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successful.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")

    df = df.dropna(subset=['order_code'])

    df["fecha_hora"] = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S%z")
    df["position"] = df["position"].astype(int)
    df["order_idx"] = df["order_idx"].astype(int)

    # Bajada de Pablo eliminar en caso esté mal el codigo
    df["order_code"] = pd.to_numeric(df["order_code"], errors='coerce')
    df = df.dropna(subset=["order_code"])

    df["order_code"] = df["order_code"].astype(int)

    print(df.head(60))

    df.info()

    print(df.columns)

    df.columns = ['vehicle', 'scenario_description', 'deploy_date', 'scenario_token',
       'fleet_sequence', 'peoneta', 'driver_name', 'total_orders',
       'total_addresses', 'total_distance', 'total_time_response',
       'approved_at', 'started_at', 'ended_at', 'trip_total_time',
       'original_total_distance', 'position', 'eta', 'real_distance',
       'planned_distance', 'stop_lat', 'stop_lng', 'stop_city', 'stop_country',
       'stop_reference', 'time_window_start', 'time_window_end', 'order_idx',
       'order_code', 'fecha_hora']
    
    df = df[['order_code','vehicle', 'scenario_description', 'deploy_date', 'scenario_token',
       'fleet_sequence', 'peoneta', 'driver_name', 'total_orders',
       'total_addresses', 'total_distance', 'total_time_response',
       'approved_at', 'started_at', 'ended_at', 'trip_total_time',
       'original_total_distance', 'position', 'eta', 'real_distance',
       'planned_distance', 'stop_lat', 'stop_lng', 'stop_city', 'stop_country',
       'stop_reference', 'time_window_start', 'time_window_end', 'order_idx',
        'fecha_hora']]

    columns = ['vehicle', 'scenario_description', 'deploy_date', 'scenario_token',
       'fleet_sequence', 'peoneta', 'driver_name', 'total_orders',
       'total_addresses', 'total_distance', 'total_time_response',
       'approved_at', 'started_at', 'ended_at', 'trip_total_time',
       'original_total_distance', 'position', 'eta', 'real_distance',
       'planned_distance', 'stop_lat', 'stop_lng', 'stop_city', 'stop_country',
       'stop_reference', 'time_window_start', 'time_window_end', 'order_idx',
       'fecha_hora']

    columns_query = ",".join(columns)
    excluded_query = ",".join(["EXCLUDED."+column for column in columns])
    values_query = "%s,"+",".join(["%s" for column in columns])
    df = df.fillna("NULL")
    records = list(df.to_records(index=False))
    
    # Change data types to native python types
    fixed_records = []
    for record in records:
        fixed_record = []
        for value in record:
            if isinstance(value, np.generic):
                fixed_record.append(value.item())
            elif value == "NULL":
                fixed_record.append(None)
            else:
                fixed_record.append(value)
        fixed_records.append(tuple(fixed_record))
    print(f"Number of records to load: {str(len(fixed_records))}")
    incremental_query = """
        INSERT INTO ecommdata.drivin_rutas_escenario (order_code,"""+columns_query+""") 
        VALUES ("""+values_query+""")
        ON CONFLICT (order_code)
        DO UPDATE SET ("""+columns_query+""") = ("""+excluded_query+""") 
    """
    print(incremental_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.executemany(incremental_query, fixed_records)
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    print("Data loaded to Postgres: ecommdata.drivin_rutas_escenario")

    return


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_drivin',
    default_args=default_args,
    description="carga y actualiza data de API driv.in, Rutas, Escenarios, Vehiculos y Ordenes",
    schedule_interval="0 * * * *",
    start_date=pendulum.datetime(2024, 5, 1, tz="America/Santiago"),
    catchup=True,
    max_active_runs=1,
    tags=["DATA", "S3", "Postgres", "Driv.in", "Capacity", "PATRICIO"],
) as dag:
    

    dag.doc_md = """
    arga y actualiza data de API driv.in, Rutas, Escenarios, Vehiculos y Ordene\n
    guardar en S3 y Upsert en postgres.
    """ 

    t0 = PythonOperator(
        task_id = 'drivin_escenarios_to_s3',
        python_callable=drivin_escenarios_to_s3,
    )
    t1 = PythonOperator(
        task_id = "drivin_escenarios_to_postgres",
        python_callable = drivin_escenarios_to_postgres,
    )
    t2 = PythonOperator(
        task_id = 'drivin_rutas_escenario_to_s3',
        python_callable=drivin_rutas_escenario_to_s3,
    )
    t3 = PythonOperator(
        task_id = "drivin_rutas_escenario_to_postgres",
        python_callable = drivin_rutas_escenario_to_postgres,
    )

    t0 >> t1 >> t2 >> t3