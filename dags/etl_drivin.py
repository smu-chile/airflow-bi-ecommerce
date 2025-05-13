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

def get_api_vehiculos(url, exception_cases):
    import requests
    import pandas as pd

    api_key = Variable.get("API_KEY_DRIVIN")
    headers = {
        'X-API-Key': api_key
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        data = response.json()
        if not isinstance(data.get('response', []), list):
            print(f"Formato inesperado en la respuesta de la API: {data}")
            exception_cases.append(url)
            return []

        lista = [
            (
                item.get('id'),
                item.get('code', ''),
                item.get('description', ''),
                item.get('detail', ''),
                item.get('model', ''),
                item.get('year', ''),
                item.get('peoneta_id', ''),
                item.get('assistant_2_id', ''),
                item.get('assistant_3_id', ''),
                item.get('device_number', ''),
                item.get('priority', ''),
                item.get('custom_1', ''),
                item.get('custom_2', ''),
                item.get('kinesis_redirect', ''),
                item.get('is_active', False),
                item.get('capacity_1', 0),
                item.get('capacity_2', 0),
                item.get('capacity_3', 0),
                item.get('capacity_4', 0),
                pd.to_datetime(item.get('shift_start')).strftime('%Y-%m-%d %H:%M:%S') if item.get('shift_start') else None,
                pd.to_datetime(item.get('shift_end')).strftime('%Y-%m-%d %H:%M:%S') if item.get('shift_end') else None,
                item.get('reload_time', 0),
                item.get('speed_factor', 1),
                item.get('days', []),
                item.get('driver', {}).get('email', ''),
                item.get('driver', {}).get('first_name', ''),
                item.get('driver', {}).get('last_name', ''),
                item.get('driver', {}).get('phone', ''),
                item.get('driver', {}).get('dni', ''),
                item.get('tags', []),
                item.get('cost_allocation_tags', []),
                item.get('device_type', ''),
                item.get('vehicle_tpye', ''),
                item.get('fleets', [])
            )
            for item in data['response']
        ]

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred for {url}: {http_err}")
        exception_cases.append(url)
        lista = []
    except requests.exceptions.RequestException as req_err:
        print(f"Request error occurred for {url}: {req_err}")
        exception_cases.append(url)
        lista = []
    except Exception as err:
        print(f"An error occurred for {url}: {err}")
        exception_cases.append(url)
        lista = []

    return lista

def get_api_direcciones(url,exception_cases):
    import requests

    api_key = Variable.get("API_KEY_DRIVIN")
    headers = {
        'X-API-Key': api_key
    }
    all_data = []
    page = 1

    while True:
        try:
            paginated_url = f"{url}?page={str(page)}"
            response = requests.get(paginated_url, headers=headers)
            response.raise_for_status()
            data = response.json()

            # Verifica si 'response' contiene datos y si es una lista
            response_data = data.get('response', [])
            if not response_data:
                # Si no hay más datos en 'response', sal del bucle
                print(f"No more data found on page {page}.")
                break

            # Procesa los datos de la respuesta
            all_data.extend([
                (
                    item.get('code'),
                    item.get('name'),
                    item.get('client'),
                    item.get('address_type'),
                    item.get('address1'),
                    item.get('address2'),
                    item.get('city'),
                    item.get('state'),
                    item.get('country'),
                    item.get('zip_code'),
                    item.get('phone'),
                    item.get('email'),
                    item.get('lat'),
                    item.get('lng'),
                    item.get('georeferenced'),
                    item.get('service_time'),
                    item.get('time_window_start'),
                    item.get('time_window_end'),
                    item.get('dispatch_date')
                )
                for item in response_data
            ])

            page += 1

        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred for {paginated_url}: {http_err}")
            exception_cases.append(paginated_url)
            break
        except requests.exceptions.RequestException as req_err:
            print(f"Request error occurred for {paginated_url}: {req_err}")
            exception_cases.append(paginated_url)
            break
        except Exception as err:
            print(f"An error occurred for {paginated_url}: {err}")
            exception_cases.append(paginated_url)
            break

    return all_data

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

def drivin_vehiculos_to_s3(ds,ts):
    import pandas as pd
    import requests
    import io
    from io import StringIO

    exec_date = ds.replace("-", "/")
    date_aux = ts.replace("-", "_")
    prefix = f"forecast_and_planning/drivin/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    url = f"https://external.driv.in/api/external/v2/vehicles"

    exception_cases = []

    lista_vehiculos = get_api_vehiculos(url,exception_cases)

    columns = ["id",
               "patente",
               "description",
               "detail",
               "model",
               "year",
               "peoneta_id",
               "assistant_2_id",
               "assistant_3_id",
               "device_number",
               "priority",
               "custom_1",
               "custom_2",
               "kinesis_redirect",
               "is_active",
               "capacity_1",
               "capacity_2",
               "capacity_3",
               "capacity_4",
               "shift_start",
               "shift_end",
               "reload_time",
               "speed_factor",
               "days",
               "email",
               "first_name",
               "last_name",
               "phone",
               "dni",
               "tags",
               "cost_allocation_tags",
               "device_type",
               "vehicle_tpye",
               "fleets"
               ]

    df = pd.DataFrame(lista_vehiculos,columns=columns)

    buffer = io.StringIO()
    df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    buffer.seek(0)

    filename = f"forecast_and_planning/drivin/{exec_date}/vehiculos/vehiculos_{date_aux}.csv"

    print(f"con fecha {ds} y nombre de filename como {filename}")
    s3_hook.load_string(buffer.getvalue(),
                key=filename,
                bucket_name=s3_bucket,
                replace=True,
                encrypt=False)

    print("se logro transformar los dataframes a archivos .csv")
    print(f"File load on S3: {prefix}")

    return filename

def drivin_vehiculos_to_postgres(ti,ts):
    import pandas as pd
    import sqlalchemy
    import numpy as np
    
    filename = ti.xcom_pull(key="return_value", task_ids=["drivin_vehiculos_to_s3"])[0]

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

    columns = ["patente",
               "description",
               "detail",
               "model",
               "year",
               "peoneta_id",
               "assistant_2_id",
               "assistant_3_id",
               "device_number",
               "priority",
               "custom_1",
               "custom_2",
               "kinesis_redirect",
               "is_active",
               "capacity_1",
               "capacity_2",
               "capacity_3",
               "capacity_4",
               "shift_start",
               "shift_end",
               "reload_time",
               "speed_factor",
               "days",
               "email",
               "first_name",
               "last_name",
               "phone",
               "dni",
               "tags",
               "cost_allocation_tags",
               "device_type",
               "vehicle_tpye",
               "fleets",
               "fecha_hora"
               ]

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
        INSERT INTO ecommdata.drivin_vehiculos (id,"""+columns_query+""") 
        VALUES ("""+values_query+""")
        ON CONFLICT (id)
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
    print("Data loaded to Postgres: ecommdata.drivin_vehiculos")
    return

def drivin_direcciones_to_s3(ds,ts):
    import pandas as pd
    import requests
    import io
    from io import StringIO

    exec_date = ds.replace("-", "/")
    date_aux = ts.replace("-", "_")
    prefix = f"forecast_and_planning/drivin/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    url = f"https://external.driv.in/api/external/v2/addresses"

    exception_cases = []

    lista_vehiculos = get_api_direcciones(url,exception_cases)

    columns = [ 'code',
                'name',
                'client',
                'address_type',
                'address1',
                'address2',
                'city',
                'state',
                'country',
                'zip_code',
                'phone',
                'email',
                'lat',
                'lng',
                'georeferenced',
                'service_time',
                'time_window_start',
                'time_window_end',
                'dispatch_date'
               ]

    df = pd.DataFrame(lista_vehiculos,columns=columns)

    df['code'] = df['code'].apply(lambda x: x[:100] if isinstance(x, str) and len(x) > 100 else x)
    df['name'] = df['name'].apply(lambda x: x[:100] if isinstance(x, str) and len(x) > 100 else x)
    df['client'] = df['client'].apply(lambda x: x[:100] if isinstance(x, str) and len(x) > 100 else x)
    df['address_type'] = df['address_type'].apply(lambda x: x[:100] if isinstance(x, str) and len(x) > 100 else x)
    df['address1'] = df['address1'].apply(lambda x: x[:100] if isinstance(x, str) and len(x) > 100 else x)
    df['address2'] = df['address2'].apply(lambda x: x[:100] if isinstance(x, str) and len(x) > 100 else x)
    df['city'] = df['city'].apply(lambda x: x[:100] if isinstance(x, str) and len(x) > 100 else x)
    df['state'] = df['state'].apply(lambda x: x[:100] if isinstance(x, str) and len(x) > 100 else x)
    df['country'] = df['country'].apply(lambda x: x[:100] if isinstance(x, str) and len(x) > 100 else x)
    df['zip_code'] = df['zip_code'].apply(lambda x: x[:100] if isinstance(x, str) and len(x) > 100 else x)
    df['phone'] = df['phone'].apply(lambda x: x[:100] if isinstance(x, str) and len(x) > 100 else x)
    df['email'] = df['email'].apply(lambda x: x[:100] if isinstance(x, str) and len(x) > 100 else x)

    df['lat'] = df['lat'].astype(float)
    df['lng'] = df['lng'].astype(float)



    buffer = io.StringIO()
    df.to_csv(buffer, header=True, index=False, encoding="utf-8", float_format='%.7f')
    buffer.seek(0)

    filename = f"forecast_and_planning/drivin/{exec_date}/direcciones/direcciones_{date_aux}.csv"

    print(f"con fecha {ds} y nombre de filename como {filename}")
    s3_hook.load_string(buffer.getvalue(),
                key=filename,
                bucket_name=s3_bucket,
                replace=True,
                encrypt=False)

    print("se logro transformar los dataframes a archivos .csv")
    print(f"File load on S3: {prefix}")

    return filename

def drivin_direcciones_to_postgres(ti,ts):
    import pandas as pd
    import sqlalchemy
    import numpy as np
    
    filename = ti.xcom_pull(key="return_value", task_ids=["drivin_direcciones_to_s3"])[0]

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

    df = df.dropna(subset=['client', 'address1'])

    df = df[['client',
            'address1',
            'code',
            'name',
            'address_type',
            'address2',
            'city',
            'state',
            'country',
            'zip_code',
            'phone',
            'email',
            'lat',
            'lng',
            'georeferenced',
            'service_time',
            'time_window_start',
            'time_window_end',
            'dispatch_date']]

    df["fecha_hora"] = ts

    columns = ['code',
                'name',
                'address_type',
                'address2',
                'city',
                'state',
                'country',
                'zip_code',
                'phone',
                'email',
                'lat',
                'lng',
                'georeferenced',
                'service_time',
                'time_window_start',
                'time_window_end',
                'dispatch_date',
               'fecha_hora'
               ]
    
    print(df.head(20))

    columns_query = ",".join(columns)
    excluded_query = ",".join(["EXCLUDED."+column for column in columns])
    values_query = "%s,%s,"+",".join(["%s" for column in columns])
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
        INSERT INTO ecommdata.drivin_direcciones (client,address1,"""+columns_query+""") 
        VALUES ("""+values_query+""")
        ON CONFLICT (client,address1)
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
    print("Data loaded to Postgres: ecommdata.drivin_direcciones")
    return

####################
# Cambios Nicolas  #
####################
def get_api_users(exception_cases):
    import requests
    import pandas as pd
    from airflow.models import Variable  

    url = "https://external.driv.in/api/external/v2/users"

    # Variables de Airflow
    api_key = Variable.get("API_KEY_DRIVIN")
    headers = {
        'X-API-Key': api_key
    }

    try:
        # Realizar la solicitud a la API
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Levanta excepción para errores HTTP 4xx/5xx

        data = response.json()

        # Procesar la respuesta de la API
        if 'response' in data and isinstance(data['response'], list):
            lista = [
                (
                    item.get('email'),
                    item.get('phone'),
                    item.get('first_name'),
                    item.get('last_name'),
                    item.get('role_name'),
                    item.get('organization'),
                    item.get('employer_name')
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

def drivin_users_to_s3(ts, ds):
    import pandas as pd
    import requests
    import io
    from airflow.models import Variable
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    # Genera fecha actual
    exec_date = ds.replace("-", "/")
    date_aux = ts.replace("-", "_")
    prefix = f"forecast_and_planning/drivin/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    # URL de usuarios 
    url = "https://external.driv.in/api/external/v2/users"

    exception_cases = []

    lista_usuarios = get_api_users(exception_cases)

    columns = [
        "email",
        "phone",
        "first_name",
        "last_name",
        "role_name",
        "organization",
        "employer_name"
    ]

    df = pd.DataFrame(lista_usuarios, columns=columns)

    # Guardar CSV
    buffer = io.StringIO()
    df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    buffer.seek(0)

    # Construir el nombre del archivo en S3 
    filename = f"forecast_and_planning/drivin/{exec_date}/users/users_{date_aux}.csv"

    print(f"Con fecha {ds} y nombre de archivo como {filename}")
    
    # Cargar el archivo en S3
    s3_hook.load_string(
        buffer.getvalue(),
        key=filename,
        bucket_name=s3_bucket,
        replace=True,
        encrypt=False
    )

    print("Se logró transformar los datos de usuarios en un archivo .csv")
    print(f"Archivo cargado en S3: {prefix}")

    return filename

def drivin_users_to_postgres(ti, ts):
    import pandas as pd
    import sqlalchemy
    import numpy as np
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from airflow.models import Variable

    filename = ti.xcom_pull(key="return_value", task_ids=["drivin_user_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: " + filename)
    if not s3_hook.check_for_key(filename, bucket_name=s3_bucket):
        raise Exception(f"Key {filename} does not exist.")

    # Obtener el archivo desde el S3
    hook_object = s3_hook.get_key(filename, bucket_name=s3_bucket)

    df = pd.read_csv(hook_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successful.")
        return

    print(f"Number of records extracted: {len(df.index)}")

    df["fecha_hora"] = ts

    columns = [
        "email",
        "phone",
        "first_name",
        "last_name",
        "role_name",
        "organization",
        "employer_name",
        "fecha_hora"
    ]

    columns_query = ",".join(columns)
    excluded_query = ",".join([f"EXCLUDED.{column}" for column in columns])
    values_query = ",".join(["%s" for _ in columns])
    df = df.fillna("NULL")
    records = list(df.to_records(index=False))

    # Convertir datos
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

    print(f"Number of records to load: {len(fixed_records)}")

    # Consulta incremental para PostgreSQL
    incremental_query = f"""
        INSERT INTO ecommdata.drivin_users ({columns_query}) 
        VALUES ({values_query})
        ON CONFLICT (email)
        DO UPDATE SET ({columns_query}) = ({excluded_query});
    """
    print(incremental_query)

    # Cargar en PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.executemany(incremental_query, fixed_records)
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    print("Data loaded to Postgres: ecommdata.drivin_users")
    return

def get_api_entrega_pruebas(exception_cases):
    from datetime import datetime
    import requests
    from airflow.models import Variable  

    current_date = datetime.now().strftime('%Y-%m-%d')
    url = f"https://external.driv.in/api/external/v2/pods?start_date={current_date}&end_date={current_date}"
    api_key = Variable.get("API_KEY_DRIVIN")
    headers = {'X-API-Key': api_key}
    exception_cases = []

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        if 'response' in data and isinstance(data['response'], list):
            lista = []
            for item in data['response']:
                for order in item.get("orders", []):
                    lista.append({
                        "planned_date": item.get("planned_date"),
                        "description": item.get("description"),
                        "scenario_token": item.get("scenario_token"),
                        "vehicle_code": item.get("vehicle_code"),
                        "vehicle_description": item.get("vehicle_description"),
                        "schema_code": item.get("schema_code"),
                        "schema_name": item.get("schema_name"),
                        "fleet_name": item.get("fleet_name"),
                        "organization_name": item.get("organization_name"),
                        "organization_alt_name": item.get("organization_alt_name"),
                        "route_is_approved": item.get("route_is_approved"),
                        "route_is_started": item.get("route_is_started"),
                        "route_is_finished": item.get("route_is_finished"),
                        "route_approved_at": item.get("route_approved_at"),
                        "route_started_at": item.get("route_started_at"),
                        "route_finished_at": item.get("route_finished_at"),
                        "driver_name": item.get("driver_name"),
                        "driver_email": item.get("driver_email"),
                        "driver_dni": item.get("driver_dni"),
                        "driver_license_number": item.get("driver_license_number"),
                        "assistant_1_name": item.get("assistant_1_name"),
                        "assistant_1_email": item.get("assistant_1_email"),
                        "route_code": item.get("route_code"),
                        "route_comment": item.get("route_comment"),
                        "employer_code": item.get("employer_code"),
                        "employer_name": item.get("employer_name"),
                        "address_name": item.get("address_name"),
                        "address_code": item.get("address_code"),
                        "address_address_1": item.get("address_address_1"),
                        "address_address_2": item.get("address_address_2"),
                        "address_city": item.get("address_city"),
                        "address_county": item.get("address_county"),
                        "address_state": item.get("address_state"),
                        "address_customer_name": item.get("address_customer_name"),
                        "address_lat": item.get("address_lat"),
                        "address_lng": item.get("address_lng"),
                        "address_postal_code": item.get("address_postal_code"),
                        "address_country": item.get("address_country"),
                        "planned_service_time": item.get("planned_service_time"),
                        "eta": item.get("eta"),
                        "eta_approved": item.get("eta_approved"),
                        "eta_started": item.get("eta_started"),
                        "trip_number": item.get("trip_number"),
                        "trip_code": item.get("trip_code"),
                        "trip_custom_1": item.get("trip_custom_1"),
                        "odometer_start": item.get("odometer_start"),
                        "odometer_end": item.get("odometer_end"),
                        "position": item.get("position"),
                        "start_position": item.get("start_position"),
                        "delivery_position": item.get("delivery_position"),
                        "time_windows": item.get("time_windows"),
                        "distance": item.get("distance"),
                        "tracked_arrival": item.get("tracked_arrival"),
                        "tracked_leave": item.get("tracked_leave"),
                        "tracked_service_time": item.get("tracked_service_time"),
                        "rating_1": item.get("rating_1"),
                        "rating_2": item.get("rating_2"),
                        "rating_3": item.get("rating_3"),
                        "customer_comment": item.get("customer_comment"),
                        "visit_arrival": item.get("visit_arrival"),
                        "visit_leave": item.get("visit_leave"),
                        "images": item.get("images"),
                        "signature": item.get("signature"),
                        "custom_fields": item.get("custom_fields"),
                        "comment": item.get("comment"),
                        "events": item.get("events"),
                        "pdf_pod": item.get("pdf_pod"),

                        # Campos renombrados para coincidir con columnas esperadas
                        "code": order.get("code"),
                        "alt_code": order.get("alt_code"),
                        "description_order": order.get("description"),
                        "address_type": order.get("address_type"),
                        "pod_arrival": order.get("pod_arrival"),
                        "pod_distance": order.get("pod_distance"),
                        "near_pod": order.get("near_pod"),
                        "pod_lat": order.get("pod_lat"),
                        "pod_lng": order.get("pod_lng"),
                        "delivery_date": order.get("delivery_date"),
                        "deploy_date": order.get("deploy_date"),
                        "billing_date": order.get("billing_date"),
                        "status": order.get("status"),
                        "status_code": order.get("status_code"),
                        "customer_status": order.get("customer_status"),
                        "load_status": order.get("load_status"),
                        "reason": order.get("reason"),
                        "reason_code": order.get("reason_code"),
                        "otif": order.get("otif"),
                        "ifd_count": order.get("ifd_count"),
                        "supplier_code": order.get("supplier_code"),
                        "supplier_name": order.get("supplier_name"),
                        "client_code": order.get("client_code"),
                        "client_name": order.get("client_name"),
                        "units": order.get("units"),
                        "units_1": order.get("units_1"),
                        "units_2": order.get("units_2"),
                        "units_3": order.get("units_3"),
                        "cusom_1": order.get("custom_1"),
                        "cusom_2": order.get("custom_2"),
                        "cusom_3": order.get("custom_3"),
                        "cusom_4": order.get("custom_4"),
                        "cusom_5": order.get("custom_5"),
                        "custom_6": order.get("custom_6"),
                        "number_1": order.get("number_1"),
                        "number_2": order.get("number_2"),
                        "number_3": order.get("number_3"),
                        "is_otd": order.get("is_otd"),
                        "items": order.get("items"),
                        "pickups": order.get("pickups"),
                        "images": order.get("images"),
                        "comment": order.get("comment")
                    })
            return lista        
        else:
            print(f"Formato inesperado en la respuesta de la API: {data}")
            exception_cases.append(url)
            return []

    except requests.exceptions.RequestException as req_err:
        print(f"Request error occurred: {req_err}")
        exception_cases.append(url)
        return []


def drivin_entrega_prueba_to_s3(ts, ds):
    import pandas as pd
    import requests
    import io
    from airflow.models import Variable
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    # Genera fecha actual
    exec_date = ds.replace("-", "/")
    date_aux = ts.replace("-", "_")
    prefix = f"forecast_and_planning/drivin/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    exception_cases = []

    lista_usuarios = get_api_entrega_pruebas(exception_cases)

    # Verificar las primeras filas de la lista de datos
    if lista_usuarios:
        print(f"Primer registro de lista_usuarios: {lista_usuarios[0]}")

    columns = ["planned_date",
                    "description",
                    "scenario_token",
                    "vehicle_code",
                    "vehicle_description",
                    "schema_code",
                    "schema_name",
                    "fleet_name",
                    "organization_name",
                    "organization_alt_name",
                    "route_is_approved",
                    "route_is_started",
                    "route_is_finished",
                    "route_approved_at",
                    "route_started_at",
                    "route_finished_at",
                    "driver_name",
                    "driver_email",
                    "driver_dni",
                    "driver_license_number",
                    "assistant_1_name",
                    "assistant_1_email",
                    "route_code",
                    "route_comment",
                    "employer_code",
                    "employer_name",
                    "address_name",
                    "address_code",
                    "address_address_1",
                    "address_address_2",
                    "address_city",
                    "address_county",
                    "address_state",
                    "address_customer_name",
                    "address_lat",
                    "address_lng",
                    "address_postal_code",
                    "address_country",
                    "planned_service_time",
                    "eta",
                    "eta_approved",
                    "eta_started",
                    "trip_number",
                    "trip_code",
                    "trip_custom_1",
                    "odometer_start",
                    "odometer_end",
                    "position",
                    "start_position",
                    "delivery_position",
                    "time_windows",
                    "distance",
                    "tracked_arrival",
                    "tracked_leave",
                    "tracked_service_time",
                    "rating_1",
                    "rating_2",
                    "rating_3",
                    "customer_comment",
                    "visit_arrival",
                    "visit_leave",
                    "images",
                    "signature",
                    "custom_fields",
                    "comment",
                    "events",
                    "pdf_pod",
                    "code",
                    "alt_code",
                    "description_order",
                    "address_type",
                    "pod_arrival",
                    "pod_distance",
                    "near_pod",
                    "pod_lat",
                    "pod_lng",
                    "delivery_date",
                    "deploy_date",
                    "billing_date",
                    "status",
                    "status_code",
                    "customer_status",
                    "load_status",
                    "reason",
                    "reason_code",
                    "otif",
                    "ifd_count",
                    "images",
                    "comment",
                    "supplier_code",
                    "supplier_name",
                    "client_code",
                    "client_name",
                    "units",
                    "units_1",
                    "units_2",
                    "units_3",
                    "cusom_1",
                    "cusom_2",
                    "cusom_3",
                    "cusom_4",
                    "cusom_5",
                    "custom_6",
                    "number_1",
                    "number_2",
                    "number_3",
                    "is_otd",
                    "items",
                    "pickups"]
      
    df = pd.DataFrame(lista_usuarios, columns=columns)

    # Guardar CSV
    buffer = io.StringIO()
    df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    buffer.seek(0)

    # Construir el nombre del archivo en S3 
    filename = f"forecast_and_planning/drivin/{exec_date}/entrega_pruebas/entrega_pruebas_{date_aux}.csv"

    print(f"Con fecha {ds} y nombre de archivo como {filename}")

    # Cargar el archivo en S3
    s3_hook.load_string(
        buffer.getvalue(),
        key=filename,
        bucket_name=s3_bucket,
        replace=True,
        encrypt=False
    )

    print("Se logró transformar los datos de entrega pruebas en un archivo .csv")
    print(f"Archivo cargado en S3: {prefix}")

    return filename

def drivin_entrega_prueba_to_postgres(ti, ts):
    import pandas as pd
    import sqlalchemy
    import numpy as np
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from airflow.models import Variable

    filename = ti.xcom_pull(key="return_value", task_ids=["drivin_entrega_prueba_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: " + filename)
    if not s3_hook.check_for_key(filename, bucket_name=s3_bucket):
        raise Exception(f"Key {filename} does not exist.")
    
    # Obtener el archivo desde el S3
    hook_object = s3_hook.get_key(filename, bucket_name=s3_bucket)
    df = pd.read_csv(hook_object.get()["Body"])

    if df.empty:
        print("There are no new nor updated records to load. Task will exit as successful.")
        return

    print(f"Number of records extracted: {len(df.index)}")

    df["fecha_hora"] = ts

    # Columnas sin duplicados + order_code
    columns = [
        "planned_date", "description", "scenario_token", "vehicle_code", "vehicle_description",
        "schema_code", "schema_name", "fleet_name", "organization_name", "organization_alt_name",
        "route_is_approved", "route_is_started", "route_is_finished", "route_approved_at",
        "route_started_at", "route_finished_at", "driver_name", "driver_email", "driver_dni",
        "driver_license_number", "assistant_1_name", "assistant_1_email", "route_code", "route_comment",
        "employer_code", "employer_name", "address_name", "address_code", "address_address_1",
        "address_address_2", "address_city", "address_county", "address_state", "address_customer_name",
        "address_lat", "address_lng", "address_postal_code", "address_country", "planned_service_time",
        "eta", "eta_approved", "eta_started", "trip_number", "trip_code", "trip_custom_1",
        "odometer_start", "odometer_end", "position", "start_position", "delivery_position",
        "time_windows", "distance", "tracked_arrival", "tracked_leave", "tracked_service_time",
        "rating_1", "rating_2", "rating_3", "customer_comment", "visit_arrival", "visit_leave",
        "images", "signature", "custom_fields", "comment", "events", "pdf_pod", "code", "alt_code",
        "description_order", "address_type", "pod_arrival", "pod_distance", "near_pod", "pod_lat",
        "pod_lng", "delivery_date", "deploy_date", "billing_date", "status", "status_code",
        "customer_status", "load_status", "reason", "reason_code", "otif", "ifd_count", 
        "supplier_code", "supplier_name", "client_code", "client_name", "units", "units_1", "units_2",
        "units_3", "cusom_1", "cusom_2", "cusom_3", "cusom_4", "cusom_5", "custom_6", "number_1",
        "number_2", "number_3", "is_otd", "items", "pickups", "fecha_hora", "order_code"
    ]

    # Validación rápida por si acaso
    missing_cols = [col for col in columns if col not in df.columns]
    if missing_cols:
        raise Exception(f"Missing expected columns in DataFrame: {missing_cols}")

    columns_query = ",".join(columns)
    excluded_query = ",".join([f"EXCLUDED.{col}" for col in columns])
    values_query = ",".join(["%s"] * len(columns))

    df = df.fillna("NULL")
    records = list(df.to_records(index=False))

    # Convertir a tipos compatibles con psycopg2
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

    print(f"Number of records to load: {len(fixed_records)}")

    incremental_query = f"""
        INSERT INTO ecommdata.drivin_entrega_prueba ({columns_query}) 
        VALUES ({values_query})
        ON CONFLICT (order_code)
        DO UPDATE SET ({columns_query}) = ({excluded_query});
    """

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.executemany(incremental_query, fixed_records)
    pg_connection.commit()
    cursor.close()
    pg_connection.close()

    print("Data loaded to Postgres: ecommdata.drivin_entrega_prueba")
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
    tags=["DATA", "S3", "Postgres", "Driv.in", "Capacity", "PATRICIO" , "NICOLAS"],
) as dag:
    

    dag.doc_md = """
    Carga y actualiza data de API driv.in, Rutas, Escenarios, Vehiculos, Ordene y direcciones\n
    guardar en S3 y Upsert en postgres.
    """ 

    t0 = PythonOperator(
        task_id = 'drivin_escenarios_to_s3',
        python_callable = drivin_escenarios_to_s3,
    )
    t1 = PythonOperator(
        task_id = "drivin_escenarios_to_postgres",
        python_callable = drivin_escenarios_to_postgres,
    )
    t2 = PythonOperator(
        task_id = 'drivin_rutas_escenario_to_s3',
        python_callable = drivin_rutas_escenario_to_s3,
    )
    t3 = PythonOperator(
        task_id = "drivin_rutas_escenario_to_postgres",
        python_callable = drivin_rutas_escenario_to_postgres,
    )
    t4 = PythonOperator(
        task_id = 'drivin_vehiculos_to_s3',
        python_callable = drivin_vehiculos_to_s3,
    )
    t5 = PythonOperator(
        task_id = "drivin_vehiculos_to_postgres",
        python_callable = drivin_vehiculos_to_postgres,
    )
    t6 = PythonOperator(
        task_id = 'drivin_direcciones_to_s3',
        python_callable = drivin_direcciones_to_s3,
    )
    t7 = PythonOperator(
        task_id = "drivin_direcciones_to_postgres",
        python_callable = drivin_direcciones_to_postgres,
    )
    t8 =  PythonOperator(
        task_id = "drivin_user_to_s3",
        python_callable = drivin_users_to_s3,
    )
    t9 =  PythonOperator(
        task_id = "drivin_users_to_postgres",
        python_callable = drivin_users_to_postgres,
    )   
    t10=  PythonOperator(
        task_id = "drivin_entrega_prueba_to_s3",
        python_callable = drivin_entrega_prueba_to_s3,
    )  
    t11   =  PythonOperator(
        task_id = "drivin_entrega_prueba_to_postgres",
        python_callable = drivin_entrega_prueba_to_postgres,
    )    

    t0 >> t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9 >> t10 >> t11 