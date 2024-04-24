from airflow import DAG
from airflow import macros
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import pendulum

def create_schedule(operadores_df, tareas_df):
    import pandas as pd
    column_names = [f"{hour:02d}:00" for hour in range(7, 24)] + [f"{hour:02d}:00" for hour in range(0, 7)]
    schedule_df = pd.DataFrame(index=operadores_df['rut'], columns=column_names)
    schedule_df = schedule_df.fillna(0)

    def is_within_task_time(hour, start_time, end_time):
        start_hour = start_time.hour
        end_hour = end_time.hour
        if end_hour < start_hour:  
            return (start_hour <= hour < 24) or (0 <= hour < end_hour)
        else:  
            return start_hour <= hour < end_hour

    for hour in range(24):
        for _, task in tareas_df.iterrows():
            task_id = task['id_tarea']
            task_start_time = task['hora_inicio']
            task_end_time = task['hora_termino']
            min_workers = task['min_operadores']
            max_workers = task['max_operadores']
            duration = task['duracion']

            if duration > 0 and is_within_task_time(hour, task_start_time, task_end_time):
                workers_needed = min(min_workers, max_workers - schedule_df.iloc[:, hour].eq(task_id).sum())
                for _, worker in operadores_df.iterrows():
                    worker_name = worker['rut']
                    entrada = int(worker['entrada'].split(':')[0])
                    salida = int(worker['salida'].split(':')[0])

                    if workers_needed > 0:
                        if salida < entrada:
                            if (entrada <= hour < 24) or (0 <= hour < salida):
                                if schedule_df.loc[worker_name, f"{hour:02d}:00"] == 0:
                                    schedule_df.loc[worker_name, f"{hour:02d}:00"] = task_id
                                    workers_needed -= 1
                                    duration -= 1
                        else:
                            if entrada <= hour < salida:
                                if schedule_df.loc[worker_name, f"{hour:02d}:00"] == 0:
                                    schedule_df.loc[worker_name, f"{hour:02d}:00"] = task_id
                                    workers_needed -= 1
                                    duration -= 1

    def is_within_shift(hour, entrada, salida):
        if salida < entrada:
            return (entrada <= hour < 24) or (0 <= hour < salida)
        else:
            return entrada <= hour < salida
    
    for hour in range(24):
        for _, task in tareas_df.iterrows():
            task_id = task['id_tarea']
            task_start_time = task['hora_inicio']
            task_end_time = task['hora_termino']
            min_workers = task['min_operadores']
            duration = task['duracion']

            if duration > 0 and is_within_task_time(hour, task_start_time, task_end_time):
                workers_assigned = schedule_df[f"{hour:02d}:00"].eq(task_id).sum()
                remaining_workers = min_workers - workers_assigned

                if remaining_workers > 0:
                    for _, worker in operadores_df.iterrows():
                        worker_name = worker['rut']
                        entrada = int(worker['entrada'].split(':')[0])
                        salida = int(worker['salida'].split(':')[0])

                        if is_within_shift(hour, entrada, salida):
                            if schedule_df.loc[worker_name, f"{hour:02d}:00"] == 0:
                                schedule_df.loc[worker_name, f"{hour:02d}:00"] = task_id
                                remaining_workers -= 1
                                duration -= 1

                                if remaining_workers == 0 or duration == 0:
                                    break

    return schedule_df

def tareas_load_to_s3(ds):
    import pandas as pd
    import numpy as np
    import os
    import io
    from io import StringIO

    exec_date = ds.replace("-", "/")
    prefix = f"dotacion/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    curr_working_directory = os.getcwd()
    print(os.getcwd())

    with open(f"{curr_working_directory}/dags/unimarc/sql/dotacion_tareas.sql", "r") as query_file:
        tareas_query = query_file.read()

    tareas_query = tareas_query.replace("{ds}", ds)

    print("Base query:")
    print(tareas_query)

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()

    df_tareas = pd.read_sql_query(tareas_query, pg_connection)

    buffer = io.StringIO()
    df_tareas.to_csv(buffer, header=True, index=False, encoding="utf-8")

    filename = f"dotacion/{exec_date}/tareas_MFC.csv"

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

def operadores_load_to_s3(ds):
    import pandas as pd
    import numpy as np
    import os
    import io
    from io import StringIO

    exec_date = ds.replace("-", "/")
    prefix = f"dotacion/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    curr_working_directory = os.getcwd()
    print(os.getcwd())

    with open(f"{curr_working_directory}/dags/unimarc/sql/dotacion_operadores.sql", "r") as query_file:
        operadores_query = query_file.read()

    operadores_query = operadores_query.replace("{ds}", ds)

    print("Base query:")
    print(operadores_query)

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()

    df_operadores = pd.read_sql_query(operadores_query, pg_connection)

    buffer = io.StringIO()
    df_operadores.to_csv(buffer, header=True, index=False, encoding="utf-8")

    filename = f"dotacion/{exec_date}/operadores_MFC.csv"

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

def disponibilidad_load_to_s3(ds):
    import pandas as pd
    import numpy as np
    import os
    import io
    from io import StringIO

    exec_date = ds.replace("-", "/")
    prefix = f"dotacion/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    curr_working_directory = os.getcwd()
    print(os.getcwd())

    with open(f"{curr_working_directory}/dags/unimarc/sql/dotacion_disponible.sql", "r") as query_file:
        disponibilidad_query = query_file.read()

    disponibilidad_query = disponibilidad_query.replace("{ds}", ds)

    print("Base query:")
    print(disponibilidad_query)

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()

    df_disponibilidad = pd.read_sql_query(disponibilidad_query, pg_connection)

    buffer = io.StringIO()
    df_disponibilidad.to_csv(buffer, header=True, index=False, encoding="utf-8")

    filename = f"dotacion/{exec_date}/disponibilidad_MFC.csv"

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

def turnos_load_to_slack(ti,ds):
    import pandas as pd
    import io
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError

    tareas_filename = ti.xcom_pull(key="return_value", task_id=["tareas_load_to_s3"])[0]
    operadores_filename = ti.xcom_pull(key="return_value", task_id=["operadores_load_to_s3"])[0]
    disponibilidad_filename = ti.xcom_pull(key="return_value", task_id=["disponibilidad_load_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+tareas_filename)
    if not s3_hook.check_for_key(tareas_filename, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % tareas_filename)

    tareas_object = s3_hook.get_key(tareas_filename, bucket_name=s3_bucket)

    print("Searching file: "+operadores_filename)
    if not s3_hook.check_for_key(operadores_filename, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % operadores_filename)

    operadores_object = s3_hook.get_key(operadores_filename, bucket_name=s3_bucket)

    print("Searching file: "+disponibilidad_filename)
    if not s3_hook.check_for_key(disponibilidad_filename, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % disponibilidad_filename)

    disponibilidad_object = s3_hook.get_key(disponibilidad_filename, bucket_name=s3_bucket)

    df_tareas = pd.read_csv(tareas_object.get()["Body"])
    if len(df_tareas.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    df_operadores = pd.read_csv(operadores_object.get()["Body"])
    if len(df_tareas.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    df_disponibilidad = pd.read_csv(disponibilidad_object.get()["Body"])
    if len(df_tareas.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    schedule_df = create_schedule(df_operadores,df_tareas)

    task_id_to_task = dict(zip(df_tareas['id_tarea'], df_tareas['nombre_tarea']))
    rut_to_nombre_operador = dict(zip(df_operadores['rut'], df_operadores['nombre_operador']))
    schedule_df = schedule_df.apply(lambda x: x.map(task_id_to_task) if x.name != 'rut' else x)
    schedule_df.index = schedule_df.index.map(rut_to_nombre_operador)
    schedule_df.index.name = 'nombre_operador'
    
    token = Variable.get("token_slack")
    client = WebClient(token=token)

    with io.BytesIO() as buffer:
        df_tareas.to_csv(buffer, index=False, encoding='utf-8')
        buffer.seek(0)
        try:
            response = client.files_upload(
                channels="cambio-de-turno-supervisores-mfc",
                file=buffer,
                filename=f"tareas_mfc_{ds}.csv",
                title="Tareas Diarias",
                initial_comment="Se adjunta el reporte de tareas para el dia."
            )
        except SlackApiError as e:
            print(f"Error al subir archivo: {e}")
    with io.BytesIO() as buffer:
        df_disponibilidad.to_csv(buffer, index=False, encoding='utf-8')
        buffer.seek(0)
        try:
            response = client.files_upload(
                channels="cambio-de-turno-supervisores-mfc",
                file=buffer,
                filename=f"dotacion_diaria_{ds}.csv",
                title="Distribucion de operadores por turnos",
                initial_comment="Se adjunta el reporte de distribucion de operadores por turnos para el dia."
            )
        except SlackApiError as e:
            print(f"Error al subir archivo: {e}")
    with io.BytesIO() as buffer:
        schedule_df.to_csv(buffer, index=True, encoding='utf-8')
        buffer.seek(0)
        try:
            response = client.files_upload(
                channels="cambio-de-turno-supervisores-mfc",
                file=buffer,
                filename=f"tareas_por_operador_mfc_{ds}.csv",
                title="Distribucion de turnos MFC",
                initial_comment="Se adjunta el reporte de tareas por operador para el dia."
            )
        except SlackApiError as e:
            print(f"Error al subir archivo: {e}")

    return
    

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_turno_supervisor_mfc',
    default_args=default_args,
    description="consulta de datos de Stock MFC, maestra reposicion desde postgres para logica de reposicion.",
    schedule_interval="0 6 * * 1,2,3,4,5",
    start_date=pendulum.datetime(2022, 8, 25, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["Turnos","Staffing","Dotacion", "MFC", "ecommdata","SLACK" ,"SERGIO"],
) as dag:

    dag.doc_md = """
    genera unidades solicitadas para mfc en picking tienda.
    """ 

    t0 = PythonOperator(
        task_id = "tareas_load_to_s3",
        python_callable = tareas_load_to_s3
    )
    t1 = PythonOperator(
        task_id = "operadores_load_to_s3",
        python_callable = operadores_load_to_s3
    )
    t2 = PythonOperator(
        task_id = "disponibilidad_load_to_s3",
        python_callable = disponibilidad_load_to_s3
    )
    t3 = PythonOperator(
        task_id = "turnos_load_to_slack",
        python_callable = turnos_load_to_slack
    )

    t0 >> t1 >> t2 >> t3