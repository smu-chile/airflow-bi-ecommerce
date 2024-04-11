from airflow import DAG
from airflow import macros
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

from pulp import LpProblem, LpVariable, lpSum, LpMinimize, LpMaximize
from datetime import datetime, timedelta
import pendulum

def create_schedule(data):
    import pandas as pd
    # Create a new LP problem
    prob = LpProblem("Scheduler", LpMinimize)
    
    # Extract data
    tasks = data['id_tarea'].tolist()
    start_times = data['hora_inicio'].tolist()
    end_times = data['hora_termino'].tolist()
    durations = data['duracion'].tolist()
    priorities = data['prioridad'].tolist()
    min_workers = data['min_operadores'].tolist()
    max_workers = data['max_operadores'].tolist()

    # Define the decision variables
    # Each variable represents whether a task is scheduled at each hour for each worker
    schedule = {(task, hour, worker): LpVariable(f'Task_{task}_Hour_{hour}_Worker_{worker}', cat='Binary')
                for task in tasks
                for hour in range(24)
                for worker in range(max_workers[task])}

    # Define the objective function
    # Minimize the sum of priorities if not enough workers are available
    prob += lpSum(priorities[task] * lpSum(schedule[task, hour, worker]
                                            for hour in range(24)
                                            for worker in range(max_workers[task]))
                  for task in tasks)

    # Constraints
    for task in tasks:
        # Ensure each task is scheduled for its duration
        prob += lpSum(schedule[task, hour, worker] for hour in range(24) for worker in range(max_workers[task])) == durations[task]

        # Ensure tasks only happen within their allowed time range
        for hour in range(24):
            for worker in range(max_workers[task]):
                if hour < start_times[task] or hour + durations[task] > end_times[task]:
                    prob += schedule[task, hour, worker] == 0

        # Ensure minimum and maximum number of workers for each task
        prob += lpSum(schedule[task, hour, worker]
                      for hour in range(24)
                      for worker in range(max_workers[task])) >= min_workers[task]
        prob += lpSum(schedule[task, hour, worker]
                      for hour in range(24)
                      for worker in range(max_workers[task])) <= max_workers[task]

    # Solve the problem
    prob.solve()

    # Extract the schedule
    schedule_df = pd.DataFrame(columns=['Task', 'Hour', 'Worker'])
    for task in tasks:
        for hour in range(24):
            for worker in range(max_workers[task]):
                if schedule[task, hour, worker].varValue == 1:
                    schedule_df = schedule_df.append({'Task': task, 'Hour': hour, 'Worker': worker}, ignore_index=True)

    return schedule_df

def turnos_load_to_s3(ds):
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

    with open(f"{curr_working_directory}/dags/unimarc/sql/turnos_mfc.sql", "r") as query_file:
        promociones_query = query_file.read()
    
    promociones_query = promociones_query.replace("{ds}", ds)

    print("Base query:")
    print(promociones_query)

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()

    df_promotions = pd.read_sql_query(promociones_query, pg_connection)
    buffer = io.StringIO()
    df_promotions.to_csv(buffer, header=True, index=False, encoding="utf-8")

    filename = f"promociones_vtex_alvi/{exec_date}/turnos_supervisor.csv"

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

def turnos_load_to_postgres(ti):
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text

    filename = ti.xcom_pull(key="return_value", task_ids=["turnos_load_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+filename)
    if not s3_hook.check_for_key(filename, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % filename)

    turnos_object = s3_hook.get_key(filename, bucket_name=s3_bucket)

    df = pd.read_csv(turnos_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")
    df.info()

    schedule = create_schedule(df)
    print(schedule)

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    # Save to PostgreSQL:

    with engine.begin() as conn:
        conn.execute("TRUNCATE ecommdata.turnos_supervisor_mfc") 
        df.to_sql(name="turnos_supervisor_mfc",
                    con=conn,         
                    schema="ecommdata",         
                    if_exists='append',         
                    index=False,         
                    chunksize=20000,         
                    method='multi')

    print("Data loaded to Postgres: ecommdata.turnos_supervisor_mfc")
    return

def turnos_load_to_slack(ti):
    import pandas as pd
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError

    filename = ti.xcom_pull(key="return_value", task_ids=["turnos_load_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+filename)
    if not s3_hook.check_for_key(filename, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % filename)

    turnos_object = s3_hook.get_key(filename, bucket_name=s3_bucket)

    df = pd.read_csv(turnos_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    headers = ["fecha", "bloque", "numero_operadores", "entrada", "salida"]

    column_widths = [len(header) for header in headers]
    for row in df:
        for index, value in enumerate(row):
            column_widths[index] = max(column_widths[index], len(str(value)))

    formatted_header = " | ".join(header.upper().ljust(column_widths[index]) for index, header in enumerate(headers))

    formatted_rows = [formatted_header]
    for row in df:
        formatted_row = " | ".join(str(value).ljust(column_widths[index]) for index, value in enumerate(row))
        formatted_rows.append(formatted_row)

    formatted_message = "```\n" + "\n".join(formatted_rows) + "\n```"
    print(formatted_message)

    token = Variable.get("token_slack")

    client = WebClient(token=token)

    try:
        response = client.chat_postMessage(channel="dotaciones-mfc", text=formatted_message)
    except SlackApiError as e:
        print(f"Error sending message: {e}")

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
    schedule_interval="0 8 * * *",
    start_date=pendulum.datetime(2022, 8, 25, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["Turnos","Staffing","Dotacion", "MFC", "ecommdata","SLACK" ,"SERGIO"],
) as dag:

    dag.doc_md = """
    genera unidades solicitadas para mfc en picking tienda.
    """ 

    t0 = PythonOperator(
        task_id = "turnos_load_to_s3",
        python_callable = turnos_load_to_s3
    )
    t1 = PythonOperator(
        task_id = "turnos_load_to_postgres",
        python_callable = turnos_load_to_postgres
    )

    t0 >> t1