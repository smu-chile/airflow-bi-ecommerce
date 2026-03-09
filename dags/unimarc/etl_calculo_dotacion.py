from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable

from utils.slack_utils import dag_success_slack, dag_failure_slack

import pendulum

def load_staffing_matrix_to_s3(ds):
    import pandas as pd
    import io
    from io import StringIO
    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"dotacion/{exec_date}/"
    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
    matriz_query = """SELECT *
        from ecommdata.matriz_dotacion;"""
    pg_hook = PostgresHook(conn_id="postgresql_conn_dev")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(matriz_query)
    results = cursor.fetchall()
    matriz=pd.DataFrame(results)
    matriz.columns = ["turno","lunes","martes","miercoles","jueves","viernes","sabado","domingo","jornada"]
    cursor.close()
    pg_connection.close()
    
    buffer = io.StringIO()
    matriz.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"dotacion/{exec_date}/matriz_{date_aux}.csv"
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

def calculate_and_load_turnos_to_s3(ti,ds):
    import pandas as pd
    from datetime import datetime, timedelta
    import io
    from io import StringIO

    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"dotacion/{exec_date}/"

    file_name = ti.xcom_pull(key="return_value", task_ids=["load_staffing_matrix_to_s3"])[0]
    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    if not s3_hook.check_for_key(file_name, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % file_name)

    s3_object = s3_hook.get_key(file_name, bucket_name=s3_bucket)
    matriz_df = pd.read_csv(s3_object.get()["Body"])

    print(matriz_df.to_string())

    operadores_query = """SELECT *
        from ecommdata.dotacion_operadores;"""
    pg_hook = PostgresHook(conn_id="postgresql_conn_dev")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(operadores_query)
    results = cursor.fetchall()
    operadores_df=pd.DataFrame(results)
    operadores_df.columns = ["id_operador","nombre_operador","rut"]
    cursor.close()
    pg_connection.close()
    
    print(operadores_df.to_string())
    start_date = datetime.strptime("2024-04-01", "%Y-%m-%d")
    end_date = datetime.strptime("2024-12-31", "%Y-%m-%d")

    auxlist = []

    for index_operador, row_operador in operadores_df.iterrows():
        start_index = index_operador % len(matriz_df)
        current_date = start_date
        while current_date <= end_date:
            for index_matriz, row_matriz in matriz_df.iloc[start_index:].iterrows():
                for day in ["lunes", "martes", "miercoles", "jueves", "viernes", "sabado", "domingo"]:
                    print(row_operador["id_operador"],row_operador["rut"],row_operador["nombre_operador"],row_matriz["turno"],current_date.strftime("%Y-%m-%d"),row_matriz[day],row_matriz["jornada"])
                    auxlist.append([row_operador["id_operador"],row_operador["rut"],row_operador["nombre_operador"],row_matriz["turno"],current_date.strftime("%Y-%m-%d"),row_matriz[day],row_matriz["jornada"]])  
                    current_date += timedelta(days=1)
                if current_date > end_date:
                    break
            start_index = 0
            if current_date > end_date:
                break
    
    turnos_df = pd.DataFrame(auxlist, columns = ['id_operador','rut','nombres','rol','fecha','bloque','jornada'])
    
    buffer = io.StringIO()
    turnos_df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"dotacion/{exec_date}/turnos_{date_aux}.csv"
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

def load_staffing_to_postgres(ti):
    import pandas as pd
    import numpy as np

    file_name = ti.xcom_pull(key="return_value", task_ids=["calculate_and_load_turnos_to_s3"])[0]
    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    if not s3_hook.check_for_key(file_name, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % file_name)

    s3_object = s3_hook.get_key(file_name, bucket_name=s3_bucket)
    turnos_df = pd.read_csv(s3_object.get()["Body"])

    operadores_query = """SELECT *
        from ecommdata.dotacion_horarios;"""
    pg_hook = PostgresHook(conn_id="postgresql_conn_dev")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(operadores_query)
    results = cursor.fetchall()
    horarios_df=pd.DataFrame(results)
    horarios_df.columns = ["bloque","entrada","salida","jornada","horas"]
    cursor.close()
    pg_connection.close()

    dotacion_df = pd.merge(turnos_df, horarios_df, on=['jornada', 'bloque'], how='left')

    columns = ["rut","nombres","rol","fecha","bloque","entrada","salida","jornada","horas"]

    columns_query = ",".join(columns)
    values_query = "%s,"+",".join(["%s" for column in columns])
    dotacion_df = dotacion_df.fillna("NULL")
    records = list(dotacion_df.to_records(index=False))
    
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
        INSERT INTO ecommdata.dotacion_mfc (id_operador,"""+columns_query+""") 
        VALUES ("""+values_query+""")
        ON CONFLICT (id_operador, fecha)
        DO NOTHING; 
    """
    print(incremental_query)
    pg_hook = PostgresHook(conn_id="postgresql_conn_dev")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.executemany(incremental_query, fixed_records)
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    print("Data loaded to Postgres")

    return


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_dotacion_mfc',
    default_args=default_args,
    description="calculo de dotacion con matriz de pesos de turnos para MFC",
    schedule=None,
    start_date=pendulum.datetime(2023, 6, 1, tz="America/Santiago"),
    catchup=False,
    tags=["catalogo", "Dotacion", "Staffing", "MFC", "unimarc", "SERGIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:
    
    dag.doc_md = """
    construir y cargar dotacion MFC
    """ 

    t0 = PythonOperator(
        task_id = "load_staffing_matrix_to_s3",
        python_callable = load_staffing_matrix_to_s3,
    )

    t1 = PythonOperator(
        task_id = "calculate_and_load_turnos_to_s3",
        python_callable = calculate_and_load_turnos_to_s3,
    )

    t2 = PythonOperator(
        task_id = "load_staffing_to_postgres",
        python_callable = load_staffing_to_postgres,
    )
    
    t0 >> t1 >> t2
    