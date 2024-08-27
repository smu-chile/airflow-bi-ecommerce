from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

import pendulum

from datetime import datetime, timedelta


def _api_onemarketer(ts):
    import requests
    import json

    exec_date = datetime.strptime(ts[:16], "%Y-%m-%dT%H:%M")
    exec_date_b = exec_date - timedelta(minutes=30)
    exec_date_e = exec_date + timedelta(minutes=30)
    exec_date_rf_b = exec_date_b.strftime("%Y-%m-%d %H:%M")
    exec_date_rf_e = exec_date_e.strftime("%Y-%m-%d %H:%M")
    print(exec_date_rf_b)
    print(exec_date_rf_e)

    url = Variable.get("url_onemarketer")

    aux_h = {
        'Connection': 'keep-alive',
        'Accept-Encoding': 'gzip, deflate, br'
    }

    parameters = {
        "fileName" : "sabana",
        "idCategory" : "0",
        "byPassAutomaticAnswer" : "false",
        "answertime" : "true",
        "asc" : "false",
        "idUser" : "0",
        "header" : f"""id_caso,operador,Operador_cierre,id_usuario,servicio,nombre,menu,submenu,nombre,email,compra,epa_respuesta,template,pedido,evento_inicio,fecha_inicio,hora_inicio,evento_cierre,fecha_cierre,hora_cierre,duracion,categoria,tiempo_1ra_respuesta,ts_1er_mensaje,tiempo_en_cola_espera,tiempo_de_espera_cliente,tot_mensajes_operador,tot_mensajes_cliente,total_mensajes,comentarios,poll_answer_time,ans1,ans2,ans3,ans4,ans5,ans6,ans7,comments""",
        "login" : "admin",
        "ri" : exec_date_rf_b,
        "rf" : exec_date_rf_e
    }

    try:
        result = requests.get(url, params=parameters, headers=aux_h)
        return result
    except Exception as e:
        print(f"Ocurrió un error: {e}")
        return None
    

def _from_api_to_postgres(ts):
    from io import StringIO
    import pandas as pd
    import sqlalchemy

    results_api = _api_onemarketer(ts)

    text_file = StringIO(results_api.text)
    df = pd.read_csv(text_file, sep=";", index_col=False)
    print(df)
    df["id_caso"] = df["id_caso"].fillna(0)
    df["id_caso"] = df["id_caso"].astype("int", errors="ignore")
    df["id_caso"] = df["id_caso"].replace(0, None)
    df["fecha_inicio"] = pd.to_datetime(df["fecha_inicio"], format="%d-%m-%Y")
    df["fecha_cierre"] = pd.to_datetime(df["fecha_inicio"], format="%d-%m-%Y")
    df_operador = df[df["operador"] != "robot"]
    df_operador = df_operador[["id_caso", "operador", "id_usuario", "servicio", "evento_inicio", "fecha_inicio", "hora_inicio", "evento_cierre", "fecha_cierre", "hora_cierre", "duracion", "tiempo_1ra_respuesta", "tiempo_en_cola_espera", "tiempo_de_espera_cliente", "tot_mensajes_operador", "tot_mensajes_cliente", "total_mensajes"]]
    df_robot = df[df["operador"] == "robot"]
    df_robot = df_robot[df_robot["template"].isna()]
    df_robot = df_robot[["id_caso", "operador", "id_usuario", "servicio", "menu", "submenu", "compra", "evento_inicio", "fecha_inicio", "hora_inicio", "evento_cierre", "fecha_cierre", "hora_cierre", "duracion"]]
    df_encuesta = df[["id_caso", "ans1", "ans2", "ans3", "ans4", "ans5", "ans6", "ans7", "comments"]]
    df_encuesta = df_encuesta.dropna(subset=['id_caso'])

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")

    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    exec_date = datetime.strptime(ts[:16], "%Y-%m-%dT%H:%M")
    exec_date_b = exec_date - timedelta(minutes=30)
    exec_date_e = exec_date + timedelta(minutes=30)
    exec_date_b = exec_date_b.strftime("%Y-%m-%dT%H:%M")
    exec_date_e = exec_date_e.strftime("%Y-%m-%dT%H:%M")

    with engine.begin() as conn:
        conn.execute(f"""
                delete
                from ecommdata.onemarketer_operador
                where fecha_inicio + hora_inicio between '{exec_date_b}' and '{exec_date_e}'))
                     """)
        df_operador.to_sql(name="onemarketer_operador",
                    con=conn,         
                    schema="ecommdata",         
                    if_exists='append',         
                    index=False,         
                    chunksize=20000,         
                    method='multi')
    print("operador DONE")
    with engine.begin() as conn:
        conn.execute(f"""
                delete
                from ecommdata.onemarketer_robot
                where fecha_inicio + hora_inicio between '{exec_date_b}' and '{exec_date_e}'))
                     """)
        df_robot.to_sql(name="onemarketer_robot",
                    con=conn,         
                    schema="ecommdata",         
                    if_exists='append',         
                    index=False,         
                    chunksize=20000,         
                    method='multi')
    
    print("robot DONE")
    with engine.begin() as conn:
        conn.execute(f"""
                delete
                from ecommdata.onemarketer_encuesta oe
                where (oe.id_caso in (select id_caso
                    from ecommdata.onemarketer_operador
                    where fecha_inicio + hora_inicio between '{exec_date_b}' and '{exec_date_e}'))
                or (oe.id_caso in (select id_caso
                    from ecommdata.onemarketer_robot
                    where fecha_inicio + hora_inicio between '{exec_date_b}' and '{exec_date_e}'))
                     """)
        df_encuesta.to_sql(name="onemarketer_encuesta",
                    con=conn,         
                    schema="ecommdata",         
                    if_exists='append',         
                    index=False,         
                    chunksize=20000,         
                    method='multi')
    print("encuesta DONE")
    return


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_onemarketer',
    default_args=default_args,
    description="utiliza la API de onemarketer para extraer el historial de conversaciones",
    schedule_interval= "0,30 * * * *",
    start_date=pendulum.datetime(2024, 8, 26, tz="America/Santiago"),
    catchup=True,
    max_active_runs = 1,
    tags=["onemarketer", "MATIAS"],
) as dag:

    dag.doc_md = """
    utiliza la API de onemarketer para extraer el historial de conversaciones
    """ 

    t0 = PythonOperator(
        task_id = "from_api_to_postgres",
        python_callable = _from_api_to_postgres,
    )

    t0
