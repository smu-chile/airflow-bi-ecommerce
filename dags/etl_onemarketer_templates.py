from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
import pendulum
from datetime import datetime, timedelta
from io import StringIO

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from utils.slack_utils import dag_success_slack, dag_failure_slack


def _extract_from_api(ts, **context):
    import requests
    import pandas as pd

    print("Inicio Extracción Onemarketer Templates")

    # ==========================
    # Calculo de fechas (Ventana de 1 hora exacta)
    # ==========================
    # ts es el logical_date (inicio del periodo en Airflow)
    exec_date = datetime.strptime(ts[:16], "%Y-%m-%dT%H:%M")
    exec_datetime_utc = pendulum.instance(exec_date).in_tz("UTC")
    local_tz = pendulum.timezone("America/Santiago")
    exec_datetime_local = exec_datetime_utc.in_timezone(local_tz)

    # Definimos la ventana: desde 1 hora antes hasta la ejecución
    exec_date_rf_b = exec_datetime_local.subtract(hours=1).strftime("%Y-%m-%d %H:%M")
    exec_date_rf_e = exec_datetime_local.strftime("%Y-%m-%d %H:%M")

    print(f"Ventana de extracción: {exec_date_rf_b} a {exec_date_rf_e}")

    # ==========================
    # Llamada API
    # ==========================
    url = Variable.get("url_onemarketer")

    parameters = {
        "fileName": "templates",
        "login": "admin",
        "ri": exec_date_rf_b,
        "rf": exec_date_rf_e,
        "header": "UserId,Status,Template,Channel,Time,Origin,Comment"
    }

    headers = {
        "Connection": "keep-alive",
        "Accept-Encoding": "gzip, deflate, br"
    }

    response = requests.get(
        url,
        params=parameters,
        headers=headers,
        timeout=60
    )

    if response.status_code != 200:
        raise Exception(f"Error API Onemarketer: {response.status_code}")

    if not response.text or "UserId;Status" not in response.text:
        print("No hay datos nuevos o la respuesta no tiene el formato esperado.")
        return "no_data_skip"

    # ==========================
    # Parse & Transform CSV
    # ==========================
    df = pd.read_csv(StringIO(response.text), sep=";")
    
    if df.empty:
        print("DataFrame vacío.")
        return "no_data_skip"

    df.columns = df.columns.str.lower()
    df = df.rename(columns={"userid": "user_id"})
    df["user_id"] = df["user_id"].astype(str)
    df["comment"] = df["comment"].fillna("")
    df["time"] = pd.to_datetime(df["time"], format="%Y-%m-%d %H:%M:%S", errors="coerce")
    df = df.dropna(subset=["time"])
    df = df.drop_duplicates(subset=["user_id", "template", "time"])

    print(f"Filas procesadas: {len(df)}")

    # Guardar en XCom para la siguiente tarea
    context['ti'].xcom_push(key='templates_data', value=df.to_json(date_format='iso'))
    
    return "load_to_postgres"


def _load_to_postgres(**context):
    import pandas as pd
    import psycopg2
    from io import StringIO

    data_json = context['ti'].xcom_pull(key='templates_data', task_ids='extract_from_api')
    if not data_json:
        print("No se recibió data desde XCom.")
        return

    df = pd.read_json(StringIO(data_json))

    # ==========================
    # Conexion Postgres
    # ==========================
    conn = psycopg2.connect(
        host=Variable.get("POSTGRESQL_HOST"),
        database=Variable.get("POSTGRESQL_DB"),
        user=Variable.get("POSTGRESQL_USER"),
        password=Variable.get("POSTGRESQL_PASSWORD")
    )
    cursor = conn.cursor()

    # ==========================
    # Idempotencia con Tabla Temporal y ON CONFLICT
    # ==========================
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    # 1. Crear tabla temporal
    cursor.execute("""
        CREATE TEMP TABLE tmp_onemarketer_templates (
            user_id TEXT,
            status TEXT,
            template TEXT,
            channel TEXT,
            time TIMESTAMP,
            origin TEXT,
            comment TEXT
        ) ON COMMIT DROP;
    """)

    # 2. Carga rápida a tabla temporal
    cursor.copy_expert(
        """
        COPY tmp_onemarketer_templates
        (user_id, status, template, channel, time, origin, comment)
        FROM STDIN WITH CSV
        """,
        buffer
    )

    # 3. Upsert (Insertar solo lo que no existe según el ux_onemarketer_templates)
    cursor.execute("""
        INSERT INTO ecommdata.onemarketer_templates 
        (user_id, status, template, channel, time, origin, comment)
        SELECT user_id, status, template, channel, time, origin, comment 
        FROM tmp_onemarketer_templates
        ON CONFLICT (user_id, template, time) DO NOTHING;
    """)

    conn.commit()
    cursor.close()
    conn.close()

    print("Carga completada con éxito (Idempotente)")


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    "etl_onemarketer_templates",
    default_args=default_args,
    description="extrae templates desde la API de onemarketer (Mejorado)",
    schedule_interval="0 * * * *",
    start_date=pendulum.datetime(2024, 8, 26, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["onemarketer", "templates", "MAURICIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    ETL que extrae templates enviados desde la API de Onemarketer
    y los almacena en ecommdata.onemarketer_templates.
    
    Mejoras aplicadas:
    - Ventana de 1 hora fija.
    - Idempotencia via ON CONFLICT DO NOTHING.
    - Ramificación (Branching) para evitar errores cuando no hay datos.
    """

    t1 = BranchPythonOperator(
        task_id="extract_from_api",
        python_callable=_extract_from_api,
    )

    t2 = PythonOperator(
        task_id="load_to_postgres",
        python_callable=_load_to_postgres,
    )

    t3 = DummyOperator(
        task_id="no_data_skip",
    )

    t1 >> [t2, t3]
0