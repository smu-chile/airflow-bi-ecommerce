from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
import pandas as pd
import sqlalchemy
import pendulum

from utils.janis_utils import _execute_mariadb_query
from utils.slack_utils import dag_success_slack, dag_failure_slack

def get_postgres_engine():
    """Helper para crear la conexión a PostgreSQL a partir de Variables de Airflow."""
    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    return sqlalchemy.create_engine(conn_url)

def load_operadores_logisticos_to_postgres():
    """
    Extrae información de compañías logísticas desde Janis Jackie (MariaDB),
    aplica transformaciones para separar operador de picking y despacho,
    y recarga la dimensión en ecommdata.operadores_logisticos_dim en PostgreSQL.
    """
    query = """
        SELECT 
            id AS id_compania_logistica,
            ref_id AS ref_id_compania,
            name AS nombre_compania_logistica,
            status AS estado,
            TRIM(SUBSTRING_INDEX(name, ' + ', 1)) AS operador_picking,
            TRIM(SUBSTRING_INDEX(name, ' + ', -1)) AS operador_despacho,
            FROM_UNIXTIME(date_created) AS fecha_creacion,
            FROM_UNIXTIME(date_modified) AS fecha_modificacion
        FROM janis_jackie.wms_logistic_companies
    """

    print("Extrayendo datos de operadores logísticos desde MariaDB (Janis Jackie)...")
    results, columns = _execute_mariadb_query(query)
    df = pd.DataFrame(results, columns=columns)
    print(f"Total registros extraídos desde MariaDB: {len(df.index)}")

    # Asegurar tipos de datos adecuados
    df["id_compania_logistica"] = pd.to_numeric(df["id_compania_logistica"], errors="coerce").astype("Int64")
    df["ref_id_compania"] = df["ref_id_compania"].astype("string")
    df["nombre_compania_logistica"] = df["nombre_compania_logistica"].astype("string")
    df["estado"] = pd.to_numeric(df["estado"], errors="coerce").astype("Int64")
    df["operador_picking"] = df["operador_picking"].astype("string")
    df["operador_despacho"] = df["operador_despacho"].astype("string")
    df["fecha_creacion"] = pd.to_datetime(df["fecha_creacion"], errors="coerce")
    df["fecha_modificacion"] = pd.to_datetime(df["fecha_modificacion"], errors="coerce")

    engine = get_postgres_engine()

    table_name = "ecommdata.operadores_logisticos_dim"
    schema_name = table_name.split(".")[0]
    table_str = table_name.split(".")[-1]

    print(f"Cargando datos en PostgreSQL ({table_name})...")
    with engine.begin() as conn:
        # Truncar e insertar dentro de la misma transacción atómica
        conn.execute(sqlalchemy.text(f"TRUNCATE TABLE {table_name};"))
        df.to_sql(
            name=table_str,
            con=conn,
            schema=schema_name,
            if_exists="append",
            index=False,
            method="multi"
        )

    print("Carga en PostgreSQL completada exitosamente.")

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0
}

with DAG(
    dag_id="etl_operadores_logisticos_dim",
    default_args=default_args,
    description="Extracción diaria de compañías logísticas desde Janis y carga a ecommdata.operadores_logisticos_dim",
    schedule_interval="0 22 * * *",  # Todos los días a las 22:00 hrs
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Santiago"),
    catchup=False,
    tags=["janis", "logistica", "operadores_logisticos_dim", "ecommdata"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    ### ETL Operadores Logísticos Dim
    Extracción diaria a las 22:00 hrs desde la tabla `janis_jackie.wms_logistic_companies` (MariaDB).
    Transforma el campo `name` para extraer `operador_picking` y `operador_despacho`.
    Recarga totalmente la tabla destino `ecommdata.operadores_logisticos_dim` en PostgreSQL.
    """

    t0 = PythonOperator(
        task_id="cargar_operadores_logisticos_dim",
        python_callable=load_operadores_logisticos_to_postgres,
    )

    t0
