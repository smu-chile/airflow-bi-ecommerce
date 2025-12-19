from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from utils.janis_utils import _execute_mariadb_query
from datetime import datetime
import pandas as pd
import sqlalchemy

from utils.slack_utils import dag_success_slack, dag_failure_slack

import pendulum

def load_offsets_to_postgres():
    # 1. Ejecutar la query en MariaDB
    query = """
    WITH latest_offsets AS (
      SELECT 
        wldo.carrier,
        wldo.delivery_range,
        wldo.offset,
        FROM_UNIXTIME(wldo.date_created) AS date_created,
        FROM_UNIXTIME(wldo.date_modified) AS date_modified,
        ROW_NUMBER() OVER (
          PARTITION BY wldo.carrier, wldo.delivery_range
          ORDER BY GREATEST(wldo.date_created, wldo.date_modified) DESC
        ) AS rn
      FROM janis_jackie.wms_logistic_delivery_offset wldo
    )
    SELECT 
      wlc.name AS nombre_transportadora,
      wlc.ref_id AS id_transportadora,
      wldr.`day`,
      wldr.time_start AS time_start,
      wldr.time_end AS time_end,
      wldr.price,
      lo.offset,
      lo.date_created,
      lo.date_modified
    FROM janis_jackie.wms_logistic_delivery_ranges wldr
    JOIN janis_jackie.wms_logistic_carriers wlc ON wldr.carrier = wlc.id
    LEFT JOIN latest_offsets lo ON lo.delivery_range = wldr.id AND lo.carrier = wlc.id AND lo.rn = 1
    WHERE wlc.status = 3
      AND wldr.status = 5;
    """

    results, columns = _execute_mariadb_query(query)
    df = pd.DataFrame(results, columns=columns)
    print(f"Number of records extracted: {len(df.index)}")

    # 2. Conexión a PostgreSQL usando Variables
    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")

    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    # 3. Truncar e insertar tabla (con begin/autocommit y método multi)
    table_name = "forecast_and_planning.offsets_janis"
    with engine.begin() as conn:
        conn.execute(f'TRUNCATE TABLE {table_name};')
        
    print(df.head())  

    df.to_sql(
        name=table_name.split('.')[-1],
        con=engine,
        schema=table_name.split('.')[0],
        if_exists="append",
        index=False,
        method="multi"  
    )

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0
}

with DAG(
    dag_id="etl_janis_offsets_to_postgres",
    default_args=default_args,
    start_date=pendulum.datetime(2022, 7, 10, tz="America/Santiago"),
    schedule_interval="30 8 * * *", # Ejecutar diariamente a las 08:30 AM
    catchup=False,
    tags=["janis", "logistica", "offsets", "forecast_and_planning", "KEVIN"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:
    dag.doc_md = """
    Extracción de offsets de Janis y carga a PostgreSQL. \n 
    Esta DAG se ejecuta diariamente a las 08:30 AM y trunca la tabla antes de insertar los nuevos datos.
    """ 

    t0 = PythonOperator(
        task_id="cargar_offsets_janis",
        python_callable=load_offsets_to_postgres,
    )

    t0
