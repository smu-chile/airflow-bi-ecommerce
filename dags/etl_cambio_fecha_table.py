from sqlalchemy.engine import create_engine
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum
import pandas as pd
import sqlalchemy
from sqlalchemy import text

def _execute_promociones_comparadas(ti):
    # Obtener las credenciales de conexión a la base de datos
    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    # Conectar a la base de datos
    connection = engine.connect()

    # Ejecutar TRUNCATE para vaciar la tabla promociones_comparadas
    truncate_query = "TRUNCATE TABLE ecommdata.promociones_comparadas"
    connection.execute(text(truncate_query))
    connection.close()

    # La query para cargar las promociones comparadas
    query = """
    WITH DatosConRank AS (
        SELECT 
            MATERIAL,
            N_PROMOCION,
            NOMBRE_PROMOCION,
            ID_EVENTO,
            DESCRIPCION_EVENTO_PROMOCIONAL,
            ID_MECANICA,
            DESCRIPCION_MECANICA,
            DESC_MATERIAL,
            UN_MEDIDA_VENTA,
            EAN,
            PRECIO_MODAL,
            PRECIO_MODAL_TOTAL,
            PRECIO_PROMOCIONAL,
            PRECIO_TOTAL_PROMOCIONAL,
            CANAL_DISTRIBUCION,
            fecha_inicio_de_promocion,
            fecha_fin_de_promocion,
            ultima_carga,
            ROW_NUMBER() OVER (PARTITION BY MATERIAL ORDER BY fecha_inicio_de_promocion DESC) AS rn
        FROM DWC_SMU.SMU.VW_FACT_WORKFLOW
    )
    SELECT 
        actual.N_PROMOCION,
        actual.NOMBRE_PROMOCION,
        actual.CANAL_DISTRIBUCION,
        actual.ID_EVENTO,
        actual.DESCRIPCION_EVENTO_PROMOCIONAL,
        actual.ID_MECANICA,
        actual.DESCRIPCION_MECANICA,
        actual.MATERIAL,
        actual.DESC_MATERIAL,
        actual.UN_MEDIDA_VENTA,
        actual.EAN,
        actual.PRECIO_MODAL,
        actual.PRECIO_MODAL_TOTAL,
        actual.PRECIO_PROMOCIONAL,
        actual.PRECIO_TOTAL_PROMOCIONAL,
        actual.fecha_inicio_de_promocion ,
        actual.fecha_fin_de_promocion, 
        anterior.fecha_inicio_de_promocion AS fecha_inicio_anterior, 
        anterior.fecha_fin_de_promocion AS fecha_fin_anterior
    FROM DatosConRank actual
    LEFT JOIN DatosConRank anterior 
        ON actual.MATERIAL = anterior.MATERIAL 
        AND actual.rn = 1 
        AND anterior.rn = 2
    WHERE actual.ultima_carga = 'X'  
    AND (actual.fecha_inicio_de_promocion <> anterior.fecha_inicio_de_promocion 
         OR actual.fecha_fin_de_promocion <> anterior.fecha_fin_de_promocion);
    """
    
    # Ejecutar la query
    df = pd.read_sql(query, engine)

    # Guardar en PostgreSQL en la tabla ecommdata.promociones_comparadas
    df.to_sql(name="promociones_comparadas",
              con=engine,
              schema="ecommdata",
              if_exists="append",
              index=False,
              chunksize=20000,
              method="multi")
    
    print("Data loaded into promociones_comparadas table in PostgreSQL.")

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0
}

with DAG(
    'promociones_comparadas_etl',
    default_args=default_args,
    description="ETL process to load promociones_comparadas to PostgreSQL",
    schedule_interval="0 9 * * *",  # Ejecutar a las 9 AM cada día
    start_date=pendulum.datetime(2021, 1, 1, tz="America/Santiago"),
    catchup=False,
    tags=["ETL", "POSTGRES", "DATA"]
) as dag:

    dag.doc_md = """
    Este DAG carga las promociones comparadas desde la base de datos a la tabla promociones_comparadas en PostgreSQL.
    """

    t1 = PythonOperator(
        task_id="load_promociones_comparadas_to_postgres",
        python_callable=_execute_promociones_comparadas
    )

    t1
