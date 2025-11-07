from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from bigquery_utils import bq_query_to_df

import pendulum
from datetime import datetime, timedelta

# =================================================================
# FUNCIONES PYTHON
# =================================================================

def render_bigquery_data():
    """
    Se conecta a BigQuery, ejecuta la query y devuelve los resultados 
    como un DataFrame de Pandas.
    """
    
    # Query para BigQuery (ya adaptada con el prefijo de proyecto/dataset/tabla)
    sql_str = """
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
        FECHA_INICIO_DE_PROMOCION,
        FECHA_FIN_DE_PROMOCION,
        ultima_carga,
        ORGANIZACION_VENTAS,
        ROW_NUMBER() OVER (
          PARTITION BY MATERIAL 
          ORDER BY FECHA_INICIO_DE_PROMOCION DESC
        ) AS rn
      FROM `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_WORKFLOW`
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
      actual.FECHA_INICIO_DE_PROMOCION,
      actual.FECHA_FIN_DE_PROMOCION,
      anterior.FECHA_INICIO_DE_PROMOCION AS FECHA_INICIO_ANTERIOR,
      anterior.FECHA_FIN_DE_PROMOCION AS FECHA_FIN_ANTERIOR,
      actual.ORGANIZACION_VENTAS
    FROM DatosConRank AS actual
    LEFT JOIN DatosConRank AS anterior
      ON actual.MATERIAL = anterior.MATERIAL
      AND actual.rn = 1
      AND anterior.rn = 2
    WHERE actual.ultima_carga = 'X'
      AND (
        actual.FECHA_INICIO_DE_PROMOCION <> anterior.FECHA_INICIO_DE_PROMOCION
        OR actual.FECHA_FIN_DE_PROMOCION <> anterior.FECHA_FIN_DE_PROMOCION
      )
      AND actual.ORGANIZACION_VENTAS IN ('1000', '7500');
    """
    
    print("Iniciando conexión a BigQuery y ejecución de query.")

    df = bq_query_to_df(sql_str)

    column_order = ['N_PROMOCION','NOMBRE_PROMOCION','CANAL_DISTRIBUCION','ID_EVENTO',
                    'DESCRIPCION_EVENTO_PROMOCIONAL','ID_MECANICA','DESCRIPCION_MECANICA',
                    'MATERIAL','DESC_MATERIAL','UN_MEDIDA_VENTA','EAN','PRECIO_MODAL','PRECIO_MODAL_TOTAL',
                    'PRECIO_PROMOCIONAL','PRECIO_TOTAL_PROMOCIONAL','FECHA_INICIO_DE_PROMOCION',
                    'FECHA_FIN_DE_PROMOCION','FECHA_INICIO_ANTERIOR','FECHA_FIN_ANTERIOR','ORGANIZACION_VENTAS']
                    
    df = df[column_order]
    
    print(f"Total de registros extraídos: {len(df.index)}")
    return df


def promos_to_postgresql(ti):
    """
    Recibe el DataFrame de XCom desde la tarea anterior y lo carga 
    directamente a PostgreSQL.
    """
    import sqlalchemy

    # Obtener el DataFrame desde la tarea 'render_bigquery_data'
    df = ti.xcom_pull(key="return_value", task_ids=["render_bigquery_data"])[0] 
    
    if df.empty:
        print("No hay registros para cargar. Tarea finalizada.")
        return
    
    print(f"Número de registros a cargar: {len(df.index)}")

    # Obtención de credenciales de PostgreSQL (variables de Airflow)
    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    # Carga a PostgreSQL
    with engine.begin() as conn:
        df.to_sql(name="promociones_comparadas",
                    con=conn,         
                    schema="ecommdata",         
                    if_exists='append',         
                    index=False,         
                    chunksize=20000,         
                    method='multi')

    print("✅ Datos guardados en PostgreSQL.")

    return


def truncate_table():
    import sqlalchemy
    """
    Lógica para truncar la tabla de PostgreSQL.
    """

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")

    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    connection = engine.connect()
    truncate_query = "TRUNCATE TABLE ecommdata.promociones_comparadas"
    connection.execute(truncate_query)
    connection.close()

    print("✅ Tabla 'ecommdata.promociones_comparadas' truncada con éxito.")

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'elt_cargar_promociones_comparadas',
    default_args=default_args,
    description='Extrae promociones comparadas desde BigQuery y las carga en la base de datos PostgreSQL.',
    schedule_interval='0 9 * * *',
    start_date=pendulum.datetime(2024, 5, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["DATA", "postgres", "ecommdata", "Promociones_comparadas", "BIGQUERY","NICOLAS"]
) as dag:

    dag.doc_md = """
        # ELT: Carga de Promociones Comparadas (BigQuery a PostgreSQL)
        
        **Flujo:**
        1. Trunca la tabla 'ecommdata.promociones_comparadas' en PostgreSQL.
        2. Ejecuta una query compleja en BigQuery para comparar las promociones y extrae los datos modificados.
        3. Carga los resultados (DataFrame) directamente en la tabla de PostgreSQL.
        """ 
    
    t0 = PythonOperator(
        task_id='truncate_table',
        python_callable=truncate_table
    )
    
    t1 = PythonOperator(
        task_id='render_bigquery_data', 
        python_callable=render_bigquery_data 
    )
    
    t2 = PythonOperator(
        task_id='promos_to_postgresql', 
        python_callable=promos_to_postgresql
    )

    t0 >> t1 >> t2