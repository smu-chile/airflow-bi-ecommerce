from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from utils.slack_utils import dag_success_slack, dag_failure_slack

import pendulum
import pandas as pd
import mysql.connector
import os

def _get_base_catalog(ti, ds):
    """
    Task 1: Obtiene la información base del catálogo utilizando la consulta base_catalogo_last_millers.sql en PostgreSQL
    """
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    
    sql_file_path = "/opt/airflow/dags/integrations/sql/base_catalogo_last_millers.sql"
    if not os.path.exists(sql_file_path):
        sql_file_path = os.path.join(os.path.dirname(__file__), "sql", "base_catalogo_last_millers.sql")
        
    with open(sql_file_path, "r", encoding="utf-8") as f:
        sql_query = f.read()
        
    df_catalog = pg_hook.get_pandas_df(sql_query)
    print(f"Task 1 completada. Se obtuvieron {len(df_catalog)} registros del catálogo base desde PostgreSQL.")
    
    # Retornar como lista de diccionarios para mantener la integridad de tipos y ceros a la izquierda
    return df_catalog.to_dict(orient="records")


def _add_sku_dimensions_and_weight(ti):
    """
    Task 2: Obtiene dimensiones y peso (alto, largo, ancho, peso) desde MariaDB (tabla skus s) y realiza el cruce por ref_id (sku-umv)
    """
    records_catalog = ti.xcom_pull(task_ids="get_base_catalog")
    if not records_catalog:
        print("No se recibieron datos de Task 1.")
        return []

    df_catalog = pd.DataFrame(records_catalog)
    
    # Construcción de ref_id con y sin ceros a la izquierda para garantizar el match con MariaDB
    sku_str = df_catalog["sku"].astype(str).str.strip()
    umv_str = df_catalog["umv"].astype(str).str.strip()
    
    sku_padded = sku_str.str.zfill(18)
    sku_unpadded = sku_str.str.lstrip("0")
    
    df_catalog["ref_id"] = sku_padded + "-" + umv_str
    df_catalog["ref_id_clean"] = sku_unpadded + "-" + umv_str
    
    # Conexión a MariaDB
    conn = mysql.connector.connect(
        user=Variable.get("JANIS_MARIADB_USER"),
        password=Variable.get("JANIS_MARIADB_PASSWORD"),
        host=Variable.get("JANIS_MARIADB_HOST"),
        port=3306,
        database=Variable.get("JANIS_MARIADB_DATABASE")
    )
    
    mariadb_query = """
    SELECT 
        TRIM(s.ref_id) AS ref_id, 
        s.freight_height AS alto, 
        s.freight_length AS largo,
        s.freight_width AS ancho, 
        s.freight_weight AS peso 
    FROM skus s;
    """
    
    cur = conn.cursor()
    cur.execute(mariadb_query)
    results = cur.fetchall()
    columns = [i[0] for i in cur.description]
    cur.close()
    conn.close()
    
    df_dimensions = pd.DataFrame(results, columns=columns)
    df_dimensions["ref_id"] = df_dimensions["ref_id"].astype(str).str.strip()
    print(f"Task 2: Extraídos {len(df_dimensions)} SKUs desde MariaDB.")
    
    # Cruce 1: Match por ref_id exacto (18 dígitos)
    df_merged = pd.merge(df_catalog, df_dimensions, on="ref_id", how="left")
    
    # Cruce 2: Para los registros sin match, reintentar por ref_id sin ceros a la izquierda
    missing_mask = df_merged["peso"].isna() & df_merged["alto"].isna()
    if missing_mask.any():
        unmatched_count = missing_mask.sum()
        print(f"Reintentando coincidencia sin ceros a la izquierda para {unmatched_count} registros...")
        df_dimensions_clean = df_dimensions.rename(columns={"ref_id": "ref_id_clean"})
        df_unmatched = df_merged[missing_mask].drop(columns=["alto", "largo", "ancho", "peso"])
        df_repaired = pd.merge(df_unmatched, df_dimensions_clean, on="ref_id_clean", how="left")
        
        df_merged.loc[missing_mask, ["alto", "largo", "ancho", "peso"]] = df_repaired[["alto", "largo", "ancho", "peso"]].values

    # Eliminar columna auxiliar de cruce
    if "ref_id_clean" in df_merged.columns:
        df_merged.drop(columns=["ref_id_clean"], inplace=True)
        
    peso_valido_count = df_merged["peso"].notna().sum()
    print(f"Task 2 completada. Total registros: {len(df_merged)}. Registros con dimensiones/peso hallados: {peso_valido_count}.")
    
    return df_merged.to_dict(orient="records")


def _load_catalog_to_postgres(ti):
    """
    Task 3: Trunca la tabla integraciones.catalogo_last_millers y carga el DataFrame final en PostgreSQL
    """
    records_merged = ti.xcom_pull(task_ids="add_sku_dimensions_and_weight")
    if not records_merged:
        print("No se recibieron datos para insertar en Postgres.")
        return

    df_final = pd.DataFrame(records_merged)
    df_final.columns = map(str.lower, df_final.columns)
    
    # Formatear el código de material a 18 dígitos para mantener estándar SAP
    if "sku" in df_final.columns:
        df_final["sku"] = df_final["sku"].astype(str).str.strip().str.zfill(18)
        
    print(f"Task 3: Insertando {len(df_final)} registros a integraciones.catalogo_last_millers...")
    
    # Truncar tabla previa antes de la carga diaria
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute("TRUNCATE TABLE integraciones.catalogo_last_millers;")
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    
    # Cargar el DataFrame a Postgres utilizando SQLAlchemy engine explícito
    from sqlalchemy import create_engine
    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = create_engine(conn_url)

    df_final.to_sql(
        name="catalogo_last_millers",
        con=engine,
        schema="integraciones",
        if_exists="append",
        index=False,
        chunksize=5000,
        method="multi"
    )
    print(f"Task 3 completada con éxito. Total filas cargadas a integraciones.catalogo_last_millers: {len(df_final)}.")
    return


default_args = {
    "owner": "ecommerce_ops",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    "proc_catalogo_last_millers",
    default_args=default_args,
    description="Extracción del catálogo base de Last Millers (Postgres), enriquecimiento con peso/dimensiones (MariaDB) y carga a Postgres",
    schedule_interval="0 6 * * *",
    start_date=pendulum.datetime(2023, 2, 21, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    concurrency=2,
    tags=["OPS", "last_millers", "catalogo", "mariadb", "peso", "RODRIGO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    DAG de procesamiento del Catálogo Unificado para Last Millers. \n
    * **Task 1 (get_base_catalog)**: Obtiene los productos base desde `base_catalogo_last_millers.sql` en PostgreSQL. \n
    * **Task 2 (add_sku_dimensions_and_weight)**: Consulta dimensiones y peso (`alto`, `largo`, `ancho`, `peso`) desde **MariaDB** (`skus s`) y realiza el cruce por `ref_id` (`sku-umv`). \n
    * **Task 3 (load_catalog_to_postgres)**: Trunca la tabla `integraciones.catalogo_last_millers` en PostgreSQL y carga el catálogo unificado final.
    """

    t1 = PythonOperator(
        task_id="get_base_catalog",
        python_callable=_get_base_catalog
    )

    t2 = PythonOperator(
        task_id="add_sku_dimensions_and_weight",
        python_callable=_add_sku_dimensions_and_weight
    )

    t3 = PythonOperator(
        task_id="load_catalog_to_postgres",
        python_callable=_load_catalog_to_postgres
    )

    t1 >> t2 >> t3
