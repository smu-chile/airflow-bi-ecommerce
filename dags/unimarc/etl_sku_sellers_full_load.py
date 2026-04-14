from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import pandas as pd
import sqlalchemy
import pendulum
from utils.janis_utils import _execute_mariadb_query
from utils.slack_utils import dag_success_slack, dag_failure_slack

def get_postgres_engine():
    """Helper para crear la conexión a PostgreSQL a partir de Variables configuradas."""
    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    return sqlalchemy.create_engine(conn_url)

def load_sku_sellers_to_postgres():
    sku_sellers_query = """
        SELECT sku, seller
        FROM janis_jackie.sku_sellers
    """
    
    # Ejecutamos la query en MariaDB
    print("Iniciando carga de sku_sellers a PostgreSQL, En Taro Adun!")
    results, columns = _execute_mariadb_query(sku_sellers_query)
    df_sku_sellers = pd.DataFrame(results, columns=columns)
    df_sku_sellers.columns = ['sku', 'seller']
    print(f"Total extraídos: {len(df_sku_sellers)}. We got the plans!")

    engine = get_postgres_engine()

    # 3. Truncar e insertar datos
    table_name = "ecommdata.sku_sellers"  # Ubicacion de la tabla en postgres
    schema_name = table_name.split('.')[0]
    table_str = table_name.split('.')[-1]
    
    from sqlalchemy import inspect
    insp = inspect(engine)
    
    with engine.begin() as conn:
        # Solo truncamos si la tabla ya existe (para evitar falla de UndefinedTable en la primera carga)
        # Usamos get_table_names() para asegurar compatibilidad con todas las versiones de SQLAlchemy
        if table_str in insp.get_table_names(schema=schema_name):
            conn.execute(sqlalchemy.text(f'TRUNCATE TABLE {table_name};'))
        
        # Insertar data nueva
        df_sku_sellers.to_sql(
            name=table_str,
            con=conn,
            schema=schema_name,
            if_exists="append", # Usamos append, o crea la tabla automáticamente si no existe gracias a pandas
            index=False,
            method="multi"
        )
    print("Carga a PostgreSQL exitosa. Adelante COM-TAC!")

def verificar_sellers_en_lista8():
    engine = get_postgres_engine()

    # Lógica enviada completamente al motor de base de datos de PostgreSQL
    # mediante una única transacción que crea la tabla, la limpia y cruza la data en memoria del motor.
    sql_query = """
        DROP TABLE IF EXISTS ecommdata.lista8_con_sellers;
        CREATE TABLE ecommdata.lista8_con_sellers (
            ref_id TEXT, 
            sellers INTEGER,
            updatePending INTEGER
        );
        
        INSERT INTO ecommdata.lista8_con_sellers (ref_id, sellers, updatePending)
        SELECT 
            l.material || '-' || l.umv AS ref_id,
            1 AS sellers,
            1 AS updatePending
        FROM ecommdata.lista8 l
        JOIN (
            -- Obtenemos los materiales base únicos que en sku_sellers tienen seller distinto a 1
            SELECT DISTINCT SPLIT_PART(CAST(s.ref_id AS TEXT), '-', 1) AS material_limpio
            FROM ecommdata.sku_sellers ss
            JOIN ecommdata.skus s ON ss.sku = s.id
            WHERE ss.seller <> 1
        ) base_sellers ON CAST(l.material AS TEXT) = base_sellers.material_limpio;
    """

    print("Ejecutando cruce integral de lista8 y sellers en base de datos PostgreSQL...")
    with engine.begin() as conn:
        conn.execute(sqlalchemy.text(sql_query))
        
    print("Guardado exitoso en ecommdata.lista8_con_sellers usando SQL nativo. Listos para despegar!")


# Argumentos base
default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0
}  

# Construcción de las tareas del Pipeline
with DAG(
    dag_id="etl_sku_sellers_to_postgres",    # Identificador del proceso
    default_args=default_args,
    start_date=pendulum.datetime(2026, 4, 14, tz="America/Santiago"),
    schedule_interval="30 8 * * *",          # Cron: todos los días a las 08:30
    catchup=False,
    tags=["janis", "sellers", "postgres"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:
    
    dag.doc_md = "Extración de data referente a sku_sellers desde mariaDB (Janis) para inyectarlo hacia un data warehouse de PostgreSQL"
    
    # Llamo a la función load_sku_sellers_to_postgres
    t0 = PythonOperator(
        task_id="ext_and_load_sku_sellers",
        python_callable=load_sku_sellers_to_postgres, 
    )
    
    t1 = PythonOperator(
        task_id="verificar_sellers_en_lista8",
        python_callable=verificar_sellers_en_lista8, 
    )
    
    t0 >> t1

