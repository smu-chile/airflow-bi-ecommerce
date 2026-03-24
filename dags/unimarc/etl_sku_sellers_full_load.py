from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import pandas as pd
import sqlalchemy
from sqlalchemy import text
import pendulum
from datetime import datetime
from utils.janis_utils import _execute_mariadb_query
from utils.slack_utils import dag_success_slack, dag_failure_slack

def load_sku_sellers_to_postgres():
    print("Iniciando carga de sku_sellers a PostgreSQL, En Taro Adun!")
    sku_sellers_query = """
        SELECT sku, seller_id
        FROM janis_jackie.sku_sellers
    """
    
    # Ejecutamos la query en MariaDB
    results, columns = _execute_mariadb_query(sku_sellers_query)
    df_sku_sellers = pd.DataFrame(results, columns=columns)
    df_sku_sellers.columns = ['sku', 'seller_id']
    print(f"Total extraídos: {len(df_sku_sellers)}. We got the plans!")

    # Conexión a PostgreSQL usando las Variables configuradas
    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    # 3. Truncar e insertar datos
    table_name = "ecommdata.sku_sellers"  # Ubicacion de la tabla en postgres
    
    with engine.begin() as conn:
        conn.execute(text(f'TRUNCATE TABLE {table_name};'))
        
        # Insertar data nueva
        df_sku_sellers.to_sql(
            name=table_name.split('.')[-1],
            con=engine,
            schema=table_name.split('.')[0],
            if_exists="append", # Usamos append porque hicimos TRUNCATE
            index=False,
            method="multi"
        )
    print("Carga a PostgreSQL exitosa. Adelante COM-TAC!")

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
    dag_id="etl_sku_sellers_full_load",    # Identificador del proceso
    default_args=default_args,
    start_date=pendulum.datetime(2026, 3, 23, tz="America/Santiago"),
    schedule_interval="30 8 * * *",          # Cron: todos los días a las 08:30
    catchup=False,
    tags=["janis", "sellers", "postgres", "MIGUEL"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:
    
    dag.doc_md = "Extración de data referente a sku_sellers desde mariaDB (Janis) para inyectarlo hacia un data warehouse de PostgreSQL"
    
    # Llamo a la función load_sku_sellers_to_postgres
    t0 = PythonOperator(
        task_id="ext_and_load_sku_sellers",
        python_callable=load_sku_sellers_to_postgres, 
    )
    
    t0
