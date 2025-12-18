from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.slack_utils import dag_success_slack, dag_failure_slack

from datetime import datetime, timedelta

import pendulum

def load_pedidos_prefactura_unimarc_to_postgres(ds):
    import pandas as pd
    import numpy as np
    import io
    import os
    import sqlalchemy
    from io import StringIO

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()

    curr_working_directory = os.getcwd()
    print(os.getcwd())

    with open(curr_working_directory+f"/dags/unimarc/sql/pedidos_prefactura_unimarc.sql", "r") as query_file:
        pedidos_prefactura_unimarc_query = query_file.read()
    
    pedidos_prefactura_unimarc_query = pedidos_prefactura_unimarc_query.replace("{ds}", ds)

    print("Base query:")
    print(pedidos_prefactura_unimarc_query)

    df_pedidos_prefactura_unimarc_query= pd.read_sql_query(pedidos_prefactura_unimarc_query, pg_connection)
    
    print(f"Number of records extracted: {len(df_pedidos_prefactura_unimarc_query.index)}")
    df_pedidos_prefactura_unimarc_query.info()

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    with engine.begin() as conn:
        conn.execute(f"delete from forecast_and_planning.pedidos_prefactura_unimarc where fecha_entrega = '{ds}'::date;")
        df_pedidos_prefactura_unimarc_query.to_sql(name="pedidos_prefactura_unimarc",
                    con=conn,         
                    schema="forecast_and_planning",         
                    if_exists='append',         
                    index=False,         
                    chunksize=20000,         
                    method='multi')

    print("Data saved to PostgreSQL.")

    return

def load_estimacion_costo_armado_to_postgres(ds):
    import pandas as pd
    import numpy as np
    import io
    import os
    import sqlalchemy
    from io import StringIO

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()

    curr_working_directory = os.getcwd()
    print(os.getcwd())

    with open(curr_working_directory+f"/dags/unimarc/sql/estimacion_costo_armado.sql", "r") as query_file:
        estimacion_costo_armado_query = query_file.read()
    
    estimacion_costo_armado_query = estimacion_costo_armado_query.replace("{ds}", ds)

    print("Base query:")
    print(estimacion_costo_armado_query)

    df_estimacion_costo_armado = pd.read_sql_query(estimacion_costo_armado_query, pg_connection)
    
    print(f"Number of records extracted: {len(df_estimacion_costo_armado.index)}")
    df_estimacion_costo_armado.info()

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    with engine.begin() as conn:
        conn.execute(f"delete from forecast_and_planning.estimacion_costo_armado where fecha_entrega = '{ds}'::date;")
        df_estimacion_costo_armado.to_sql(name="estimacion_costo_armado",
                    con=conn,         
                    schema="forecast_and_planning",         
                    if_exists='append',         
                    index=False,         
                    chunksize=20000,         
                    method='multi')

    print("Data saved to PostgreSQL.")

    return

def load_estimacion_costo_asegurado_to_postgres(ds):
    import pandas as pd
    import numpy as np
    import io
    import os
    import sqlalchemy
    from io import StringIO

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()

    curr_working_directory = os.getcwd()
    print(os.getcwd())

    with open(curr_working_directory+f"/dags/unimarc/sql/estimacion_costo_asegurado.sql", "r") as query_file:
        estimacion_costo_asegurado_query = query_file.read()
    
    estimacion_costo_asegurado_query = estimacion_costo_asegurado_query.replace("{ds}", ds)

    print("Base query:")
    print(estimacion_costo_asegurado_query)

    df_estimacion_costo_asegurado = pd.read_sql_query(estimacion_costo_asegurado_query, pg_connection)
    
    print(f"Number of records extracted: {len(df_estimacion_costo_asegurado.index)}")
    df_estimacion_costo_asegurado.info()

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    with engine.begin() as conn:
        conn.execute(f"delete from forecast_and_planning.estimacion_costo_asegurado where fecha_entrega = '{ds}'::date;")
        df_estimacion_costo_asegurado.to_sql(name="estimacion_costo_asegurado",
                    con=conn,         
                    schema="forecast_and_planning",         
                    if_exists='append',         
                    index=False,         
                    chunksize=20000,         
                    method='multi')

    print("Data saved to PostgreSQL.")

    return

def load_estimacion_gastos_adicionales_to_postgres(ds):
    import pandas as pd
    import numpy as np
    import io
    import os
    import sqlalchemy
    from io import StringIO

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()

    curr_working_directory = os.getcwd()
    print(os.getcwd())

    with open(curr_working_directory+f"/dags/unimarc/sql/estimacion_gastos_adicionales.sql", "r") as query_file:
        estimacion_gastos_adicionales_query = query_file.read()

    print("Base query:")
    print(estimacion_gastos_adicionales_query)

    df_estimacion_gastos_adicionales = pd.read_sql_query(estimacion_gastos_adicionales_query, pg_connection)
    
    print(f"Number of records extracted: {len(df_estimacion_gastos_adicionales.index)}")
    df_estimacion_gastos_adicionales.info()

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    with engine.begin() as conn:
        conn.execute(f"delete from forecast_and_planning.estimacion_gastos_adicionales where fecha_entrega = '{ds}'::date;")
        df_estimacion_gastos_adicionales.to_sql(name="estimacion_gastos_adicionales",
                    con=conn,         
                    schema="forecast_and_planning",         
                    if_exists='append',         
                    index=False,         
                    chunksize=20000,         
                    method='multi')

    print("Data saved to PostgreSQL.")

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_estimacion_costos_logisticos',
    default_args=default_args,
    description="carga diaria de tablas para calcular costos logisticos",
    schedule_interval="0 7 * * *",
    start_date=pendulum.datetime(2024, 12, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["DATA", "ecommdata", "costos", "Unimarc", "forecast_and_planning", "PATRICIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Carga diaria tablas para determinar costos logisticos\n
    """ 

    t0 = PythonOperator(
        task_id = "load_pedidos_prefactura_unimarc_to_postgres",
        python_callable = load_pedidos_prefactura_unimarc_to_postgres,
    )

    t1 = PythonOperator(
        task_id = "load_estimacion_costo_armado_to_postgres",
        python_callable = load_estimacion_costo_armado_to_postgres,
    )

    t2 = PythonOperator(
        task_id = "load_estimacion_costo_asegurado_to_postgres",
        python_callable = load_estimacion_costo_asegurado_to_postgres,
    )

    t3 = PythonOperator(
        task_id = "load_estimacion_gastos_adicionales_to_postgres",
        python_callable = load_estimacion_gastos_adicionales_to_postgres,
    )

    t0 >> t1 >> t2 >> t3