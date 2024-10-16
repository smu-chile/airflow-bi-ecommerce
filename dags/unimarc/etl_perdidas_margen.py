from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pendulum

def _load_to_postgres(ti,ds):
    import pandas as pd
    import numpy as np
    import os

    curr_working_directory = os.getcwd()
    print("Current working directory:", curr_working_directory)

    with open(curr_working_directory + f"/dags/unimarc/sql/perdidas_margen.sql", "r") as query_file:
        perdidas_margen_query = query_file.read()

    perdidas_margen_query = perdidas_margen_query.replace("{ds}", ds)

    print("Base query:")
    print(perdidas_margen_query)

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()

    df_margen = pd.read_sql_query(perdidas_margen_query, pg_connection)
    df_margen.info()

    df_margen = df_margen[[
        "fecha",
        "descuento_colaborador",
        "descuento_referido",
        "descuento_club_ahorro",
        "descuento_cupon_reclamo",
        "descuento_diamante",
        "descuento_platino",
        "descuento_oro",
        "descuento_unipay",
        "perdida_sustitucion",
        "venta_total_neta_sin_descuentos",
        "venta_total_neta",
        "desfase_sellout"
    ]]


    df_margen['descuento_colaborador'] = df_margen['descuento_colaborador'].astype(int)
    df_margen['descuento_referido'] = df_margen['descuento_referido'].astype(int)
    df_margen['descuento_club_ahorro'] = df_margen['descuento_club_ahorro'].astype(int)
    df_margen['descuento_cupon_reclamo'] = df_margen['descuento_cupon_reclamo'].astype(int)
    df_margen['descuento_diamante'] = df_margen['descuento_diamante'].astype(int)
    df_margen['descuento_platino'] = df_margen['descuento_platino'].astype(int)
    df_margen['descuento_oro'] = df_margen['descuento_oro'].astype(int)
    df_margen['descuento_unipay'] = df_margen['descuento_unipay'].astype(int)
    df_margen['perdida_sustitucion'] = df_margen['perdida_sustitucion'].astype(int)
    df_margen['venta_total_neta_sin_descuentos'] = df_margen['venta_total_neta_sin_descuentos'].astype(int)
    df_margen['venta_total_neta'] = df_margen['venta_total_neta'].astype(int)
    df_margen['desfase_sellout'] = df_margen['desfase_sellout'].astype(int)

    columns = [
        "descuento_colaborador",
        "descuento_referido",
        "descuento_club_ahorro",
        "descuento_cupon_reclamo",
        "descuento_diamante",
        "descuento_platino",
        "descuento_oro",
        "descuento_unipay",
        "perdida_sustitucion",
        "venta_total_neta_sin_descuentos",
        "venta_total_neta",
        "desfase_sellout"
    ]

    columns_query = ",".join(columns)
    excluded_query = ",".join(["EXCLUDED."+column for column in columns])
    values_query = "%s,"+",".join(["%s" for column in columns])
    df_margen = df_margen.fillna("NULL")
    records = list(df_margen.to_records(index=False))
    
    # Change data types to native python types
    fixed_records = []
    for record in records:
        fixed_record = []
        for value in record:
            if isinstance(value, np.generic):
                fixed_record.append(value.item())
            elif value == "NULL":
                fixed_record.append(None)
            else:
                fixed_record.append(value)
        fixed_records.append(tuple(fixed_record))
    print(f"Number of records to load: {str(len(fixed_records))}")
    incremental_query = """
        INSERT INTO ventas_unimarc.costos_ventas_sku_tienda (fecha,"""+columns_query+""") 
        VALUES ("""+values_query+""")
        ON CONFLICT (fecha)
        DO UPDATE SET ("""+columns_query+""") = ("""+excluded_query+""") 
    """
    print(incremental_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.executemany(incremental_query, fixed_records)
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    print("Data loaded to Postgres")

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'elt_perdidas_margen',
    default_args=default_args,
    description="Calculo de costos de margen a nivel ordenes",
    schedule_interval="0 4 * * *",
    start_date=pendulum.datetime(2023, 6, 6, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["JANIS", "ordenes", "margen", "promocion", "SERGIO"],
) as dag:

    dag.doc_md = """
    Calculo de costos de margen a nivel ordenes.
    """

    t0 = PythonOperator(
        task_id="_load_to_postgres",
        python_callable=_load_to_postgres
    )

    t0