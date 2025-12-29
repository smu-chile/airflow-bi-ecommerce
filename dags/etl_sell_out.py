from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

from utils.bigquery_utils import bq_query_to_df
from utils.slack_utils import dag_success_slack, dag_failure_slack

import pendulum

from datetime import datetime, timedelta

def render_netezza_view(ds):
    
    sql_str= f"""
    select *
        from `cl-cda-prod.DS_CDA_BI_USR.FACT_CUBO_ECOMMERCE_PRINCIPAL` 
        where DATE(SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', FECHA_CREACION_VTEX)) >= date_sub(cast('{ds}' as date), interval 1 day)
        and DATE(SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', FECHA_CREACION_VTEX)) < cast('{ds}' as date)
        """
    print(sql_str)

    df = bq_query_to_df(sql_str)
    
    return df


def sell_out_to_s3(ds):
    import pandas as pd
    import numpy as np
    import io
    from io import StringIO

    print("comenzando S3")
    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"sell_out_/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    df = render_netezza_view(ds)

    print("Todo bien hasta acá en la extracción de DWC")

    # Cambiando columnas a minusculas
    df.columns = df.columns.str.lower()

    df = df[['desc_organizacion',
    'id_centro',
    'desc_centro',
    'canal_wf',
    'fecha_vta',
    'fecha_creacion_vtex',
    'numtrx_vta',
    'nro_cotiza',
    'id_proveedor',
    'id_wf',
    'desc_promo_wf',
    'id_cod_cat',
    'des_cod_cat',
    'id_grupo_articulo',
    'desc_grupo_articulo',
    'id_seccion',
    'id_negocio',
    'id_linea',
    'desc_linea',
    'desc_negocio',
    'cod_mat',
    'des_mat',
    'ean',
    'umv',
    'umv_cnt',
    'marca',
    'tipo_promo',
    'tipo_doc',
    'unid_vta_promo',
    'unid_vtex',
    'venta_bruta',
    'venta_neta',
    'gasto_sellout']]
    
    print("\nHasta acá todo bien al filtrar las columnas :D\n")
    
    df.columns = ['desc_organizacion',
    'id_tienda',
    'desc_centro',
    'canal_wf',
    'fecha_vta',
    'fecha_creacion_vtex',
    'numtrx_vta',
    'nro_cotiza',
    'id_proveedor',
    'id_wf',
    'desc_promo_wf',
    'id_cod_cat',
    'des_cod_cat',
    'id_grupo_articulo',
    'desc_grupo_articulo',
    'id_seccion',
    'id_negocio',
    'id_linea',
    'desc_linea',
    'desc_negocio',
    'material',
    'des_mat',
    'ean',
    'umv',
    'umv_cnt',
    'marca',
    'tipo_promo',
    'tipo_doc',
    'unid_vta_promo',
    'unid_vtex',
    'venta_bruta',
    'venta_neta',
    'gasto_sellout']

    print("\nHasta acá todo bien renombrando las columnas :D\n")

    print(df.info())

    buffer = io.StringIO()
    df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"sell_out_/{exec_date}/sell_out_{date_aux}.csv"
    buffer.seek(0)
    print("se logro transformar el dataframe a un archivo .csv")
    print(f"con fecha {ds} y nombre de filename como {filename}")
    s3_hook.load_string(buffer.getvalue(),
                key=filename,
                bucket_name=s3_bucket,
                replace=True,
                encrypt=False)
    
    print(f"File load on S3: {prefix}")

    return filename

def sell_out_to_postgresql(ti):
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text

    filename = ti.xcom_pull(key="return_value", task_ids=["sell_out_to_s3"])[0]
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+filename)
    if not s3_hook.check_for_key(filename, bucket_name=s3_bucket):
        raise Exception(f"Key {filename} does not exist.")

    s_stock_object = s3_hook.get_key(filename, bucket_name=s3_bucket)
    df = pd.read_csv(s_stock_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return

    print(f"Filas extraídas: {len(df)}")

    df["umv_cnt"] = df.apply(lambda r: r["umv_cnt"]/1000 if r["umv"] in ["KG","KGV"] else r["umv_cnt"], axis=1)
    df["material"] = df["material"].apply(lambda x: str(x).zfill(18))
    df["id_tienda"] = df["id_tienda"].apply(lambda x: str(x).zfill(4))

    key_cols = [
        "numtrx_vta", 
        "id_tienda", 
        "material", 
        "ean", 
        "nro_cotiza",
        "id_wf",
        "desc_promo_wf",
        "tipo_promo",
        "tipo_doc"
    ]
    
    # Nos aseguramos de no tener duplicados internos en el CSV (nos quedamos con el último reproceso)
    df = df.drop_duplicates(subset=key_cols, keep="last")
    print(f"Filas a insertar tras limpieza interna: {len(df)}")

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    engine = sqlalchemy.create_engine(f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}")

    # Preparar strings para SQL dinámico
    all_cols = [f'"{c}"' for c in df.columns]
    cols_csv_str = ", ".join(all_cols)
    
    cols_con_nulls = ["nro_cotiza", "id_wf", "desc_promo_wf", "tipo_promo"]

    where_parts = []
    for c in key_cols:
        if c in cols_con_nulls:
            # Para manejar los [NULL]
            where_parts.append(f't."{c}" IS NOT DISTINCT FROM s."{c}"')
        else:
            # Esto permite que Postgres use el INDEX SCAN de forma directa
            where_parts.append(f't."{c}" = s."{c}"')
    
    where_key = " AND ".join(where_parts)

    #Upsert (Stage -> Merge)
    with engine.begin() as conn:
        # Tabla temporal
        conn.execute(text('CREATE TEMP TABLE staging_sell_out (LIKE catalogo.sell_out) ON COMMIT DROP;'))

        # Insertar datos nuevos a temporal
        df.to_sql(
            name="staging_sell_out",
            con=conn,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=10000
        )

        # BORRAR POR LLAVE: Elimina cualquier versión vieja de estas mismas transacciones
        del_sql = f"""
            DELETE FROM catalogo.sell_out t
            USING staging_sell_out s
            WHERE {where_key};
        """
        res = conn.execute(text(del_sql))
        print(f"Registros actualizados/reemplazados en DB: {res.rowcount}")

        # INSERTAR la versión nueva
        ins_sql = f"""
            INSERT INTO catalogo.sell_out ({cols_csv_str})
            SELECT {cols_csv_str}
            FROM staging_sell_out;
        """
        conn.execute(text(ins_sql))

    print("Carga final OK.")
    

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_sell_out',
    default_args=default_args,
    description="cargar tabla sell_out",
    schedule_interval= "0 11 * * *",
    start_date=pendulum.datetime(2023, 10, 9, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["DATA", "postgres", "ecommdata", "sell_out", "S3", "PATRICIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:
    

    dag.doc_md = """
    Extrae tabla sell_out de dwC, lo carga a S3 y postgresql en un intervalo de 1 dias por fecha creacion. \n
    Insert diario 11 am.
    """ 

    t0 = PythonOperator(
        task_id = "sell_out_to_s3",
        python_callable = sell_out_to_s3,
    )

    t1 = PythonOperator(
        task_id = "sell_out_to_postgresql",
        python_callable = sell_out_to_postgresql,
    )


    t0 >> t1