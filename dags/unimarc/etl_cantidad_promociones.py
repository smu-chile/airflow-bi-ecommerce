from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime, timedelta

import pendulum

def load_cantidad_promociones_to_s3(ds):
    import pandas as pd
    import numpy as np
    import io
    from io import StringIO

    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"cantidad_promociones/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    df_promos = pd.DataFrame()

    
    cantidad_promociones_query = f"""SELECT
            gs::date AS dia,
            COUNT(wp.material) AS cantidad_promociones_activas,
            wp.id_mecanica,
            wp.descripcion_mecanica,
            wp.canal_distribucion
        FROM
            generate_series(
                '{ds}'::date,
                '{ds}'::date + interval '21 days',
                interval '1 day'
            ) gs
        JOIN
            ecommdata.workflow_promociones wp ON gs::date BETWEEN wp.fecha_inicio_de_promocion AND wp.fecha_fin_de_promocion
            AND wp.id_mecanica <> ALL (ARRAY[36, 99, 84, 12, 37, 51, 93, 53, 96, 77, 59])
        GROUP BY
            gs::date, wp.id_mecanica, wp.descripcion_mecanica, wp.canal_distribucion
        ORDER BY
            gs::date, wp.id_mecanica, wp.descripcion_mecanica, wp.canal_distribucion;"""
    print(cantidad_promociones_query)

    cursor.execute(cantidad_promociones_query)
    results = cursor.fetchall()
    columns_name = [i[0] for i in cursor.description]

    df_temp = pd.DataFrame(results, columns=columns_name)
    df_promos = pd.concat([df_promos, df_temp], ignore_index=True)

    cursor.close()
    pg_connection.close()

    buffer = io.StringIO()
    df_promos.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"cantidad_promociones/{exec_date}/cantidad_promociones_{date_aux}.csv"
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

def load_cantidad_promociones_to_postgres(ti):
    import pandas as pd
    import numpy as np
    import sqlalchemy

    cantidad_promociones_file = ti.xcom_pull(key="return_value", task_ids=["load_cantidad_promociones_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+cantidad_promociones_file)
    if not s3_hook.check_for_key(cantidad_promociones_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % cantidad_promociones_file)

    limit_object = s3_hook.get_key(cantidad_promociones_file, bucket_name=s3_bucket)

    df = pd.read_csv(limit_object.get()["Body"])

    columns = ["cantidad_promociones_activas", "descripcion_mecanica"]

    columns_query = ",".join(columns)
    values_query = "%s,"+",".join(["%s" for column in columns])
    df = df.fillna("NULL")
    records = list(df.to_records(index=False))
    
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
        INSERT INTO ecommdata.cantidad_promociones_diarias (dia,id_mecanica,"""+columns_query+""",canal_distribucion) 
        VALUES ("""+values_query+""")
        ON CONFLICT (dia,id_mecanica,canal_distribucion)
        DO NOTHING; 
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
    'etl_cantidad_promociones',
    default_args=default_args,
    description="Extracción de datos de tabla workflow_promociones y posterior carga de cantidad de promociones diarias segmentadas por mecanica",
    schedule_interval="0 7 * * *",
    start_date=pendulum.datetime(2022, 8, 11, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "ecommdata", "cantidad_promociones", "Unimarc", "workflow_promociones"],
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla de productos_excluidos_por_tienda de Janis Unimarc a Workspace. \n
    """ 
    t0 = PythonOperator(
        task_id = "load_cantidad_promociones_to_s3",
        python_callable = load_cantidad_promociones_to_s3,
    )

    t1 = PythonOperator(
        task_id = "load_cantidad_promociones_to_postgres",
        python_callable = load_cantidad_promociones_to_postgres
    )

    t0 >> t1