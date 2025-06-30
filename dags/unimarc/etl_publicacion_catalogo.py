from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.dummy import DummyOperator

from datetime import datetime, timedelta

import pendulum

def _check_time(ts):
    
    exec_datetime = datetime.strptime(ts[:16], "%Y-%m-%dT%H:%M")
    exec_datetime_utc = pendulum.timezone("utc").convert(exec_datetime)
    local_tz = pendulum.timezone("America/Santiago")
    exec_datetime_local = local_tz.convert(exec_datetime_utc)
    exec_datetime_local_str = exec_datetime_local.strftime("%Y-%m-%dT%H:%M")
    print(exec_datetime_local_str)

    time_str = exec_datetime_local_str.split("T")[1]
    if (time_str == "20:00") or (time_str == "01:00"):
        return "task_skip"
    else:
        return "load_table_publicacion_catalogo"

def _store_periodic_data(ts):
    import boto3, tempfile, os
    import pandas as pd

    dt_string = ts[:16]
    curr_datetime = dt_string.replace('T',' ')
    curr_dt_object = datetime.strptime(curr_datetime, "%Y-%m-%d %H:%M")
    past_dt_object = curr_dt_object - timedelta(weeks = 2)
    past_datetime = past_dt_object.strftime("%Y/%d/%m/%H%M")
    prefix = "ecommdata/publicacion_catalogo/"+past_datetime
    file_name = prefix+"publicacion_catalogo_periodico.csv"
    #Obtener datos de la tabla publicacion_catalogo
    select_query = f"""
        select *
        from ecommdata.publicacion_catalogo pc
        where pc.fecha_hora < '{ts}'::timestamp - interval '14 days' and pc.fecha_hora::time <> '12:00:00'
    """
    print(select_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    # ── escribimos CSV en “trocitos” dentro de un archivo temporal ────────
    chunksize = 100000
    total_size = 0
    with tempfile.NamedTemporaryFile("w+", delete=False, suffix=".csv") as tmp:
        for i, chunk in enumerate(
                pd.read_sql_query(select_query, pg_connection, chunksize=chunksize)
        ):
            # header solo en el primer chunk
            total_size += len(chunk)
            chunk.to_csv(tmp, header=(i == 0), index=False, encoding="utf-8")
            tmp.flush()                      # fuerza escritura al disco
            print(f"chunk {i+1}: {len(chunk)} filas")

        tmp.seek(0)                          # vuelve al inicio pa’ el upload

        # ── upload a S3 (sin re-cargar todo a RAM) ────────────────────────
        access_key = Variable.get("AWS_ACCESS_KEY")
        secret_key = Variable.get("AWS_SECRET_KEY")
        bucket_name = Variable.get("AWS_S3_BUCKET_NAME")
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="us-east-1",
        )
        s3_client.upload_file(tmp.name, bucket_name, file_name)
        print(f"Subido a s3://{bucket_name}/{file_name}")
    print(f"Total de filas procesadas y subidas: {total_size}")
    os.remove(tmp.name)  # limpia el .csv local
    return

def _delete_periodic_data(ts):

    delete_query = f"""
        delete
        from ecommdata.publicacion_catalogo pc
        where pc.fecha_hora < '{ts}'::timestamp - interval '14 days' and pc.fecha_hora::time <> '12:00:00'
    """

    print(delete_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(delete_query)
    pg_connection.commit()
    cursor.close()

    return

def _store_daily_data(ts):
    from io import StringIO
    import boto3
    import pandas as pd

    dt_string = ts[:16]
    curr_datetime = dt_string.replace('T',' ')
    curr_dt_object = datetime.strptime(curr_datetime, "%Y-%m-%d %H:%M")
    past_dt_object = curr_dt_object - timedelta(weeks = 2)
    past_datetime = past_dt_object.strftime("%Y/%d/%m/%H%M")
    prefix = "ecommdata/publicacion_catalogo/"+past_datetime
    file_name = prefix+"publicacion_catalogo_diario.csv"

    select_query = f"""
        select *
        from ecommdata.publicacion_catalogo pc
        where pc.fecha_hora < '{ts}'::timestamp - interval '28 days'
    """
    print(select_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    df = pd.read_sql_query(select_query, pg_connection)

    buffer = StringIO()
    df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    buffer.seek(0)

    access_key = Variable.get("AWS_ACCESS_KEY")
    secret_key = Variable.get("AWS_SECRET_KEY")
    bucket_name = Variable.get("AWS_S3_BUCKET_NAME")
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name = "us-east-1"
    )
    response = s3_client.put_object(
        Bucket=bucket_name, Key=file_name, Body=buffer.getvalue()
    )

    return

def _delete_daily_data(ts, ds):

    delete_date = macros.ds_add(ds, -28)
    print(delete_date)

    delete_date_split = delete_date.split("-")
    part_year = delete_date_split[0]
    part_month = delete_date_split[1]
    part_day = delete_date_split[2]
    partition_name = f"publicacion_catalogo_y{part_year}m{part_month}d{part_day}"
    
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()

    partition_exists_query = f"""
        select exists(
            select * 
            from information_schema.tables 
            where table_name='{partition_name}'
        );
    """

    cursor.execute(partition_exists_query)
    partition_exists = cursor.fetchone()[0]

    if partition_exists:
        drop_query = f"""
            DROP TABLE ecommdata.{partition_name};
        """
        print(drop_query)
        cursor.execute(drop_query)

    else:
        delete_query = f"""
            delete
            from ecommdata.publicacion_catalogo pc
            where pc.fecha_hora < '{ts}'::timestamp - interval '28 days'
        """

        print(delete_query)
        cursor.execute(delete_query)
    pg_connection.commit()
    cursor.close()

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_publicacion_catalogo',
    default_args=default_args,
    description="Carga de tabla publicacion catalogo",
    schedule_interval="0 1,4/4 * * *",
    start_date=pendulum.datetime(2022, 10, 12, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["DATA", "publicacion_catalogo", "ecommdata", "unimarc", "MATIAS"],
) as dag:

    dag.doc_md = """
    Carga de tabla publicacion_catalogo. El resultado final queda en ecommdata.
    """ 
    t0 = ExternalTaskSensor(
        task_id="wait_for_stock",
        external_dag_id='etl_stock_incremental_load',
        external_task_id=None,
        allowed_states=['success'],
        failed_states=['failed']
    )

    t1 = BranchPythonOperator(
        task_id='check_time',
        python_callable=_check_time,
    )
    
    t_dummy = DummyOperator(
            task_id='task_skip',
        )

    t2 = PostgresOperator(
        task_id = "load_table_publicacion_catalogo",
        postgres_conn_id="postgresql_conn",
        sql="sql/publicacion_catalogo.sql",
    )

    t3 = PostgresOperator(
        task_id = "load_table_publicacion_dia_tienda_surtido",
        postgres_conn_id="postgresql_conn",
        sql="sql/publicacion_dia_tienda_surtido.sql",
    )

    t4 = PostgresOperator(
        task_id = "load_table_publicacion_dia_tienda_evento",
        postgres_conn_id="postgresql_conn",
        sql="sql/publicacion_dia_tienda_evento.sql",
    )

    t5 = PostgresOperator(
        task_id = "load_table_publicacion_dia_tienda_infaltable",
        postgres_conn_id="postgresql_conn",
        sql="sql/publicacion_dia_tienda_infaltable.sql",
    )

    t6 = PostgresOperator(
        task_id = "load_table_publicacion_dia_tienda_top_300",
        postgres_conn_id="postgresql_conn",
        sql="sql/publicacion_dia_tienda_top_300.sql",
    )

    t7 = PostgresOperator(
        task_id = "load_table_publicacion_dia_tienda_surtido_y_con_marca",
        postgres_conn_id="postgresql_conn",
        sql="sql/publicacion_dia_tienda_surtido_y_con_marca.sql",
    )

    t8 = PostgresOperator(
        task_id = "publicacion_dia_tienda_surtido_marca_proveedor",
        postgres_conn_id="postgresql_conn",
        sql="sql/publicacion_dia_tienda_surtido_marca_proveedor.sql",
    )

    t9 = PythonOperator(
        task_id = "store_periodic_data",
        python_callable = _store_periodic_data
    )

    t10 = PythonOperator(
        task_id = "delete_periodic_data",
        python_callable = _delete_periodic_data
    )

    t11 = PythonOperator(
        task_id = "store_daily_data",
        python_callable = _store_daily_data
    )

    t12 = PythonOperator(
        task_id = "delete_daily_data",
        python_callable = _delete_daily_data
    )

    t0 >> t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9 >> t10 >> t11 >> t12
    t1 >> t_dummy
