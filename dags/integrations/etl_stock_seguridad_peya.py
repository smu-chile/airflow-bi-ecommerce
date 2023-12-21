from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator

import pendulum

def _check_time(ts):
    from datetime import datetime, timedelta

    exec_datetime = datetime.strptime(ts[:16], "%Y-%m-%dT%H:%M")
    exec_datetime_utc = pendulum.timezone("utc").convert(exec_datetime)
    local_tz = pendulum.timezone("America/Santiago")
    exec_datetime_local = local_tz.convert(exec_datetime_utc)
    exec_datetime_local_str = exec_datetime_local.strftime("%Y-%m-%dT%H:%M")
    print(exec_datetime_local_str)

    time_str = exec_datetime_local_str.split("T")[1]
    if (time_str == "17:00") or (time_str == "21:00") or (time_str == "01:00") or (time_str == "13:00"):
        return "task_skip"
    elif (time_str == "05:00"):
        return "stock_ventas_tiendas_to_s3_am"
    else:
        return "stock_ventas_tiendas_to_s3_pm"

def venta_tienda():
    import pandas as pd
    ventas_skus_tienda_query = """select date_part('dow',fecha) as dia,
                        date_part('week',fecha) as semana,
                        lpad(id_tienda,4,'0') as id_tienda,
                        lpad(material,18,'0') as material , 
                        umv, 
                        sum(venta_umv) as venta_umv
                        from ecommdata.venta_sku_tienda vst
                        where id_tienda in (select ltrim(id,'0')
                            from integraciones.tiendas_last_millers tlm 
                            where (id_peya is not null 
                            or id_peya_botilleria is not null
                            or peya_market is not null))
                        group by fecha, id_tienda, material, umv"""
    print(ventas_skus_tienda_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(ventas_skus_tienda_query)
    results = cursor.fetchall()
    results = pd.DataFrame(results)
    results.columns = ["dia","semana","id_tienda","umv","venta_umv"]
    cursor.close()
    pg_connection.close()
    return results

def venta_tienda():
    import pandas as pd
    ventas_skus_tienda_query = """select date_part('dow',fecha) as dia,
                        date_part('week',fecha) as semana,
                        lpad(id_tienda,4,'0') as id_tienda,
                        lpad(material,18,'0') as material , 
                        umv, 
                        sum(venta_umv) as venta_umv
                        from ecommdata.venta_sku_tienda vst
                        where id_tienda in (select ltrim(id,'0')
                            from integraciones.tiendas_last_millers tlm 
                            where (id_peya is not null 
                            or id_peya_botilleria is not null
                            or peya_market is not null))
                        group by fecha, id_tienda, material, umv"""
    print(ventas_skus_tienda_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(ventas_skus_tienda_query)
    results = cursor.fetchall()
    results = pd.DataFrame(results)
    results.columns = ["dia","semana","id_tienda","umv","venta_umv"]
    cursor.close()
    pg_connection.close()
    return results


def stock_ventas_tiendas_to_s3_am(ds):
    import pandas as pd
    import numpy as np
    import io
    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"stock_seguridad_peya/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("cargando venta\n")
    df_ventas = venta_tienda()
    print("venta_tienda cargada")

    buffer = io.StringIO()
    df_final.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"stock_seguridad_peya/{exec_date}/stock_seguridad_peya_pm_{date_aux}.csv"
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


def stock_ventas_tiendas_to_s3_pm():
    print("Carga s3 de la tarde")
    return

def carga_stock_seguridad_janis_am():
    print("carga janis de la mañana")
    return

def carga_stock_seguridad_janis_pm():
    print("carga janis de la tarde")
    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_stock_seguridad_peya',
    default_args=default_args,
    description="cargar stock de seguridad peya",
    schedule_interval="0 1/4 * * *",
    start_date=pendulum.datetime(2023, 6, 12, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "Janis", "integraciones", "stock", "stock_seguridad", "ventas", "peya"],
) as dag:
    

    dag.doc_md = """
    Carga stock de seguridad alvi \n
    guardar en S3.
    """ 
    t0 = BranchPythonOperator(
        task_id='check_time',
        python_callable=_check_time,
    )

    t_dummy = DummyOperator(
            task_id='task_skip',
        )

    t1_am = PythonOperator(
        task_id = "stock_ventas_tiendas_to_s3_am",
        python_callable = stock_ventas_tiendas_to_s3_am,
    )

    t1_pm = PythonOperator(
        task_id = "stock_ventas_tiendas_to_s3_pm",
        python_callable = stock_ventas_tiendas_to_s3_pm,
    )

    t2_am = PythonOperator(
        task_id = "carga_stock_seguridad_janis_am",
        python_callable = carga_stock_seguridad_janis_am
    )

    t2_pm = PythonOperator(
        task_id = "carga_stock_seguridad_janis_pm",
        python_callable = carga_stock_seguridad_janis_pm
    )

    t0 >> t1_am >> t2_am
    t0 >> t1_pm >> t2_pm
    t0 >> t_dummy