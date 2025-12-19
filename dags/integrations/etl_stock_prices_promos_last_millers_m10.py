from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from utils.slack_utils import dag_success_slack, dag_failure_slack

import pendulum

def query_to_df(query):
    import pandas as pd
    print(query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query)
    column_names = [desc[0] for desc in cursor.description]
    results = cursor.fetchall()
    results = pd.DataFrame(results, columns=column_names)
    print(results.head(20))
    cursor.close()
    pg_connection.close()
    return results

def last_millers_m10_to_s3(ds):
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text
    import io
    from io import StringIO

    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"last_millers_m10/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    try:
        query = f"""
            select  
            s.id_tienda,
            s2.ean_primario::varchar as ean,
            s.material, 
            s.umv as unidad_de_medida, 
            s2.multiplicador_unidad_medida  as multiplicador_unidad,
            s.descripcion_producto as nombre,
            m.nombre as marca,
            s.stock as stock_unitario,
            pm.precio as precio,
            w.promo as precio_promocional
            from ecommdata_m10.stock s
            left join ecommdata_m10.productos p 
            on concat(p.material, '-', replace(p.unidad_medida, 'ST','UN')) = concat(s.material,'-',s.umv)
            left join (select lpad(pm.codigo_material::varchar,18,'0') as material, pm.umv,max(pm.precio_modal) as precio
                        from ecommdata_m10.precio_modal pm 
                        group by lpad(pm.codigo_material::varchar,18,'0'), pm.umv) as pm
            on concat(pm.material, '-', pm.umv) = concat(s.material,'-',s.umv)
            left join (select lpad(material::varchar(18),18,'0') as material,
                        REPLACE(un_medida_venta, 'ST', 'UN') as umv,
                        min(precio_promocional) as promo
                        from ecommdata_m10.workflow w
                        where fecha_inicio_de_promocion <= current_date +1
                        and fecha_fin_de_promocion >= current_date +1
                        and organizacion_ventas = '3000'
                        and desc_promocion in  ('PRECIO FIJO' , '% DE DESCUENTO')
                        and(nombre_promocion LIKE '%CICLO%'
                            or nombre_promocion LIKE '%PUNTA DE PRECIO%'
                            or nombre_promocion LIKE '%PERECIBLES%'
                            or nombre_promocion LIKE '%LOS ELE%'
                            or nombre_promocion LIKE '%LAS 10 AL CHANCHO%')
                        group by lpad(material::varchar(18),18,'0'),REPLACE(un_medida_venta, 'ST', 'UN')) as w
            on concat(w.material, '-', w.umv) = concat(s.material,'-',s.umv)
            left join ecommdata.skus s2 
            on s2.ref_id = concat(s.material,'-',s.umv)
            left join ecommdata.productos p2 
            on concat(s.material,'-',s.umv) = p2.ref_id 
            left join ecommdata.marcas m 
            on p2.id_marca = m.id 
            where s2.ean_primario is not null
            and s.fecha_carga = (select max(fecha_carga) from ecommdata_m10.stock)
            and pm.precio is not null
            and s.stock > 0
            and s.bloqueos is not true
            and m.nombre is not null
            and s.id_tienda in ('3512','3552','3540','3227','3580','3546','3547','3564','3579','3570','3036');
            """
        df = query_to_df(query)
        print(f"informacion obtenida de la Query: {df.info()}")

        buffer = io.StringIO()
        df.to_csv(buffer, header=True, index=False, encoding="utf-8")
        filename = f"last_millers_m10/{exec_date}/last_millers_m10_{date_aux}.csv"
        buffer.seek(0)
        print("se logro transformar el dataframe a un archivo .csv")
        print(f"con fecha {ds} y nombre de filename como {filename}")
        s3_hook.load_string(buffer.getvalue(),
                    key=filename,
                    bucket_name=s3_bucket,
                    replace=True,
                    encrypt=False)
    
        print(f"File load on S3: {filename}")
        return "last_millers_m10_to_postgres"

    except Exception as err:
        print(f"error: {err}")
        return "fallo_last_millers_m10_to_s3"
    
def last_millers_m10_to_postgres(ds):
    print('\n carga de productos sap a postgresql')
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text

    BASE_S3_PATH = "last_millers_m10/"
    curr_datetime = ds.replace("-", "_")
    exec_date = ds.replace("-", "/")
    prefix = BASE_S3_PATH+exec_date+"/"

    filename = f"{prefix}last_millers_m10_{curr_datetime}.csv"
    print(filename)
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+filename)
    if not s3_hook.check_for_key(filename, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % filename)

    s_stock_object = s3_hook.get_key(filename, bucket_name=s3_bucket)

    df = pd.read_csv(s_stock_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")
    df['material'] = df['material'].apply(lambda x: str(x).zfill(18))
    df['id_tienda'] = df['id_tienda'].apply(lambda x: str(x).zfill(4))
    df = df[df['ean'].astype(str).str.isnumeric()]
    # Convertir a int
    df['ean'] = df['ean'].astype(int)
    df.info()
    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    # Save to PostgreSQL:
    with engine.begin() as conn:
        conn.execute("TRUNCATE integraciones.lm_stock_precio_promo_10") 
        df.to_sql(name="lm_stock_precio_promo_10",
                    con=conn,         
                    schema="integraciones",         
                    if_exists='append',         
                    index=False,         
                    chunksize=20000,         
                    method='multi')

    print("Data loaded to Postgres: integraciones.lm_stock_precio_promo_10")
    return


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_stock_prices_promos_last_millers_10',
    default_args=default_args,
    description="cargar stock,precios y promos a la tabla lss_millers_promos_m10",
    schedule_interval="30 12 * * *",
    start_date=pendulum.datetime(2024, 6, 1, tz="America/Santiago"),
    catchup=False,
    tags=["DATA","last_millers","M10","integraciones","stock","prices","promos","PATRICIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:
    

    dag.doc_md = """
    cargar stock,precios y promos a la tabla lss_millers_promos de M10\n
    guardar en S3 y postgresql.
    """ 
    t_dummy = DummyOperator(
        task_id='fallo_last_millers_m10_to_s3',
    )

    t0  = BranchPythonOperator(
        task_id = "last_millers_m10_to_s3",
        python_callable = last_millers_m10_to_s3
    )

    t1  = PythonOperator(
        task_id = "last_millers_m10_to_postgres",
        python_callable = last_millers_m10_to_postgres
    )
    t2 = TriggerDagRunOperator(
        task_id="proc_rappi_stock_integration_m10",
        trigger_dag_id="proc_rappi_stock_integration_m10",
        wait_for_completion=False
    )

    t0 >> t1 >> t2
    t0 >> t_dummy
