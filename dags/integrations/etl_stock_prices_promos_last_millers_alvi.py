from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule


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

def last_millers_alvi_to_s3(ds):
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text
    import io
    from io import StringIO

    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"last_millers_alvi/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    try:
        query = f"""WITH precios AS (
                        select p.ref_id, t.id as id_tienda, max(p.precio)
                        from ecommdata_alvi.precios p 
                        left join ecommdata_alvi.tiendas t 
                        on p.id_tienda_janis = t.id_janis 
                        where p.valido_desde <= current_date 
                        and p.valido_hasta >= current_date
                        and p.ref_id is not null
                        and p.cantidad_minima_sku = 1
                        and t.status = 1
                        and t.id_janis is not null
                        and t.id in ('3098','3092')
                        group by p.ref_id, t.id
                        )
                    select s.id_tienda,
                    s2.ean_primario as "ean",
                    s.material, 
                    split_part(s.ref_id,'-',2) as "unidad_de_medida",
                    s.multiplicador_unidad_medida as "multiplicador_unidad",
                    s.descripcion  as "nombre",
                    m.nombre as "marca",
                    s.stock_janis as stock_unitario,
                    p2.precio
                    from ecommdata_alvi.stock s
                    left join ecommdata_alvi.skus s2 
                    on s2.ref_id = s.ref_id 
                    left join ecommdata_alvi.productos p 
                    on p.ref_id = s.ref_id
                    left join ecommdata_alvi.marcas m 
                    on p.id_marca = m.id 
                    left join precios as p2
                    on p2.ref_id = s.ref_id  and p2.id_tienda = s.id_tienda 
                    where ultima_actualizacion = (select max(ultima_actualizacion) from ecommdata_alvi.stock)
                    and s.stock_janis > 0
                    and s.surtido_ecommerce is true 
                    and s.id_tienda is not null
                    and s.material is not null
                    and s.descripcion is not null 
                    and s.c1 not in ('No trabajar','Fizzmod Categoria')
                    and s2.ean_primario is not null
                    and m.nombre is not null
                    and s.id_tienda in ('3092','3098')--lista_tiendas
                    and p2.precio is not null"""
        df = query_to_df(query)
        print(f"informacion obtenida de la Query: {df.info()}")

        buffer = io.StringIO()
        df.to_csv(buffer, header=True, index=False, encoding="utf-8")
        filename = f"last_millers_alvi/{exec_date}/last_millers_alvi_{date_aux}.csv"
        buffer.seek(0)
        print("se logro transformar el dataframe a un archivo .csv")
        print(f"con fecha {ds} y nombre de filename como {filename}")
        s3_hook.load_string(buffer.getvalue(),
                    key=filename,
                    bucket_name=s3_bucket,
                    replace=True,
                    encrypt=False)
    
        print(f"File load on S3: {filename}")
        return "last_millers_alvi_to_postgres"

    except Exception as err:
        print(f"error: {err}")
        return "fallo_last_millers_alvi_to_s3"
    
def last_millers_alvi_to_postgres(ds):
    print('\n carga de productos sap a postgresql')
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text

    BASE_S3_PATH = "last_millers_alvi/"
    curr_datetime = ds.replace("-", "_")
    exec_date = ds.replace("-", "/")
    prefix = BASE_S3_PATH+exec_date+"/"

    filename = f"{prefix}last_millers_alvi_{curr_datetime}.csv"
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
    df['ean'] = df['ean'].apply(lambda x: int(x))
    df.info()
    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    # Save to PostgreSQL:
    with engine.begin() as conn:
        conn.execute("TRUNCATE integraciones.lm_stock_precio_promo_alvi") 
        df.to_sql(name="lm_stock_precio_promo_alvi",
                    con=conn,         
                    schema="integraciones",         
                    if_exists='append',         
                    index=False,         
                    chunksize=20000,         
                    method='multi')

    print("Data loaded to Postgres: integraciones.lm_stock_precio_promo_alvi")
    return


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_stock_prices_promos_last_millers_alvi',
    default_args=default_args,
    description="cargar stock,precios y promos a la tabla lss_millers_promos",
    schedule_interval="30 9,13,17,21 * * *",
    start_date=pendulum.datetime(2023, 6, 12, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "last_millers", "integraciones", "stock", "prices", "promos","PATRICIO"],
) as dag:
    

    dag.doc_md = """
    cargar stock,precios y promos a la tabla lss_millers_promos de Alvi\n
    guardar en S3 y postgresql.
    """ 
    t_dummy = DummyOperator(
        task_id='fallo_last_millers_alvi_to_s3',
    )

    t0  = BranchPythonOperator(
        task_id = "last_millers_alvi_to_s3",
        python_callable = last_millers_alvi_to_s3
    )

    t1  = PythonOperator(
        task_id = "last_millers_alvi_to_postgres",
        python_callable = last_millers_alvi_to_postgres
    )

    t0 >> t1
    t0 >> t_dummy