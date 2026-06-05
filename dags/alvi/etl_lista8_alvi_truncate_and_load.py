from airflow import DAG
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

from utils.slack_utils import dag_failure_slack, dag_success_slack

from datetime import datetime, timedelta

import pendulum

def _stopper_lista8(ts):

    exec_date = datetime.strptime(ts[:10], "%Y-%m-%d") + timedelta(days=1)
    exec_date = exec_date.strftime("%Y/%m/%d")
    prefix = f"datastage/L8_alvi/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    s3_file_list = s3_hook.list_keys(s3_bucket, prefix=prefix)
    s3_file_list = list(filter(lambda x: (x[-3:] == 'CSV'), s3_file_list))
    print(f"Files detected: {s3_file_list}")

    query = """
        select count(1) as tiendas_activas
        from ecommdata_alvi.tiendas t
        where t.status = 1 and t.id != '1';
    """

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    active_stores = results[0][0]
    stores_found = len(s3_file_list)
    print(f"active stores: {active_stores}")
    print(f"stores found: {stores_found}")

    if stores_found >= active_stores:
        return
    else:
        raise Exception(f"Not all active stores found")

def _load_lista8(ts):
    import pandas as pd
    import sqlalchemy

    exec_date = datetime.strptime(ts[:10], "%Y-%m-%d") + timedelta(days=1)
    exec_date = exec_date.strftime("%Y/%m/%d")
    prefix = f"datastage/L8_alvi/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    s3_file_list = s3_hook.list_keys(s3_bucket, prefix=prefix)
    print(f"Files detected: {s3_file_list}")
    column_types = {
        "CENTRO": "str",
        "MATERIAL":	"str",
        "UM VTA":	"str",
        "NEGOCIO": "str",
        "SECCION": "str",
        "LINEA": "str",
        "CATEGORIA": "str",
        "GRUPO ARTICULO": "str",
        "PRECIO REGULAR": "int",
        "PRECIO PROMOCIONAL": "int",
        "DESCRIPCION": "str",
        "STOCK X UMV": "float",
        "SUSTITUTO": "bool"
    }

    column_names = {
        "CENTRO": "id_tienda",
        "MATERIAL":	"material",
        "UM VTA":	"umv",
        "NEGOCIO": "negocio",
        "SECCION": "seccion",
        "LINEA": "linea",
        "CATEGORIA": "categoria",
        "GRUPO ARTICULO": "grupo_articulo",
        "PRECIO REGULAR": "precio_regular",
        "PRECIO PROMOCIONAL": "precio_promocional",
        "DESCRIPCION": "descripcion",
        "STOCK X UMV": "stock_x_umv",
        "SUSTITUTO": "sustituto"
    }

    from concurrent.futures import ThreadPoolExecutor, as_completed

    def process_single_file(s3_bucket, s3_file):
        from airflow.hooks.S3_hook import S3Hook
        local_s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
        lista8_object = local_s3_hook.get_key(s3_file, bucket_name=s3_bucket)
        df = pd.read_csv(lista8_object.get()["Body"], sep=";")
        df["STOCK X UMV"] = df["STOCK X UMV"].str.replace(',','.')
        df['SUSTITUTO'] = df['SUSTITUTO'].fillna('Y')
        df['SUSTITUTO'] = df['SUSTITUTO'].map({'X': True, 'Y': False})
        df = df.astype(column_types)
        return df

    dataframe_list = []
    valid_files = [f for f in s3_file_list if f.endswith((".csv", ".CSV"))]
    print(f"Iniciando carga paralela de {len(valid_files)} archivos...")
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(process_single_file, s3_bucket, f): f for f in valid_files}
        for future in as_completed(futures):
            df = future.result()
            dataframe_list.append(df)
            
    df_full = pd.concat(dataframe_list, ignore_index=True)
    df_full = df_full.rename(columns=column_names)
    df_full["fecha"] = exec_date
    df_full["id_tienda"] = df_full["id_tienda"].str.zfill(4)
    df_full["material"] = df_full["material"].str.zfill(18)
    df_full["excluido"] = False

    # Drop duplicates
    df_full = df_full.drop_duplicates()
    print("Number of records to be loaded: "+str(len(df_full.index)))

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    # === APLICACIÓN DE EXCLUSIONES EN MEMORIA (OPTIMIZACIÓN) ===
    print("Iniciando exclusiones en memoria (Pandas)...")
    try:
        df_pe = pd.read_sql("SELECT material, umv FROM catalogo.productos_excluidos_alvi", engine)
        df_pe['material'] = df_pe['material'].astype(str).str.zfill(18)
        df_pe = df_pe.drop_duplicates(subset=['material', 'umv'])
        df_pe['_in_pe'] = True
        
        df_full = df_full.merge(df_pe, on=['material', 'umv'], how='left')
        df_full.loc[df_full['_in_pe'] == True, 'excluido'] = True
        df_full = df_full.drop(columns=['_in_pe'])
        
        print("✅ Exclusiones en Pandas aplicadas exitosamente.")
    except Exception as e:
        print(f"⚠️ Error al aplicar exclusiones en Pandas: {e}")

    # Save to PostgreSQL:
    import io
    
    # Preparar el buffer en memoria ANTES de tocar la base de datos
    buffer = io.StringIO()
    df_full.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
    buffer.seek(0)
    
    # Ejecutar TRUNCATE e INSERT en una sola transacción atómica
    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cursor:
            cursor.execute("TRUNCATE ecommdata_alvi.lista8;")
            columns_str = ','.join(df_full.columns)
            cursor.copy_expert(f"COPY ecommdata_alvi.lista8 ({columns_str}) FROM STDIN WITH CSV DELIMITER '\t' NULL '\\N'", buffer)
        raw_conn.commit()
    finally:
        raw_conn.close()

    # IMPORTANTE: Forzar actualización de estadísticas del motor SQL para prevenir Hash Join vs Nested Loop bugs
    with engine.begin() as conn:
        print("Actualizando estadísticas de la tabla (ANALYZE)...")
        conn.execute("ANALYZE ecommdata_alvi.lista8;")

    print("Data saved to PostgreSQL. Table: ecommdata_alvi.lista8")

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_lista8_alvi_datastage_truncate_and_load',
    default_args=default_args,
    description="Carga de datos de lista8 alvi desde bucket de S3 al workspace de Postgresql.",
    schedule_interval="0 7 * * *",
    start_date=pendulum.datetime(2022, 7, 3, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["DATA", "SAP", "ecommdata_alvi", "lista8", "PATRICIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Extracción de archivos csv de lista8 alvi desde bucket de S3, transformación y carga de datos en tabla ecommdata_alvi.lista8. \n
    Un sensor espera por 3 horas la presencia de un archivo bandera (.TRG) que indique que la carga de los csv de datos está completa. \n
    Se realiza previamente un truncado de todos los datos y posteriormente se realiza la carga del día
    """ 
    t0 = S3KeySensor(
        task_id = "wait_for_lista8_alvi_flag_file",
        bucket_key = "datastage/L8_alvi/{{(execution_date + macros.timedelta(days=1)).strftime('%Y/%m/%d')}}/LISTA_8A.TRG",
        bucket_name = Variable.get("AWS_S3_BUCKET_NAME"),
        aws_conn_id = "aws_s3_connection",
        timeout = 60*60,
        retries = 3,
        retry_delay = timedelta(minutes=1),
    )

    t1 = PythonOperator(
        task_id = "stopper_lista8_alvi",
        python_callable = _stopper_lista8
    )

    t2 = PythonOperator(
        task_id = "load_lista8_alvi",
        python_callable = _load_lista8
    )


    t0 >> t1 >> t2
