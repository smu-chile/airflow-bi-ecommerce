from airflow import DAG
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator


from datetime import datetime, timedelta

import pendulum

def _stopper_lista8(ts):
    import pandas as pd
    import re

    exec_date = datetime.strptime(ts[:10], "%Y-%m-%d") + timedelta(days=1)
    exec_date = exec_date.strftime("%Y/%m/%d")
    prefix = f"datastage/L8/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    s3_file_list = s3_hook.list_keys(s3_bucket, prefix=prefix)
    s3_file_list = list(filter(lambda x: (x[-3:] == 'CSV'), s3_file_list))
    print(f"Files detected: {s3_file_list}")

    query = """
       select id 
    from ecommdata.tiendas t
    where t.status = 1
    and t.id <> '1917';
    """

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    df = pd.DataFrame(results)
    df.columns = ["id_tienda"]
    active_stores = df["id_tienda"].unique()
    stores_found = s3_file_list
    stores_found = [i.split('.CSV', 1)[0] for i in stores_found]
    stores_found = [i.split('-')[3] for i in stores_found]
    print(f"active stores: {active_stores}")
    print(f"stores found: {stores_found}")
    tiendas_faltantes = set(active_stores)-set(stores_found)
    tiendas_faltantes_lista = list(tiendas_faltantes)
    
    if len(tiendas_faltantes_lista) == 0:
        return
    else:
        raise Exception(f"No se encontraron las siguientes tiendas: {tiendas_faltantes_lista}")

def _load_lista8(ts):
    import pandas as pd
    import sqlalchemy

    exec_date = datetime.strptime(ts[:10], "%Y-%m-%d") + timedelta(days=1)
    exec_date = exec_date.strftime("%Y/%m/%d")
    prefix = f"datastage/L8/{exec_date}/"
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
        "SUSTITUTO": "bool",
        "BLOQ.CENTRO": "bool",
        "BLOQ.FORMATO": "bool",
        "CATALOGADO": "bool"
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
        "SUSTITUTO": "sustituto",
        "BLOQ.CENTRO": "bloq_centro",
        "BLOQ.FORMATO": "bloq_formato",
        "CATALOGADO": "catalogado" 
    }

    dataframe_list = []
    for s3_file in s3_file_list:
        if not s3_file.endswith((".csv", ".CSV")):
            # Skip empty any non-csv file
            continue
        print(f"Loading file: {s3_file}")
        lista8_object = s3_hook.get_key(s3_file, bucket_name=s3_bucket)
        df = pd.read_csv(lista8_object.get()["Body"], sep=";")
        df["STOCK X UMV"] = df["STOCK X UMV"].str.replace(',','.')
        df['SUSTITUTO'] = df['SUSTITUTO'].fillna('Y')
        df['SUSTITUTO'] = df['SUSTITUTO'].map({'X': True, 'Y': False})
        
        for col in ["BLOQ.CENTRO", "BLOQ.FORMATO", "CATALOGADO"]: # Asegura que las nuevas columnas sean booleanas y existan
            if col not in df.columns:
                df[col] = False 
            # Asegura que todo sea booleano (maneja posibles combinatorias o strings)
            df[col] = df[col].map({'X': True, 'Y': False, 
                                   1: True, 0: False, 
                                   '1': True, '0': False, 
                                   True: True, False: False, 
                                   'True': True, 'False': False,
                                   'SI': True, 'NO': False,
                                   'S': True, 'N': False})
            
            # Si quedaron NaN transformar (por si acaso)
            df[col] = df[col].fillna(False) # Asigna False a las otras columnas si es NaN

        df = df.astype(column_types)
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

    # Save to PostgreSQL:

    with engine.begin() as conn:
        conn.execute("TRUNCATE ecommdata.lista8") 
        df_full.to_sql(name="lista8",
                    con=conn,         
                    schema="ecommdata",         
                    if_exists='append',         
                    index=False,         
                    chunksize=20000,         
                    method='multi')
        conn.execute("""
            UPDATE ecommdata.lista8 l
            SET excluido = True
            FROM catalogo.productos_excluidos pe
            WHERE l.material = pe.material and l.umv = pe.umv
        """)

    print("Data saved to PostgreSQL. Table: ecommdata.lista8")

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_lista8_datastage_truncate_and_load',
    default_args=default_args,
    description="Carga de datos de lista8 desde bucket de S3 al workspace de Postgresql.",
    schedule_interval="0 7 * * *",
    start_date=pendulum.datetime(2022, 7, 3, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["DATA", "SAP", "ecommdata", "lista8", "PATRICIO"],
) as dag:

    dag.doc_md = """
    Extracción de archivos csv de lista8 desde bucket de S3, transformación y carga de datos en tabla ecommdata_unimarc.lista8. \n
    Un sensor espera por 1 hora la presencia de un archivo bandera (.TRG) que indique que la carga de los csv de datos está completa. \n
    Se realiza previamente un truncado de todos los datos y posteriormente se realiza la carga del día
    """ 
    t0 = S3KeySensor(
        task_id = "wait_for_lista8_flag_file",
        bucket_key = "datastage/L8/{{(execution_date + macros.timedelta(days=1)).strftime('%Y/%m/%d')}}/LISTA_8.TRG",
        bucket_name = Variable.get("AWS_S3_BUCKET_NAME"),
        aws_conn_id = "aws_s3_connection",
        timeout = 60*60,
        retries = 3,
        retry_delay = timedelta(minutes=1),
    )

    t1 = PythonOperator(
        task_id = "stopper_lista8",
        python_callable = _stopper_lista8
    )

    t2 = PythonOperator(
        task_id = "load_lista8",
        python_callable = _load_lista8
    )

    t0 >> t1 >> t2
