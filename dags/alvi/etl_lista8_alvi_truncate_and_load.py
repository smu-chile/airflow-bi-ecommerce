from airflow import DAG
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator


from datetime import datetime, timedelta



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
        "Tienda": "str",
        "Material":	"str",
        "UM Vta": "str",
        "Línea": "str",
        "Categoría": "str",
        "Descripción Gr Art": "str",
        "P/Vta Reg.": "int",
        "P/Vta Prom.": "int",
        "Descripción": "str",
        " Stock x UMV": "float"
    }

    column_names = {
        "Tienda": "id_tienda",
        "Material":	"material",
        "UM Vta": "umv",
        "Línea": "linea",
        "Categoría": "categoria",
        "Descripción Gr Art": "grupo_articulo",
        "P/Vta Reg.": "precio_regular",
        "P/Vta Prom.": "precio_promocional",
        "Descripción": "descripcion",
        " Stock x UMV": "stock_x_umv"
    }

    dataframe_list = []
    for s3_file in s3_file_list:
        if not s3_file.endswith((".csv", ".CSV")):
            # Skip empty any non-csv file
            continue
        print(f"Loading file: {s3_file}")
        lista8_object = s3_hook.get_key(s3_file, bucket_name=s3_bucket)
        df = pd.read_csv(lista8_object.get()["Body"], sep=";", header = 5)
        df[" Stock x UMV"] = df[" Stock x UMV"].str.replace(',','.')
        df.drop(df[df['Tienda'] == 'Tienda'].index, inplace = True)
        df.dropna(subset = ['Tienda'], inplace = True)
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
    
    conn_url = "postgresql+psycopg2://"+username+":"+password+"@"+host+":5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    # Save to PostgreSQL:

    with engine.begin() as conn:
        conn.execute("TRUNCATE ecommdata_alvi.lista8_alvi") 
        df_full.to_sql(name="lista8_alvi",
                    con=conn,         
                    schema="ecommdata_alvi",         
                    if_exists='append',         
                    index=False,         
                    chunksize=20000,         
                    method='multi')
        conn.execute("""
            UPDATE ecommdata_alvi.lista8_alvi l
            SET excluido = True
            FROM catalogo.productos_excluidos pe
            WHERE l.material = pe.material and l.umv = pe.umv
        """)

    print("Data saved to PostgreSQL. Table: ecommdata_alvi.lista8_alvi")

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
    schedule_interval="0 10 * * *",
    start_date=datetime(2022, 7, 3),
    catchup=False,
    max_active_runs = 1,
    tags=["DATA", "SAP", "ecommdata_alvi", "lista8", "alvi"],
) as dag:

    dag.doc_md = """
    Extracción de archivos csv de lista8 alvi desde bucket de S3, transformación y carga de datos en tabla ecommdata_alvi.lista8_alvi. \n
    Un sensor espera por 3 horas la presencia de un archivo bandera (.TRG) que indique que la carga de los csv de datos está completa. \n
    Se realiza previamente un truncado de todos los datos y posteriormente se realiza la carga del día
    """ 
    t0 = S3KeySensor(
        task_id = "wait_for_lista8_flag_file",
        bucket_key = "datastage/L8_alvi/{{(execution_date + macros.timedelta(days=1)).strftime('%Y/%m/%d')}}/LISTA_8A.TRG",
        bucket_name = Variable.get("AWS_S3_BUCKET_NAME"),
        aws_conn_id = "aws_s3_connection",
        timeout = 60*60,
        retries = 3,
        retry_delay = timedelta(minutes=1),
    )


    t1 = PythonOperator(
        task_id = "load_lista8",
        python_callable = _load_lista8
    )


    t0 >> t1
