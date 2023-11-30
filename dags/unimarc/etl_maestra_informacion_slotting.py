from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

import pendulum

def render_netezza_view():
    from io import StringIO
    import os
    import jaydebeapi
    import pandas as pd

    sql_str = "SELECT * FROM DWC_SMU.SMU.VW_DIM_SKU_ATTR"
    print(sql_str)

    dsn_database = Variable.get("DW_SECRET_DATABASE") 
    dsn_hostname = Variable.get("DW_SECRET_HOSTNAME")
    dsn_port = "5480" 
    dsn_uid = Variable.get("DW_SECRET_USER")
    dsn_pwd = Variable.get("DW_PASSWORD")
    jdbc_driver_name = "org.netezza.Driver" 
    jdbc_driver_loc = os.path.join('/opt/airflow/include/jdbcdriver/nzjdbc.jar')

    connection_string='jdbc:netezza://'+dsn_hostname+':'+dsn_port+'/'+dsn_database
    
    conn = jaydebeapi.connect(jdbc_driver_name, 
                                connection_string, {'user': dsn_uid, 'password': dsn_pwd},
                                jars=jdbc_driver_loc)

    cur = conn.cursor()
    cur.execute(sql_str)
    result = cur.fetchall()
    column_names = [desc[0] for desc in cur.description]
    df = pd.DataFrame(result, columns=column_names)
    df = df[['SKU_KEY','ALTURA','ANCHO','BRND_ID','CATEGORIA_MATERIAL_DESC',
             'CONDICION_DE_ALMACENAJE','CONTENIDO_BRUTO','CONTENIDO_NETO',
             'GDS_PD_TP_DSC','GRADO_ACLOHOLICO','LONGITUD','MARCA_PROPIA',
             'NUMERADOR_UMP','PAIS_ORIGEN_ID','PESO_BRUTO','PESO_NETO',
             'SKU_PRODUCT','UM_CONTENIDO','UMB','UNIDAD','UNIDAD_DE_MEDIDA_PEDIDO',
             'UNIDAD_DE_VOLUMEN','UNIDAD_LAA','UNIDAD_PESO','VOLUMEN','VIDA_UTIL']]
    cur.close()
    conn.close()

    return df

def productos_mfc():
    import pandas as pd
    productos_mfc_query = """select pt.ref_id,
                p.material,
                msp.descripcion_sap,
                c.n1 as categoria_1,
                c.n2 as categoria_2,
                c.n3 as categoria_3,
                m.nombre as nombre_marca,
                msp.nombre_proveedor,
                msp.id_proveedor,
                split_part(pt.ref_id,'-',2) as umv,
                msp.ump,
                msp.peso_bruto,
                venta_dia.venta_dia
                from ecommdata.productos_tienda pt 
                left join ecommdata.productos p 
                on pt.ref_id = p.ref_id 
                left join ecommdata.categorias c 
                on p.id_categoria = c.id
                left join ecommdata.marcas m 
                on p.id_marca = m.id 
                left join ecommdata.maestra_sku_proveedor msp 
                on p.material = msp.material
                left join (select concat(lpad(material,18,'0'),'-',umv) as ref_id,
                    round((sum(venta_umv)/30),2) as venta_dia
                    from ecommdata.venta_sku_tienda vst
                    where id_tienda = '1917'
                    group by concat(lpad(material,18,'0'),'-',umv)
                    ) as venta_dia
                on venta_dia.ref_id = pt.ref_id
                where pt.id_tienda = '1917'
                and pt.activo is true"""
    print(productos_mfc_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(productos_mfc_query)
    results = cursor.fetchall()
    results=pd.DataFrame(results)
    print(results)
    results.columns = ["ref_id","material","descripcion","categoria_1","categoria_2","categoria_3",
                       "marca","proveedor","id_proveer","umv","ump","peso_bruto","venta_diaria_30d"]
    cursor.close()
    pg_connection.close()
    return results



def load_slotting_to_s3(ds):
    import pandas as pd
    import io
    from io import StringIO
    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"slotting/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Empezando carga de productos MFC\n")
    df_productos_mfc = productos_mfc()
    print("Terminada carga de productos MFC\n")
    print("Empezando carga de atributos skus\n")
    df_atributos_skus = render_netezza_view()
    print("Terminada carga de atributos skus\n")
    df_atributos_skus.columns = [ 'SKU_KEY','ALTURA','ANCHO','BRND_ID','CATEGORIA_MATERIAL_DESC',
             'CONDICION_DE_ALMACENAJE','CONTENIDO_BRUTO','CONTENIDO_NETO',
             'GDS_PD_TP_DSC','grado_alcoholico','LONGITUD','MARCA_PROPIA',
             'NUMERADOR_UMP','PAIS_ORIGEN_ID','PESO_BRUTO_2','PESO_NETO',
             'material','UM_CONTENIDO','UMB','UNIDAD','UNIDAD_DE_MEDIDA_PEDIDO',
             'UNIDAD_DE_VOLUMEN','UNIDAD_LAA','UNIDAD_PESO','VOLUMEN','VIDA_UTIL']
    
    df_slotting = df_productos_mfc.merge(df_atributos_skus, how='left', on="material")
    df_slotting = df_slotting.drop_duplicates(subset=['ref_id'])
    df_slotting["VOLUMEN"] = df_slotting["VOLUMEN"].apply(lambda x: str(x) if x is not None else None)
    df_slotting["VOLUMEN"] = df_slotting["VOLUMEN"].apply(lambda x: str(x) if x is not None else None)
    df_slotting["PESO_NETO"] = df_slotting["PESO_NETO"].apply(lambda x: str(x) if x is not None else None)
    df_slotting["PESO_BRUTO_2"] = df_slotting["PESO_BRUTO_2"].apply(lambda x: str(x) if x is not None else None)
    df_slotting["CONTENIDO_NETO"] = df_slotting["CONTENIDO_NETO"].apply(lambda x: str(x) if x is not None else None)
    df_slotting["CONTENIDO_BRUTO"] = df_slotting["CONTENIDO_BRUTO"].apply(lambda x: str(x) if x is not None else None)
    df_slotting["ALTURA"] = df_slotting["ALTURA"].apply(lambda x: str(x) if x is not None else None)
    df_slotting["ANCHO"] = df_slotting["ANCHO"].apply(lambda x: str(x) if x is not None else None)

    df_slotting.columns = df_slotting.columns.str.lower()
    print(df_slotting.info())

    buffer = io.StringIO()
    df_slotting.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"slotting/{exec_date}/slotting_{date_aux}.csv"
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


def load_slotting_to_postgres(ti):
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text

    filename = ti.xcom_pull(key="return_value", task_ids=["load_slotting_to_s3"])[0]

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
    df = df.applymap(lambda x: str(x))
    print(df.info())

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    with engine.begin() as conn:
        conn.execute("TRUNCATE ecommdata.maestra_slotting")
        df.to_sql(name="maestra_slotting",
                    con=conn,         
                    schema="ecommdata",         
                    if_exists='append',         
                    index=False,         
                    chunksize=20000,         
                    method='multi')

    print("Data saved to PostgreSQL.")

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_maestra_informacion_slotting',
    default_args=default_args,
    description="cargar tabla slotting",
    schedule_interval= "30 10 * * *",
    start_date=pendulum.datetime(2023, 6, 14, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "postgres", "ecommdata_unimarc", "slotting","MFC"],
) as dag:
    

    dag.doc_md = """
    Desde postgres carga la base de la tabla de productos mfc con venta promedio y carga desde DW atributos de los skus \n
    Insert diario.
    """ 

    t0 = PythonOperator(
        task_id = "load_slotting_to_s3",
        python_callable = load_slotting_to_s3,
    )

    t1 = PythonOperator(
        task_id = "load_slotting_to_postgres",
        python_callable = load_slotting_to_postgres,
    )


    t0 >> t1