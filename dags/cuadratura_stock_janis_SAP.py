from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable


import pendulum

from datetime import datetime, timedelta

def stock_lista8(ds):
    import pandas as pd
    stock_tiendas_query = """select _t.*
                    from( 
                    select s.fecha,
                    s.ref_id,
                    l.id_tienda,
                    l.stock_x_umv,
                    s.stock_janis,
                    case
                        when (l.umv in ('DIS','CJ')) then trunc(l.stock_x_umv,0) 
                        when (l.umv in ('KG','KGV')) then round(l.stock_x_umv/s.multiplicador_unidad_medida,0)
                        else l.stock_x_umv
                    end as stock_sap,
                    s.multiplicador_unidad_medida
                    from ecommdata.lista8 as l
                    inner Join ecommdata.stock as s
                    on l.fecha = s.fecha and l.id_tienda = s.id_tienda and s.ref_id = CONCAT(LPAD(l.material, 18, '0'), '-', l.umv)
                    and l.umv <> 'PAQ'
                    and l.id_tienda not in ('1917')) as _t 
                    group by 
                    _t.fecha,
                    _t.ref_id,
                    _t.id_tienda,
                    _t.stock_x_umv,
                    _t.stock_janis,
                    _t.stock_sap,
                    _t.multiplicador_unidad_medida"""
    print(stock_tiendas_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(stock_tiendas_query)
    results = cursor.fetchall()
    results=pd.DataFrame(results)
    print(results)
    results.columns = ["fecha","ref_id","id_tienda","stock_l8","stock_janis","stock_calculado","multiplicador_medida"]
    cursor.close()
    pg_connection.close()
    return results

def skus_carnes_padre_hijo():
    import pandas as pd
    stock_carnes_padre_hijo = """select s.erp_id,s.ref_id,s.nombre_sku,c.n1, pt.id_tienda
                                from ecommdata.skus as s
                                left join ecommdata.productos as p
                                on s.ref_id = p.ref_id
                                left join ecommdata.categorias as c
                                on p.id_categoria = c.id
                                left join ecommdata.productos_tienda as pt
                                on s.ref_id = pt.ref_id
                                where s.ref_id LIKE '%-KG' 
                                or s.ref_id LIKE '%-KGV'
                                or c.n1 = 'Carnes'
                                or p.material <> s.erp_id"""
    print(stock_carnes_padre_hijo)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(stock_carnes_padre_hijo)
    results = cursor.fetchall()
    results=pd.DataFrame(results)
    print(results)
    results.columns = ["material","ref_id","descripcion","categoria","id_tienda"]
    cursor.close()
    pg_connection.close()
    return results

def render_netezza_view(id_tienda,id_material,ds):
    import jaydebeapi
    import os

    sql_str= """SELECT sa.SKU_PRODUCT AS material ,
                NBR_ITM AS stock ,
                ou.ou_id AS id_tienda ,
                SA.NM AS nombre ,
                DATE_VALUE as fecha
                FROM DWC_SMU.SMU.VW_FACT_STOCK S 
                LEFT JOIN DWC_SMU.SMU.VW_DIM_SKU_ATTR SA
                ON SA.SKU_KEY = S.SKU_KEY 
                LEFT JOIN DWC_SMU.SMU.VW_DIM_ORGANIZATION_UNIT OU 
                ON OU.OU_KEY = S.OU_KEY 
                LEFT JOIN DWC_SMU.SMU.VW_DIM_ALMACEN A 
                ON A.ALMACEN_KEY =S.ALMACEN_KEY 
                LEFT JOIN DWC_SMU.SMU.VW_DIM_PARTICULARIDAD PART 
                ON S.PARTICULARIDAD_KEY =PART.PARTICULARIDAD_KEY 
                WHERE A.ALMACEN_COD = '0001' 
                AND S.APLICA_STOCK = 'S' 
                AND DATE_VALUE = '"""+ds+"""'::date + 1
                AND OU.OU_ID in ('"""+id_tienda+"""') 
                AND PART.PARTICULARIDAD_COD = 'A' 
                AND S.TIPO_STOCK_KEY IN (9161419180, 9145314683) 
                AND sa.SKU_PRODUCT in ('"""+id_material+"""');"""
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
    df = cur.fetchall()
    print(df)
    cur.close()
    conn.close()

    return df

def cuadratura_to_s3(ds):
    import pandas as pd
    import numpy as np
    import io
    from io import StringIO
    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"cuadratura/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    df = stock_lista8(ds)
    df.stock_janis = df.stock_janis.fillna(0)
    print("se ha descargado correctamente el stock filtrado por lista 8! \n")
    df_padre_hijo = skus_carnes_padre_hijo()
    df_padre_hijo = df_padre_hijo[df_padre_hijo["id_tienda"].isnull() == False]
    print("se ha descargado correctamente la tabla de skus padre e hijo \n")

    list_material = []
    list_tienda = []

    df_temp = df_padre_hijo

    list_material = df_temp['material'].tolist()
    list_tienda = df_temp['id_tienda'].tolist()

    list_material = list(dict.fromkeys(list_material))
    print("lista de materiales\n")
    print(list_material)
    list_tienda = list(dict.fromkeys(list_tienda))
    print("lista de tiendas\n")
    print(list_tienda)

    list_tienda = ' '.join(list_tienda)
    list_tienda = list_tienda.replace(" ", "','")
    list_material = ' '.join(list_material)
    list_material = list_material.replace(" ", "','")
    
    df_dw = render_netezza_view(list_tienda,list_material,ds)
    df_aux = pd.DataFrame(df_dw)
    df_aux.columns = ["material","stock","id_tienda","nombre","fecha"]
    print("se ha descargado correctamente la data de DW\n")
    print(df_aux)

    df_final = df.merge(df_padre_hijo, how = "left", on = ["id_tienda","ref_id"])
    df_final = df_final.drop_duplicates()
    df_final = df_final.merge(df_aux, how = "left", on = ["id_tienda","material"])
    df_final = df_final.drop_duplicates()
    df_final = df_final[["fecha_x","ref_id","id_tienda","stock_l8","stock_janis","stock_calculado","stock","multiplicador_medida"]]
    df_final = df_final.fillna(0)

    df_final['stock_calculado'] = pd.to_numeric(df_final['stock_calculado'],errors = 'coerce')
    df_final['multiplicador_medida'] = pd.to_numeric(df_final['multiplicador_medida'],errors = 'coerce')

    condlist = [df_final["stock"] != 0,
                df_final["stock"] == 0]
    choicelist = [df_final["stock"]/df_final["multiplicador_medida"], df_final["stock_calculado"]]
    
    df_final["stock_calculado"] = np.select(condlist, choicelist)
    df_final["stock_calculado"] = round(df_final["stock_calculado"],0)
    df_final = df_final[["fecha_x","ref_id","id_tienda","stock_l8","stock_janis","stock_calculado"]]

    df_final.columns = ["fecha","ref_id","id_tienda","stock_x_umv","stock_janis","stock_sap"]
    
    df_final["stock_janis"] = df_final["stock_janis"].astype(int)
    df_final["stock_sap"] = df_final["stock_sap"].astype(int)
    df_final['stock_x_umv'] = df_final['stock_x_umv'].astype(float, errors = 'raise')
    
    print("transformación de datos lista! \n")
    print(df_final.info())

    buffer = io.StringIO()
    df_final.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"cuadratura/{exec_date}/cuadratura_{date_aux}.csv"
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


def cuadratura_to_postgres(ti):
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text

    filename = ti.xcom_pull(key="return_value", task_ids=["cuadratura_to_s3"])[0]

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
    print(df.info())

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    with engine.begin() as conn:
        conn.execute("TRUNCATE catalogo.cuadratura_stock")
        df.to_sql(name="cuadratura_stock",
                    con=conn,         
                    schema="catalogo",         
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
    'etl_cuadratura_tiendas',
    default_args=default_args,
    description="cargar tabla cuadratura",
    schedule_interval= "30 9 * * *",
    start_date=pendulum.datetime(2023, 6, 14, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "postgres", "ecommdata_unimarc", "cuadratura", "unimarc", "PATRICIO"],
) as dag:
    

    dag.doc_md = """
    generar dataframe a partir de lista8, DW, skus y otras cositas mas. \n
    Insert diario.
    """ 

    t0 = PythonOperator(
        task_id = "cuadratura_to_s3",
        python_callable = cuadratura_to_s3,
        op_kwargs = {
            "schema": "ecommdata_unimarc",
            "table_name": "stock", 
            "updated_at_field": "fecha_modificacion_unixtime",
            "is_unixtime": True
        }
    )

    t1 = PythonOperator(
        task_id = "cuadratura_to_postgres",
        python_callable = cuadratura_to_postgres,
        op_kwargs = {
            "table_name": "stock", 
            "xcom_updated_date_task_id": "get_max_updated_at_date_atributos", 
            "updated_column": "date_modified"
        }
    )


    t0 >> t1
