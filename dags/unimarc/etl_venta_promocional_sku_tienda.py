from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

import pendulum

from datetime import datetime, timedelta

def fecha_promos_skus(ds):
    import pandas as pd
    ventas_skus_tienda_query = f"""select _t.fecha_inicio, _t.fecha_fin, STRING_AGG(QUOTE_LITERAL(_t.ref_id), ', ') AS ref_ids
                                FROM (select concat(wp.material, '-',case when wp.umv = 'ST' then 'UN'else wp.umv end) as ref_id,
                                    fecha_inicio_de_promocion as fecha_inicio,
                                    fecha_fin_de_promocion as fecha_fin,
                                    sum(fecha_fin_de_promocion-fecha_inicio_de_promocion) as dias
                                    from ecommdata.workflow_promociones wp
                                    where wp.fecha_fin_de_promocion <= '{ds}'::date
                                    and wp.fecha_inicio_de_promocion >= '{ds}'::date-30
                                    and concat(material, '-',case when wp.umv = 'ST' then 'UN'else wp.umv end) in (
                                        select distinct concat (wp.material,'-',
                                        case 
                                            when wp.umv = 'ST' then 'UN'
                                            else wp.umv
                                        end) as ref_id
                                        from ecommdata.workflow_promociones wp
                                        left join ecommdata.lista8 l 
                                        on wp.material = l.material
                                        where wp.fecha_inicio_de_promocion >= '{ds}'::date-30
                                        and wp.fecha_fin_de_promocion <= '{ds}'::date 
                                        and wp.id_mecanica not in (12,22,25,26,27,36,50,67,72,84,99,37,51,53,59,77,82,93,96,123)
                                        and wp.id_evento not in (551)
                                        and l.material is not null)
                                    and id_mecanica not in (12,22,25,26,27,36,50,67,72,84,99,37,51,53,59,77,82,93,96,123)
                                    and id_evento not in (551)
                                    group by concat(wp.material, '-',case when wp.umv = 'ST' then 'UN'else wp.umv end) ,fecha_inicio_de_promocion,fecha_fin_de_promocion
                                    order by concat(wp.material, '-',case when wp.umv = 'ST' then 'UN'else wp.umv end)) as _t
                                GROUP BY _t.fecha_inicio, _t.fecha_fin;"""
    print(ventas_skus_tienda_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(ventas_skus_tienda_query)
    results = cursor.fetchall()
    results=pd.DataFrame(results)
    results.columns = ["fecha_inicio","fecha_fin","lista_materiales"]
    cursor.close()
    pg_connection.close()
    return results

def ventas(list_material,fecha_inicio,fecha_fin):
    import pandas as pd
    ventas_skus_tienda_query = f"""select lpad(vst.id_tienda,4,'0'),
            concat(lpad(vst.material,18,'0'),'-',vst.umv),
            sum(vst.venta_umv)
            from ecommdata.venta_sku_tienda vst 
            left join ecommdata.tiendas t
            on t.id = lpad(vst.id_tienda,4,'0')
            where vst.fecha >= '{fecha_inicio}'::date --fecha inicio
            and vst.fecha <= '{fecha_fin}'::date -- fecha fin
            and concat(lpad(vst.material,18,'0'),'-',vst.umv) in ({list_material})
            and t.status = 1
            and vst.venta_umv >= 0
            group by vst.id_tienda, concat(lpad(vst.material,18,'0'),'-',vst.umv)"""
    print(ventas_skus_tienda_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(ventas_skus_tienda_query)
    results = cursor.fetchall()
    results=pd.DataFrame(results)
    results.columns = ["id_tienda","material","prom_ventas"]
    cursor.close()
    pg_connection.close()
    return results

def venta_promocional_to_s3(ds):
    import pandas as pd
    import numpy as np
    import math
    import io
    print("\ninicio venta_promocional_to_s3\n")
    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"venta_promocional/{exec_date}"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    df_grouped = fecha_promos_skus(ds)

    df_final = pd.DataFrame()
    for i in range(len(df_grouped.index)):
        aux_list = []
        aux_list = df_grouped.lista_materiales[i]
        df_aux = ventas(aux_list,str(df_grouped.fecha_inicio[i]),str(df_grouped.fecha_fin[i]))
        df_final = pd.concat([df_final, df_aux])

    df_final["prom_ventas"]= df_final["prom_ventas"].apply(np.ceil)
    print("\ntermino de concatenar los df de ventas por rango de fechas")

    df_final = df_final.groupby(['id_tienda', 'material'], as_index=False)['prom_ventas'].sum()
    print("\ntermino de agrupar y sumar la venta")

    df_final.info()
    print(df_final.head(20))

    buffer = io.StringIO()
    df_final.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"{prefix}/venta_promocional_sku_tienda_{date_aux}.csv"
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

def venta_promocional_to_postgresql(ti):
    print("todo bien por acá")
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text

    filename = ti.xcom_pull(key="return_value", task_ids=["venta_promocional_to_s3"])[0]

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
    df.info()

    df['id_tienda'] = df['id_tienda'].apply(lambda x: str(x).zfill(4))
    df.columns = ['id_tienda','ref_id','ventas_75d']

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    with engine.begin() as conn:
        conn.execute("TRUNCATE ecommdata.venta_promocional")
        df.to_sql(name="venta_promocional",
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
    'etl_venta_promocional_sku_tienda',
    default_args=default_args,
    description="cargar venta promocional por sku tiendas",
    schedule_interval="0 8 * * *",
    start_date=pendulum.datetime(2024, 2, 19, tz="America/Santiago"),
    catchup=False,
    tags=["DATA","ventas", "unimarc", "apoteosicos", "PATRICIO"],
) as dag:
    

    dag.doc_md = """
    Carga venta promocional nivel sku tienda ultimos 75 dias \n
    guardar en S3.
    """ 

    t0 = PythonOperator(
        task_id='venta_promocional_to_s3',
        python_callable=venta_promocional_to_s3,
    )
    
    t1 = PythonOperator(
        task_id = "venta_promocional_to_postgresql",
        python_callable = venta_promocional_to_postgresql,
    )

    t0 >> t1
