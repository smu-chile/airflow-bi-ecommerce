from airflow import DAG
from airflow import macros
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import pendulum

def venta_mfc_semana():
    import pandas as pd
    ventas_query = """select mrm.*,
                    vpsm.domingo, vpsm.lunes, vpsm.martes,vpsm.miercoles,vpsm.jueves,vpsm.viernes,vpsm.sabado,
                    um.mfc_is_item_side,
                    smt.quantity_on_hand as "stock_takeoff",
                    s.stock_janis,
                    s.multiplicador_unidad_medida 
                    from ecommdata.maestra_reposicion_mfc mrm 
                    left join ecommdata.venta_prom_semanal_mfc vpsm
                    on vpsm.material = mrm.material
                    left join ecommdata.ubicacion_mfc um 
                    on mrm.material = um.sap_code 
                    left join ecommdata.stock_mfc_takeoff smt
                    on split_part(smt.tom_id,'-',1) = mrm.material
                    left join ecommdata.stock s 
                    on s.material = mrm.material
                    where vpsm.material is not null
                    and smt.fecha = (select max(fecha) from ecommdata.stock_mfc_takeoff smt2 )
                    and s.ultima_actualizacion = (select max(ultima_actualizacion) from ecommdata.stock s)
                    and s.id_tienda = '1917';
                    """
    print(ventas_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(ventas_query)
    column_names = [desc[0] for desc in cursor.description]
    results = cursor.fetchall()
    results = pd.DataFrame(results, columns=column_names)
    print(results.head(20))
    cursor.close()
    pg_connection.close()

    return results

def reposicion():
    import pandas as pd
    ventas_query = """select msr.material,p.nombre as "descripcion_material",solicitado
                from ecommdata.mfc_solicitud_reposicion msr
                left join ecommdata.productos p 
                on p.material = msr.material;
                    """
    print(ventas_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(ventas_query)
    column_names = [desc[0] for desc in cursor.description]
    results = cursor.fetchall()
    results = pd.DataFrame(results, columns=column_names)
    print(results.head(20))
    cursor.close()
    pg_connection.close()

    return results
def calcular_venta_futura(row, dias_de_la_semana, nombre_dia):
        index_dia_actual = dias_de_la_semana.index(nombre_dia)
        ventas_futuras = 0
        # Asegura que el 'lead_time' es un entero y maneja casos donde podría ser NaN o similar
        lead_time = int(row.get('lead_time', 0))
        for i in range(lead_time):
            dia = dias_de_la_semana[(index_dia_actual + i) % len(dias_de_la_semana)]
            ventas_futuras += row.get(dia, 0)  # Asume 0 si no hay datos para ese día
        return ventas_futuras

def reposicion_to_s3(ds):
    import pandas as pd
    import numpy as np
    import io

    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"mfc_reposicion/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    df = venta_mfc_semana()  # Esta función debe devolver el DataFrame necesario

    fecha = datetime.strptime(ds, '%Y-%m-%d')
    print(fecha)
    dia_de_la_semana = (fecha.weekday()+1)%7
    print(dia_de_la_semana)
    nombre_dia = ['domingo', 'lunes', 'martes', 'miercoles', 'jueves', 'viernes', 'sabado'][dia_de_la_semana]
    print(nombre_dia)

    # Aplicamos la condición del contador igual a 0
    df = df[df['contador'] == 0]
    df.info()

    dias_de_la_semana = ['domingo', 'lunes', 'martes', 'miercoles', 'jueves', 'viernes', 'sabado']
    df['venta_futura'] = df.apply(calcular_venta_futura, args=(dias_de_la_semana, nombre_dia), axis=1)

    # Lógica para decidir si se necesita reponer actualizada para usar venta_futura
    df['stock_takeoff'] = df['stock_takeoff'].fillna(0)

    condlist = [
        df["venta_futura"] > df["minimo"],
        df["venta_futura"] <= df["minimo"]
    ]
    choicelist = [True, False]
    df["reponer"] = np.select(condlist, choicelist)

    condlist = [
        df["reponer"] == False,
        (df["reponer"] == True) & (df["stock_takeoff"] > df["venta_futura"]),
        (df["reponer"] == True) & (df["stock_takeoff"] <= df["venta_futura"])
    ]
    choicelist = [False, False, True]
    df["reponer"] = np.select(condlist, choicelist)

    # Ajustar 'solicitado' en función de 'maximo' y 'minimo'
    df["venta_hoy"] = df[str(nombre_dia)]
    df["stock_objetivo"] = df["doh_objetivo"] * df["venta_hoy"]
    print(df.head())
    df["solicitado"] = df["stock_objetivo"] + df["venta_futura"] - df["stock_takeoff"]
    df["solicitado"] = np.select(
        [df["solicitado"] > df["maximo"], df["solicitado"] < df["minimo"]],
        [df["maximo"], df["minimo"]],
        default=df["solicitado"]
    )

    df['multiplicador_unidad_medida'] = df['multiplicador_unidad_medida'].astype(float)
    df["solicitado"] = np.ceil(df["solicitado"] / df["multiplicador_unidad_medida"]) * df["multiplicador_unidad_medida"]
    # Mantenemos solo los registros donde 'reponer' es True o 1 ?
    df = df[df["reponer"] == 1]
    df = df[df['solicitado'] > 0]

    # Convertimos el DataFrame a un archivo CSV y lo cargamos a S3
    buffer = io.StringIO()
    df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"mfc_reposicion/{exec_date}/mfc_reposicion_{date_aux}.csv"
    buffer.seek(0)
    s3_hook.load_string(buffer.getvalue(), key=filename, bucket_name=s3_bucket, replace=True)

    print(f"Archivo cargado en S3: {prefix}{filename}")
    return filename

def reposicion_to_postgres(ti):
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text

    filename = ti.xcom_pull(key="return_value", task_ids=["reposicion_to_s3"])[0]

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
    df["material"] = df["material"].apply(lambda x: str(x).zfill(18))
    df = df[['material','maximo','minimo','stock_janis','stock_takeoff','venta_futura','reponer','solicitado']]
    df.columns = ['material','maximo','minimo','stock_janis','stock_takeoff','venta','reponer','solicitado']
    df['reponer'] = df['reponer'].astype(bool)
    df = df.drop_duplicates()
    df.info()

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    with engine.begin() as conn:
        conn.execute("TRUNCATE ecommdata.mfc_solicitud_reposicion") 
        df.to_sql(name="mfc_solicitud_reposicion",
                    con=conn,         
                    schema="ecommdata",         
                    if_exists='append',         
                    index=False,         
                    chunksize=20000,         
                    method='multi')

    print("Data saved to PostgreSQL.")

    return


def reposicion_to_slack():
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError
    import io
    import pandas as pd

    df = reposicion()

    with io.BytesIO() as buffer:
        df.to_csv(buffer, index=False, encoding='utf-8')
        buffer.seek(0)
        
        token = Variable.get("token_slack")
        
        client = WebClient(token=token)
        
        try:
            response = client.files_upload(
                channels="alertas-reposiciones-mfc",
                file=buffer,
                filename="reporte_reposicion.csv",
                title="Reporte de Reposición",
                initial_comment="Aquí está el reporte de reposición actualizado."
            )
        except SlackApiError as e:
            print(f"Error al subir archivo: {e}")

    return


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_reposicion_mfc',
    default_args=default_args,
    description="consulta de datos de Stock MFC, maestra reposicion desde postgres para logica de reposicion.",
    schedule_interval="50 17 * * *",
    start_date=pendulum.datetime(2022, 8, 25, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["DATA", "MFC", "ecommdata","SLACK" ,"PATRICIO"],
) as dag:

    dag.doc_md = """
    genera unidades solicitadas para mfc en picking tienda.
    """ 

    t0 = PythonOperator(
        task_id = "reposicion_to_s3",
        python_callable = reposicion_to_s3
    )
    t1 = PythonOperator(
        task_id = "reposicion_to_postgres",
        python_callable = reposicion_to_postgres
    )
    t2 = PythonOperator(
        task_id = "reposicion_to_slack",
        python_callable = reposicion_to_slack
    )
    t3 = PostgresOperator(
        task_id = "update_contador",
        postgres_conn_id = "postgresql_conn",
        sql = """BEGIN;
            UPDATE ecommdata.maestra_reposicion_mfc
            SET contador = contador - 1;
            UPDATE ecommdata.maestra_reposicion_mfc
            SET contador = lead_time
            WHERE contador < 0;
            COMMIT;"""
    )
    t0 >> t1 >> t2 >> t3