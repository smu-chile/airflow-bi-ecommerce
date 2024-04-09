from airflow import DAG
from airflow import macros
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import pendulum

def venta_mfc_semana():
    import pandas as pd
    ventas_query = """select mrm.*,
                    vpsm.domingo, vpsm.lunes, vpsm.martes,vpsm.miercoles,vpsm.jueves,vpsm.viernes,vpsm.sabado,
                    um.mfc_is_item_side,
                    0::float as stock_takeoff,
                    0::float as "stock_janis",
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
    results = cursor.fetchall()
    cursor.close()
    pg_connection.close()

    return results

def reposicion_to_s3(ds):
    import pandas as pd
    import numpy as np
    import io

    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"mfc_reposicion/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    df = venta_mfc_semana()

    fecha = datetime.strptime(ds, '%Y-%m-%d')
    dia_de_la_semana = fecha.weekday()
    dia_de_la_semana = (dia_de_la_semana +1)%7
    dias = {0: 'domingo', 1: 'lunes', 2: 'martes', 3: 'miercoles', 4: 'jueves', 5: 'viernes', 6: 'sabado'}
    nombre_dia = (lambda x: dias[x])(dia_de_la_semana)

    df = df[['material', 'minimo','maximo','doh_objetivo','lead_time',str(nombre_dia),'stock_janis','stock_takeoff','mfc_is_item_side','multiplicador_unidad_medida']]

    df["venta"] = pd.to_numeric(df[str(nombre_dia)], errors='coerce')
    df['stock_janis'] = df['stock_janis'].fillna(0)

    condlist = [
                df["venta"] > df["minimo"],
                df["venta"] <= df["minimo"]
    ]
    choicelist = [True, False]
    df["reponer"] = np.select(condlist, choicelist)

    condlist = [
                df["reponer"] == False,
                (df["reponer"] == True) & (df["stock_janis"] > df["venta"]),
                (df["reponer"] == True) & (df["stock_janis"] <= df["venta"])                    
    ]
    choicelist = [False, False, True]
    df["reponer"] = np.select(condlist, choicelist)

    df.info()

    df = df[df["reponer"] == True]

    df["stock_objetivo"] = df["doh_objetivo"]*df["venta"]
    df["reponer"] = df["stock_objetivo"]+df["lead_time"]*df["venta"]-df["stock_janis"]
    df.info()

    condlist = [df["reponer"] > df["maximo"],
                df["reponer"] <= df["maximo"]]
    choicelist = [df["maximo"], df["reponer"]]
    df["solicitado"] = np.select(condlist, choicelist)

    condlist = [df["solicitado"] <= df["minimo"],
                df["solicitado"] > df["minimo"]]
    choicelist = [df["minimo"], df["solicitado"]]
    df["solicitado"] = np.select(condlist, choicelist)

    df['multiplicador_unidad_medida'] = df['multiplicador_unidad_medida'].astype(float)
    df["solicitado"] = np.ceil(df["solicitado"] / df["multiplicador_unidad_medida"]) * df["multiplicador_unidad_medida"]

    df = df.drop_duplicates()

    df.info()

    buffer = io.StringIO()
    df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"mfc_reposicion/{exec_date}/mfc_reposicion_{date_aux}.csv"
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
    df = df[['material','maximo','minimo','stock_janis','stock_takeoff','venta','reponer','solicitado']]
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

    df = reposicion()
    headers = ["material", "descripcion_material", "solicitado"]

    column_widths = [len(header) for header in headers]
    for row in df:
        for index, value in enumerate(row):
            column_widths[index] = max(column_widths[index], len(str(value)))

    formatted_header = " | ".join(header.upper().ljust(column_widths[index]) for index, header in enumerate(headers))

    formatted_rows = [formatted_header]
    for row in df:
        formatted_row = " | ".join(str(value).ljust(column_widths[index]) for index, value in enumerate(row))
        formatted_rows.append(formatted_row)

    formatted_message = "```\n" + "\n".join(formatted_rows) + "\n```"
    print(formatted_message)

    token = Variable.get("token_slack")

    client = WebClient(token=token)

    try:
        response = client.chat_postMessage(channel="alertas-reposiciones-mfc", text=formatted_message)
    except SlackApiError as e:
        print(f"Error sending message: {e}")

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
    schedule_interval="0 8 * * *",
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
    t0 >> t1 >> t2