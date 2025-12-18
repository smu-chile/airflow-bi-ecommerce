from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.S3_hook import S3Hook
from airflow import macros

from utils.slack_utils import dag_success_slack, dag_failure_slack

import pendulum
from datetime import datetime

# Función para limpiar clientes
def limpiar_clientes(ti, ds):
    import pandas as pd
    import io
    from io import StringIO
    
    #exec_date = macros.ds_add(ds, -7)
    #date_aux = macros.ds_add(ds, -7)
    exec_date = ds
    date_aux = ds
    exec_date = exec_date.replace("-", "/")
    date_aux = date_aux.replace("-", "_")


    print(f"DS: {ds}")
    print(f"Exec date: {exec_date}")
    print(f"Date aux: {date_aux}")

    print("Ejecutando limpieza de xCluster para la fecha: ", date_aux)

    file_name = f"clientesretenidos/{exec_date}/retenidos_{date_aux}.csv"
    s3_hook = S3Hook(aws_conn_id='aws_s3_connection')
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    buffer = io.StringIO()
    buffer.seek(0)
    # Descargar el archivo desde S3
    print("Searching file: "+ file_name)
    if not s3_hook.check_for_key(file_name, bucket_name=s3_bucket):
        print(f"Archivo {file_name} no encontrado en el bucket {s3_bucket}. Continuando...")
        return  # Salir de la función si el archivo no existe
    
    users_to_clean = s3_hook.get_key(file_name, bucket_name=s3_bucket)
    print("File downloaded from S3")

    # Cargar el archivo CSV con los usuarios a limpiar
    df_users_to_clean = pd.read_csv(users_to_clean.get()["Body"])

    # Iterar sobre cada usuario y limpiar el xCluster
    for index, row in df_users_to_clean.iterrows():
        user_profile_id = row["user_profile_id"]
        limpiar_xCluster(user_profile_id)

    print("Limpieza de xCluster completada.")

# Función para obtener clientes retenidos desde PostgreSQL
def get_clientes_retenidos():
    import psycopg2
    import pandas as pd
    # Establecer conexión a la base de datos y ejecutar la consulta
    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn = psycopg2.connect(
        host=host,
        user=username,
        password=password,
        database=database
    )
    cursor = conn.cursor()
    cursor.execute("""  SELECT
                pu.user_profile_id,
                mpupi.rut,
                mpupi.estado,
                mpupi.tipo_membresia,
                mpupi.fecha_inicio,
                mpupi.fecha_fin,
                pu.recencia,
                du.email
                FROM
                power_bi.membresias_por_user_profile_id mpupi
                LEFT JOIN
                ecommdata.calendario c 
                ON mpupi.fecha_inicio <= c.fecha 
                AND mpupi.fecha_fin >= c.fecha + 7
                LEFT JOIN
                analytics_and_growth.perfil_usuario pu 
                ON pu.user_profile_id = mpupi.user_profile_id
                left join 
                analytics_and_growth.detalle_usuario du 
                on du.user_profile_id = pu.user_profile_id 
                WHERE
                c.fecha = CURRENT_DATE
                AND mpupi.estado IN ('confirmada', 'renovada') -- solo membresías NO de prueba
                AND pu.recencia >= 40
                AND mpupi.rut not IN (
                '160091169', '20121243K', '86194627', '91526778', '211622695', 
                '206662638', '196720677', '216742362', '196365605', '210314148', 
                '196838775', '206646306', '210202641', '196068147', '196882960', 
                '211816015', '210695176', '206816619', '196395717', '213540904', 
                '208246291', '190913597', '216251199', '20990523K', '200717376', 
                '203436610', '201655501', '207319988', '215347647', '151635415', 
                '217860911', '196377271', '19637728k', '217093813', '20164447K', 
                '194380763', '201100305', '206650982', '194014694', '215081524', 
                '206657820', '196379487', '208071734', '192022258', '176986301', 
                '156786616', '281459643', '157909967', '169357706', '188894571', 
                '256821796', '165480678', '182940224', '26194935', '269511036', 
                '22426421', '27002792k', '177412031', '275078220', '157487302', 
                '182996939', '177252409', '182955647', '130679684', '268674705', 
                '194380097', 'LM972263', '156361283', '190873901', '184672103', 
                '257513661', '174335761', '262226816', '185519155', '157191306', 
                '170824792', '163654970', '130278159', '134582987', '205576592', 
                '160172045', '159294153', '58638722', '278222004', '242166388', 
                '134662212', '228990191', '159695298', '179606399', '277467194', 
                '176989793');""")
    
    # Crear DataFrame con los resultados de la consulta
    df_clientes = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    
    cursor.close()
    conn.close()
    
    print("Shape of clientes: ", df_clientes.shape)
    print("Type of clientes: ", df_clientes.info())

    return df_clientes

# Función para actualizar el campo xCluster en VTEX
def limpiar_xCluster(document_id, max_retries=3, delay=10):
    import requests
    import time

    API_URL = "https://unimarc.vtexcommercestable.com.br/api/dataentities/CL"
    API_KEY = Variable.get("X_VTEX_API_AppKey")
    API_TOKEN = Variable.get("X_VTEX_API_AppToken")
    HEADERS = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-VTEX-API-AppKey": API_KEY,
        "X-VTEX-API-AppToken": API_TOKEN
    }

    update_url = f"{API_URL}/documents"
    update_payload = {
        "userId": document_id,
        "xCluster": None
    }

    for attempt in range(max_retries):
        response = requests.patch(update_url, json=update_payload, headers=HEADERS)

        if response.status_code == 200:
            print(f"✅ xCluster limpiado (null) para {document_id} en intento {attempt + 1}")
            return True
        elif response.status_code == 304:
            print(f"⏩ xCluster ya estaba limpio para {document_id}, intento {attempt + 1}")
            return True
        else:
            print(f"⚠️ Error limpiando xCluster ({document_id}), intento {attempt + 1}: {response.status_code}")
            time.sleep(delay)

    print(f"❌ Falló la limpieza de xCluster para {document_id} tras {max_retries} intentos.")
    return False

# Función para actualizar el campo xCluster en VTEX
# Función para actualizar el campo xCluster en VTEX con reintentos
def actualizar_xCluster(document_id, xCluster_value, max_retries=3, delay=10):
    import requests
    import time

    API_URL = "https://unimarc.vtexcommercestable.com.br/api/dataentities/CL"
    API_KEY = Variable.get("X_VTEX_API_AppKey")
    API_TOKEN = Variable.get("X_VTEX_API_AppToken")
    HEADERS = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-VTEX-API-AppKey": API_KEY,
        "X-VTEX-API-AppToken": API_TOKEN
    }

    update_url = f"{API_URL}/documents"
    update_payload = {
        "userId": document_id,
        "xCluster": xCluster_value
    }

    for attempt in range(max_retries):
        response = requests.patch(update_url, json=update_payload, headers=HEADERS)

        if response.status_code == 200:
            print(f"✅ xCluster actualizado para {document_id} en intento {attempt + 1}")
            return True
        elif response.status_code == 304:
            print(f"⏩ xCluster ya tenía el valor para {document_id}, intento {attempt + 1}")
            return True
        else:
            print(f"⚠️ Error actualizando xCluster ({document_id}), intento {attempt + 1}: {response.status_code}")
            time.sleep(delay)

    print(f"❌ Falló la actualización de xCluster para {document_id} tras {max_retries} intentos.")
    return False


# Función para etiquetar clientes como retenidos
def tagear_clientes_retenidos(ds, ti):
    import pandas as pd
    from datetime import datetime
    import io
    from io import StringIO
    s3_hook = S3Hook(aws_conn_id='aws_s3_connection')
    bucket_name = Variable.get("AWS_S3_BUCKET_NAME")
    
    df_clientes_retenidos = get_clientes_retenidos()
    xCluster_value = "retenidos"
    updated_users = []
    exec_date = macros.ds_add(ds, 7)
    date_aux = macros.ds_add(ds, 7)
    exec_date = exec_date.replace("-", "/")
    date_aux = date_aux.replace("-", "_")
    print(f"DS: {ds}")
    print(f"Exec date: {exec_date}")
    print(f"Date aux: {date_aux}")

    print(f"Procesando {len(df_clientes_retenidos)} clientes retenidos.")

    for index, row in df_clientes_retenidos.iterrows():
        user_profile_id = row["user_profile_id"]
        if actualizar_xCluster(user_profile_id, xCluster_value):
            updated_users.append(row)
    
    if updated_users:
        # Crear un DataFrame con los usuarios actualizados
        df_updated = pd.DataFrame(updated_users)
        # Guardar el DataFrame en un archivo CSV
        buffer = io.StringIO()
        df_updated.to_csv(buffer, header=True, index=False, encoding="utf-8")
        filename = f"clientesretenidos/{exec_date}/retenidos_{date_aux}.csv"

        # Subir el archivo CSV a S3
        buffer.seek(0)
        print("Se logro transformar el dataframe a un archivo .csv\n")
        print(f"Con fecha {exec_date} y nombre de filename como {filename}")
        s3_hook.load_string(buffer.getvalue(),
                    key=filename,
                    bucket_name=bucket_name,
                    replace=True,
                    encrypt=False)
        print(f"Archivo '{filename}' subido a S3 en el bucket '{bucket_name}'.")
        return filename
    else:
        print("No se actualizó ningún xCluster.")
        return


# Función para cargar los datos a la tabla ecommdata.clientes_retenidos
def cargar_datos_a_tabla(ds, ti):
    import psycopg2
    import pandas as pd

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn = psycopg2.connect(
        host=host,
        user=username,
        password=password,
        database=database
    )
    cursor = conn.cursor()

    df_clientes_retenidos = get_clientes_retenidos()

    for index, row in df_clientes_retenidos.iterrows():
        cursor.execute("""
            INSERT INTO ecommdata.clientes_retenidos (
                user_profile_id, rut, estado, tipo_membresia,
                fecha_inicio, fecha_fin, recencia, email, fecha_modificacion
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        """, (
            row["user_profile_id"], row["rut"], row["estado"], row["tipo_membresia"],
            row["fecha_inicio"], row["fecha_fin"], row["recencia"], row["email"]
        ))

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Datos cargados en la tabla ecommdata.clientes_retenidos.")

# Definir el DAG
default_args = {
    'owner': 'ecommerce_data',
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
    'etl_clientes_retenidos',
    default_args=default_args,
    description='DAG para gestionar el campo xCluster en VTEX de clientes con Recencia',
    schedule_interval= "30 8 * * 3", #Se ejecuta todos los miercoles.
    start_date =pendulum.datetime(2025, 2, 5, tz="America/Santiago"),
    catchup=False,
    tags = ["xCluster", "VTEX", "Clientes", "KEVIN", "Unimarc"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag: 
    dag.doc_md = """
    Gestion de campo xCluster para cliente con 40 dias de recencia. \n
    Se realiza cada miercoles.\n
    """ 
    t0 = PythonOperator(
        task_id='limpiar_clientes',
        python_callable=limpiar_clientes
    )

    t1 = PythonOperator(
        task_id='tagear_clientes_retenidos',
        python_callable=tagear_clientes_retenidos
    )

    t2 = PostgresOperator(
        task_id = "truncate_table",
        postgres_conn_id="postgresql_conn",
        sql="""
        TRUNCATE TABLE ecommdata.clientes_retenidos;
        """,
    )

    t3 = PythonOperator(
        task_id = "cargar_datos_a_tabla",
        python_callable=cargar_datos_a_tabla
    )

# Definir las dependencias
t0 >> t1 >> t2 >> t3