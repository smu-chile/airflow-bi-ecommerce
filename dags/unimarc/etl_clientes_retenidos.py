from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow import macros

import pendulum
from datetime import datetime


# Función para limpiar clientes
def limpiar_clientes(ti, ds):
    import pandas as pd
    import io
    from io import StringIO
    
    exec_date = macros.ds_add(ds, -7)
    date_aux = macros.ds_add(ds, -7)

    exec_date = exec_date.replace("-", "/")
    date_aux = date_aux.replace("-", "_")

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
    cursor.execute("""  select
                pu.user_profile_id ,
                mpupi.rut ,
                mpupi.estado ,
                mpupi.tipo_membresia ,
                mpupi.fecha_inicio ,
                mpupi.fecha_fin ,
                pu.recencia 
                from power_bi.membresias_por_user_profile_id mpupi 
                left join ecommdata.calendario c on mpupi.fecha_inicio <= c.fecha and mpupi.fecha_fin >= fecha+7
                left join analytics_and_growth.perfil_usuario pu on pu.user_profile_id = mpupi.user_profile_id 
                where c.fecha = current_date
                and estado in ('confirmada', 'renovada') --solo membresias NO de prueba
                and recencia >= 40;""")
    
    # Crear DataFrame con los resultados de la consulta
    df_clientes = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    
    cursor.close()
    conn.close()
    
    print("Shape of clientes: ", df_clientes.shape)
    print("Type of clientes: ", df_clientes.info())

    return df_clientes

# Función para actualizar el campo xCluster en VTEX
def limpiar_xCluster(document_id):
    import requests
    
    # Configuración de la API de VTEX
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
        "xCluster": None  # Establecer xCluster a null
    }
    
    response = requests.patch(update_url, json=update_payload, headers=HEADERS)
    
    if response.status_code == 200:
        print(f"Campo xCluster limpiado (establecido a null) para el documento {document_id}")
        return True  # Retorna True si la actualización fue exitosa
    else:
        print(f"Error al limpiar xCluster para el documento {document_id}: {response.status_code}")
        return False  # Retorna False si hubo un error
    
# Función para actualizar el campo xCluster en VTEX
def actualizar_xCluster(document_id, xCluster_value):
    import requests
    # Configuración de la API de VTEX
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
        "xCluster": xCluster_value  # Establecer xCluster a null
    }
    
    response = requests.patch(update_url, json=update_payload, headers=HEADERS)
    
    if response.status_code == 200:
        print(f"Campo xCluster establecido como retenidos para el documento {document_id}")
        return True  # Retorna True si la actualización fue exitosa
    else:
        print(f"Error al asignar campo xCluster para el documento {document_id}: {response.status_code}")
        return False  # Retorna False si hubo un error

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

    for index, row in df_clientes_retenidos.iterrows():
        user_profile_id = row["user_profile_id"]
        if actualizar_xCluster(user_profile_id, xCluster_value):
            updated_users.append(row)
    
    if updated_users:
        exec_date = ds.replace("-", "/")
        date_aux = ds.replace("-", "_")
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
    else:
        print("No se actualizó ningún xCluster.")
    print("Proceso finalizado.")
    return filename

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

# Definir las dependencias
t0 >> t1