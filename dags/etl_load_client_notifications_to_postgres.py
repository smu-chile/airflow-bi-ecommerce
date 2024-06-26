from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable


import pendulum

from datetime import datetime, date, timedelta

def mongo_to_postgres(ts):
    import pandas as pd
    from pymongo import MongoClient
    import numpy as np
    import pytz

    mongo_user = Variable.get("MONGODB_ORQ_USER")
    mongo_pass = Variable.get("MONGODB_ORQ_PASSWORD")
    mongo_cluster_name = Variable.get("MONGODB_ORQ_CLUSTER")
    mongo_db = Variable.get("MONGODB_ORQ_DATABASE_CN")

    mongo_client = MongoClient("mongodb+srv://"+mongo_user+":"+mongo_pass+"@"+mongo_cluster_name+".reeld.mongodb.net/"+mongo_db+"?authMechanism=SCRAM-SHA-1")
    mongo_collection = mongo_client[mongo_db]["slack"]

    execution_date = datetime.strptime(ts[:10], "%Y-%m-%d")
    local_tz = pytz.timezone("America/Santiago")
    date_from = local_tz.localize(execution_date).astimezone(pytz.utc)
    date_to = date_from + timedelta(days=1)

    print(date_from)
    print(date_to)

    x = mongo_collection.find({"actionDetail.dateCreated": {"$gte": date_from, "$lt": date_to}})

    x = mongo_collection.find()
    documents = list(x)

    new_documents = []

    for document in documents:
        sequence = document["sequence"]
        for actionDetail in document["actionDetail"]:
            new_document = {}
            new_document["sequence"] = sequence
            new_document["actionName"] = actionDetail["actionName"]
            new_document["dateCreated"] = actionDetail["dateCreated"]
            new_documents.append(new_document)

    if len(new_documents) == 0:
        print("No records found.")
        return
          
    df = pd.DataFrame(new_documents)
    print(df)
    
    column_names = {
        "sequence": "id_orden",
        "actionName": "nombre_accion",
        "dateCreated": "fecha_creacion"
    }

    column_types = {
        "id_orden": "int",
        "nombre_accion": "str",
        "fecha_creacion": "str"
    }

    df = df.rename(columns=column_names)

    for column in column_types.keys():
        if column not in df.columns:
            df[column] = None

    df = df.astype(column_types, errors="ignore")


    columns = ["fecha_creacion"]

    columns_query = ",".join(columns)
    excluded_query = ",".join(["EXCLUDED."+column for column in columns])
    values_query = "%s, %s,"+",".join(["%s" for column in columns])
    df = df.fillna("NULL")
    records = list(df.to_records(index=False))
    
    # Change data types to native python types
    fixed_records = []
    for record in records:
        fixed_record = []
        for value in record:
            if isinstance(value, np.generic):
                fixed_record.append(value.item())
            elif value == "NULL":
                fixed_record.append(None)
            else:
                fixed_record.append(value)
        fixed_records.append(tuple(fixed_record))
    print(f"Number of records to load: {str(len(fixed_records))}")
    incremental_query = """
        INSERT INTO ecommdata.notificaciones_clientes (id_orden, nombre_accion,"""+columns_query+""") 
        VALUES ("""+values_query+""")
        ON CONFLICT (id_orden, nombre_accion)
        DO UPDATE SET ("""+columns_query+""") = ("""+excluded_query+""") 
    """
    print(incremental_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.executemany(incremental_query, fixed_records)
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    print("Data loaded to Postgres: ecommdata.ubicacion_mfc")
    return

default_args = {
        "owner": "ecommerce_data",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
    }
with DAG(
        'etl_load_client_notifications_to_postgres',
        default_args=default_args,
        description="Carga a postgres de notificación de clientes desde el orquestador mongoDB.",
        schedule_interval="0 3 * * *",
        start_date=pendulum.datetime(2023, 3, 19, tz="America/Santiago"),
        catchup=True,
        max_active_runs = 1,
        tags=["mongo", "postgres", "MATIAS"],
    ) as dag:
        dag.doc_md = """
            Carga a postgres de notificación de clientes desde el orquestador mongoDB.
        """ 

        t0 = PythonOperator(
            task_id = "mongo_to_postgres",
            python_callable = mongo_to_postgres,
        )

        t0