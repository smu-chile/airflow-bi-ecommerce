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
    mongo_db = Variable.get("MONGODB_ORQ_DATABASE_OS")
    mongo_client = MongoClient(f"mongodb+srv://{mongo_user}:{mongo_pass}@{mongo_cluster_name}.reeld.mongodb.net/{mongo_db}?authMechanism=SCRAM-SHA-1")
    mongo_collection = mongo_client[mongo_db]["lastmiller_planning"]

    #execution_date = datetime.strptime(ts[:10], "%Y-%m-%d")
    #local_tz = pytz.timezone("America/Santiago")
    #date_from = local_tz.localize(execution_date).astimezone(pytz.utc)
    #date_to = date_from + timedelta(days=1)

    #print(date_from)
    #print(date_to)

    x = mongo_collection.find()
    documents = list(x)

    new_documents = []

    for document in documents:
        _id = str(document["_id"])
        createdAt = document["createdAt"]
        updatedAt = document["updatedAt"]
        driverName = document["driver"]["name"]
        driverDocumentNumber = document["driver"]["documentNumber"]
        driverMobileNumber = document["driver"]["mobileNumber"]
        documentOperator = document["documentOperator"]
        for row in document["assignmentControl"]:
            new_document = {}
            new_document["_id"] = _id
            new_document["createdAt"] = createdAt
            new_document["updatedAt"] = updatedAt
            new_document["driverName"] = driverName
            try:
                new_document["driverDocumentNumber"] = driverDocumentNumber
            except Exception as e:
                new_document["driverDocumentNumber"] = None
            try:
                new_document["driverMobileNumber"] = driverMobileNumber
            except Exception as e:
                new_document["driverMobileNumber"] = None
            new_document["documentOperator"] = documentOperator
            new_document["sequence"] = row["sequence"]
            new_document["stopNumber"] = row["stopNumber"]
            new_document["priority"] = row["priority"]
            new_document["chargingGroup"] = row["chargingGroup"]
            new_document["chargingTruck"] = row["chargingTruck"]
            new_document["serviceWindow"] = row["serviceWindow"]
            new_document["stageByDateTime"] = row["stageByDateTime"]
            new_document["ultima_fecha_orden"] = False
            new_documents.append(new_document)


    if len(new_documents) == 0:
        print("No records found.")
        return
          
    df = pd.DataFrame(new_documents)
    print(df)
    
    column_names = {
        "_id": "id_viaje",
        "sequence": "id_orden",
        "createdAt": "fecha_creacion",
        "updatedAt": "fecha_modificacion",
        "driverName": "nombre_conductor",
        "driverDocumentNumber": "documento_conductor",
        "driverMobileNumber": "numero_conductor",
        "documentOperator": "documento_operador",
        "stopNumber": "numero_parada",
        "priority": "prioridad",
        "chargingGroup": "grupo_de_carga",
        "chargingTruck": "patente",
        "serviceWindow": "ventana_de_servicio",
        "stageByDateTime": "stage_by_datetime"

    }   

    column_types = {
        "id_viaje": "str",
        "id_orden": "int",
        "fecha_creacion": "str",
        "fecha_modificacion": "str",
        "nombre_conductor": "str",
        "documento_conductor": "str",
        "numero_conductor": "int",
        "documento_operador": "str",
        "numero_parada": "int",
        "prioridad": "int",
        "grupo_de_carga": "int",
        "patente": "str",
        "ventana_de_servicio": "str",
        "stage_by_datetime": "str",
        "ultima_fecha_orden": "bool"
        
    }

    df = df.rename(columns=column_names)

    for column in column_types.keys():
        if column not in df.columns:
            df[column] = None

    df = df.astype(column_types, errors="ignore")


    columns = [
        "fecha_creacion",
        "fecha_modificacion",
        "nombre_conductor",
        "documento_conductor",
        "numero_conductor",
        "documento_operador",
        "numero_parada",
        "prioridad",
        "grupo_de_carga",
        "patente",
        "ventana_de_servicio",
        "stage_by_datetime",
        "ultima_fecha_orden"
        ]

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
        INSERT INTO integraciones.lm_planning (id_viaje, id_orden,"""+columns_query+""") 
        VALUES ("""+values_query+""")
        ON CONFLICT (id_viaje, id_orden)
        DO UPDATE SET """+columns_query+""" = """+excluded_query+""" 
    """
    print(incremental_query)
    update_query = """
        with mlp as (
            select lp.id_orden, max(lp.fecha_modificacion) as fecha_modificacion
            from integraciones.lm_planning lp
            group by lp.id_orden)
        update integraciones.lm_planning lp
        set ultima_fecha_orden = true
        from mlp
        where lp.id_orden = mlp.id_orden and lp.fecha_modificacion = mlp.fecha_modificacion
    """
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.executemany(incremental_query, fixed_records)
    cursor.execute(update_query)
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    print("Data loaded to Postgres: integraciones.lm_planning")
    return

default_args = {
        "owner": "ecommerce_data",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
    }
with DAG(
        'etl_load_lm_planning',
        default_args=default_args,
        description="Carga a postgres el planning de last millers desde mongoDB.",
        schedule_interval="0 3 * * *",
        start_date=pendulum.datetime(2023, 7, 1, tz="America/Santiago"),
        max_active_runs = 1,
        tags=["mongo", "postgres", "MATIAS", "last_millers"],
    ) as dag:
        dag.doc_md = """
            Carga a postgres el planning de last millers desde mongoDB.
        """ 

        t0 = PythonOperator(
            task_id = "mongo_to_postgres",
            python_callable = mongo_to_postgres,
        )

        t0