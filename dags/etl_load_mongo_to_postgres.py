from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable


import pendulum

from datetime import datetime, date



def mongo_to_postgres():
    import pandas as pd
    from pymongo import MongoClient
    import numpy as np

    mongo_user = Variable.get("MONGODB_ORQ_USER")
    mongo_pass = Variable.get("MONGODB_ORQ_PASSWORD")
    mongo_cluster_name = Variable.get("MONGODB_ORQ_CLUSTER")
    mongo_db = Variable.get("MONGODB_ORQ_DATABASE")

    mongo_client = MongoClient("mongodb+srv://"+mongo_user+":"+mongo_pass+"@"+mongo_cluster_name+".reeld.mongodb.net/"+mongo_db+"?authMechanism=SCRAM-SHA-1")
    mongo_collection = mongo_client[mongo_db]["im_products"]
    documents = mongo_collection.find()

    print("mongodb+srv://"+mongo_user+":"+mongo_pass+"@"+mongo_cluster_name+".reeld.mongodb.net/"+mongo_db+"?authMechanism=SCRAM-SHA-1")
    print("se ha conectado a mongodb")
    
    data = []

    for doc in documents:
        row = {
            "_id": str(doc["_id"]),
            "SapCode": doc["SapCode"],
            "EanCode": doc["EanCode"],
            "Store": doc["Store"],
            "MeasurementUnit": doc["MeasurementUnit"],
            "MFCIsItemInside": doc["MFCIsItemInside"],
            "createdAt": doc["createdAt"],
            "updatedAt": doc["updatedAt"]
        }

        data.append(row)
    df = pd.DataFrame(data)
    print(df)
    print("se va a renombras las columnas ")
    df.columns = ["_id","sap_code","ean_code","store","measurement_unit","mfc_is_item_side","created_date","update_date"]
    print("se han renombrado las columnas ")
    df['created_date'] = df['created_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
    df['update_date'] = df['update_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
    print(df)

    # # Ensure correct datatypes:
    df["_id"] = df["_id"].astype("str", errors="ignore")
    df["sap_code"] = df["sap_code"].astype("str", errors="ignore")
    df["ean_code"] = df["ean_code"].astype("str", errors="ignore")
    df["store"] = df["store"].astype("str", errors="ignore")
    df["measurement_unit"] = df["measurement_unit"].astype("str", errors="ignore")
    df["mfc_is_item_side"] = df["mfc_is_item_side"].astype("str", errors="ignore")


    columns = ["sap_code","ean_code","store","measurement_unit","mfc_is_item_side","created_date","update_date"]

    columns_query = ",".join(columns)
    excluded_query = ",".join(["EXCLUDED."+column for column in columns])
    values_query = "%s,"+",".join(["%s" for column in columns])
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
        INSERT INTO ecommdata.ubicacion_mfc (_id,"""+columns_query+""") 
        VALUES ("""+values_query+""")
        ON CONFLICT (_id)
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
        'etl_load_mongo_to_postgres',
        default_args=default_args,
        description="carga a postgres desde mongodb",
        schedule_interval="0 8 * * *",
        start_date=pendulum.datetime(2023, 5, 24, tz="America/Santiago"),
        catchup=False,
        max_active_runs = 1,
        tags=["mongo", "postgres", "PATRICIO"],
    ) as dag:
        dag.doc_md = """
        funciona. \n
        UPSERT incremental basado en fecha_modificacion_unixtime.
        """ 

        t0 = PythonOperator(
            task_id = "mongo_to_postgres",
            python_callable = mongo_to_postgres,
        )

        t0