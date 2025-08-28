from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from datetime import datetime, timedelta

# Funcion auxiliar para carga de dataframes con upsert
def _upsert_records(df, table_name, engine, schema="ecommdata"):
    import numpy as np
    import pandas as pd

    if df.empty:
        print("No records to insert.")
        return

    # Auto-cast para columnas de fecha (imitando to_sql)
    for col in df.columns:
        if col.lower().startswith("fecha"):
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    columns = [c for c in df.columns if c != "id"]
    columns_query = ",".join(columns)
    excluded_query = ",".join([f"EXCLUDED.{c}" for c in columns])
    values_query = "%s," + ",".join(["%s" for _ in columns])

    df = df.fillna("NULL")
    records = list(df.to_records(index=False))

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

    incremental_query = f"""
        INSERT INTO {schema}.{table_name} (id,{columns_query})
        VALUES ({values_query})
        ON CONFLICT (id)
        DO UPDATE SET ({columns_query}) = ({excluded_query});
    """
    print(f"Upserting {len(fixed_records)} records into {table_name}...")

    with engine.begin() as conn:
        conn.execute(incremental_query, fixed_records)


def _payments_incremental_load(ts):
    import pandas as pd
    import pymongo
    import pytz
    from sqlalchemy import create_engine

    mongo_user = Variable.get("MIDDLEWARE_PAGOS_MONGODB_USER")
    mongo_pass = Variable.get("MIDDLEWARE_PAGOS_MONGODB_PASSWORD")
    mongo_db = Variable.get("MIDDLEWARE_PAGOS_MONGODB_DATABASE")
    mongo_host = Variable.get("MIDDLEWARE_PAGOS_MONGODB_HOST")
    myclient = pymongo.MongoClient("mongodb+srv://"+mongo_user+":"+mongo_pass+"@"+mongo_host+"/"+mongo_db+"?authSource=admin&ssl=true")
    
    mydb = myclient[mongo_db]
    mycollection = mydb["payments"]

    execution_date = datetime.strptime(ts[:10], "%Y-%m-%d")
    local_tz = pytz.timezone("America/Santiago")
    date_from = local_tz.localize(execution_date).astimezone(pytz.utc)
    date_to = date_from + timedelta(days=1)

    print(date_from)
    print(date_to)

    x = mycollection.find({"createdAt": {"$gte": date_from, "$lt": date_to}})
    documents = list(x)
    print(f"Documentos encontrados {len(documents)}")

    if len(documents) == 0:
        print("No records found.")
        return

    df = pd.DataFrame(documents)

    df = df[df["buyOrder"].str.isnumeric()]

    column_names = {
        "_id": "id",
        "buyOrder": "pedido",
        "buyAmount": "monto_venta",
        "status": "estado",
        "channel": "canal",
        "salesChannel": "canal_venta",
        "balance": "balance",
        "inscription": "inscripcion",
        "gateway": "operador",
        "commerceCode": "codigo_comercio",
        "createdAt": "fecha_creacion"
    }

    column_types = {
        "id": "string",
        "pedido": "int64",
        "monto_venta": "int",
        "estado": "string",
        "canal": "string",
        "canal_venta": "int",
        "balance": "int",
        "inscripcion": "string",
        "operador": "string",
        "codigo_comercio": "string",
        "fecha_creacion": "string"
    }

    df = df.rename(columns=column_names)

    for column in column_types.keys():
        if column not in df.columns:
            df[column] = None

    df = df[[
        "id",
        "pedido",
        "monto_venta",
        "estado",
        "canal",
        "canal_venta",
        "balance",
        "inscripcion",
        "operador",
        "codigo_comercio",
        "fecha_creacion"
    ]]

    df = df.astype(column_types, errors="ignore")
    df["canal_venta"] = pd.to_numeric(df["canal_venta"], errors="coerce")

    psql_host = Variable.get("POSTGRESQL_HOST")
    psql_user = Variable.get("POSTGRESQL_USER")
    psql_pass = Variable.get("POSTGRESQL_PASSWORD")
    psql_db = Variable.get("POSTGRESQL_DB")
    engine = create_engine("postgresql://"+psql_user+":"+psql_pass+"@"+psql_host+":5432/"+psql_db)

    # Se insertan los datos
    print(f"Insertando {len(df.index)} registros...")
    _upsert_records(df, "mw_pagos", engine)

    return

def _operations_incremental_load(ts, ti):
    import pandas as pd
    import pymongo
    import pytz
    from sqlalchemy import create_engine

    mongo_user = Variable.get("MIDDLEWARE_PAGOS_MONGODB_USER")
    mongo_pass = Variable.get("MIDDLEWARE_PAGOS_MONGODB_PASSWORD")
    mongo_db = Variable.get("MIDDLEWARE_PAGOS_MONGODB_DATABASE")
    mongo_host = Variable.get("MIDDLEWARE_PAGOS_MONGODB_HOST")
    myclient = pymongo.MongoClient("mongodb+srv://"+mongo_user+":"+mongo_pass+"@"+mongo_host+"/"+mongo_db+"?authSource=admin&ssl=true")
    
    mydb = myclient[mongo_db]
    mycollection = mydb["operations"]

    execution_date = datetime.strptime(ts[:10], "%Y-%m-%d")
    local_tz = pytz.timezone("America/Santiago")
    date_from = local_tz.localize(execution_date).astimezone(pytz.utc)
    date_to = date_from + timedelta(days=1)

    print(date_from)
    print(date_to)

    x = mycollection.find({"createdAt": {"$gte": date_from, "$lt": date_to}})
    documents = list(x)
    print(f"Documentos encontrados {len(documents)}")

    if len(documents) == 0:
        print("No records found.")
        return

    df = pd.DataFrame(documents)

    column_names = {
        "_id": "id",
        "type": "tipo",
        "status": "estado",
        "channel": "canal",
        "errorMsg": "mensaje_error",
        "inscriptionId": "id_inscripcion",
        "paymentId": "id_pago",
        "createdAt": "fecha_creacion"
    }

    column_types = {
        "id": "string",
        "tipo": "string",
        "estado": "string",
        "canal": "string",
        "mensaje_error": "string",
        "id_inscripcion": "string",
        "id_pago": "string",
        "fecha_creacion": "string"
    }

    df = df.rename(columns=column_names)

    for column in column_types.keys():
        if column not in df.columns:
            df[column] = None

    df = df[[
        "id",
        "tipo",
        "estado",
        "canal",
        "mensaje_error",
        "id_inscripcion",
        "id_pago",
        "fecha_creacion"
    ]]

    df = df.astype(column_types, errors="ignore")

    id_cobros = []
    id_refunds = []
    for index, row in df.iterrows():
        if row["tipo"] in ("payment-transaction-authorize", "pos-pre-settlement"):
            id_cobros.append(row["id"])
        elif row["tipo"] == "payment-transaction-refund":
            id_refunds.append(row["id"])
        else:
            continue

    psql_host = Variable.get("POSTGRESQL_HOST")
    psql_user = Variable.get("POSTGRESQL_USER")
    psql_pass = Variable.get("POSTGRESQL_PASSWORD")
    psql_db = Variable.get("POSTGRESQL_DB")
    engine = create_engine("postgresql://"+psql_user+":"+psql_pass+"@"+psql_host+":5432/"+psql_db)

    # Se insertan los datos
    print(f"Insertando {len(df.index)} registros...")
    _upsert_records(df, "mw_operaciones", engine)

    # Send id lists to S3
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    date_path = ts[:10].replace("-","/")
    s3_path = f"mw_pagos/{date_path}/"
    charges_key = s3_path+"charges"
    refunds_key = s3_path+"refunds"

    s3_hook.load_string(str(id_cobros),charges_key,bucket_name=s3_bucket,replace=True)
    s3_hook.load_string(str(id_refunds),refunds_key,bucket_name=s3_bucket,replace=True)

    ti.xcom_push(key='charges_list_file', value=charges_key)
    ti.xcom_push(key='refunds_list_file', value=refunds_key)

    return charges_key, refunds_key

def _interactions_incremental_load(ti):
    from bson.objectid import ObjectId
    import pandas as pd
    import pymongo
    from sqlalchemy import create_engine

    mongo_user = Variable.get("MIDDLEWARE_PAGOS_MONGODB_USER")
    mongo_pass = Variable.get("MIDDLEWARE_PAGOS_MONGODB_PASSWORD")
    mongo_db = Variable.get("MIDDLEWARE_PAGOS_MONGODB_DATABASE")
    mongo_host = Variable.get("MIDDLEWARE_PAGOS_MONGODB_HOST")
    myclient = pymongo.MongoClient("mongodb+srv://"+mongo_user+":"+mongo_pass+"@"+mongo_host+"/"+mongo_db+"?authSource=admin&ssl=true")
    mydb = myclient[mongo_db]
    mycollection = mydb["thirdPartyInteractions"]

    charges_file = ti.xcom_pull(key="charges_list_file", task_ids=["operations_incremental_load"])[0]
    refunds_file = ti.xcom_pull(key="refunds_list_file", task_ids=["operations_incremental_load"])[0]
    
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+charges_file)
    if not s3_hook.check_for_key(charges_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % charges_file)
    print("Searching file: "+refunds_file)
    if not s3_hook.check_for_key(refunds_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % refunds_file)

    charges_object = s3_hook.get_key(charges_file, bucket_name=s3_bucket)
    refunds_object = s3_hook.get_key(refunds_file, bucket_name=s3_bucket)

    id_cobros_string = charges_object.get()["Body"].read().decode('utf-8')[1:-1]
    id_cobros = id_cobros_string.split(",") if id_cobros_string != "" else []
    id_cobros = [id.replace("'", "").strip() for id in id_cobros]
    id_refunds_string = refunds_object.get()["Body"].read().decode('utf-8')[1:-1]
    id_refunds = id_refunds_string.split(",") if id_refunds_string != "" else []
    id_refunds = [id.replace("'", "").strip() for id in id_refunds]

    new_documents = []

    # Cobros:
    print(f"Cobros: {len(id_cobros)}")
    count = 0
    flag = True
    while(flag):
        operation_ids_sub = id_cobros[count*1000:(count+1)*1000]
        print(f"FROM {count*1000}")
        print(len(operation_ids_sub))
        if len(operation_ids_sub) == 0:
            flag = False
            break
        
        operation_objects = [ObjectId(id) for id in operation_ids_sub]

        x = mycollection.find({"operation": {"$in": operation_objects}})
        documents = list(x)
        print(len(documents))

        for document in documents:
            new_document = {}
            request = document["request"]
            response = document["response"]
            if "transbank" in request["url"]:
                new_document["id"] = str(document["_id"])
                new_document["id_operacion"] = document["operation"]
                new_document["api_status_code"] = response.get("code", None)
                new_document["secuencia"] = document.get("sequence", None)
                new_document["url"] = request["url"]
                new_document["metodo"] = request.get("method", None)
                new_document["fecha_creacion"] = request["createdAt"]
                try:
                    if type(response["body"]) is not dict:
                        new_document["fecha_transaccion"] = response["createdAt"]
                        new_document["mensaje_error"] = "Invalid response body."
                        new_document["estado"] = "FAILED"
                        new_document["monto"] = request["body"]["details"][0]["amount"]
                        new_document["orden"] = request["body"]["buy_order"]
                    else:
                        new_document["fecha_transaccion"] = response["body"].get("transaction_date", None)
                        new_document["orden"] = request["body"]["buy_order"]
                        details = response["body"]["details"][0]
                        new_document["monto"] = details["amount"]
                        new_document["codigo_comercio"] = details["commerce_code"]
                        new_document["codigo_respuesta_tbk"] = details["response_code"]
                        new_document["estado"] = details["status"]
                        new_document["tipo_pago"] = details["payment_type_code"]
                        new_document["codigo_autorizacion"] = details.get("authorization_code", None)
                        new_document["mensaje_error"] = None
                except KeyError as e:
                    body = response.get("body", {})
                    new_document["mensaje_error"] = body.get("error_message", None)
                new_documents.append(new_document)
            else:
                continue
        count = count + 1 

    # Refunds:
    print(f"Refunds: {len(id_refunds)}")
    count = 0
    flag = True
    while(flag):
        operation_ids_sub = id_refunds[count*1000:(count+1)*1000]
        print(f"FROM {count*1000}")
        print(len(operation_ids_sub))
        if len(operation_ids_sub) == 0:
            flag = False
            break
        
        operation_objects = [ObjectId(id) for id in operation_ids_sub]

        x = mycollection.find({"operation": {"$in": operation_objects}})
        documents = list(x)
        print(len(documents))

        for document in documents:
            new_document = {}
            request = document["request"]
            response = document["response"]
            if "transbank" in request["url"]:
                new_document["id"] = str(document["_id"])
                new_document["id_operacion"] = document["operation"]
                new_document["api_status_code"] = response.get("code", None)
                new_document["secuencia"] = document.get("sequence", None)
                new_document["url"] = request["url"]
                new_document["metodo"] = request.get("method", None)
                new_document["fecha_creacion"] = request["createdAt"]
                try:
                    if type(response["body"]) is not dict:
                        new_document["fecha_transaccion"] = response["createdAt"]
                        new_document["mensaje_error"] = "Invalid response body."
                        new_document["estado"] = "FAILED"
                        new_document["monto"] = request["body"].get("amount", None)
                        new_document["orden"] = request["body"].get("detail_buy_order", None)
                    else:
                        new_document["orden"] = request["body"]["detail_buy_order"]
                        new_document["codigo_comercio"] = request["body"]["commerce_code"]
                        new_document["estado"] = response["body"]["type"]
                        new_document["monto"] = -response["body"]["nullified_amount"]
                        new_document["codigo_autorizacion"] = response["body"]["authorization_code"]
                        new_document["codigo_respuesta_tbk"] = response["body"]["response_code"]
                        new_document["fecha_transaccion"] = response["body"].get("authorization_date", None)
                        new_document["mensaje_error"] = None
                        new_document["tipo_pago"] = None
                except KeyError as e:
                    body = response.get("body", {})
                    new_document["monto"] = -request["body"].get("amount", 0)
                    new_document["mensaje_error"] = body.get("error_message", None)
                    new_document["fecha_transaccion"] = request.get("createdAt", None)
                new_documents.append(new_document)
            else:
                continue
        count = count + 1 

    print("---------------------------------------")
    print(len(new_documents))

    if len(new_documents) == 0:
        print("No records found.")
        return

    df = pd.DataFrame(new_documents)

    column_types = {
        "id": "string",
        "id_operacion": "string",
        "api_status_code": "int",
        "secuencia": "int",
        "url": "string",
        "mensaje_error": "string",
        "monto": "int",
        "codigo_autorizacion": "string",
        "codigo_comercio": "string",
        "metodo": "string",
        "orden": "string",
        "codigo_respuesta_tbk": "int",
        "estado": "string",
        "tipo_pago": "string"
    }

    df = df.astype(column_types, errors="ignore")

    df["codigo_autorizacion"] = df["codigo_autorizacion"].str.zfill(6)

    psql_host = Variable.get("POSTGRESQL_HOST")
    psql_user = Variable.get("POSTGRESQL_USER")
    psql_pass = Variable.get("POSTGRESQL_PASSWORD")
    psql_db = Variable.get("POSTGRESQL_DB")
    engine = create_engine("postgresql://"+psql_user+":"+psql_pass+"@"+psql_host+":5432/"+psql_db)

    # Se insertan los datos
    print(f"Insertando {len(df.index)} registros...")
    _upsert_records(df, "mw_interacciones_tbk", engine)

    return

def _inscriptions_incremental_load(ts):
    import pandas as pd
    import pymongo
    import pytz
    from sqlalchemy import create_engine

    mongo_user = Variable.get("MIDDLEWARE_PAGOS_MONGODB_USER")
    mongo_pass = Variable.get("MIDDLEWARE_PAGOS_MONGODB_PASSWORD")
    mongo_db = Variable.get("MIDDLEWARE_PAGOS_MONGODB_DATABASE")
    mongo_host = Variable.get("MIDDLEWARE_PAGOS_MONGODB_HOST")
    myclient = pymongo.MongoClient("mongodb+srv://"+mongo_user+":"+mongo_pass+"@"+mongo_host+"/"+mongo_db+"?authSource=admin&ssl=true")
    
    mydb = myclient[mongo_db]
    mycollection = mydb["inscriptions"]

    execution_date = datetime.strptime(ts[:10], "%Y-%m-%d")
    local_tz = pytz.timezone("America/Santiago")
    date_from = local_tz.localize(execution_date).astimezone(pytz.utc)
    date_to = date_from + timedelta(days=1)

    print(date_from)
    print(date_to)

    x = mycollection.find({"createdAt": {"$gte": date_from, "$lt": date_to}})
    documents = list(x)
    print(f"Documentos encontrados {len(documents)}")

    if len(documents) == 0:
        print("No records found.")
        return

    new_documents = []
    for record in documents:
        new_document = {}
        inscription_id = str(record["_id"])
        new_document["id"] = inscription_id
        new_document["num_tarjeta"] = record.get("cardNumber", None)
        new_document["tipo_tarjeta"] = record.get("cardType", None)
        new_document["canal"] = record.get("channel", None)
        new_document["fecha_creacion"] = record.get("createdAt", None)
        new_document["fecha_borrado"] = record.get("deletedAt", None)
        new_document["procesador"] = record.get("gateway", {}).get("name", None)
        new_document["cod_autorizacion"] = record.get("gateway", {}).get("data", {}).get("authorizationCode", None)
        new_document["tipo"] = record.get("type", None)
        new_document["secuencia"] = record.get("sequence", None)
        new_documents.append(new_document)

    df = pd.DataFrame(new_documents)

    column_types = {
        "id": "string",
        "tipo": "string",
        "num_tarjeta": "string",
        "tipo_tarjeta": "string",
        "canal": "string",
        "fecha_creacion": "string",
        "fecha_borrado": "string",
        "procesador": "string",
        "cod_autorizacion": "int",
        "secuencia": "int"
    }

    for column in column_types.keys():
        if column not in df.columns:
            df[column] = None

    df = df[[
        "id",
        "tipo",
        "num_tarjeta",
        "tipo_tarjeta",
        "canal",
        "fecha_creacion",
        "fecha_borrado",
        "procesador",
        "cod_autorizacion",
        "secuencia"
    ]]

    df = df.astype(column_types, errors="ignore")

    psql_host = Variable.get("POSTGRESQL_HOST")
    psql_user = Variable.get("POSTGRESQL_USER")
    psql_pass = Variable.get("POSTGRESQL_PASSWORD")
    psql_db = Variable.get("POSTGRESQL_DB")
    engine = create_engine("postgresql://"+psql_user+":"+psql_pass+"@"+psql_host+":5432/"+psql_db)

    # Se insertan los datos
    print(f"Insertando {len(df.index)} registros...")
    _upsert_records(df, "mw_inscripciones", engine)

    return

def _transfers_incremental_load(ts):
    import numpy as np
    import pandas as pd
    import pymongo

    max_dates_query = """
        SELECT MAX(fecha_creacion)
            , MAX(fecha_modificacion)
        FROM ecommdata.mw_transferencias
    """

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(max_dates_query)
    max_dates = cursor.fetchone()
    max_created_at = max_dates[0]
    max_updated_at = max_dates[1]
    cursor.close()
    pg_connection.close()

    if max_created_at is None:
        max_created_at = datetime.strptime("1999-01-01", "%Y-%m-%d")
    if max_updated_at is None:
        max_updated_at = datetime.strptime("1999-01-01", "%Y-%m-%d")
    
    print(f"CreatedAt from: {max_created_at}")
    print(f"UpdatedAt from: {max_updated_at}")

    mongo_user = Variable.get("MIDDLEWARE_PAGOS_MONGODB_USER")
    mongo_pass = Variable.get("MIDDLEWARE_PAGOS_MONGODB_PASSWORD")
    mongo_db = Variable.get("MIDDLEWARE_PAGOS_MONGODB_DATABASE")
    mongo_host = Variable.get("MIDDLEWARE_PAGOS_MONGODB_HOST")
    myclient = pymongo.MongoClient("mongodb+srv://"+mongo_user+":"+mongo_pass+"@"+mongo_host+"/"+mongo_db+"?authSource=admin&ssl=true")
    
    mydb = myclient[mongo_db]
    mycollection = mydb["transfers"]

    x = mycollection.find({
        "$or": [
            {
                "createAt": {
                    "$gt": max_created_at
                }
            },
            {
                "updatedAt": {
                    "$gt": max_updated_at
                }
            }
        ]
    })
    documents = list(x)
    print(f"Documentos encontrados {len(documents)}")

    if len(documents) == 0:
        print("No records found.")
        return

    new_documents = []
    for document in documents:
        new_document = {}
        new_document["id"] = str(document["_id"])
        new_document["id_orden"] = document.get("orderId", None)
        new_document["monto"] = document.get("orderAmount", None)
        new_document["estado"] = document.get("operationStatus", None)
        new_document["tienda"] = document.get("store", None)
        new_document["nombre_tienda"] = document.get("storeName", None)
        new_document["fecha_creacion"] = document.get("createAt", None)
        new_document["fecha_modificacion"] = document.get("updatedAt", None)
        new_document["id_transaccion"] = document.get("transactionId", None)
        new_document["canal"] = document.get("channelType", None)
        new_document["referencia"] = document.get("reference", None)

        new_documents.append(new_document)

    df = pd.DataFrame(new_documents)

    columns = [
        "id_orden",
        "monto",
        "estado",
        "tienda",
        "nombre_tienda",
        "fecha_creacion",
        "fecha_modificacion",
        "id_transaccion",
        "canal",
        "referencia"
    ]

    column_types = {
        "id": "string",
        "id_orden": "string",
        "monto": "int",
        "estado": "string",
        "tienda": "string",
        "nombre_tienda": "string",
        "fecha_creacion": "string",
        "fecha_modificacion": "string",
        "id_transaccion": "string",
        "canal": "string",
        "referencia": "string"
    }

    df = df.astype(column_types, errors="ignore")

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
        INSERT INTO ecommdata.mw_transferencias (id,"""+columns_query+""") 
        VALUES ("""+values_query+""")
        ON CONFLICT (id)
        DO UPDATE SET ("""+columns_query+""") = ("""+excluded_query+""") ;
    """
    print(incremental_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.executemany(incremental_query, fixed_records)
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    print("Data loaded to Postgres")

    return

def _vtex_payments_incremental_load():

    import numpy as np
    import pandas as pd
    import pymongo

    max_dates_query = """
        SELECT MAX(fecha_creacion)
            , MAX(fecha_modificacion)
        FROM ecommdata.mw_pagos_vtex
    """

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(max_dates_query)
    max_dates = cursor.fetchone()
    max_created_at = max_dates[0]
    max_updated_at = max_dates[1]
    cursor.close()
    pg_connection.close()

    if max_created_at is None:
        max_created_at = datetime.strptime("1999-01-01", "%Y-%m-%d")
    if max_updated_at is None:
        max_updated_at = datetime.strptime("1999-01-01", "%Y-%m-%d")
    
    print(f"CreatedAt from: {max_created_at}")
    print(f"UpdatedAt from: {max_updated_at}")

    mongo_user = Variable.get("MIDDLEWARE_PAGOS_MONGODB_USER")
    mongo_pass = Variable.get("MIDDLEWARE_PAGOS_MONGODB_PASSWORD")
    mongo_db = Variable.get("MIDDLEWARE_PAGOS_MONGODB_DATABASE")
    mongo_host = Variable.get("MIDDLEWARE_PAGOS_MONGODB_HOST")
    myclient = pymongo.MongoClient("mongodb+srv://"+mongo_user+":"+mongo_pass+"@"+mongo_host+"/"+mongo_db+"?authSource=admin&ssl=true")
    
    mydb = myclient[mongo_db]
    mycollection = mydb["vtexPayments"]

    x = mycollection.find({
        "$or": [
            {
                "createdAt": {
                    "$gt": max_created_at
                }
            },
            {
                "updatedAt": {
                    "$gt": max_updated_at
                }
            }
        ]
    })
    documents = list(x)
    print(f"Documentos encontrados {len(documents)}")

    if len(documents) == 0:
        print("No records found.")
        return

    new_documents = []
    for document in documents:
        new_document = {}
        new_document["id"] = str(document["_id"])
        new_document["id_operacion"] = str(document.get("operation", None))
        new_document["estado"] = document.get("status", None)
        new_document["canal"] = document.get("channelSlug", None)
        new_document["id_transaccion"] = document.get("transactionId", None)
        new_document["id_orden"] = document.get("orderId", None)
        new_document["monto"] = document.get("amount", None)
        new_document["valor"] = document.get("value", None)
        new_document["medio_de_pago"] = document.get("paymentMethod", None)
        new_document["fecha_creacion"] = document.get("createdAt", None)
        new_document["fecha_modificacion"] = document.get("updatedAt", None)

        new_documents.append(new_document)

    df = pd.DataFrame(new_documents)

    columns = [
        "id_operacion",
        "estado",
        "canal",
        "id_transaccion",
        "id_orden",
        "monto",
        "valor",
        "medio_de_pago",
        "fecha_creacion",
        "fecha_modificacion"
    ]

    column_types = {
        "id": "string",
        "id_operacion": "string",
        "estado": "string",
        "canal": "string",
        "id_transaccion": "string",
        "id_orden": "string",
        "monto": "int",
        "valor": "int",
        "medio_de_pago": "string",
        "fecha_creacion": "string",
        "fecha_modificacion": "string"
    }

    df = df.astype(column_types, errors="ignore")

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
        INSERT INTO ecommdata.mw_pagos_vtex (id,"""+columns_query+""") 
        VALUES ("""+values_query+""")
        ON CONFLICT (id)
        DO UPDATE SET ("""+columns_query+""") = ("""+excluded_query+""") ;
    """
    print(incremental_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.executemany(incremental_query, fixed_records)
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    print("Data loaded to Postgres")

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_mw_pagos_tablas_incremental_load',
    default_args=default_args,
    description="Extracción y carga de tablas: pagos; operaciones e interacciones desde Middleware de Pagos hasta el Workspace en Postgresql.",
    schedule_interval="0 */3 * * *",
    start_date=datetime(2022, 4, 1),
    catchup=False,
    max_active_runs = 1,
    tags=["DATA", "middleware_pagos", "ecommdata", "mw_pagos", "mw_operaciones", "mw_interacciones", "mw_inscripciones", "MATIAS"],
) as dag:

    dag.doc_md = """
    Extracción y carga de tablas pagos, operaciones e interacciones desde Middleware de Pagos. \n
    Carga diaria de tablas completas. \n
    """ 
    t0 = PythonOperator(
        task_id = "payments_incremental_load",
        python_callable = _payments_incremental_load
    )

    t1 = PythonOperator(
        task_id = "operations_incremental_load",
        python_callable = _operations_incremental_load
    )

    t2 = PythonOperator(
        task_id = "interactions_incremental_load",
        python_callable = _interactions_incremental_load
    )

    t3 = PythonOperator(
        task_id = "inscriptions_incremental_load",
        python_callable = _inscriptions_incremental_load
    )

    t4 = PythonOperator(
        task_id = "transfers_incremental_load",
        python_callable = _transfers_incremental_load
    )

    t5 = PythonOperator(
        task_id = "vtex_payments_incremental_load",
        python_callable = _vtex_payments_incremental_load
    )

    t0 >> t1 >> t2 >> t3 >> t4 >> t5
