from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from datetime import datetime, timedelta, date
import boto3
import botocore
import json
import pymongo
import psycopg2
import pytz
import requests

def check_process_run():
    mongo_user = Variable.get("MONGODB_USER")
    mongo_pass = Variable.get("MONGODB_PASSWORD")
    mongo_cluster_name = Variable.get("MONGODB_CLUSTER")
    mongo_db = Variable.get("MONGODB_DATABASE")
    mongo_client = pymongo.MongoClient("mongodb+srv://"+mongo_user+":"+mongo_pass+"@"+mongo_cluster_name+".lppxi.mongodb.net/"+mongo_db+"?retryWrites=true&w=majority&authSource=admin")
    mongo_collection = mongo_client[mongo_db]["stock_processed_files"]

    access_key = Variable.get("AWS_ACCESS_KEY")
    secret_key = Variable.get("AWS_SECRET_KEY")
    bucket_name = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_resource = boto3.resource("s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name="us-east-1")
    bucket = s3_resource.Bucket(bucket_name)

    local_tz = pytz.timezone('America/Santiago')
    curr_local_datetime = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(local_tz)
    prefix = "ecommops/stock_adjustment/"+curr_local_datetime.strftime("%Y/%m/%d/")
    file_name = prefix+"stage_2.csv"

    # if False, skip all downstream tasks:
    if mongo_collection.count_documents(filter={"file_name": file_name}) > 0:
        print("File already processed: "+file_name)
        return False
    else:
        try:
            bucket.Object(file_name).get()
        except botocore.exceptions.ClientError as e:
            print("File not found: "+file_name)
            return False
    print("File found: "+file_name)
    print("Starting process...")
    return True

def read_stock_adjustment_stage2_s3_file():
    access_key = Variable.get("AWS_ACCESS_KEY")
    secret_key = Variable.get("AWS_SECRET_KEY")
    bucket_name = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    print("BUCKET:")
    print(bucket_name)
    s3_resource = boto3.resource("s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name="us-east-1")
    bucket = s3_resource.Bucket(bucket_name)
    local_tz = pytz.timezone('America/Santiago')
    curr_local_datetime = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(local_tz)
    prefix = "ecommops/stock_adjustment/"+curr_local_datetime.strftime("%Y/%m/%d/")
    object_name = prefix+"stage_2.csv"
    s3_object = bucket.Object(object_name)

    mongo_user = Variable.get("MONGODB_USER")
    mongo_pass = Variable.get("MONGODB_PASSWORD")
    mongo_cluster_name = Variable.get("MONGODB_CLUSTER")
    mongo_db = Variable.get("MONGODB_DATABASE")
    mongo_client = pymongo.MongoClient("mongodb+srv://"+mongo_user+":"+mongo_pass+"@"+mongo_cluster_name+".lppxi.mongodb.net/"+mongo_db+"?retryWrites=true&w=majority&authSource=admin")
    mongo_collection = mongo_client[mongo_db]["stock_changes_stage2"]

    current_timestamp = datetime.utcnow()

    count = 0
    for line in s3_object.get()["Body"].iter_lines():
        if count == 0:
            headers = line.decode("UTF-8").split(";")
        else:
            row_data = line.decode("UTF-8").split(";")
            document = dict(zip(headers, row_data))
            document["timestamp"] = current_timestamp

            mongo_collection.insert_one(document)
        count = count + 1

    return {
        "current_timestamp": current_timestamp.strftime("%Y-%m-%dT%H:%M:%S"),
        "file_name": object_name
        }

def change_stock_on_janis_api(ti):
    mongo_user = Variable.get("MONGODB_USER")
    mongo_pass = Variable.get("MONGODB_PASSWORD")
    mongo_cluster_name = Variable.get("MONGODB_CLUSTER")
    mongo_db = Variable.get("MONGODB_DATABASE")
    mongo_client = pymongo.MongoClient("mongodb+srv://"+mongo_user+":"+mongo_pass+"@"+mongo_cluster_name+".lppxi.mongodb.net/"+mongo_db+"?retryWrites=true&w=majority&authSource=admin")
    mongo_collection = mongo_client[mongo_db]["stock_changes_stage2"]

    current_date = date.today()

    documents = mongo_collection.find({"timestamp": {"$gte": datetime(current_date.year, current_date.month, current_date.day)}})
    print(len(list(documents)))
    payload = []
    for document in documents:
        payload_element = {}
        payload_element["IdSku"] = document["material_uv"].split("-")[0]
        payload_element["Quantity"] = 0
        payload_element["Store"] = document["store_ref_id"]
        payload_element["warehouseRefId"] = document["warehouse_ref_id"]
        payload.append(payload_element)

    print(len(payload))
    if Variable.get("ENV") == "develop":
        payload = [
            {
                "IdSku": "000000000000633967",
                "Quantity": 90000,
                "Store": "telem-coti",
                "warehouseRefId": "001"
            },
            {
                "IdSku": "000000000000009780",
                "Quantity": 90000,
                "Store": "telem-coti",
                "warehouseRefId": "001"
            }
        ]
    print(payload)

    headers = {
        "Content-Type": "application/json",
        "janis-api-key": Variable.get("JANIS_API_KEY"),
        "janis-api-secret": Variable.get("JANIS_API_SECRET"),
        "janis-client": Variable.get("JANIS_CLIENT")
    }
    api_url = Variable.get("JANIS_API_URL")
    namespace = "stock"
    response = requests.post(api_url+namespace, data=json.dumps(payload), headers=headers)
    status = response.status_code
    
    xcom_dict = ti.xcom_pull(key="return_value", task_ids=["read_stock_adjustment_stage2_s3_file"])[0]
    current_timestamp = xcom_dict["current_timestamp"]
    file_name = xcom_dict["file_name"]
    process_metadata = {
        "api_response": response.json(),
        "status": response.status_code,
        "timestamp": current_timestamp,
        "file_name": file_name
    }

    return process_metadata

def record_updated_stocks(ti):
    process_metadata = dict(ti.xcom_pull(key="return_value", task_ids=["change_stock_on_janis_api"])[0])
    mongo_user = Variable.get("MONGODB_USER")
    mongo_pass = Variable.get("MONGODB_PASSWORD")
    mongo_cluster_name = Variable.get("MONGODB_CLUSTER")
    mongo_db = Variable.get("MONGODB_DATABASE")
    mongo_client = pymongo.MongoClient("mongodb+srv://"+mongo_user+":"+mongo_pass+"@"+mongo_cluster_name+".lppxi.mongodb.net/"+mongo_db+"?retryWrites=true&w=majority&authSource=admin")
    mongo_collection = mongo_client[mongo_db]["stock_changes_stage2"]

    current_date = date.today()

    mongo_query = mongo_collection.find({"timestamp": {"$gte": datetime(current_date.year, current_date.month, current_date.day)}})
    documents = list(mongo_query)
    n_records = len(list(documents))
    print(n_records)

    query_values = []
    columns = ["material", "material_uv", "store_ref_id", "warehouse_ref_id", "tienda", "stock", "descripcion", "etapa", "timestamp"]
    for document in documents:
        print(document)
        query_tuple = (document["material"], document["material_uv"],
                        document["store_ref_id"], document["warehouse_ref_id"],  
                        document["tienda"], document["stock_janis"], document["descripcion"], 
                        2, document["timestamp"].strftime("%Y-%m-%dT%H:%M:%S"))
        query_values.append(query_tuple)

    # Connect to Postgres DB:
    conn = psycopg2.connect(host=Variable.get("POSTGRESQL_HOST"), 
                            database=Variable.get("POSTGRESQL_DB"), 
                            user=Variable.get("POSTGRESQL_USER"), 
                            password=Variable.get("POSTGRESQL_PASSWORD"),
                            options="-c search_path=ecommops")

    query = "INSERT INTO stock_adjustment"
    query = query + "("+",".join(columns)+")"
    query_values = ",".join([str(value) for value in query_values])
    query = query + f" VALUES {query_values}"

    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()

    process_metadata["extra"] = {"n_records": n_records}
    return process_metadata

def record_process_metadata(ti):
    process_metadata = dict(ti.xcom_pull(key="return_value", task_ids=["record_updated_stocks"])[0])
    process_metadata["timestamp"] = datetime.strptime(process_metadata["timestamp"], "%Y-%m-%dT%H:%M:%S")
    mongo_user = Variable.get("MONGODB_USER")
    mongo_pass = Variable.get("MONGODB_PASSWORD")
    mongo_cluster_name = Variable.get("MONGODB_CLUSTER")
    mongo_db = Variable.get("MONGODB_DATABASE")
    mongo_client = pymongo.MongoClient("mongodb+srv://"+mongo_user+":"+mongo_pass+"@"+mongo_cluster_name+".lppxi.mongodb.net/"+mongo_db+"?retryWrites=true&w=majority&authSource=admin")
    mongo_collection = mongo_client[mongo_db]["stock_processed_files"]

    mongo_collection.insert_one(process_metadata)
    return

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}
with DAG(
    'stock_adjustment_stage2',
    default_args=default_args,
    description="DAG to mantain 0 stock on those SKU that have already been adjusted the previous day",
    schedule="0 10 * * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["OPS", "Janis"],
) as dag:

    dag.doc_md = """
    Segunda etapa de ajuste de Stock en Janis.
    """ 

    t0 = ShortCircuitOperator(
        task_id = "check_process_run",
        python_callable = check_process_run
    )

    t1 = PythonOperator(
        task_id = "read_stock_adjustment_stage2_s3_file",
        python_callable = read_stock_adjustment_stage2_s3_file,
    )

    t2 = PythonOperator(
        task_id = "change_stock_on_janis_api",
        python_callable = change_stock_on_janis_api,
    )

    t3 = PythonOperator(
        task_id = "record_updated_stocks",
        python_callable = record_updated_stocks
    )

    t4 = PythonOperator(
        task_id = "record_process_metadata",
        python_callable = record_process_metadata
    )

    t0 >> t1 >> t2 >> t3 >> t4
