from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import boto3
import pymongo
import pytz

def read_stock_adjustment_stage2_s3_file():
    access_key = Variable.get("AWS_ACCESS_KEY")
    secret_key = Variable.get("AWS_SECRET_KEY")
    bucket_name = Variable.get("AWS_S3_BUCKET_NAME")
    print("BUCKET:")
    print(bucket_name)
    s3_resource = boto3.resource("s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name="us-east-1")
    bucket = s3_resource.Bucket(bucket_name)
    local_tz = pytz.timezone('America/Santiago')
    curr_local_datetime = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(local_tz)
    prefix = "ecommops/stock_adjustment/"+curr_local_datetime.strftime("%Y/%m/%d/")
    object_name = prefix+"stage_2.csv"
    s3_object = bucket.Object(object_name)
    print(s3_object)

    mongo_user = Variable.get("MONGODB_USER")
    mongo_pass = Variable.get("MONGODB_PASSWORD")
    mongo_cluster_name = Variable.get("MONGODB_CLUSTER")
    mongo_db = Variable.get("MONGODB_DATABASE")
    mongo_client = pymongo.MongoClient("mongodb+srv://"+mongo_user+":"+mongo_pass+"@"+mongo_cluster_name+".lppxi.mongodb.net/"+mongo_db+"?retryWrites=true&w=majority&authSource=admin")
    mongo_collection = mongo_client[mongo_db]["stock_changes_stage2"]

    current_timestamp = datetime.utcnow()
    payload = []

    count = 0
    for line in s3_object.get()["Body"].iter_lines():
        if count == 0:
            headers = line.decode("UTF-8").split(";")
        else:
            row_data = line.decode("UTF-8").split(";")
            document = dict(zip(headers, row_data))
            document["timestamp"] = current_timestamp

            payload_element = {}
            payload_element["IdSku"] = document["material_uv"].split("-")[0]
            payload_element["Quantity"] = 0
            payload_element["Store"] = document["store_ref_id"]
            payload_element["warehouseRefId"] = document["warehouse_ref_id"]
            payload.append(payload_element)
            mongo_collection.insert_one(document)
        count = count + 1

    return "OK"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}
with DAG(
    'test-dag',
    default_args=default_args,
    description='A DAG test',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 's3'],
) as dag:
    t1 = PythonOperator(
        task_id='read_s3_file',
        python_callable = read_stock_adjustment_stage2_s3_file,
    )
    dag.doc_md = """
    This is a documentation placed anywhere
    """ 
    t1
