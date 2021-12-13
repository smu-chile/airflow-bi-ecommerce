from airflow.models import Variable

from datetime import datetime
from io import StringIO

import boto3
import botocore
import mysql.connector
import pandas as pd

BASE_S3_PATH = "janis/replica/"

def load_full_table_to_s3(table_name):
    curr_datetime = datetime.utcnow()
    prefix = BASE_S3_PATH+"stock/"+curr_datetime.strftime("%Y/%m/%d/%H%M_")
    file_name = prefix+"stock.csv"

    query = f"SELECT * FROM janis_jackie.{table_name} ;"

    try:
        conn = mysql.connector.connect(
            user=Variable.get("JANIS_MARIADB_USER"),
            password=Variable.get("JANIS_MARIADB_PASSWORD"),
            host=Variable.get("JANIS_MARIADB_HOST"),
            port=3306,
            database=Variable.get("JANIS_MARIADB_DATABASE")
        )
    except mysql.connector.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        return

    # Get Cursor
    cur = conn.cursor()
    cur.execute(query)
    results = cur.fetchall()
    columns = [i[0] for i in cur.description]
    cur.close()
    conn.close()

    print(len(results))
    print(columns)

    df = pd.DataFrame(results, columns=columns)
    buffer = StringIO()

    df.to_csv(buffer, header=True, index=False)
    buffer.seek(0)

    access_key = Variable.get("AWS_ACCESS_KEY")
    secret_key = Variable.get("AWS_SECRET_KEY")
    bucket_name = Variable.get("AWS_S3_BUCKET_NAME")
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name = "us-east-1"
    )
    response = s3_client.put_object(
        Bucket=bucket_name, Key=file_name, Body=buffer.getvalue()
    )

    return file_name
