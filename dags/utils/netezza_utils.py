from airflow.models import Variable

from datetime import datetime
from io import StringIO
import os

import boto3
import jaydebeapi
import pandas as pd

BASE_S3_PATH = "data_warehouse/"

def netezza_full_table_load_to_s3(table_name):
    curr_datetime = datetime.utcnow()
    prefix = BASE_S3_PATH+table_name+"/"+curr_datetime.strftime("%Y/%m/%d/%H%M_")
    file_name = prefix+table_name+".csv"    

    sql_str = f"SELECT * FROM {table_name}"

    dsn_database = Variable.get("DW_SECRET_DATABASE") 
    dsn_hostname = Variable.get("DW_SECRET_HOSTNAME")
    dsn_port = "5480" 
    dsn_uid = Variable.get("DW_SECRET_USER")
    dsn_pwd = Variable.get("DW_PASSWORD")
    jdbc_driver_name = "org.netezza.Driver" 
    jdbc_driver_loc = os.path.join('/opt/airflow/include/jdbcdriver/nzjdbc.jar')

    connection_string='jdbc:netezza://'+dsn_hostname+':'+dsn_port+'/'+dsn_database
    

    url = '{0}:user={1};password={2}'.format(connection_string, dsn_uid, dsn_pwd)

    
    conn = jaydebeapi.connect(jdbc_driver_name, 
                                connection_string, {'user': dsn_uid, 'password': dsn_pwd},
                                jars=jdbc_driver_loc)

    cur = conn.cursor()
    cur.execute(sql_str)
    results = cur.fetchall()
    columns = [i[0] for i in cur.description]

    print(columns)
    print(results[0])
    cur.close()
    conn.close()

    df = pd.DataFrame(results, columns=columns)
    buffer = StringIO()

    print(len(df.index))
    print(df)
    df.to_csv(buffer, header=True, index=False, encoding="utf-8")
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