from airflow.models import Variable

from datetime import datetime
from io import StringIO

import boto3
import mysql.connector
import pandas as pd

BASE_S3_PATH = "janis/replica/"

def _execute_mariadb_query(query):
    conn = mysql.connector.connect(
        user=Variable.get("JANIS_MARIADB_USER"),
        password=Variable.get("JANIS_MARIADB_PASSWORD"),
        host=Variable.get("JANIS_MARIADB_HOST"),
        port=3306,
        database=Variable.get("JANIS_MARIADB_DATABASE")
    )

    # Get Cursor
    cur = conn.cursor()
    cur.execute(query)
    results = cur.fetchall()
    columns = [i[0] for i in cur.description]
    cur.close()
    conn.close()

    print(f"Number of records extracted: {len(results)}")
    print(columns)

    return results, columns


def load_full_table_to_s3(table_name, where=None):
    curr_datetime = datetime.utcnow()
    prefix = BASE_S3_PATH+table_name+"/"+curr_datetime.strftime("%Y/%m/%d/%H%M_")
    file_name = prefix+table_name+".csv"

    query = f"SELECT * FROM janis_jackie.{table_name} "
    if where is not None:
        query = query + f"WHERE {where} ;"

    results, columns = _execute_mariadb_query(query)

    df = pd.DataFrame(results, columns=columns)
    buffer = StringIO()

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

def load_custom_query_to_s3(ts, query, query_name, extra_prefix=None):
    print("Execution datetime: " + ts)
    curr_datetime = ts[:16].replace("-", "/").replace("T", "/").replace(":", "")
    prefix = BASE_S3_PATH+query_name+"/"+curr_datetime+"_"
    if extra_prefix is not None:
        prefix = prefix+extra_prefix+"_"
    file_name = prefix+query_name+".csv"

    results, columns = _execute_mariadb_query(query)

    df = pd.DataFrame(results, columns=columns)
    buffer = StringIO()

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

def incremental_load_table_s3(ti,
                              ts,
                              table_name, 
                              xcom_created_date_task_id=None, 
                              created_column=None,
                              from_unixtime=False,
                              xcom_updated_date_task_id=None, 
                              updated_column=None, 
                              where=None, 
                              extra_prefix=None):
    # Verify if there is enough incremental parameters:
    if created_column is None and updated_column is None:
        print("ERROR: No incremental column given.")
        raise(Exception("No incremental columns found."))
    
    print("Execution datetime: " + ts)
    curr_datetime = ts[:16].replace("-", "/").replace("T", "/").replace(":", "")
    prefix = BASE_S3_PATH+table_name+"/"+curr_datetime+"_"
    if extra_prefix is not None:
        prefix = prefix+extra_prefix+"_"
    file_name = prefix+table_name+".csv"

    sql_str = f"SELECT * FROM janis_jackie.{table_name} WHERE "
    date_query_strings = []
    if created_column is not None:
        created_date = ti.xcom_pull(key="return_value", task_ids=[xcom_created_date_task_id])[0]
        print("created_date:")
        print(created_date)
        if created_date is None:
            created_date = '1970-01-01'
        if from_unixtime:
            created_query = f"FROM_UNIXTIME({created_column}) > '{created_date}'"
        else:
            created_query = f"{created_column} > '{created_date}'"
        date_query_strings.append(created_query)
    if updated_column is not None:
        updated_date = ti.xcom_pull(key="return_value", task_ids=[xcom_updated_date_task_id])[0]
        print("updated_date:")
        print(updated_date)
        if updated_date is None:
            updated_date = '1970-01-01'
        if from_unixtime:  
            updated_query = f"FROM_UNIXTIME({updated_column}) > '{updated_date}'"
        else:
            updated_query = f"{updated_column} > '{updated_date}'"
        date_query_strings.append(updated_query)
    
    sql_str = sql_str + " AND ".join(date_query_strings)

    if where is not None:
        sql_str = sql_str + " AND " + where
    
    print(sql_str)

    results, columns = _execute_mariadb_query(sql_str)

    df = pd.DataFrame(results, columns=columns)
    buffer = StringIO()

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

def incremental_unixtime_load_table_s3(ti,
                              ts,
                              table_name, 
                              xcom_created_date_task_id=None, 
                              created_column=None,
                              xcom_updated_date_task_id=None, 
                              updated_column=None, 
                              where=None, 
                              extra_prefix=None,
                              inclusive=None):
    # Verify if there is enough incremental parameters:
    if created_column is None and updated_column is None:
        print("ERROR: No incremental column given.")
        raise(Exception("No incremental columns found."))
    
    print("Execution datetime: " + ts)
    curr_datetime = ts[:16].replace("-", "/").replace("T", "/").replace(":", "")
    prefix = BASE_S3_PATH+table_name+"/"+curr_datetime+"_"
    if extra_prefix is not None:
        prefix = prefix+extra_prefix+"_"
    file_name = prefix+table_name+".csv"

    sql_str = f"SELECT * FROM janis_jackie.{table_name} WHERE "
    date_query_strings = []
    if created_column is not None:
        created_date = ti.xcom_pull(key="return_value", task_ids=[xcom_created_date_task_id])[0]
        print("created_date:")
        print(created_date)
        if created_date is None:
            created_date = 0
        if inclusive is None:
            created_query = f"{created_column} > {created_date}"
        if inclusive is True:
            created_query = f"{created_column} >= {created_date}"
        date_query_strings.append(created_query)
    if updated_column is not None:
        updated_date = ti.xcom_pull(key="return_value", task_ids=[xcom_updated_date_task_id])[0]
        print("updated_date:")
        print(updated_date)
        if updated_date is None:
            updated_date = 0
        if inclusive is None:
            updated_query = f"{updated_column} > {updated_date}"
        if inclusive is True:
            updated_query = f"{updated_column} >= {updated_date}"
        date_query_strings.append(updated_query)
    
    sql_str = sql_str + " AND ".join(date_query_strings)

    if where is not None:
        sql_str = sql_str + " AND " + where
    
    print(sql_str)

    results, columns = _execute_mariadb_query(sql_str)

    df = pd.DataFrame(results, columns=columns)
    buffer = StringIO()

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
