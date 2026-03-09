from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from io import StringIO
import os

import jaydebeapi
import pandas as pd

BASE_S3_PATH = "data_warehouse/"

def netezza_full_table_load_to_s3(ts, table_name, where=None, date_query=None, aws_conn_id="aws_s3_connection", extra_prefix=None):
    print("Execution datetime: " + ts)
    curr_datetime = ts[:16].replace("-", "/").replace("T", "/").replace(":", "")
    prefix = BASE_S3_PATH+table_name+"/"+curr_datetime+"_"
    if extra_prefix is not None:
        prefix = prefix+extra_prefix+"_"
    file_name = prefix+table_name+".csv"    

    print("File to be created: "+file_name)

    sql_str = f"SELECT * FROM {table_name}"
    if where is not None:
        sql_str = sql_str + " WHERE " + where
    if date_query is not None:
        date_query = date_query % ts[:10]
        sql_str = sql_str + " AND " + date_query

    print(sql_str)

    dsn_database = Variable.get("DW_SECRET_DATABASE") 
    dsn_hostname = Variable.get("DW_SECRET_HOSTNAME")
    dsn_port = "5480" 
    dsn_uid = Variable.get("DW_SECRET_USER")
    dsn_pwd = Variable.get("DW_PASSWORD")
    jdbc_driver_name = "org.netezza.Driver" 
    jdbc_driver_loc = os.path.join('/opt/airflow/include/jdbcdriver/nzjdbc.jar')

    connection_string='jdbc:netezza://'+dsn_hostname+':'+dsn_port+'/'+dsn_database
    
    conn = jaydebeapi.connect(jdbc_driver_name, 
                                connection_string, {'user': dsn_uid, 'password': dsn_pwd},
                                jars=jdbc_driver_loc)

    cur = conn.cursor()
    cur.execute(sql_str)
    results = cur.fetchall()
    columns = [i[0] for i in cur.description]

    print(columns)
    cur.close()
    conn.close()

    df = pd.DataFrame(results, columns=columns)
    buffer = StringIO()

    print("Number of records:")
    print(len(df.index))
    df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    buffer.seek(0)

    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    s3_hook.load_string(buffer.getvalue(),
                  key=file_name,
                  bucket_name=s3_bucket,
                  replace=True,
                  encrypt=False)

    return file_name

def render_netezza_view(view_name):
    """
    Simple select query over a specific View on Netezza DW
    to start rendering process and speed up future queries.
    Returns None
    """

    sql_str = "SELECT * FROM "+view_name+" LIMIT 100"
    print(sql_str)

    dsn_database = Variable.get("DW_SECRET_DATABASE") 
    dsn_hostname = Variable.get("DW_SECRET_HOSTNAME")
    dsn_port = "5480" 
    dsn_uid = Variable.get("DW_SECRET_USER")
    dsn_pwd = Variable.get("DW_PASSWORD")
    jdbc_driver_name = "org.netezza.Driver" 
    jdbc_driver_loc = os.path.join('/opt/airflow/include/jdbcdriver/nzjdbc.jar')

    connection_string='jdbc:netezza://'+dsn_hostname+':'+dsn_port+'/'+dsn_database
    
    conn = jaydebeapi.connect(jdbc_driver_name, 
                                connection_string, {'user': dsn_uid, 'password': dsn_pwd},
                                jars=jdbc_driver_loc)

    cur = conn.cursor()
    cur.execute(sql_str)
    cur.close()
    conn.close()

    return

def netezza_incremental_load_to_s3(ti,
                                   ts,
                                   table_name, 
                                   xcom_update_date_task_id, 
                                   update_column, 
                                   where=None, 
                                   aws_conn_id="aws_s3_connection", 
                                   extra_prefix=None):
    print("Execution datetime: " + ts)
    curr_datetime = ts[:16].replace("-", "/").replace("T", "/").replace(":", "")
    prefix = BASE_S3_PATH+table_name+"/"+curr_datetime+"_"
    if extra_prefix is not None:
        prefix = prefix+extra_prefix+"_"
    file_name = prefix+table_name+".csv"

    update_date = ti.xcom_pull(key="return_value", task_ids=[xcom_update_date_task_id])[0]
    sql_str = f"SELECT * FROM {table_name} "
    sql_str = sql_str + f"WHERE {update_column} > {update_date} "

    if where is not None:
        sql_str = sql_str + " AND " + where
    
    print(sql_str)

    dsn_database = Variable.get("DW_SECRET_DATABASE") 
    dsn_hostname = Variable.get("DW_SECRET_HOSTNAME")
    dsn_port = "5480" 
    dsn_uid = Variable.get("DW_SECRET_USER")
    dsn_pwd = Variable.get("DW_PASSWORD")
    jdbc_driver_name = "org.netezza.Driver" 
    jdbc_driver_loc = os.path.join('/opt/airflow/include/jdbcdriver/nzjdbc.jar')

    connection_string='jdbc:netezza://'+dsn_hostname+':'+dsn_port+'/'+dsn_database
    
    conn = jaydebeapi.connect(jdbc_driver_name, 
                                connection_string, {'user': dsn_uid, 'password': dsn_pwd},
                                jars=jdbc_driver_loc)

    cur = conn.cursor()
    cur.execute(sql_str)
    results = cur.fetchall()
    columns = [i[0] for i in cur.description]

    print(columns)
    cur.close()
    conn.close()

    df = pd.DataFrame(results, columns=columns)
    buffer = StringIO()

    print("Number of records:")
    print(len(df.index))
    df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    buffer.seek(0)

    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    s3_hook.load_string(buffer.getvalue(),
                  key=file_name,
                  bucket_name=s3_bucket,
                  replace=True,
                  encrypt=False)

    return file_name

def load_custom_query_to_s3(ts, query, query_name, aws_conn_id="aws_s3_connection", extra_prefix=None):

    print("Execution datetime: " + ts)
    curr_datetime = ts[:16].replace("-", "/").replace("T", "/").replace(":", "")
    prefix = BASE_S3_PATH+query_name+"/"+curr_datetime+"_"
    if extra_prefix is not None:
        prefix = prefix+extra_prefix+"_"
    file_name = prefix+query_name+".csv"    

    print("SQL Query:\n"+query)
    print("File to be created: "+file_name)

    dsn_database = Variable.get("DW_SECRET_DATABASE") 
    dsn_hostname = Variable.get("DW_SECRET_HOSTNAME")
    dsn_port = "5480" 
    dsn_uid = Variable.get("DW_SECRET_USER")
    dsn_pwd = Variable.get("DW_PASSWORD")
    jdbc_driver_name = "org.netezza.Driver" 
    jdbc_driver_loc = os.path.join('/opt/airflow/include/jdbcdriver/nzjdbc.jar')

    connection_string='jdbc:netezza://'+dsn_hostname+':'+dsn_port+'/'+dsn_database
    
    conn = jaydebeapi.connect(jdbc_driver_name, 
                                connection_string, {'user': dsn_uid, 'password': dsn_pwd},
                                jars=jdbc_driver_loc)

    cur = conn.cursor()
    cur.execute(query)
    results = cur.fetchall()
    columns = [i[0] for i in cur.description]

    print(columns)
    cur.close()
    conn.close()

    df = pd.DataFrame(results, columns=columns)
    buffer = StringIO()

    print("Number of records:")
    print(len(df.index))
    df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    buffer.seek(0)

    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    s3_hook.load_string(buffer.getvalue(),
                  key=file_name,
                  bucket_name=s3_bucket,
                  replace=True,
                  encrypt=False)

    return file_name
