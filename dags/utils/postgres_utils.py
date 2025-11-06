from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook

def query_to_df(query):
    import pandas as pd
    print(query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query)
    column_names = [desc[0] for desc in cursor.description]
    results = cursor.fetchall()
    results = pd.DataFrame(results, columns=column_names)
    print(results.head(20))
    cursor.close()
    pg_connection.close()
    return results

def get_max_updated_at_value(schema, table_name, updated_at_field, postgres_conn_id="postgresql_conn", is_unixtime=False):
    query = f"SELECT MAX({updated_at_field}) FROM {schema}.{table_name};"
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    print(results)
    cursor.close()
    pg_connection.close()
    updated_at_date = results[0][0]
    if updated_at_date is None:
        return None
    if is_unixtime:
        return updated_at_date
    return updated_at_date.strftime("%Y-%m-%d %H:%M:%S")

def is_empty_table(schema, table_name, postgres_conn_id="postgresql_conn"):
    query = f"""
        SELECT COUNT(1)
        FROM {schema}.{table_name};
    """
    print(query)
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query)
    result = cursor.fetchone()
    count = result[0]
    return count == 0

def load_custom_query_to_s3(ts, query, query_name, aws_conn_id="aws_s3_connection", extra_prefix=None):
    from io import StringIO
    import pandas as pd
    import os

    BASE_S3_PATH = "data_warehouse/"
    print("Execution datetime: " + ts)
    curr_datetime = ts[:16].replace("-", "/").replace("T", "/").replace(":", "")
    prefix = BASE_S3_PATH+query_name+"/"+curr_datetime+"_"
    if extra_prefix is not None:
        prefix = prefix+extra_prefix+"_"
    file_name = prefix+query_name+".csv"    

    print("SQL Query:\n"+query)
    print("File to be created: "+file_name)

    df = query_to_df(query)
    buffer = StringIO()

    print("Number of records:")
    print(len(df.index))
    df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    buffer.seek(0)

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    s3_hook.load_string(buffer.getvalue(),
                  key=file_name,
                  bucket_name=s3_bucket,
                  replace=True,
                  encrypt=False)

    return file_name