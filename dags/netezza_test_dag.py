from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from utils.janis_utils import load_full_table_to_s3

from datetime import datetime

import jaydebeapi

import os

def netezza_test():
    dsn_database = Variable.get("DW_SECRET_DATABASE") 
    dsn_hostname = Variable.get("DW_SECRET_HOSTNAME")
    dsn_port = "5480" 
    dsn_uid = Variable.get("DW_SECRET_USER")
    dsn_pwd = Variable.get("DW_PASSWORD")
    jdbc_driver_name = "org.netezza.Driver" 
    jdbc_driver_loc = os.path.join('/opt/airflow/include/jdbcdriver/nzjdbc.jar')

    sql_str = "select now()"

    connection_string='jdbc:netezza://'+dsn_hostname+':'+dsn_port+'/'+dsn_database

    url = '{0}:user={1};password={2}'.format(connection_string, dsn_uid, dsn_pwd)
    print("Connection String: " + connection_string)

    conn = jaydebeapi.connect(jdbc_driver_name, 
                                connection_string, {'user': dsn_uid, 'password': dsn_pwd},
                                jars=jdbc_driver_loc)

    curs = conn.cursor()
    curs.execute(sql_str)
    result = curs.fetchall()

    print(result[0])
    curs.close()
    conn.close()
    return

default_args = {
    "owner": "dw_test",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'Netezza_connection_healthcheck',
    default_args=default_args,
    description="Netezza test",
    schedule="*/10 * * * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["DW", "MATIAS"],
) as dag:

    dag.doc_md = """
    Netezza connection healthcheck.
    """ 
    t0 = PythonOperator(
        task_id = "netezza_test",
        python_callable = netezza_test
    )

    t0