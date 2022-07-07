from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from datetime import datetime, timedelta

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'test_smtp_connections',
    default_args=default_args,
    description="Prueba de distintos métodos de conexión con servidor SMTP.",
    schedule_interval=None,
    start_date=datetime(2022, 7, 3),
    catchup=False,
    max_active_runs = 1,
    tags=["SMTP", "test", "email"],
) as dag:

    dag.doc_md = """
    Prueba de distintos métodos de conexión con servidor SMTP.
    """ 
    t0 = BashOperator(
        task_id = "ping_local_smtp_server_hostname",
        bash_command = "ping smtprelay.unimarc.local"
    )

    t1 = BashOperator(
        task_id = "ping_local_smtp_server_ip",
        bash_command = "ping 10.42.31.196"
    )

    t2 = BashOperator(
        task_id = "ping_outlook_smtp_server",
        bash_command = "ping smtp.office365.com"
    )
