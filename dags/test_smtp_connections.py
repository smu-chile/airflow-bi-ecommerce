from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from datetime import datetime

def send_test_email(host):
    from redmail import EmailSender
    email_sender = EmailSender(
        host=host,
        port="25"
    )
    email_sender.send(
        subject="test email subject",
        sender="reportes_ecommerce@smu.cl",
        receivers=["iurizar@smu.cl"],
        text="Hi, this is an email."
    )
    return

def send_test_outlook_email():
    from redmail import outlook
    outlook.username = Variable.get("TEST_OUTLOOK_USER_SECRET")
    outlook.password = Variable.get("TEST_OUTLOOK_PASSWORD")

    # And then you can send emails
    outlook.send(
        subject="Example email",
        receivers=['iurizar@smu.cl'],
        text="Hi, this is an outlook email."
    )
    return

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

    t0 = PythonOperator(
        task_id = "test_redmail_with_local_smtp_server_ip",
        python_callable = send_test_email,
        op_kwargs = {"host": "10.42.31.222"}
    )

    t1 = PythonOperator(
        task_id = "test_redmail_with_local_smtp_server_hostname",
        python_callable = send_test_email,
        op_kwargs = {"host": "smtprelay.unimarc.local"}
    )

    t2 = PythonOperator(
        task_id = "test_redmail_with_outlook_smtp_server",
        python_callable = send_test_outlook_email
    )
