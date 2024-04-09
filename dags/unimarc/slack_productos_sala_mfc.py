from airflow import DAG
from airflow import macros
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import pendulum



def listado_productos_sala_mfc():
    import pandas as pd
    lp_query = """select op.id_orden as pedido, op.ref_id, op.ean, op.descripcion, oj.id_cliente_janis, du.nombre, du.apellido, du.fono, um.mfc_is_item_side, d.inicio_ventana::date as fecha, d.inicio_ventana::time as inicio_ventana, d.termino_ventana::time as termino_ventana  
                    from ecommdata.orden_productos op
                    inner join ecommdata.ordenes_janis oj on oj.id = op.id_orden
                    inner join ecommdata.ubicacion_mfc um on CONCAT(um.sap_code, '-', um.measurement_unit) = op.ref_id
                    inner join ecommdata.despachos d on d.id_orden = oj.id
                    inner join analytics_and_growth.perfil_usuario pu on pu.id_cliente_janis = oj.id_cliente_janis
                    inner join analytics_and_growth.detalle_usuario du on du.user_profile_id = pu.user_profile_id
                    where oj.id_tienda_janis = 25 and um.mfc_is_item_side = 'FLO';
                    """
    print(lp_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(lp_query)
    column_names = [desc[0] for desc in cursor.description]
    results = cursor.fetchall()
    results = pd.DataFrame(results, columns=column_names)
    print(results.head(20))
    cursor.close()
    pg_connection.close()

    return results

def send_to_slack():
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError

    df = listado_productos_sala_mfc()
    token = Variable.get("token_slack_2")
    client = WebClient(token=token)

    try:
        client.files_upload(
        channels = Variable.get("canal_slack_lps_mfc"),
        initial_comment = "Listado de productos sala en mfc",
        filename = "listado_productos_sala_mfc.csv",
        content = df)
    except SlackApiError as e:
        print(f"Error sending message: {e}")

    return


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'slack_productos_sala_mfc',
    default_args=default_args,
    description="Envio de productos sala MFC a Slack",
    schedule_interval="30 * * * *",
    start_date=pendulum.datetime(2024, 4, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["DATA", "MFC", "ecommdata", "SLACK" ,"MATIAS"],
) as dag:

    dag.doc_md = """
    Envio de productos sala MFC a Slack
    """ 

    t0 = PythonOperator(
        task_id = "send_to_slack",
        python_callable = send_to_slack
    )
    t0
