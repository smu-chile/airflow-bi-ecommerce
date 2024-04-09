from airflow import DAG
from airflow import macros
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import pendulum



def listado_productos_sala_mfc(ts, ds):
    import pandas as pd

    print("CHECKING TIME")
    print(ts)

    exec_datetime = datetime.strptime(ts[:16], "%Y-%m-%dT%H:%M")
    exec_datetime_utc = pendulum.timezone("utc").convert(exec_datetime)
    local_tz = pendulum.timezone("America/Santiago")
    exec_datetime_local = local_tz.convert(exec_datetime_utc)
    exec_datetime_local_str = exec_datetime_local.strftime("%Y-%m-%dT%H:%M")
    print(exec_datetime_local_str)

    time_str = exec_datetime_local_str.split("T")[1]
    print(time_str)

    v_time = None
    v2_time = None

    if time_str == "19:30":
        v_time = macros.ds_add(ds, -1) + " 9:00"
        v2_time = macros.ds_add(ds, -1) + " 10:00:00"
    elif time_str == "7:30":
        v_time = ds + " 11:00"
    elif time_str == "8:30":
        v_time = ds + " 12:00"
    elif time_str == "9:15":
        v_time = ds + " 13:00"
    elif time_str == "10:15":
        v_time = ds + " 14:00"
    elif time_str == "11:15":
        v_time = ds + " 15:00"
    elif time_str == "12:15":
        v_time = ds + " 16:00"
    elif time_str == "13:15":
        v_time = ds + " 17:00"
    elif time_str == "14:15":
        v_time = ds + " 18:00"
    elif time_str == "15:15":
        v_time = ds + " 19:00"
    elif time_str == "16:15":
        v_time = ds + " 20:00"
    
    results = []

    if v_time is not None:

        lp_query = f"""select op.id_orden as pedido, op.ref_id, op.ean, op.descripcion, oj.id_cliente_janis, du.nombre, du.apellido, du.fono, um.mfc_is_item_side, d.inicio_ventana::date as fecha, d.inicio_ventana::time as inicio_ventana, d.termino_ventana::time as termino_ventana  
                        from ecommdata.orden_productos op
                        inner join ecommdata.ordenes_janis oj on oj.id = op.id_orden
                        inner join ecommdata.ubicacion_mfc um on CONCAT(um.sap_code, '-', um.measurement_unit) = op.ref_id
                        inner join ecommdata.despachos d on d.id_orden = oj.id
                        inner join analytics_and_growth.perfil_usuario pu on pu.id_cliente_janis = oj.id_cliente_janis
                        inner join analytics_and_growth.detalle_usuario du on du.user_profile_id = pu.user_profile_id
                        where oj.id_tienda_janis = 25 and um.mfc_is_item_side = 'FLO' and d.inicio_ventana = '{v_time}';
                        """
        print(lp_query)
        pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
        pg_connection = pg_hook.get_conn()
        cursor = pg_connection.cursor()
        cursor.execute(lp_query)
        column_names = [desc[0] for desc in cursor.description]
        result_1 = cursor.fetchall()
        result_1 = pd.DataFrame(result_1, columns=column_names)
        print(result_1.head(20))
        cursor.close()
        pg_connection.close()
        results.append(result_1)
    else:
        results.append(None)

    if v2_time is not None:

        lp_query = f"""select op.id_orden as pedido, op.ref_id, op.ean, op.descripcion, oj.id_cliente_janis, du.nombre, du.apellido, du.fono, um.mfc_is_item_side, d.inicio_ventana::date as fecha, d.inicio_ventana::time as inicio_ventana, d.termino_ventana::time as termino_ventana  
                        from ecommdata.orden_productos op
                        inner join ecommdata.ordenes_janis oj on oj.id = op.id_orden
                        inner join ecommdata.ubicacion_mfc um on CONCAT(um.sap_code, '-', um.measurement_unit) = op.ref_id
                        inner join ecommdata.despachos d on d.id_orden = oj.id
                        inner join analytics_and_growth.perfil_usuario pu on pu.id_cliente_janis = oj.id_cliente_janis
                        inner join analytics_and_growth.detalle_usuario du on du.user_profile_id = pu.user_profile_id
                        where oj.id_tienda_janis = 25 and um.mfc_is_item_side = 'FLO' and d.inicio_ventana = '{v2_time}';
                        """
        print(lp_query)
        pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
        pg_connection = pg_hook.get_conn()
        cursor = pg_connection.cursor()
        cursor.execute(lp_query)
        column_names = [desc[0] for desc in cursor.description]
        result_2 = cursor.fetchall()
        result_2 = pd.DataFrame(result_2, columns=column_names)
        print(result_2.head(20))
        cursor.close()
        pg_connection.close()
        results.append(result_2)
    else:
        results.append(None)

    return results

def send_to_slack(ts, ds):
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError
    import io

    df_list = listado_productos_sala_mfc(ts, ds)
    
    if df_list[0] is not None:
        print("SENDING DF1")
        df = df_list[0]
        buffer = io.StringIO()
        df.to_csv(buffer, header=True, index=False, encoding="utf-8", sep=';')
        buffer.seek(0)
        token = Variable.get("token_slack_2")
        client = WebClient(token=token)

        try:
            client.files_upload(
            channels = Variable.get("canal_slack_lps_mfc"),
            initial_comment = "Listado de productos sala en mfc",
            filename = "listado_productos_sala_mfc.csv",
            content = buffer.getvalue())
        except SlackApiError as e:
            print(f"Error sending message: {e}")
    else:
        print("SKIPPING TASK")
        return
    
    if df_list[1] is not None:
        print("SENDING DF2")
        df = df_list[1]
        buffer = io.StringIO()
        df.to_csv(buffer, header=True, index=False, encoding="utf-8", sep=';')
        buffer.seek(0)
        token = Variable.get("token_slack_2")
        client = WebClient(token=token)

        try:
            client.files_upload(
            channels = Variable.get("canal_slack_lps_mfc"),
            initial_comment = "Listado de productos sala en mfc",
            filename = "listado_productos_sala_mfc.csv",
            content = buffer.getvalue())
        except SlackApiError as e:
            print(f"Error sending message: {e}")
    else:
        print("SKIPPING SENDING DF2")
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
    schedule_interval="0,15,30 7-16,19 * * *",
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
