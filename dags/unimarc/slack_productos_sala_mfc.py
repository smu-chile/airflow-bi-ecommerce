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

    if time_str == "17:30":
        v_time = macros.ds_add(ds, 1) + " 09:00"
        v2_time = macros.ds_add(ds, 1) + " 10:00:00"
    elif time_str == "07:15":
        v_time = ds + " 11:00"
    elif time_str == "08:15":
        v_time = ds + " 12:00"
    elif time_str == "09:00":
        v_time = ds + " 13:00"
    elif time_str == "10:00":
        v_time = ds + " 14:00"
    elif time_str == "11:00":
        v_time = ds + " 15:00"
    elif time_str == "12:00":
        v_time = ds + " 16:00"
    elif time_str == "13:00":
        v_time = ds + " 17:00"
    elif time_str == "14:00":
        v_time = ds + " 18:00"
    elif time_str == "15:00":
        v_time = ds + " 19:00"
    elif time_str == "16:00":
        v_time = ds + " 20:00"
    
    results = []
    results_c = []
    results_q = []
    results_FULL = []

    if v_time is not None:

        lp_query = f"""select op.id_orden as pedido, op.ref_id, op.ean, op.descripcion, oj.id_cliente_janis, du.nombre, du.apellido, du.fono, um.mfc_is_item_side, d.inicio_ventana::date as fecha, d.inicio_ventana::time as inicio_ventana, d.termino_ventana::time as termino_ventana, op.precio_lista, op.unidades_solicitadas, op.unidades_pickeadas, op.unidad_de_medida, op.multiplicador_unidad, op.multiplicador_unidad*op.unidades_solicitadas  as Und_Kg_solicitado, op.nota as comentario 
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

        #Carnes

        lpc_query = f"""select op.id_orden as pedido, op.ref_id, op.ean, op.descripcion, oj.id_cliente_janis, du.nombre, du.apellido, du.fono, um.mfc_is_item_side, d.inicio_ventana::date as fecha, d.inicio_ventana::time as inicio_ventana, d.termino_ventana::time as termino_ventana, op.precio_lista, op.unidades_solicitadas, op.unidades_pickeadas, op.unidad_de_medida, op.multiplicador_unidad, op.multiplicador_unidad*op.unidades_solicitadas  as Und_Kg_solicitado, op.nota as comentario 
                        from ecommdata.orden_productos op
                        inner join ecommdata.ordenes_janis oj on oj.id = op.id_orden
                        inner join ecommdata.ubicacion_mfc um on CONCAT(um.sap_code, '-', um.measurement_unit) = op.ref_id
                        inner join ecommdata.despachos d on d.id_orden = oj.id
                        inner join analytics_and_growth.perfil_usuario pu on pu.id_cliente_janis = oj.id_cliente_janis
                        inner join analytics_and_growth.detalle_usuario du on du.user_profile_id = pu.user_profile_id
                        inner join ecommdata.productos p on op.ref_id = p.ref_id
                        inner join ecommdata.categorias c on p.id_categoria = c.id
                        where oj.id_tienda_janis = 25 and um.mfc_is_item_side = 'FLO' and d.inicio_ventana = '{v_time}' and c.n1 = 'Carnes';
                        """
        print(lpc_query)
        pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
        pg_connection = pg_hook.get_conn()
        cursor = pg_connection.cursor()
        cursor.execute(lpc_query)
        column_names = [desc[0] for desc in cursor.description]
        result_1c = cursor.fetchall()
        result_1c = pd.DataFrame(result_1c, columns=column_names)
        print(result_1c.head(20))
        cursor.close()
        pg_connection.close()
        results_c.append(result_1c)

        #Quesos y Fiambres

        lpq_query = f"""select op.id_orden as pedido, op.ref_id, op.ean, op.descripcion, oj.id_cliente_janis, du.nombre, du.apellido, du.fono, um.mfc_is_item_side, d.inicio_ventana::date as fecha, d.inicio_ventana::time as inicio_ventana, d.termino_ventana::time as termino_ventana, op.precio_lista, op.unidades_solicitadas, op.unidades_pickeadas, op.unidad_de_medida, op.multiplicador_unidad, op.multiplicador_unidad*op.unidades_solicitadas  as Und_Kg_solicitado, op.nota as comentario 
                        from ecommdata.orden_productos op
                        inner join ecommdata.ordenes_janis oj on oj.id = op.id_orden
                        inner join ecommdata.ubicacion_mfc um on CONCAT(um.sap_code, '-', um.measurement_unit) = op.ref_id
                        inner join ecommdata.despachos d on d.id_orden = oj.id
                        inner join analytics_and_growth.perfil_usuario pu on pu.id_cliente_janis = oj.id_cliente_janis
                        inner join analytics_and_growth.detalle_usuario du on du.user_profile_id = pu.user_profile_id
                        inner join ecommdata.productos p on op.ref_id = p.ref_id
                        inner join ecommdata.categorias c on p.id_categoria = c.id
                        where oj.id_tienda_janis = 25 and um.mfc_is_item_side = 'FLO' and d.inicio_ventana = '{v_time}' and c.n1 = 'Quesos y Fiambres';
                        """
        print(lpq_query)
        pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
        pg_connection = pg_hook.get_conn()
        cursor = pg_connection.cursor()
        cursor.execute(lpq_query)
        column_names = [desc[0] for desc in cursor.description]
        result_1q = cursor.fetchall()
        result_1q = pd.DataFrame(result_1q, columns=column_names)
        print(result_1q.head(20))
        cursor.close()
        pg_connection.close()
        results_q.append(result_1q)

        #REG Y FLO

        lp_query_FULL = f"""select op.id_orden as pedido, op.ref_id, op.ean, op.descripcion, oj.id_cliente_janis, du.nombre, du.apellido, du.fono, um.mfc_is_item_side, d.inicio_ventana::date as fecha, d.inicio_ventana::time as inicio_ventana, d.termino_ventana::time as termino_ventana, op.precio_lista, op.unidades_solicitadas, op.unidades_pickeadas, op.unidad_de_medida, op.multiplicador_unidad, op.multiplicador_unidad*op.unidades_solicitadas  as Und_Kg_solicitado, op.nota as comentario 
                        from ecommdata.orden_productos op
                        inner join ecommdata.ordenes_janis oj on oj.id = op.id_orden
                        inner join ecommdata.ubicacion_mfc um on CONCAT(um.sap_code, '-', um.measurement_unit) = op.ref_id
                        inner join ecommdata.despachos d on d.id_orden = oj.id
                        inner join analytics_and_growth.perfil_usuario pu on pu.id_cliente_janis = oj.id_cliente_janis
                        inner join analytics_and_growth.detalle_usuario du on du.user_profile_id = pu.user_profile_id
                        where oj.id_tienda_janis = 25 and d.inicio_ventana = '{v_time}';
                        """
        print(lp_query_FULL)
        pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
        pg_connection = pg_hook.get_conn()
        cursor = pg_connection.cursor()
        cursor.execute(lp_query_FULL)
        column_names = [desc[0] for desc in cursor.description]
        result_1_FULL = cursor.fetchall()
        result_1_FULL = pd.DataFrame(result_1_FULL, columns=column_names)
        print(result_1_FULL.head(20))
        cursor.close()
        pg_connection.close()
        results_FULL.append(result_1_FULL)

    else:
        results.append(None)
        results_c.append(None)
        results_q.append(None)
        results_FULL.append(None)

    if v2_time is not None:

        lp_query = f"""select op.id_orden as pedido, op.ref_id, op.ean, op.descripcion, oj.id_cliente_janis, du.nombre, du.apellido, du.fono, um.mfc_is_item_side, d.inicio_ventana::date as fecha, d.inicio_ventana::time as inicio_ventana, d.termino_ventana::time as termino_ventana, op.precio_lista, op.unidades_solicitadas, op.unidades_pickeadas, op.unidad_de_medida, op.multiplicador_unidad, op.multiplicador_unidad*op.unidades_solicitadas  as Und_Kg_solicitado, op.nota as comentario
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

        #Carnes

        lpc_query = f"""select op.id_orden as pedido, op.ref_id, op.ean, op.descripcion, oj.id_cliente_janis, du.nombre, du.apellido, du.fono, um.mfc_is_item_side, d.inicio_ventana::date as fecha, d.inicio_ventana::time as inicio_ventana, d.termino_ventana::time as termino_ventana, op.precio_lista, op.unidades_solicitadas, op.unidades_pickeadas, op.unidad_de_medida, op.multiplicador_unidad, op.multiplicador_unidad*op.unidades_solicitadas  as Und_Kg_solicitado, op.nota as comentario 
                        from ecommdata.orden_productos op
                        inner join ecommdata.ordenes_janis oj on oj.id = op.id_orden
                        inner join ecommdata.ubicacion_mfc um on CONCAT(um.sap_code, '-', um.measurement_unit) = op.ref_id
                        inner join ecommdata.despachos d on d.id_orden = oj.id
                        inner join analytics_and_growth.perfil_usuario pu on pu.id_cliente_janis = oj.id_cliente_janis
                        inner join analytics_and_growth.detalle_usuario du on du.user_profile_id = pu.user_profile_id
                        inner join ecommdata.productos p on op.ref_id = p.ref_id
                        inner join ecommdata.categorias c on p.id_categoria = c.id
                        where oj.id_tienda_janis = 25 and um.mfc_is_item_side = 'FLO' and d.inicio_ventana = '{v2_time}' and c.n1 = 'Carnes';
                        """
        print(lpc_query)
        pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
        pg_connection = pg_hook.get_conn()
        cursor = pg_connection.cursor()
        cursor.execute(lpc_query)
        column_names = [desc[0] for desc in cursor.description]
        result_2c = cursor.fetchall()
        result_2c = pd.DataFrame(result_2c, columns=column_names)
        print(result_2c.head(20))
        cursor.close()
        pg_connection.close()
        results_c.append(result_2c)

        #Quesos y Fiambres

        lpq_query = f"""select op.id_orden as pedido, op.ref_id, op.ean, op.descripcion, oj.id_cliente_janis, du.nombre, du.apellido, du.fono, um.mfc_is_item_side, d.inicio_ventana::date as fecha, d.inicio_ventana::time as inicio_ventana, d.termino_ventana::time as termino_ventana, op.precio_lista, op.unidades_solicitadas, op.unidades_pickeadas, op.unidad_de_medida, op.multiplicador_unidad, op.multiplicador_unidad*op.unidades_solicitadas  as Und_Kg_solicitado, op.nota as comentario 
                        from ecommdata.orden_productos op
                        inner join ecommdata.ordenes_janis oj on oj.id = op.id_orden
                        inner join ecommdata.ubicacion_mfc um on CONCAT(um.sap_code, '-', um.measurement_unit) = op.ref_id
                        inner join ecommdata.despachos d on d.id_orden = oj.id
                        inner join analytics_and_growth.perfil_usuario pu on pu.id_cliente_janis = oj.id_cliente_janis
                        inner join analytics_and_growth.detalle_usuario du on du.user_profile_id = pu.user_profile_id
                        inner join ecommdata.productos p on op.ref_id = p.ref_id
                        inner join ecommdata.categorias c on p.id_categoria = c.id
                        where oj.id_tienda_janis = 25 and um.mfc_is_item_side = 'FLO' and d.inicio_ventana = '{v2_time}' and c.n1 = 'Quesos y Fiambres';
                        """
        print(lpq_query)
        pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
        pg_connection = pg_hook.get_conn()
        cursor = pg_connection.cursor()
        cursor.execute(lpq_query)
        column_names = [desc[0] for desc in cursor.description]
        result_2q = cursor.fetchall()
        result_2q = pd.DataFrame(result_2q, columns=column_names)
        print(result_2q.head(20))
        cursor.close()
        pg_connection.close()
        results_q.append(result_2q)

        #REG Y FLO

        lp_query_FULL = f"""select op.id_orden as pedido, op.ref_id, op.ean, op.descripcion, oj.id_cliente_janis, du.nombre, du.apellido, du.fono, um.mfc_is_item_side, d.inicio_ventana::date as fecha, d.inicio_ventana::time as inicio_ventana, d.termino_ventana::time as termino_ventana, op.precio_lista, op.unidades_solicitadas, op.unidades_pickeadas, op.unidad_de_medida, op.multiplicador_unidad, op.multiplicador_unidad*op.unidades_solicitadas  as Und_Kg_solicitado, op.nota as comentario 
                        from ecommdata.orden_productos op
                        inner join ecommdata.ordenes_janis oj on oj.id = op.id_orden
                        inner join ecommdata.ubicacion_mfc um on CONCAT(um.sap_code, '-', um.measurement_unit) = op.ref_id
                        inner join ecommdata.despachos d on d.id_orden = oj.id
                        inner join analytics_and_growth.perfil_usuario pu on pu.id_cliente_janis = oj.id_cliente_janis
                        inner join analytics_and_growth.detalle_usuario du on du.user_profile_id = pu.user_profile_id
                        where oj.id_tienda_janis = 25 and d.inicio_ventana = '{v2_time}';
                        """
        print(lp_query_FULL)
        pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
        pg_connection = pg_hook.get_conn()
        cursor = pg_connection.cursor()
        cursor.execute(lp_query_FULL)
        column_names = [desc[0] for desc in cursor.description]
        result_2_FULL = cursor.fetchall()
        result_2_FULL = pd.DataFrame(result_2_FULL, columns=column_names)
        print(result_2_FULL.head(20))
        cursor.close()
        pg_connection.close()
        results_FULL.append(result_2_FULL)
    else:
        results.append(None)
        results_c.append(None)
        results_q.append(None)
        results_FULL.append(None)

    return [results,results_c, results_q, results_FULL]

def send_to_slack(ts, ds):
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError
    import io
    import pandas as pd

    df_list = listado_productos_sala_mfc(ts, ds)[0]
    df_list_c = listado_productos_sala_mfc(ts, ds)[1]
    df_list_q = listado_productos_sala_mfc(ts, ds)[2]
    df_list_FULL = listado_productos_sala_mfc(ts, ds)[3]
    
    if df_list[0] is not None:
        print("SENDING DF1")
        df = df_list[0]
        buffer = io.BytesIO()
        writer = pd.ExcelWriter(buffer, engine='xlsxwriter')
        df.to_excel(writer, header=True, index=False, sheet_name='Sheet1')
        writer.close()
        buffer.seek(0)
        token = Variable.get("token_slack_2")
        client = WebClient(token=token)

        try:
            client.files_upload(
            channels = Variable.get("canal_slack_lps_mfc"),
            initial_comment = "Listado de productos sala en mfc",
            filename = "listado_productos_sala_mfc.xlsx",
            content = buffer.getvalue())
        except SlackApiError as e:
            print(f"Error sending message: {e}")
        
        #Carnes

        print("SENDING DF1C")
        df = df_list_c[0]
        buffer = io.BytesIO()
        writer = pd.ExcelWriter(buffer, engine='xlsxwriter')
        df.to_excel(writer, header=True, index=False, sheet_name='Sheet1')
        writer.close()
        buffer.seek(0)
        token = Variable.get("token_slack_2")
        client = WebClient(token=token)

        try:
            client.files_upload(
            channels = Variable.get("canal_slack_lps_mfc"),
            initial_comment = "Listado de productos sala carnes en mfc",
            filename = "listado_productos_sala_mfc_c.xlsx",
            content = buffer.getvalue())
        except SlackApiError as e:
            print(f"Error sending message: {e}")

        #Carnes

        print("SENDING DF1Q")
        df = df_list_q[0]
        buffer = io.BytesIO()
        writer = pd.ExcelWriter(buffer, engine='xlsxwriter')
        df.to_excel(writer, header=True, index=False, sheet_name='Sheet1')
        writer.close()
        buffer.seek(0)
        token = Variable.get("token_slack_2")
        client = WebClient(token=token)

        try:
            client.files_upload(
            channels = Variable.get("canal_slack_lps_mfc"),
            initial_comment = "Listado de productos sala quesos y fiambres en mfc",
            filename = "listado_productos_sala_mfc_q.xlsx",
            content = buffer.getvalue())
        except SlackApiError as e:
            print(f"Error sending message: {e}")
        
        #FLO Y REG

        print("SENDING DF1FULL")
        df = df_list_FULL[0]
        buffer = io.BytesIO()
        writer = pd.ExcelWriter(buffer, engine='xlsxwriter')
        df.to_excel(writer, header=True, index=False, sheet_name='Sheet1')
        writer.close()
        buffer.seek(0)
        token = Variable.get("token_slack_2")
        client = WebClient(token=token)

        try:
            client.files_upload(
            channels = Variable.get("canal_slack_lps_mfc"),
            initial_comment = "Listado de productos BACKUP FULL",
            filename = "listado_productos_BACKUP_FULL_mfc.xlsx",
            content = buffer.getvalue())
        except SlackApiError as e:
            print(f"Error sending message: {e}")

        
    else:
        print("SKIPPING TASK")
        return
    
    if df_list[1] is not None:
        print("SENDING DF2")
        df = df_list[1]
        buffer = io.BytesIO()
        writer = pd.ExcelWriter(buffer, engine='xlsxwriter')
        df.to_excel(writer, header=True, index=False, sheet_name='Sheet1')
        writer.close()
        buffer.seek(0)
        token = Variable.get("token_slack_2")
        client = WebClient(token=token)

        try:
            client.files_upload(
            channels = Variable.get("canal_slack_lps_mfc"),
            initial_comment = "Listado de productos sala en mfc",
            filename = "listado_productos_sala_mfc.xlsx",
            content = buffer.getvalue())
        except SlackApiError as e:
            print(f"Error sending message: {e}")

        print("SENDING DF2C")
        df = df_list_c[1]
        buffer = io.BytesIO()
        writer = pd.ExcelWriter(buffer, engine='xlsxwriter')
        df.to_excel(writer, header=True, index=False, sheet_name='Sheet1')
        writer.close()
        buffer.seek(0)
        token = Variable.get("token_slack_2")
        client = WebClient(token=token)

        try:
            client.files_upload(
            channels = Variable.get("canal_slack_lps_mfc"),
            initial_comment = "Listado de productos sala carnes en mfc",
            filename = "listado_productos_sala_mfc_c.xlsx",
            content = buffer.getvalue())
        except SlackApiError as e:
            print(f"Error sending message: {e}")
        
        print("SENDING DF2Q")
        df = df_list_q[1]
        buffer = io.BytesIO()
        writer = pd.ExcelWriter(buffer, engine='xlsxwriter')
        df.to_excel(writer, header=True, index=False, sheet_name='Sheet1')
        writer.close()
        buffer.seek(0)
        token = Variable.get("token_slack_2")
        client = WebClient(token=token)

        try:
            client.files_upload(
            channels = Variable.get("canal_slack_lps_mfc"),
            initial_comment = "Listado de productos sala quesos y fiambres en mfc",
            filename = "listado_productos_sala_mfc_q.xlsx",
            content = buffer.getvalue())
        except SlackApiError as e:
            print(f"Error sending message: {e}")

        print("SENDING DF2FULL")
        df = df_list_FULL[1]
        buffer = io.BytesIO()
        writer = pd.ExcelWriter(buffer, engine='xlsxwriter')
        df.to_excel(writer, header=True, index=False, sheet_name='Sheet1')
        writer.close()
        buffer.seek(0)
        token = Variable.get("token_slack_2")
        client = WebClient(token=token)

        try:
            client.files_upload(
            channels = Variable.get("canal_slack_lps_mfc"),
            initial_comment = "Listado de productos BACKUP FULL",
            filename = "listado_productos_BACKUP_FULL_mfc.xlsx",
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
    schedule_interval="0,15,30 7-18,19 * * *",
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
