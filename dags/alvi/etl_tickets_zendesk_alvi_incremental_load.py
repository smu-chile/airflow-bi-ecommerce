from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.slack_utils import dag_failure_slack, dag_success_slack

from datetime import datetime
import pendulum

def _load_ticket_zendesk_to_s3(ts, ds):
    from utils.getTicketFields import get_ticket_fields
    from utils.getTicketFormTipificacion import get_ticket_form_tipificacion
    from utils.listTicketsUpdatedLast4Hours import get_tickets_updated_last_4_hours
    from utils.getZendeskDataFromURL import get_zendesk_data_from_url
    from utils.getFieldValueFromIDField import get_value_from_key
    from utils.helperToGetTipologias import helpers_to_get_tipologias
    from utils.getTipologias import get_tipologias
    from utils.getEstado import get_estado
    from utils.getFieldValueFromIDFieldNumeric import get_value_from_id_field_numeric
    from utils.getAuditsByTicketId import get_audits_by_ticket_id
    from utils.getMetricsByTicketId import get_metrics_by_ticket_id
    from utils.getUserById import get_user_by_id
    from utils.getIdTienda import get_id_tienda
    from utils.cut_otros import truncate_text
    from utils.getTicketFormCentroDeAyuda import get_ticket_form_tipificacion_centro_ayuda
    from utils.helperToGetTipologiasCentroAyuda import helpers_to_get_tipologias_centro_ayuda
    from utils.getTipologiasCentroDeAyuda import get_tipologias_centro_de_ayuda

    
    import pandas as pd
    import io
    from datetime import timedelta
    
    url = Variable.get("ZENDESK_URL")
    API_KEY = Variable.get("ZENDESK_API_KEY")
    tickets_to_print = []
    fields = get_ticket_fields(url, API_KEY)
    formulario_tipificacion = get_ticket_form_tipificacion(url, API_KEY)

    helpers = helpers_to_get_tipologias(formulario_tipificacion, fields)

    formulario_tipificacion_centro_ayuda = get_ticket_form_tipificacion_centro_ayuda(url, API_KEY)
    helpers_centro_ayuda = helpers_to_get_tipologias_centro_ayuda(formulario_tipificacion_centro_ayuda, fields)

    ts_from = ((datetime.strptime(ts[:19], '%Y-%m-%dT%H:%M:%S')) + timedelta(hours=-1))
    ts_from = ts_from.strftime("%Y-%m-%dT%H:%M:%S")
    ts_until = datetime.strptime(ts[:19], '%Y-%m-%dT%H:%M:%S')
    ts_until = ts_until.strftime("%Y-%m-%dT%H:%M:%S")

    tickets_response = get_tickets_updated_last_4_hours(ts_from, ts_until, 'alvi', 1, url, API_KEY)
    tickets = tickets_response['results']
    tickets_to_search = tickets_response['count']
    next_page = tickets_response['next_page']
    print('cantidad de respuestas esperadas:', tickets_to_search)
    
    while len(tickets) < tickets_to_search:
        response_from_url = get_zendesk_data_from_url(next_page, API_KEY)
        tickets_for_append = response_from_url['results']
        tickets.extend(tickets_for_append)
        new_url = response_from_url['next_page']
        next_page = new_url
    
    print([ticket['id'] for ticket in tickets])
    print('largo del arreglo:', len(tickets))
    
    for ticket in tickets:
        print(ticket['id'])
        id = ticket['id']
        external_id = ticket['external_id']
        via = ticket['via']
        status = ticket['status']
        created_at = ticket['created_at']
        updated_at = ticket['updated_at']
        requester_id = ticket['requester_id']
        array_campos_personalizados = ticket['custom_fields']
        tags = ticket['tags']
        
        tipologias = get_tipologias(array_campos_personalizados, fields, helpers)
        tipologias_centro_de_ayuda = get_tipologias_centro_de_ayuda(array_campos_personalizados, fields, helpers_centro_ayuda)
        closed_by_merge = 'closed_by_merge' in tags
        print('id_ticket:', id)
        nombre_tienda = get_value_from_key(360052677134, array_campos_personalizados, fields)
        print(nombre_tienda)
        
        ticket_json = {
            'id_ticket': id,
            'estado': get_estado(status),
            'fecha_actualizacion': updated_at,
            'fecha_creacion': created_at,
            'fecha_cierre': None,
            'motivo': tipologias['motivo'],
            'motivo_devolucion': get_value_from_key(360053290773, array_campos_personalizados, fields),
            'via_devolucion': get_value_from_key(360052307694, array_campos_personalizados, fields),
            'estado_devolucion': get_value_from_key(360054407433, array_campos_personalizados, fields),
            'fecha_devolucion': get_value_from_id_field_numeric(360053290793, array_campos_personalizados),
            'tienda': nombre_tienda,
            'id_tienda': get_id_tienda(nombre_tienda),
            'gestion': get_value_from_key(360052307914, array_campos_personalizados, fields),
            'canal': get_value_from_key(1900006876165, array_campos_personalizados, fields),
            'id_reclamo_sernac': get_value_from_key(5262834532375, array_campos_personalizados, fields),
            'numero_pedido': get_value_from_id_field_numeric(360056187993, array_campos_personalizados),
            'numero_boleta': get_value_from_id_field_numeric(1500004207001, array_campos_personalizados),
            'id_caso_janis': get_value_from_id_field_numeric(360052503074, array_campos_personalizados),
            'monto_devolucion': get_value_from_id_field_numeric(360053290593, array_campos_personalizados),
            'tipo1': tipologias['tipo1'],
            'tipo2': tipologias['tipo2'],
            'tipo3': tipologias['tipo3'],
            'total_dias_hasta_resolucion': None,
            'cerrado_por_merge': closed_by_merge,
            'fecha_primera_respuesta': None,
            'horas_primera_respuesta': None,
            'id_ticket_fusionado': None,
            'ids_tickets_hijos': None,
            'id_caso_chatbot': get_value_from_id_field_numeric(11440875268759, array_campos_personalizados),
            'monto_cupon': get_value_from_key(14714485034135, array_campos_personalizados, fields),
            'tipo_nc': get_value_from_key(11399111912087, array_campos_personalizados, fields),
            'folio_nc': get_value_from_id_field_numeric(360053290533, array_campos_personalizados),
            'medio_de_pago': get_value_from_key(360053290693, array_campos_personalizados, fields),
            'fecha_emision_nc': get_value_from_id_field_numeric(1500003696702, array_campos_personalizados),
            'sku_producto_afectado': get_value_from_id_field_numeric(6454270157847, array_campos_personalizados),
            'estado_inscripcion': get_value_from_key(15803739697687, array_campos_personalizados, fields),
            'sso_origen': get_value_from_key(15928986371223, array_campos_personalizados, fields),
            'tipo_de_comerciante': get_value_from_key(14941626293655, array_campos_personalizados, fields),
            'id_agente_resolutor': None,
            'id_remitente': requester_id,
            'rol_remitente': None,
            'analista_responsable': get_value_from_key(15516227194647, array_campos_personalizados, fields),
            'user_profile_id': external_id,
            'area_picking_mfc': get_value_from_key(16537458692631, array_campos_personalizados, fields),
            'nombre_pickeador': get_value_from_id_field_numeric(16537260072599, array_campos_personalizados),
            'motivo_de_cancelacion': get_value_from_key(7147271944599, array_campos_personalizados, fields),
            'motivo_de_cancelacion_otro':truncate_text(get_value_from_id_field_numeric(7150722487959, array_campos_personalizados)),
            'motivo_centro_de_ayuda': tipologias_centro_de_ayuda['motivo'],
            'tipo1_centro_de_ayuda': tipologias_centro_de_ayuda['tipo1'],
            'tipo2_centro_de_ayuda': tipologias_centro_de_ayuda['tipo2'],
            'tipo3_centro_de_ayuda': tipologias_centro_de_ayuda['tipo3'],
            'tipo_de_registro':get_value_from_key(23322081338519, array_campos_personalizados, fields),
            'estado_de_inscripcion':get_value_from_key(15803739697687, array_campos_personalizados, fields)
        }
        
        estado = ticket_json['estado']
        if estado in ['Closed', 'Solved']:
            metrics = get_metrics_by_ticket_id(id, url, API_KEY)
            if metrics:
                dias_resolucion = metrics['full_resolution_time_in_minutes']['calendar'] / 1440
                horas_primera_respuesta = metrics['first_resolution_time_in_minutes']['business'] / 60
                ticket_json['fecha_cierre'] = metrics['solved_at']
                ticket_json['total_dias_hasta_resolucion'] = round(dias_resolucion, 2)
                ticket_json['horas_primera_respuesta'] = round(horas_primera_respuesta, 2)
        
        audits = get_audits_by_ticket_id(id, url, API_KEY)
        if isinstance(audits, list):
            evento_cierre = [e for e in audits if any(
                u['type'] == 'Change' and u['value'] in ['solved', 'closed']
                and u['field_name'] == 'status' and u['previous_value'] in ['open', 'new', 'hold', 'pending']
                for u in e['events']
            )]
            evento_cierre_obj = evento_cierre[-1] if evento_cierre else {}
            resolutor = evento_cierre_obj.get('author_id', None) if evento_cierre else None
            merged = [e for e in audits if e['via']['source']['rel'] == 'merge']
            ticket_ids = lambda a: a['via']['source']['from']['ticket_ids'] if 'via' in a and 'source' in a['via'] and 'from' in a['via']['source'] and 'ticket_ids' in a['via']['source']['from'] else []
            audits_merged = [{
                'id_ticket_fusionado': e['via']['source']['from']['ticket_id'],
                'ids_ticket_hijos': ticket_ids(e)
            } for e in merged]

            info_fusion = {
                'id_ticket': id,
                'id_ticket_fusionado': ','.join({str(e['id_ticket_fusionado']) for e in audits_merged if e['id_ticket_fusionado'] is not None}),
                'ids_ticket_hijos': ','.join({str(i) for e in audits_merged for i in e['ids_ticket_hijos']})
            }
            ticket_json['id_ticket_fusionado'] = info_fusion['id_ticket_fusionado']
            ticket_json['ids_tickets_hijos'] = info_fusion['ids_ticket_hijos']
            ticket_json['id_agente_resolutor'] = resolutor

            remitente = get_user_by_id(requester_id, url, API_KEY)
            ticket_json['rol_remitente'] = remitente.get('role', None)
        
        
        tickets_to_print.append(ticket_json)
    df = pd.DataFrame(tickets_to_print)

    exec_date = ds.replace("-", "/")
    date_aux = ts.replace("-", "_".replace("T", "").replace(":", ""))

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    buffer = io.StringIO()
    df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"zendesk_alvi/{exec_date}/tickets_zendesk_alvi{date_aux}.csv"
    buffer.seek(0)
    print("se logro transformar el dataframe a un archivo .csv")
    print(f"con fecha {ds} y nombre de filename como {filename}")
    s3_hook.load_string(buffer.getvalue(),
                key=filename,
                bucket_name=s3_bucket,
                replace=True,
                encrypt=False)
    print(f"File saved on S3")

    return filename

def _save_tickets_zendesk_in_postgres(ti):
    import numpy as np
    import pandas as pd
    
    zendesk_file = ti.xcom_pull(key="return_value", task_ids=["load_ticket_zendesk_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+zendesk_file)
    if not s3_hook.check_for_key(zendesk_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % zendesk_file)

    zendesk_object = s3_hook.get_key(zendesk_file, bucket_name=s3_bucket)

    df = pd.read_csv(zendesk_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")

    df['monto_cupon'] = pd.to_numeric(df['monto_cupon'], errors='coerce')
 
    df['id_tienda'] = df['id_tienda'].apply(lambda x: str(int(x)).zfill(4) if pd.notna(x) and x == x else '')
    df['id_tienda'] = df['id_tienda'].astype(str)


    df = df.astype({
        'id_ticket': "int",
        'estado': "string",
        'fecha_actualizacion': "string",
        'fecha_creacion': "string",
        'fecha_cierre': "string",
        'motivo': "string",
        'motivo_devolucion': "string",
        'via_devolucion': "string",
        'estado_devolucion': "string",
        'fecha_devolucion': "string",
        'tienda': "string",
        'gestion': "string",
        'canal': "string",
        'id_reclamo_sernac': "string",
        'numero_pedido': "int",
        'numero_boleta': "int",
        'id_caso_janis': "int",
        'monto_devolucion': "int",
        'tipo1': "string",
        'tipo2': "string",
        'tipo3': "string",
        'total_dias_hasta_resolucion': "float",
        'cerrado_por_merge': "bool",
        'fecha_primera_respuesta': "string",
        'horas_primera_respuesta': "float",
        'id_ticket_fusionado': "int",
        'ids_tickets_hijos': "string",
        'id_caso_chatbot': "int",
        'monto_cupon': "int",
        'tipo_nc': "string",
        'folio_nc': "string",
        'medio_de_pago': "string",
        'fecha_emision_nc': "string",
        'sku_producto_afectado': "string",
        'estado_inscripcion': "string",
        'sso_origen': "string",
        'tipo_de_comerciante': "string",
        'id_agente_resolutor': "int",
        'id_remitente': "int",
        'rol_remitente': "string",
        'analista_responsable': "string",
        'user_profile_id': "string",
        'area_picking_mfc': "string",
        'nombre_pickeador': "string",
        'motivo_de_cancelacion': "string",
        'motivo_de_cancelacion_otro': "string",
        'motivo_centro_de_ayuda': "string",
        'tipo1_centro_de_ayuda': "string",
        'tipo2_centro_de_ayuda': "string",
        'tipo3_centro_de_ayuda': "string",
        'tipo_de_registro': "string",
        'estado_de_inscripcion': "string"

    }, errors="ignore")

    columns = [
        'estado',
        'fecha_actualizacion',
        'fecha_creacion',
        'fecha_cierre',
        'motivo',
        'motivo_devolucion',
        'via_devolucion',
        'estado_devolucion',
        'fecha_devolucion',
        'tienda',
        'id_tienda',
        'gestion',
        'canal',
        'id_reclamo_sernac',
        'numero_pedido',
        'numero_boleta',
        'id_caso_janis',
        'monto_devolucion',
        'tipo1',
        'tipo2',
        'tipo3',
        'total_dias_hasta_resolucion',
        'cerrado_por_merge',
        'fecha_primera_respuesta',
        'horas_primera_respuesta',
        'id_ticket_fusionado',
        'ids_tickets_hijos',
        'id_caso_chatbot',
        'monto_cupon',
        'tipo_nc',
        'folio_nc',
        'medio_de_pago',
        'fecha_emision_nc',
        'sku_producto_afectado',
        'estado_inscripcion',
        'sso_origen',
        'tipo_de_comerciante',
        'id_agente_resolutor',
        'id_remitente',
        'rol_remitente',
        'analista_responsable',
        'user_profile_id',
        'area_picking_mfc',
        'nombre_pickeador',
        'motivo_de_cancelacion',
        'motivo_de_cancelacion_otro',
        'motivo_centro_de_ayuda',
        'tipo1_centro_de_ayuda',
        'tipo2_centro_de_ayuda',
        'tipo3_centro_de_ayuda',
        'tipo_de_registro',
        'estado_de_inscripcion'
    
    ]
    columns_query = ",".join(columns)
    excluded_query = ",".join(["EXCLUDED."+column for column in columns])
    values_query = "%s,"+",".join(["%s" for column in columns])
    df = df.fillna("NULL")
    records = list(df.to_records(index=False))
    
    # Change data types to native python types
    fixed_records = []
    for record in records:
        fixed_record = []
        for value in record:
            if isinstance(value, np.generic):
                fixed_record.append(value.item())
            elif value == "NULL":
                fixed_record.append(None)
            else:
                fixed_record.append(value)
        fixed_records.append(tuple(fixed_record))
    print(f"Number of records to lo.ad: {str(len(fixed_records))}")
    incremental_query = """
        INSERT INTO analytics_and_growth.tickets_zendesk_alvi (id_ticket,"""+columns_query+""") 
        VALUES ("""+values_query+""")
        ON CONFLICT (id_ticket)
        DO UPDATE SET ("""+columns_query+""") = ("""+excluded_query+""")
    """
    print(incremental_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.executemany(incremental_query, fixed_records)
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    print("Data loaded to Postgres")

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_tickets_zendesk_alvi_incremental_load_from_api',
    default_args=default_args,
    description="Extracción y carga de tabla tickets desde Zendesk hasta Workspace.",
    schedule_interval="0 * * * *",
    start_date=pendulum.datetime(2023, 8, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["Zendesk", "Alvi", "analytics_and_growth", "MATIAS"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    "Extracción y carga de tabla tickets desde Zendesk hasta Workspace."
    """ 
    t0 = PythonOperator(
        task_id = "load_ticket_zendesk_to_s3",
        python_callable = _load_ticket_zendesk_to_s3
    )

    t1 = PythonOperator(
        task_id = "save_tickets_zendesk_in_postgres",
        python_callable =  _save_tickets_zendesk_in_postgres,
    )

    t0 >> t1