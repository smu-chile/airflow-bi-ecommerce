from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.sensors.external_task import ExternalTaskSensor

import pendulum

def get_and_send_audits_daily():
    """
    Consulta las auditorías del día (según ds) 
    y las envía por Slack en un Excel.
    """

    from sqlalchemy.ext.automap import automap_base
    from sqlalchemy.orm import Session
    from sqlalchemy import func, distinct, MetaData
    from sqlalchemy.dialects import postgresql

    import pandas as pd
    from slack_sdk import WebClient
    import requests, json, io, os

    # Conectar y reflejar tablas
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    engine  = pg_hook.get_sqlalchemy_engine()

    # Metadata limitado a tablas interesadas
    metadata = MetaData(schema="ecommdata")
    metadata.reflect(
        bind=engine,
        only=["ordenes_janis", "orden_cambios_de_estado", "tiendas", "despachos"]
    )
    Base = automap_base(metadata=metadata) 
    Base.prepare()

    OrdenesJanis  = Base.classes.ordenes_janis
    OrdenCambios  = Base.classes.orden_cambios_de_estado
    Tiendas       = Base.classes.tiendas
    Despachos     = Base.classes.despachos

    # Fecha a filtrar (día de ejecución)
    session = Session(engine)
    fecha_consulta = pendulum.now("America/Santiago").date()

    # Query ORM
    q = (
        session.query(
            Tiendas.id.label("tienda"),
            Despachos.id_transportadora.label("transportadora"),
            func.count(distinct(OrdenesJanis.id)).label("total auditorias")
        )
        .outerjoin(OrdenCambios, OrdenCambios.id_orden == OrdenesJanis.janis_id)
        .outerjoin(Tiendas,      Tiendas.id_janis   == OrdenesJanis.id_tienda_janis)
        .outerjoin(Despachos,    Despachos.id_orden       == OrdenesJanis.id)
        .filter(
            OrdenCambios.estado_nuevo == 5,
            func.date(OrdenesJanis.fecha_facturacion) == fecha_consulta
        )
        .group_by(
            Tiendas.id,
            Despachos.id_transportadora)
        .order_by(func.count(distinct(OrdenesJanis.id)).desc())
    )
    
    print(f"↪ fecha_consulta = {fecha_consulta}")

    df = pd.read_sql(q.statement, engine)
    session.close()

    # Si no hay datos, avisar y salir
    token      = Variable.get("token_slack_bot")
    channel_id = Variable.get("tocken_slack_channel_sac_audits")
    client     = WebClient(token=token)

    if df.empty:
        client.chat_postMessage(
            channel=channel_id,
            text=f"<!channel> 🔍 No se encontraron auditorías para {fecha_consulta}"
        )
        return

    # Crear Excel en memoria
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Auditorias")
    buffer.seek(0)

    # Subir a Slack
    file_name = f"auditorias_{fecha_consulta}.xlsx"
    file_size = buffer.getbuffer().nbytes

    # obtener URL de subida
    upload_url_resp = requests.post(
        "https://slack.com/api/files.getUploadURLExternal",
        data={
            "filename": file_name,
            "length": str(file_size),
            "token": token
        }
    ).json()

    upload_url = upload_url_resp.get("upload_url")
    file_id    = upload_url_resp.get("file_id")

    if not upload_url:
        raise RuntimeError(f"Error getUploadURLExternal: {upload_url_resp}")

    # subir bytes
    up_resp = requests.post(
        upload_url,
        data=buffer,
        headers={"Content-Type":"application/octet-stream"}
    )
    if up_resp.status_code != 200:
        raise RuntimeError(f"Error subiendo archivo: {up_resp.text}")

    # completar subida
    complete_payload = {
        "files": [{"id": file_id}],
        "channel_id": channel_id,
        "initial_comment": f"<!channel> 🕵️ Auditorías para {fecha_consulta}"
    }
    comp = requests.post(
        "https://slack.com/api/files.completeUploadExternal",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        },
        data=json.dumps(complete_payload)
    ).json()
    if not comp.get("ok"):
        raise RuntimeError(f"Error completeUploadExternal: {comp}")

    print(f"✅ Auditorías de {fecha_consulta} enviadas correctamente.")


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'etl_alertas_sac_daily_audits',
    default_args=default_args,
    description="Generación de alertas para SAC",
    schedule_interval="30 21 * * *", 
    start_date=pendulum.datetime(2025, 3, 30, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["Alertas", "SAC", "Pedidos", "Auditoría", "FRANCISCO"]
) as dag:
    
    dag.doc_md = """
    Alertas para SAC, asociada a la cantidad de auditorías realizadas por tienda.
    """ 

    t0 = ExternalTaskSensor(
        task_id="wait_for_janis_cambios_de_estados",
        external_dag_id='etl_ordenes_janis_cambios_de_estado_incremental_load',
        external_task_id=None,
        allowed_states=['success'],
        failed_states=['failed']
    )

    t1 = PythonOperator(
        task_id="get_and_send_audits_daily",
        python_callable=get_and_send_audits_daily,
        provide_context=True,
    )

    t0 >> t1