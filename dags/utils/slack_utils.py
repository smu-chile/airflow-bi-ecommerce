from __future__ import annotations
from typing import Optional
import json
import io
import pendulum
import requests
from airflow.models import Variable
from slack_sdk import WebClient

# ======================
# Mensajes de texto
# ======================

def send_text_message(
    channel_var_name: str, 
    text: str,):
    """
    Envía un mensaje de texto a un canal de Slack.

    Parámetros
    ----------
    channel_var_name : str
        Nombre de la Airflow Variable que contiene el ID del canal.
    text : str
        Mensaje a enviar (puede incluir markdown de Slack).
    """
    client = get_slack_client()
    channel_id = get_channel_id(channel_var_name)
    resp = client.chat_postMessage(channel=channel_id, text=text)
    return resp


# ======================
# Subida de archivos binarios
# ======================

def upload_bytes_to_slack(
    file_name: str,
    data_bytes: bytes,
    channel_var_name: str,
    initial_comment: str = "",
    share_publicly: bool = False,
):
    """
    Sube un archivo binario (bytes) a Slack usando el flujo:
      1) files.getUploadURLExternal
      2) POST a upload_url (binario)
      3) files.completeUploadExternal
      4) (opcional) files.sharedPublicURL

    Parámetros
    ----------
    file_name : str
        Nombre con el que aparecerá el archivo en Slack.
    data_bytes : bytes
        Contenido del archivo en bytes.
    channel_var_name : str
        Nombre de la Variable de Airflow que contiene el ID de canal.
    initial_comment : str, opcional
        Comentario que acompaña al archivo al publicarlo.
    share_publicly : bool, opcional
        Si True, se llama a files.sharedPublicURL para obtener un link público.
    """
    token = get_slack_token()
    channel_id = get_channel_id(channel_var_name)

    # 1) pedir URL de subida
    upload_url_resp = requests.post(
        "https://slack.com/api/files.getUploadURLExternal",
        data={
            "filename": file_name,
            "length": str(len(data_bytes)),
            "token": token,
        },
    ).json()
    upload_url = upload_url_resp.get("upload_url")
    file_id = upload_url_resp.get("file_id")
    if not upload_url:
        raise RuntimeError(f"Error getUploadURLExternal: {upload_url_resp}")

    # 2) subir bytes
    up_resp = requests.post(
        upload_url,
        data=data_bytes,
        headers={"Content-Type": "application/octet-stream"},
    )
    if up_resp.status_code != 200:
        raise RuntimeError(f"Error subiendo {file_name}: {up_resp.text}")

    # 3) completar subida
    complete_payload = {
        "files": [{"id": file_id}],
        "channel_id": channel_id,
        "initial_comment": initial_comment,
    }
    comp = requests.post(
        "https://slack.com/api/files.completeUploadExternal",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        data=json.dumps(complete_payload),
    ).json()
    if not comp.get("ok"):
        raise RuntimeError(f"Error completeUploadExternal {file_name}: {comp}")

    # 4) (opcional) compartir públicamente
    if share_publicly:
        share_payload = {
            "channel": channel_id,
            "file": file_id,
        }
        share_resp = requests.post(
            "https://slack.com/api/files.sharedPublicURL",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json;charset=utf-8",
            },
            data=json.dumps(share_payload),
        ).json()
        if not share_resp.get("ok"):
            raise RuntimeError(f"Error al compartir públicamente {file_name}: {share_resp}")

    return comp

# ======================
# Helpers sobre pandas.DataFrame
# ======================

def upload_df_as_excel(
    df,
    base_name: str,
    channel_var_name: str,
    initial_comment: str = "",
    sheet_name: str = "Sheet1",
    share_publicly: bool = False,
):
    """
    Convierte un DataFrame a un archivo Excel en memoria y lo sube a Slack.

    El nombre del archivo queda como: f"{base_name}_YYYY-MM-DD.xlsx"

    Parámetros
    ----------
    df : pandas.DataFrame
        Datos a exportar.
    base_name : str
        Prefijo del nombre de archivo (sin fecha ni extensión).
    channel_var_name : str
        Nombre de la Variable de Airflow que contiene el ID de canal.
    initial_comment : str, opcional
        Comentario que acompaña al archivo.
    sheet_name : str, opcional
        Nombre de la hoja de Excel.
    share_publicly : bool, opcional
        Si además se quiere compartir públicamente el archivo.
    """
    import pandas as pd  # import local para no acoplar el módulo si no se usa

    fecha_str = str(pendulum.now("America/Santiago").date())
    file_name = f"{base_name}_{fecha_str}.xlsx"

    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    buffer.seek(0)

    data_bytes = buffer.getvalue()

    return upload_bytes_to_slack(
        file_name=file_name,
        data_bytes=data_bytes,
        channel_var_name=channel_var_name,
        initial_comment=initial_comment,
        share_publicly=share_publicly,
    )


def upload_df_as_csv(
    df,
    base_name: str,
    channel_var_name: str,
    initial_comment: str = "",
    share_publicly: bool = False,
):
    """
    Convierte un DataFrame a CSV en memoria y lo sube a Slack.

    El nombre del archivo queda como: f"{base_name}_YYYY-MM-DD.csv"
    """
    import pandas as pd
    import io as _io

    fecha_str = str(pendulum.now("America/Santiago").date())
    file_name = f"{base_name}_{fecha_str}.csv"

    buf = _io.StringIO()
    df.to_csv(buf, index=False)
    data_bytes = buf.getvalue().encode("utf-8")

    return upload_bytes_to_slack(
        file_name=file_name,
        data_bytes=data_bytes,
        channel_var_name=channel_var_name,
        initial_comment=initial_comment,
        share_publicly=share_publicly,
    )

# ======================
# Callbacks de DAG
# ======================

def dag_success_slack(context):
    """
    Callback genérico para DAGs exitosos.
    Envía un mensaje a un canal estándar (ej. 'token_slack_carga_tiendas').
    """
    dag_id = context["dag"].dag_id
    run_id = context["dag_run"].run_id

    text = f":large_green_circle: DAG *{dag_id}* finalizó OK\n*Run*: `{run_id}`"
    send_text_message("token_slack_success_channel", text)


def dag_failure_slack(context):
    """
    Callback genérico para DAGs fallidos.
    Incluye el último task y la URL de log.
    """
    dag_id = context["dag"].dag_id
    dag_run = context["dag_run"]
    ti = context.get("task_instance")

    text = (
        f":red_circle: DAG *{dag_id}* FALLÓ\n"
        f"*Run*: `{dag_run.run_id}`\n"
        f"*Último task*: `{ti.task_id if ti else 'n/a'}`\n"
        f"*Log*: {ti.log_url if ti else 'n/a'}"
    )
    send_text_message("token_slack_failed_channel", text)


# ======================
# Helpers base internos
# ======================

def get_slack_token() -> str:
    return Variable.get("token_slack_bot")


def get_channel_id(channel_var_name: str) -> str:
    return Variable.get(channel_var_name)


def get_slack_client() -> WebClient:
    """
    Devuelve un cliente WebClient de slack_sdk listo para usar.
    Estándar: siempre usa el token almacenado en 'token_slack_bot'.
    """
    return WebClient(token=get_slack_token())