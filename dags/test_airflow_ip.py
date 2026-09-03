from datetime import datetime
import os
import platform
import socket
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator


def get_airflow_server_ip():
    """
    Obtiene e imprime la IP Privada (local/red interna) y la IP Pública de salida (Egress/NAT).
    """
    hostname = socket.gethostname()
    
    # 1. IP Privada / Local
    try:
        private_ip = socket.gethostbyname(hostname)
    except Exception as e:
        private_ip = f"No detectada ({e})"

    # 2. IP Pública de salida (Egress / NAT Gateway)
    public_ip = "No detectada"
    endpoints = [
        "https://api.ipify.org",
        "https://checkip.amazonaws.com",
        "https://ifconfig.me/ip"
    ]
    for url in endpoints:
        try:
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200 and resp.text.strip():
                public_ip = resp.text.strip()
                break
        except Exception:
            continue

    print("\n" + "=" * 70)
    print("           INFORMACIÓN DE RED DEL SERVIDOR / WORKER AIRFLOW")
    print("=" * 70)
    print(f"  • Hostname:                 {hostname}")
    print(f"  • IP Privada (Red Interna): {private_ip}")
    print(f"  • IP Pública (Salida/NAT):  {public_ip}")
    print("-" * 70)
    print(f"  • Sistema Operativo:        {platform.system()} {platform.release()}")
    print(f"  • Usuario del Sistema:      {os.getenv('USER') or os.getenv('USERNAME') or 'N/A'}")
    print("=" * 70 + "\n")


default_args = {
    "owner": "ecommerce_data",
    "retries": 0,
    "email_on_failure": False,
}

with DAG(
    dag_id="test_airflow_server_ip",
    default_args=default_args,
    description="Obtiene la IP Privada y Pública del servidor/worker Airflow (Manual)",
    schedule_interval=None,  # Solo ejecución manual
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["TEST", "NETWORK", "INFRA"],
) as dag:

    dag.doc_md = """
    ### Consulta de IP del Servidor Airflow
    Muestra en los logs del task:
    - **IP Privada**: IP asignada al contenedor/servidor en la red interna.
    - **IP Pública (Egress)**: IP de salida hacia internet o redes externas (útil para whitelists en Firewalls/Bases de Datos).
    """

    task_get_ip = PythonOperator(
        task_id="get_server_ip",
        python_callable=get_airflow_server_ip,
    )
