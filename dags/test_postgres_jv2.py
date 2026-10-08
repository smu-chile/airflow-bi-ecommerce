from datetime import datetime
import psycopg2

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator


def test_auth_jv2():
    host = Variable.get("HOST_JV2")
    port = Variable.get("PUERTO_JV2", default_var="5432")
    user = Variable.get("USER_JV2")
    password = Variable.get("PASS_JV2")
    # Si definen DB_JV2 usa esa base, de lo contrario prueba con 'postgres'
    database = Variable.get("DB_JV2", default_var="postgres")

    print("=" * 70)
    print(f"[*] Iniciando prueba de conexión y autenticación:")
    print(f"    - Host: {host}")
    print(f"    - Puerto: {port}")
    print(f"    - Usuario: {user}")
    print(f"    - Base de Datos: {database}")
    print("=" * 70)

    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=database,
            connect_timeout=10,
        )

        cursor = conn.cursor()
        cursor.execute("SELECT version(), current_database(), current_user, now();")
        version, db_name, current_user, server_time = cursor.fetchone()

        print("\n" + "=" * 70)
        print("  ✓ ¡CONEXIÓN Y AUTENTICACIÓN EXITOSA!")
        print("=" * 70)
        print(f"  - Motor: {version}")
        print(f"  - Base de datos conectada: {db_name}")
        print(f"  - Usuario autenticado: {current_user}")
        print(f"  - Hora del servidor: {server_time}")
        print("=" * 70)

        cursor.close()
        conn.close()

    except psycopg2.OperationalError as e:
        print("\n" + "=" * 70)
        print("  ✗ ERROR DE CONEXIÓN / AUTENTICACIÓN:")
        print("=" * 70)
        print(f"  Detalle del error:\n  {e}")
        print("=" * 70)
        raise e


default_args = {
    "owner": "ecommerce_data",
    "retries": 0,
    "email_on_failure": False,
}

with DAG(
    dag_id="test_postgres_jv2_auth",
    default_args=default_args,
    description="Test de conectividad y autenticación PostgreSQL JV2 (Manual)",
    schedule_interval=None,  # Solo ejecución manual
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["TEST", "POSTGRES", "JV2"],
) as dag:

    dag.doc_md = """
    ### Test de Conexión y Autenticación PostgreSQL JV2
    Valida alcance de red y autenticación utilizando las variables:
    - `HOST_JV2`
    - `PUERTO_JV2` (default: 5432)
    - `USER_JV2`
    - `PASS_JV2`
    - `DB_JV2` (opcional, default: postgres)
    """

    t1_test_auth = PythonOperator(
        task_id="test_postgres_auth",
        python_callable=test_auth_jv2,
    )
