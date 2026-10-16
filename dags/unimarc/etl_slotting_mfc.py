from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

from utils.slack_utils import dag_success_slack, dag_failure_slack


def check_insumos_mfc():
    """
    Verifica que las tablas de insumo requeridas para el cálculo de slotting
    tengan registros antes de truncar las tablas maestras de slotting.
    """
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            # 1. Verificar datos_logisticos_mfc (cargado por Supply)
            cursor.execute("SELECT COUNT(*) FROM ecommdata.datos_logisticos_mfc;")
            dlm_count = cursor.fetchone()[0]
            print(f"Registros en ecommdata.datos_logisticos_mfc: {dlm_count}")
            if dlm_count == 0:
                raise Exception(
                    "La tabla ecommdata.datos_logisticos_mfc está vacía. "
                    "Esta tabla es suministrada por Supply y es requerida para calcular el slotting MFC."
                )

            # 2. Verificar maestra_slotting
            cursor.execute("SELECT COUNT(*) FROM ecommdata.maestra_slotting;")
            ms_count = cursor.fetchone()[0]
            print(f"Registros en ecommdata.maestra_slotting: {ms_count}")
            if ms_count == 0:
                raise Exception(
                    "La tabla ecommdata.maestra_slotting está vacía. "
                    "Asegúrese de que el DAG etl_maestra_informacion_slotting se haya ejecutado."
                )


def validar_resultado_slotting():
    """
    Valida que las 4 tablas de escenarios de slotting MFC contengan datos
    después de la ejecución del script SQL.
    """
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    tablas = [
        "ecommdata.maestra_slotting_mfc",
        "ecommdata.maestra_slotting_mfc_150",
        "ecommdata.maestra_slotting_mfc_250",
        "ecommdata.maestra_slotting_mfc_500",
    ]

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            resumen = {}
            for tabla in tablas:
                cursor.execute(f"SELECT COUNT(*) FROM {tabla};")
                count = cursor.fetchone()[0]
                resumen[tabla] = count
                print(f"Total registros cargados en {tabla}: {count}")

            if resumen["ecommdata.maestra_slotting_mfc"] == 0:
                raise Exception(
                    "La tabla ecommdata.maestra_slotting_mfc quedó vacía tras la ejecución del script."
                )

            print("Validación de tablas de slotting MFC finalizada exitosamente.")


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "etl_slotting_mfc",
    default_args=default_args,
    description="Calculo y carga manual de escenarios de slotting MFC (maestra_slotting_mfc)",
    schedule_interval=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "postgres", "ecommdata", "MFC", "slotting", "manual"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:
    dag.doc_md = """
    ### ETL Slotting MFC (Ejecución Manual)
    Calcula los escenarios de slotting para el MFC (tienda 0917) poblando:
    - `ecommdata.maestra_slotting_mfc` (Base: mult. venta = 1)
    - `ecommdata.maestra_slotting_mfc_150` (Mult. venta = 2.5)
    - `ecommdata.maestra_slotting_mfc_250` (Mult. venta = 4)
    - `ecommdata.maestra_slotting_mfc_500` (Mult. venta = 8)

    **Insumo crítico**: `ecommdata.datos_logisticos_mfc` (suministrado por Supply).
    """

    t0_check = PythonOperator(
        task_id="check_insumos_mfc",
        python_callable=check_insumos_mfc,
    )

    t1_slotting = PostgresOperator(
        task_id="ejecutar_slotting_mfc",
        postgres_conn_id="postgresql_conn",
        sql="sql/slotting_mfc.sql",
    )

    t2_validar = PythonOperator(
        task_id="validar_resultado_slotting",
        python_callable=validar_resultado_slotting,
    )

    t0_check >> t1_slotting >> t2_validar
