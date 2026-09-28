from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import pendulum

# -------------------------------------------------------------
# Función de verificación segura de permisos en BigQuery
# -------------------------------------------------------------
def check_bq_permissions(**context):
    """
    Verifica de forma 100% segura los permisos en BigQuery sobre un dataset objetivo:
    1. Acceso y lectura del dataset/tablas.
    2. Creación de una tabla de prueba temporal con nombre único (_test_permission_check_<ts>).
    3. Inserción de 1 registro de prueba.
    4. Lectura (SELECT) del registro insertado.
    5. Eliminación inmediata (DROP) de la tabla de prueba en bloque try/finally.
    
    ¿Por qué no daña ningún dato?
    - Crea una tabla temporal nueva, nunca toca ninguna tabla existente.
    - Se elimina inmediatamente en el 'finally', dejando el dataset exactamente igual que antes.
    - Como red de seguridad adicional, la tabla temporal tiene auto-expiración de 1 hora.
    """
    import time
    from google.oauth2 import service_account
    from google.cloud import bigquery

    TARGET_PROJECT = "cl-cda-unidata-dev"
    TARGET_DATASET = "DS_UNIDATA_CRM"
    dataset_ref = f"{TARGET_PROJECT}.{TARGET_DATASET}"
    temp_table_name = f"_test_permission_check_{int(time.time())}"
    table_ref = f"{dataset_ref}.{temp_table_name}"

    sa_info = Variable.get("BIGQUERY_CREDENTIALS", deserialize_json=True)
    sa_email = sa_info.get("client_email", "Desconocido")
    billing_project = sa_info.get("project_id", "cl-ecommerce-analytics")

    print("=" * 65)
    print("🔍 VERIFICACIÓN DE PERMISOS BIGQUERY")
    print(f"  • Service Account : {sa_email}")
    print(f"  • Billing Project : {billing_project}")
    print(f"  • Target Dataset  : {dataset_ref}")
    print(f"  • Test Table      : {table_ref} (temporal/aislada)")
    print("=" * 65)

    creds = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client = bigquery.Client(
        project=billing_project,
        credentials=creds,
    )

    results = {
        "dataset_access": False,
        "read_tables": False,
        "create_table": False,
        "insert_data": False,
        "select_test": False,
        "drop_table": False,
    }

    # 1. Probar acceso y lectura al Dataset
    print("\n[PASO 1] Verificando acceso y lectura al dataset...")
    try:
        ds = client.get_dataset(dataset_ref)
        print(f"  ✅ Acceso exitoso a dataset '{ds.dataset_id}' (Ubicación: {ds.location})")
        results["dataset_access"] = True
    except Exception as e:
        print(f"  ❌ Falló get_dataset: {e}")

    try:
        tables = list(client.list_tables(dataset_ref, max_results=5))
        print(f"  ✅ Listar tablas exitoso. Encontradas {len(tables)} tablas:")
        for t in tables:
            print(f"     - Tabla existente: {t.table_id}")
        results["read_tables"] = True
    except Exception as e:
        print(f"  ❌ Falló list_tables: {e}")

    # 2. Probar CREATE, INSERT y SELECT de prueba
    table_created = False
    try:
        print(f"\n[PASO 2] Probando creación de tabla temporal '{temp_table_name}'...")
        sql_create = f"""
            CREATE TABLE `{table_ref}` (
                id INT64,
                test_col STRING,
                created_at TIMESTAMP
            )
            OPTIONS(
                expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR),
                description="Tabla temporal de prueba para verificar permisos de Airflow"
            );
        """
        client.query(sql_create).result()
        table_created = True
        results["create_table"] = True
        print(f"  ✅ CREATE TABLE exitoso en '{table_ref}'")

        print(f"\n[PASO 3] Probando INSERT en tabla temporal...")
        sql_insert = f"""
            INSERT INTO `{table_ref}` (id, test_col, created_at)
            VALUES (1, 'test_airflow_permissions', CURRENT_TIMESTAMP());
        """
        client.query(sql_insert).result()
        results["insert_data"] = True
        print(f"  ✅ INSERT INTO exitoso en '{table_ref}'")

        print(f"\n[PASO 4] Probando SELECT de verificación en la tabla creada...")
        sql_select = f"SELECT * FROM `{table_ref}`;"
        df_check = client.query(sql_select).to_dataframe()
        print(f"  ✅ SELECT exitoso. Filas leídas: {len(df_check)}")
        print(df_check)
        results["select_test"] = True

    except Exception as e:
        print(f"  ❌ Error en operaciones de escritura: {e}")

    finally:
        # Siempre eliminamos la tabla de prueba si llegó a crearse
        if table_created:
            print(f"\n[PASO 5] Limpieza: Eliminando tabla temporal '{table_ref}'...")
            try:
                client.delete_table(table_ref, not_found_ok=True)
                results["drop_table"] = True
                print(f"  ✅ DROP TABLE exitoso. El dataset quedó 100% limpio sin residuos.")
            except Exception as e:
                print(f"  ⚠️ Error al eliminar tabla temporal (expirará en 1 hora automáticamente): {e}")

    # Resumen final
    print("\n" + "=" * 65)
    print("📊 RESUMEN FINAL DE PERMISOS:")
    print(f"  1. Acceso al Dataset (get_dataset) : {'✅ PERMITIDO' if results['dataset_access'] else '❌ DENEGADO'}")
    print(f"  2. Listar Tablas (list_tables)     : {'✅ PERMITIDO' if results['read_tables'] else '❌ DENEGADO'}")
    print(f"  3. Crear Tablas (CREATE TABLE)     : {'✅ PERMITIDO' if results['create_table'] else '❌ DENEGADO'}")
    print(f"  4. Insertar Datos (INSERT INTO)    : {'✅ PERMITIDO' if results['insert_data'] else '❌ DENEGADO'}")
    print(f"  5. Leer Datos (SELECT)             : {'✅ PERMITIDO' if results['select_test'] else '❌ DENEGADO'}")
    print(f"  6. Limpieza (DROP TABLE)           : {'✅ LIMPIEZA OK' if results['drop_table'] else 'N/A'}")
    print("=" * 65)

    if not all([results['dataset_access'], results['read_tables'], results['create_table'], results['insert_data']]):
        print(f"\n💡 ACCIÓN REQUERIDA EN GCP:")
        print(f"Para habilitar lectura y escritura en '{dataset_ref}', solicitar al administrador de GCP:")
        print(f"Asignar sobre el dataset '{dataset_ref}' a la cuenta de servicio:")
        print(f"  👉 {sa_email}")
        print(f"El rol de BigQuery:")
        print(f"  • 'roles/bigquery.dataEditor' (Permite leer, crear tablas e insertar datos)")
        print(f"O a nivel granular los permisos IAM:")
        print(f"  • bigquery.datasets.get")
        print(f"  • bigquery.tables.get")
        print(f"  • bigquery.tables.getData")
        print(f"  • bigquery.tables.list")
        print(f"  • bigquery.tables.create")
        print(f"  • bigquery.tables.updateData")
        print("=" * 65)


default_args = {
    "owner": "ecommerce_data",
    "retries": 0,
    "email_on_failure": False,
}

with DAG(
    dag_id="test_gcp_var_creds",
    description="Test de permisos BigQuery en cl-cda-unidata-dev.DS_UNIDATA_CRM",
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["TEST", "Francisco", "BIGQUERY"],
    default_args=default_args,
    schedule_interval=None,   # Ejecución manual bajo demanda
) as dag:

    t1 = PythonOperator(
        task_id="check_bigquery_permissions",
        python_callable=check_bq_permissions,
    )