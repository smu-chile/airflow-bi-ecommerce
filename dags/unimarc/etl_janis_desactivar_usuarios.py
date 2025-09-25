from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum

def extract_and_load_inactive_pickers(**context):
    import mysql.connector
    import pandas as pd

    # --- Query en MariaDB ---
    query = """
        SELECT 
            a.id AS id_picker,
            a.employee_id,
            a.firstname,
            a.lastname,
            a.email,
            a.username,
            a.document,
            a.profile,
            a.status,
            DATE_FORMAT(FROM_UNIXTIME(a.date_created), '%Y-%m-%d %H:%i:%s') AS date_created,
            DATE_FORMAT(FROM_UNIXTIME(a.date_modified), '%Y-%m-%d %H:%i:%s') AS date_modified,
            DATE_FORMAT(MAX(FROM_UNIXTIME(pp.day)), '%Y-%m-%d %H:%i:%s') AS last_pick
        FROM admins a
        LEFT JOIN picking_productivities pp
            ON a.id = pp.picker
        WHERE a.status = 1
          AND a.profile IN (4,25)
          AND (a.email NOT LIKE '%@smu%' AND a.email NOT LIKE '%@unimarc%')
        GROUP BY 
            a.id, a.employee_id, a.firstname, a.lastname,
            a.email, a.username, a.document, a.profile,
            a.status, a.date_created, a.date_modified
        HAVING 
            MAX(pp.day) IS NOT NULL
            AND MAX(FROM_UNIXTIME(pp.day)) <= CURDATE() - INTERVAL 60 DAY;
    """

    # --- Conexión MariaDB ---
    conn = mysql.connector.connect(
        user=Variable.get("JANIS_MARIADB_USER"),
        password=Variable.get("JANIS_MARIADB_PASSWORD"),
        host=Variable.get("JANIS_MARIADB_HOST"),
        port=3306,
        database=Variable.get("JANIS_MARIADB_DATABASE")
    )
    cur = conn.cursor()
    cur.execute(query)
    results = cur.fetchall()
    columns = [i[0] for i in cur.description]
    cur.close()
    conn.close()

    if not results:
        print("⚠️ No se encontraron usuarios inactivos para insertar")
        return

    df = pd.DataFrame(results, columns=columns)
    print(f"👉 Registros encontrados: {len(df)}")

    # --- Construcción del insert ---
    columns_query = ",".join(df.columns)
    values_placeholder = ",".join(["%s"] * len(df.columns))
    insert_query = f"""
        INSERT INTO ecommdata.usuarios_desactivados ({columns_query})
        VALUES ({values_placeholder})
        ON CONFLICT (id_picker,inserted_at) DO NOTHING;
    """

    records = df.to_records(index=False).tolist()

    # --- Conexión Postgres ---
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_conn = pg_hook.get_conn()
    pg_cursor = pg_conn.cursor()

    pg_cursor.executemany(insert_query, records)
    pg_conn.commit()
    pg_cursor.close()
    pg_conn.close()

    print(f"✅ Cargados {len(records)} registros en Postgres")
    return len(records)


# --- Configuración del DAG ---
default_args = {
    "owner": "ecommerce_ops",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    "etl_janis_desactivar_usuarios",
    default_args=default_args,
    description="Carga a Postgres los pickers de Janis con última fecha de picking mayor a 60 días",
    schedule_interval="0 7 * * *",
    start_date=pendulum.datetime(2025, 6, 4, tz="America/Santiago"),
    catchup=False,
    tags=["Janis", "Usuarios", "Pickers", "Kevin"],
) as dag:

    t0 = PythonOperator(
        task_id="export_inactive_pickers",
        python_callable=extract_and_load_inactive_pickers,
    )

    t0
