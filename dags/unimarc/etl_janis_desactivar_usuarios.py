from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum

from utils.slack_utils import dag_success_slack, dag_failure_slack

def extract_and_load_inactive_pickers():
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
    return 

def deactivate_users_in_janis():
    import requests
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_conn = pg_hook.get_conn()
    pg_cursor = pg_conn.cursor()

    # 👇 Aquí eliges la lógica: todos los activos o solo los del día actual
    pg_cursor.execute("""
        SELECT id_picker, firstname, lastname, username, email, profile, document
        FROM ecommdata.usuarios_desactivados
        WHERE inserted_at::date = CURRENT_DATE
    """)
    users = pg_cursor.fetchall()
    cols = [desc[0] for desc in pg_cursor.description]
    pg_cursor.close()
    pg_conn.close()

    if not users:
        print("⚠️ No hay usuarios para desactivar en Janis")
        return 0
    print(f"👉 Usuarios a desactivar: {len(users)}")

    updated = 0
    
    # 🌐 Config API Janis
    base_url = Variable.get("JANIS_API_URL")

    api_url = f"{base_url}user"
    
    headers = {
        "janis-api-key": Variable.get("JANIS_API_KEY"),
        "janis-api-secret": Variable.get("JANIS_API_SECRET"),
        "janis-client": Variable.get("JANIS_CLIENT"),
        "Connection": "keep-alive"
    }

    for u in users:
        user = dict(zip(cols, u))
        body = {
            "firstname": user["firstname"],
            "lastname": user["lastname"],
            "username": user["username"],
            "email": user["email"],
            "profileId": str(user["profile"]),
            "documentNumber": user["document"],
            "status": "inactive"
        }
        url = f"{api_url}/{user['id_picker']}"
        try:
            r = requests.put(url, json=body, headers=headers, timeout=15)
            if r.status_code in [200, 201]:
                print(f"✅ Usuario {user['id_picker']} desactivado en Janis")
                updated += 1
            else:
                print(f"⚠️ Error desactivando {user['id_picker']}: {r.status_code} {r.text}")
        except Exception as e:
            print(f"❌ Exception con {user['id_picker']}: {e}")

    print(f"👉 Total desactivados: {updated}")
    return 


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
    schedule_interval="0 7 15 * *", #Corre el día 15 de cada mes a las 07:00
    start_date=pendulum.datetime(2025, 6, 4, tz="America/Santiago"),
    catchup=False,
    tags=["Janis", "Usuarios", "Pickers", "Kevin"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    t0 = PythonOperator(
        task_id="export_inactive_pickers",
        python_callable=extract_and_load_inactive_pickers,
    )
    t1 = PythonOperator(
        task_id="deactivate_users_in_janis",
        python_callable=deactivate_users_in_janis,
    )

    t0>>t1
