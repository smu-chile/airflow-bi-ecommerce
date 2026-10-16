from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from pymongo import MongoClient

from utils.slack_utils import dag_success_slack, dag_failure_slack


def sync_osr_to_mongo():
    """
    Extrae los productos aprobados para OSR (no mermables) desde PostgreSQL
    (ecommdata.maestra_slotting_mfc) y sincroniza la coleccion
    INTEGRATIONS.osr_item_conversions en MongoDB.
    """
    # 1. Extracción desde PostgreSQL
    query = """
        SELECT DISTINCT 
            TRIM(msm.ref_id) AS ref_id,
            true AS osr
        FROM ecommdata.maestra_slotting_mfc msm
        WHERE msm.candidato_merma ILIKE '%no%'
          AND msm.restriccion_merma_00 ILIKE '%no%'
          AND msm.ref_id IS NOT NULL;
    """
    print(f"Ejecutando query de extracción en PostgreSQL:\n{query}")
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    with pg_hook.get_conn() as conn:
        df = pd.read_sql(query, conn)

    total_candidatos = len(df)
    print(f"Total candidatos OSR extraídos de maestra_slotting_mfc: {total_candidatos}")

    # Seguridad: Si no hay candidatos, abortar para evitar truncar la colección con 0 registros
    if total_candidatos == 0:
        raise Exception(
            "La query no retornó ningún candidato OSR desde ecommdata.maestra_slotting_mfc. "
            "Se cancela la sincronización a MongoDB para proteger los datos existentes. "
            "Verifique que etl_slotting_mfc se haya ejecutado correctamente."
        )

    # 2. Conexión a MongoDB
    mongo_user = Variable.get("MONGODB_ORQ_USER")
    mongo_pass = Variable.get("MONGODB_ORQ_PASSWORD")
    mongo_cluster_name = Variable.get("MONGODB_ORQ_CLUSTER")
    mongo_db = Variable.get("MONGODB_ORQ_DATABASE_INTEGRATIONS", default_var="INTEGRATIONS")
    mongo_collection_name = Variable.get("MONGODB_OSR_COLLECTION", default_var="osr_item_conversions")

    mongo_uri = (
        f"mongodb+srv://{mongo_user}:{mongo_pass}@{mongo_cluster_name}.reeld.mongodb.net/{mongo_db}"
        "?authMechanism=SCRAM-SHA-1"
    )
    print(f"Conectando a MongoDB: cluster={mongo_cluster_name}, db={mongo_db}, collection={mongo_collection_name}")
    mongo_client = MongoClient(mongo_uri)
    collection = mongo_client[mongo_db][mongo_collection_name]

    # 3. Preparación de documentos
    docs = [
        {
            "refId": str(row["ref_id"]).strip(),
            "osr": bool(row["osr"])
        }
        for _, row in df.iterrows()
    ]

    print(f"Ejemplo de documento a insertar: {docs[0] if docs else 'None'}")

    # 4. Truncar y recargar la colección
    del_result = collection.delete_many({})
    print(f"Documentos antiguos eliminados en {mongo_collection_name}: {del_result.deleted_count}")

    # Inserción en bloques (chunks) para estabilidad
    chunk_size = 1000
    total_insertados = 0
    for i in range(0, len(docs), chunk_size):
        chunk = docs[i : i + chunk_size]
        ins_result = collection.insert_many(chunk)
        total_insertados += len(ins_result.inserted_ids)

    print(f"Inserción completa exitosa: {total_insertados} documentos insertados en {mongo_collection_name}.")

    # Asegurar índice por refId para consultas rápidas
    collection.create_index("refId")
    print("Índice sobre 'refId' verificado.")


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "etl_sync_mongo_osr_item_conversions",
    default_args=default_args,
    description="Sincronizacion manual de candidatos OSR desde Postgres a MongoDB osr_item_conversions",
    schedule_interval=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Santiago"),
    catchup=False,
    tags=["mongo", "postgres", "OSR", "MFC", "slotting", "manual"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:
    dag.doc_md = """
    ### ETL Sincronización OSR a MongoDB (Ejecución Manual)
    Filtra los productos de `ecommdata.maestra_slotting_mfc` que no rompen restricción de merma:
    ```sql
    select distinct ref_id, true as osr
    from ecommdata.maestra_slotting_mfc msm
    where msm.candidato_merma ilike '%no%'
      and msm.restriccion_merma_00 ilike '%no%';
    ```
    Y los inserta en la colección `osr_item_conversions` de la base de datos `INTEGRATIONS` en MongoDB.
    """

    t0_sync = PythonOperator(
        task_id="sync_osr_to_mongo",
        python_callable=sync_osr_to_mongo,
    )

    t0_sync
