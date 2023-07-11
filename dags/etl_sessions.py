from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow import macros

from datetime import datetime

import pendulum

def getDataFromProperty(property_id, ds):
    import pandas as pd
    from google.analytics.data_v1beta import BetaAnalyticsDataClient
    from google.analytics.data_v1beta.types import Dimension, Metric, OrderBy,RunReportRequest,Dimension,Metric,DateRange
    from google.analytics.data_v1beta.types import OrderBy
    from google.oauth2 import service_account

    credential_dict = {
    "type": "service_account",
    "project_id": Variable.get("SESSIONS_GD_PROJECT_ID"),
    "private_key_id": Variable.get("SESSIONS_GD_PRIVATE_KEY_ID"),
    "private_key": Variable.get("SESSIONS_GD_PRIVATE_KEY"),
    "client_email": Variable.get("SESSIONS_GD_CLIENT_EMAIL"),
    "client_id": Variable.get("SESSIONS_GD_CLIENT_ID"),
    "auth_uri": Variable.get("SESSIONS_GD_AUTH_URI"),
    "token_uri": Variable.get("SESSIONS_GD_TOKEN_URI"),
    "auth_provider_x509_cert_url": Variable.get("SESSIONS_GD_AUTH_PROVIDER"),
    "client_x509_cert_url": Variable.get("SESSIONS_GD_CERT_URL")
    }
    
    c_var = service_account.Credentials.from_service_account_info(credential_dict)
    client = BetaAnalyticsDataClient(credentials=c_var)

    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name="date")],
        metrics=[Metric(name="sessions"),Metric(name="engagedSessions"),Metric(name="totalUsers")],
        date_ranges=[DateRange(start_date=macros.ds_add(ds, -7), end_date=ds)],
        order_bys=[OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="date"),desc=False)]
    )
    ##request2=RunReportRequest()
    response = client.run_report(request)

    data = []
    for row in response.rows:
        fecha_obj = datetime.strptime(row.dimension_values[0].value, '%Y%m%d')
        fecha_formateada = fecha_obj.strftime('%Y-%m-%d')
        data.append([fecha_formateada, row.metric_values[0].value, row.metric_values[1].value, row.metric_values[2].value])

    # Crear un DataFrame de pandas con los datos
    df = pd.DataFrame(data, columns=['fecha',f'sessions_{property_id}',f'engagedSessions_{property_id}',f'totalUsers_{property_id}'])
    return df

def _sessions_to_s3(ds):
    import pandas as pd
    from io import StringIO
    
    df_uni_app = getDataFromProperty("256987911", ds)
    df_uni_web = getDataFromProperty("290739730", ds)
    df_alvi_app= getDataFromProperty("309369468", ds)
    df_alvi_web= getDataFromProperty("307311889", ds)

    df_merged = pd.merge(df_uni_app, df_uni_web, on='fecha', how='outer')
    df_merged = pd.merge(df_merged, df_alvi_app, on='fecha', how='outer')
    df_merged = pd.merge(df_merged, df_alvi_web, on='fecha', how='outer')

    # Renombrar las columnas para mayor claridad
    df_merged.rename(columns={
        f'sessions_256987911': 'sesiones_app_unimarc',
        f'engagedSessions_256987911': 'sesiones_engagement_app_unimarc',
        f'totalUsers_256987911': 'usuarios_app_unimarc',
        f'sessions_290739730': 'sesiones_web_unimarc',
        f'engagedSessions_290739730': 'sesiones_engagement_web_unimarc',
        f'totalUsers_290739730': 'usuarios_web_unimarc',
        f'sessions_309369468': 'sesiones_app_alvi',
        f'engagedSessions_309369468': 'sesiones_engagement_app_alvi',
        f'totalUsers_309369468': 'usuarios_app_alvi',
        f'sessions_307311889': 'sesiones_web_alvi',
        f'engagedSessions_307311889': 'sesiones_engagement_web_alvi',
        f'totalUsers_307311889': 'usuarios_web_alvi'
    }, inplace=True)

    curr_datetime = ds.replace("-", "/")
    prefix = "sesiones/"+curr_datetime+"_"
    file_name = prefix+"sesiones.csv"

    buffer = StringIO()

    print("Number of records:")
    print(len(df_merged.index))
    df_merged.to_csv(buffer, header=True, index=False, encoding="utf-8")
    buffer.seek(0)

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
    s3_hook.load_string(buffer.getvalue(),
                  key=file_name,
                  bucket_name=s3_bucket,
                  replace=True,
                  encrypt=False)

    return file_name

    

def _load_sessions_table(ti):
    import pandas as pd
    import sqlalchemy
    
    sessions_file = ti.xcom_pull(key="return_value", task_ids=["sessions_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+sessions_file)
    if not s3_hook.check_for_key(sessions_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % sessions_file)

    sessions_object = s3_hook.get_key(sessions_file, bucket_name=s3_bucket)

    df = pd.read_csv(sessions_object.get()["Body"])
    
    print(f"Number of records extracted: {len(df.index)}")

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = "postgresql+psycopg2://"+username+":"+password+"@"+host+":5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    # Save to PostgreSQL:
    df.to_sql(name="sesiones",
                con=engine,         
                schema="analytics_and_growth",         
                if_exists='append',         
                index=False,         
                chunksize=20000,         
                method='multi')
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
    'etl_sesiones',
    default_args=default_args,
    description="Extracción y carga de sesiones desde Google Drive de Analytics hasta Workspace.",
    schedule_interval="0 4 * * 1",
    start_date=pendulum.datetime(2023, 7, 10, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["sesiones", "analytics_and_growth", "google_drive"],
) as dag:

    dag.doc_md = """
    Extracción y carga de sesiones desde Google Drive de Analytics hasta Workspace.
    """ 
    
    t0 = PythonOperator(
        task_id = "sessions_to_s3",
        python_callable = _sessions_to_s3
    )

    t1 = PythonOperator(
        task_id = "load_sessions_table",
        python_callable = _load_sessions_table
    )

    t0 >> t1
