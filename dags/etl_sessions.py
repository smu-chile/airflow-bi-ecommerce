from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime

import pendulum

def getLoggedSessions(property_id,canal,formato,ds):
    import pandas as pd
    from google.analytics.data_v1beta import BetaAnalyticsDataClient
    from google.analytics.data_v1beta.types import Dimension, Metric, OrderBy,RunReportRequest,Dimension,Metric,DateRange,FilterExpression,Filter,FilterExpressionList
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
    filter_expression_list = FilterExpressionList()
    filter_expression_1 = FilterExpression(
        filter=Filter(
        field_name="signedInWithUserId",
        string_filter=Filter.StringFilter(value="yes"),
        ))
    '''
    filter_expression_2 = FilterExpression(
        filter=Filter(
        field_name="eventName",
        string_filter=Filter.StringFilter(value="select_promotion"),
        ))
    '''
    filter_expression_list.expressions.append(filter_expression_1)
    #filter_expression_list.expressions.append(filter_expression_2)

    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name="date")],
        metrics=[Metric(name="sessions")],
        date_ranges=[DateRange(start_date=macros.ds_add(ds, -7), end_date=ds)],
        dimension_filter=FilterExpression(
            and_group=filter_expression_list
        ),
        order_bys=[OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="date"),desc=False)]
    )
    response = client.run_report(request)

    data = []
    for row in response.rows:
        fecha_obj = datetime.strptime(row.dimension_values[0].value, '%Y%m%d')
        fecha_formateada = fecha_obj.strftime('%Y-%m-%d')
        data.append([fecha_formateada,row.metric_values[0].value])

    # Crear un DataFrame de pandas con los datos
    formato = 'unimarc' if property_id in ["290739730","256987911"] else 'alvi'
    df = pd.DataFrame(data, columns=['fecha',f'sesiones_logueadas_{canal}_{formato}'])
    return df

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
    df_sesiones_logueadas_web_unimarc = getLoggedSessions("290739730","web","unimarc",ds)
    df_sesiones_logueadas_app_unimarc = getLoggedSessions("256987911","app","unimarc",ds)
    df_sesiones_logueadas_web_alvi= getLoggedSessions("307311889","web","alvi",ds)
    df_sesiones_logueadas_app_alvi = getLoggedSessions("309369468","app","alvi",ds)

    df_merged = pd.merge(df_uni_app, df_uni_web, on='fecha', how='outer')
    df_merged = pd.merge(df_merged, df_alvi_app, on='fecha', how='outer')
    df_merged = pd.merge(df_merged, df_alvi_web, on='fecha', how='outer')
    df_merged = pd.merge(df_merged, df_sesiones_logueadas_web_unimarc, on='fecha', how='outer')
    df_merged = pd.merge(df_merged, df_sesiones_logueadas_app_unimarc, on='fecha', how='outer')
    df_merged = pd.merge(df_merged, df_sesiones_logueadas_web_alvi, on='fecha', how='outer')
    df_merged = pd.merge(df_merged, df_sesiones_logueadas_app_alvi, on='fecha', how='outer')

    # Renombrar las columnas para mayor claridad
    df_merged.rename(columns={
        f'sessions_256987911': 'sesiones_app_unimarc',
        f'sessions_290739730': 'sesiones_web_unimarc',
        f'sessions_309369468': 'sesiones_app_alvi',
        f'sessions_307311889': 'sesiones_web_alvi',
        f'engagedSessions_256987911': 'sesiones_engagement_app_unimarc',
        f'engagedSessions_290739730': 'sesiones_engagement_web_unimarc',
        f'engagedSessions_309369468': 'sesiones_engagement_app_alvi',
        f'engagedSessions_307311889': 'sesiones_engagement_web_alvi',
        f'totalUsers_256987911': 'usuarios_app_unimarc',
        f'totalUsers_290739730': 'usuarios_web_unimarc',
        f'totalUsers_309369468': 'usuarios_app_alvi',
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
    import numpy as np
    
    sessions_file = ti.xcom_pull(key="return_value", task_ids=["sessions_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+sessions_file)
    if not s3_hook.check_for_key(sessions_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % sessions_file)

    sessions_object = s3_hook.get_key(sessions_file, bucket_name=s3_bucket)

    df = pd.read_csv(sessions_object.get()["Body"])
    
    print(f"Number of records extracted: {len(df.index)}")

    columns = [
        "fecha",
        "sesiones_app_unimarc",
        "sesiones_web_unimarc",
        "sesiones_app_alvi",
        "sesiones_web_alvi",
        "sesiones_engagement_app_unimarc",
        "sesiones_engagement_web_unimarc",
        "sesiones_engagement_app_alvi",
        "sesiones_engagement_web_alvi",
        "usuarios_app_unimarc",
        "usuarios_web_unimarc",
        "usuarios_app_alvi",
        "usuarios_web_alvi",
        "sesiones_logueadas_app_unimarc",
        "sesiones_logueadas_web_unimarc",
        "sesiones_logueadas_app_alvi",
        "sesiones_logueadas_web_alvi"
    ]

    df = df[columns]

    columns = [
        "sesiones_app_unimarc",
        "sesiones_web_unimarc",
        "sesiones_app_alvi",
        "sesiones_web_alvi",
        "sesiones_engagement_app_unimarc",
        "sesiones_engagement_web_unimarc",
        "sesiones_engagement_app_alvi",
        "sesiones_engagement_web_alvi",
        "usuarios_app_unimarc",
        "usuarios_web_unimarc",
        "usuarios_app_alvi",
        "usuarios_web_alvi",
        "sesiones_logueadas_app_unimarc",
        "sesiones_logueadas_web_unimarc",
        "sesiones_logueadas_app_alvi",
        "sesiones_logueadas_web_alvi"
    ]

    column_types = {
        "fecha":"string",
        "sesiones_app_unimarc":"int",
        "sesiones_web_unimarc":"int",
        "sesiones_app_alvi":"int",
        "sesiones_web_alvi":"int",
        "sesiones_engagement_app_unimarc":"int",
        "sesiones_engagement_web_unimarc":"int",
        "sesiones_engagement_app_alvi":"int",
        "sesiones_engagement_web_alvi":"int",
        "usuarios_app_unimarc":"int",
        "usuarios_web_unimarc":"int",
        "usuarios_app_alvi":"int",
        "usuarios_web_alvi":"int",
        "sesiones_logueadas_app_unimarc":"int",
        "sesiones_logueadas_web_unimarc":"int",
        "sesiones_logueadas_app_alvi":"int",
        "sesiones_logueadas_web_alvi":"int"
    }

    df = df.astype(column_types, errors="ignore")

    columns_query = ",".join(columns)
    excluded_query = ",".join(["EXCLUDED."+column for column in columns])
    values_query = "%s,"+",".join(["%s" for column in columns])
    df = df.fillna("NULL")
    records = list(df.to_records(index=False))
    
    # Change data types to native python types
    fixed_records = []
    for record in records:
        fixed_record = []
        for value in record:
            if isinstance(value, np.generic):
                fixed_record.append(value.item())
            elif value == "NULL":
                fixed_record.append(None)
            else:
                fixed_record.append(value)
        fixed_records.append(tuple(fixed_record))
    print(f"Number of records to load: {str(len(fixed_records))}")
    incremental_query = """
        INSERT INTO analytics_and_growth.sesiones (fecha,"""+columns_query+""") 
        VALUES ("""+values_query+""")
        ON CONFLICT (fecha)
        DO UPDATE SET ("""+columns_query+""") = ("""+excluded_query+""") ;
    """
    print(incremental_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.executemany(incremental_query, fixed_records)
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
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
    schedule_interval="30 8 * * *",
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
