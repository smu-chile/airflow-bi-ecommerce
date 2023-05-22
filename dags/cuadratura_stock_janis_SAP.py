from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

import pendulum
from datetime import datetime, timedelta

def stock_lista8():
    host = "bi-ecommerce-postgres-prod-master.cuuchupawrpt.us-east-1.rds.amazonaws.com"
    database = "postgres"
    username = "pmardonesg"
    password = "GOkh_noaql"
    conn_url = "postgresql+psycopg2://"+username+":"+password+"@"+host+":5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    conn = psycopg2.connect(database=database,
                            host=host,
                            user=username,
                            password=password,
                            port=5432)

    cursor = conn.cursor()
    cursor.execute("""select _t.*
                    from( 
                    select s.fecha,
                    s.ref_id,
                    l.id_tienda,
                    l.stock_x_umv,
                    s.stock_janis,
                    case
                        when (l.umv in ('DIS','CJ')) then trunc(l.stock_x_umv,0) 
                        when (l.umv in ('KG','KGV')) then round(l.stock_x_umv/s.multiplicador_unidad_medida,0)
                        else l.stock_x_umv
                    end as stock_sap,
                    s.multiplicador_unidad_medida
                    from ecommdata.lista8 as l
                    inner Join ecommdata.stock as s
                    on l.fecha = s.fecha and l.id_tienda = s.id_tienda and s.ref_id = CONCAT(LPAD(l.material, 18, '0'), '-', l.umv)  
                    where l.fecha = current_date and l.umv <> 'PAQ') as _t 
                    group by 
                    _t.fecha,
                    _t.ref_id,
                    _t.id_tienda,
                    _t.stock_x_umv,
                    _t.stock_janis,
                    _t.stock_sap,
                    _t.multiplicador_unidad_medida""")
    df_base = cursor.fetchall()
    df_base=pd.DataFrame(df_base)
    df_base.columns = ["fecha","ref_id","id_tienda","stock_l8","stock_janis","stock_calculado","multiplicador_medida"]
    #conn.close()
    
    return df_base

def skus_carnes_padre_hijo():
    host = "bi-ecommerce-postgres-prod-master.cuuchupawrpt.us-east-1.rds.amazonaws.com"
    database = "postgres"
    username = "pmardonesg"
    password = "GOkh_noaql"
    conn_url = "postgresql+psycopg2://"+username+":"+password+"@"+host+":5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    conn = psycopg2.connect(database=database,
                            host=host,
                            user=username,
                            password=password,
                            port=5432)

    cursor = conn.cursor()
    cursor.execute("""select sk.erp_id ,sk.ref_id, sk.nombre_sku, st.c1, st.id_tienda
                    from ecommdata.skus as sk
                    left join ecommdata.stock as st
                    on sk.ref_id = st.ref_id
                    where substring(sk.ref_id,strpos(sk.ref_id,'-')+1,length(sk.ref_id)-strpos(sk.ref_id,'-')) in ('KG','KGV')
                    and split_part(sk.ref_id,'-',1) <> erp_id
                    and st.c1 = 'Carnes'""")
    df_base = cursor.fetchall()
    df_base=pd.DataFrame(df_base)
    df_base.columns = ["material","ref_id","descripcion","categoria","id_tienda"]
    #conn.close()
    
    return df_base

def render_netezza_view(id_tienda,id_material,ds):
    sql_str= "SELECT sa.SKU_PRODUCT AS material , NBR_ITM AS stock , ou.ou_id AS id_tienda , SA.NM AS nombre , DATE_VALUE as fecha FROM DWC_SMU.SMU.VW_FACT_STOCK S LEFT JOIN DWC_SMU.SMU.VW_DIM_SKU_ATTR SA ON SA.SKU_KEY = S.SKU_KEY LEFT JOIN DWC_SMU.SMU.VW_DIM_ORGANIZATION_UNIT OU ON OU.OU_KEY = S.OU_KEY LEFT JOIN DWC_SMU.SMU.VW_DIM_ALMACEN A ON A.ALMACEN_KEY =S.ALMACEN_KEY LEFT JOIN DWC_SMU.SMU.VW_DIM_PARTICULARIDAD PART ON S.PARTICULARIDAD_KEY =PART.PARTICULARIDAD_KEY WHERE A.ALMACEN_COD = '0001' AND S.APLICA_STOCK = 'S' AND DATE_VALUE = '"+ds+"'::date AND OU.OU_ID in ('{id_t}') AND PART.PARTICULARIDAD_COD = 'A' AND S.TIPO_STOCK_KEY IN (9161419180, 9145314683) AND sa.SKU_PRODUCT in ('{id_m}');"

    dsn_database = "NZ_BU" 
    dsn_hostname = "10.43.223.12"
    dsn_port = "5480" 
    dsn_uid = "pamardonesg"
    dsn_pwd = "Josofia22."
    jdbc_driver_name = "org.netezza.Driver" 
    jdbc_driver_loc = os.path.join('C:/Users/pmardonesg/Desktop/cuadratura/nzjdbc.jar')

    connection_string='jdbc:netezza://'+dsn_hostname+':'+dsn_port+'/'+dsn_database
    
    conn = jaydebeapi.connect(jdbc_driver_name, 
                                connection_string, {'user': dsn_uid, 'password': dsn_pwd},
                                jars=jdbc_driver_loc)

    cur = conn.cursor()
    cur.execute(sql_str.format(id_t= id_tienda,id_m = id_material))
    df = cur.fetchall()
    #print(df)
    #cur.close()
    #conn.close()

    return df
