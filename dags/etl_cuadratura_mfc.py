from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

from io import StringIO
import os
import pendulum
import jaydebeapi


def stock_x_l8(ds):
    #esta funcion se consulta las tablas stock y lista8
    #stock filtrado por lista8 con fecha actual y tienda 1917
    # de stock se extrar Janis y de lista8 se extrar SAP
    #si el UMV del sku es KG o KGV se divide por multiplicador_unidad_medida para transformar el dato a unidades
    import pandas as pd
    stock_l8_query = """select _t.*
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
                    where l.fecha = '"""+ds+"""'::date 
                    and l.umv <> 'PAQ'
                    and l.id_tienda = '1917') as _t 
                    group by 
                    _t.fecha,
                    _t.ref_id,
                    _t.id_tienda,
                    _t.stock_x_umv,
                    _t.stock_janis,
                    _t.stock_sap,
                    _t.multiplicador_unidad_medida"""
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    #print(stock_l8_query)
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(stock_l8_query)
    results = cursor.fetchall()
    results=pd.DataFrame(results)
    results.columns = ["fecha","ref_id","id_tienda","stock_l8","stock_janis","stock_calculado","multiplicador_medida"]
    print(results)
    cursor.close()
    pg_connection.close()
    return results

def sku_erp_padre():
    #esta funcion se consulta las tablas Skus, productos, categoria y productos tienda
    #condiciones pueden ser:
    #   -UMV sea KG o KGV
    #   -Categoria c1 sea 'Carnes'
    #   -erp_id sea distinto de material
    import pandas as pd
    sku_erp_query = """select s.erp_id,s.ref_id,s.nombre_sku,c.n1, pt.id_tienda
                    from ecommdata.skus as s
                    left join ecommdata.productos as p
                    on s.ref_id = p.ref_id
                    left join ecommdata.categorias as c
                    on p.id_categoria = c.id
                    left join ecommdata.productos_tienda as pt
                    on s.ref_id = pt.ref_id
                    where s.ref_id LIKE '%-KG' 
                    or s.ref_id LIKE '%-KGV'
                    or c.n1 = 'Carnes'
                    or p.material <> s.erp_id;"""
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    #print(sku_erp_query)
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(sku_erp_query)
    results = cursor.fetchall()
    results=pd.DataFrame(results)
    results.columns = ["material","ref_id","descripcion","categoria","id_tienda"]
    print(results)
    cursor.close()
    pg_connection.close()
    return results

def stock_mfc(ds):
    import pandas as pd
    stock_mfc_query = """SELECT id_tienda,
                    CONCAT(LPAD(material, 18, '0'), '-', unidad_venta) as ref_id,
                    stock as stock_janis,
                    fecha_carga
                    from ecommdata.stock_mfc
                    where fecha_carga = '"""+ds+"""'::date 
                    and id_tienda = '1917'"""
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    #print(stock_mfc_query)
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(stock_mfc_query)
    results = cursor.fetchall()
    results=pd.DataFrame(results)
    results.columns = ["id_tienda","ref_id","stock_mfc","fecha_carga"]
    print(results)
    cursor.close()
    pg_connection.close()
    return results

def l8_0917(ds):
    import pandas as pd
    l8_0917_query = """select material||'-'||umv as ref_id, stock_x_umv
                    from ecommdata.lista8
                    where fecha = '"""+ds+"""'::date 
                    and id_tienda = '0917'"""
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    #print(l8_0917_query)
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(l8_0917_query)
    results = cursor.fetchall()
    results=pd.DataFrame(results)
    results.columns = ["id_tienda","ref_id","stock_mfc","fecha_carga"]
    print(results)
    cursor.close()
    pg_connection.close()
    return results

def ubicaciones_mfc(ds):
    import pandas as pd
    ubi_mfc_query = """
                    """
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    #print(ubi_mfc_query)
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(ubi_mfc_query)
    results = cursor.fetchall()
    results=pd.DataFrame(results)
    results.columns = ["id_tienda","ref_id","stock_mfc","fecha_carga"]
    print(results)
    cursor.close()
    pg_connection.close()
    return results


def render_netezza_view(id_material,ds):

    sql_str= """SELECT sa.SKU_PRODUCT AS material ,
                NBR_ITM AS stock ,
                ou.ou_id AS id_tienda ,
                SA.NM AS nombre ,
                DATE_VALUE as fecha
                FROM DWC_SMU.SMU.VW_FACT_STOCK S 
                LEFT JOIN DWC_SMU.SMU.VW_DIM_SKU_ATTR SA
                ON SA.SKU_KEY = S.SKU_KEY 
                LEFT JOIN DWC_SMU.SMU.VW_DIM_ORGANIZATION_UNIT OU 
                ON OU.OU_KEY = S.OU_KEY 
                LEFT JOIN DWC_SMU.SMU.VW_DIM_ALMACEN A 
                ON A.ALMACEN_KEY =S.ALMACEN_KEY 
                LEFT JOIN DWC_SMU.SMU.VW_DIM_PARTICULARIDAD PART 
                ON S.PARTICULARIDAD_KEY =PART.PARTICULARIDAD_KEY 
                WHERE A.ALMACEN_COD = '0001' 
                AND S.APLICA_STOCK = 'S' 
                AND DATE_VALUE = '"""+ds+"""' ::date
                AND OU.OU_ID in ('1917','0917') 
                AND PART.PARTICULARIDAD_COD = 'A' 
                AND S.TIPO_STOCK_KEY IN (9161419180, 9145314683) 
                AND sa.SKU_PRODUCT in ('"""+id_material+"""');"""
    
    dsn_database = Variable.get("DW_SECRET_DATABASE") 
    dsn_hostname = Variable.get("DW_SECRET_HOSTNAME")
    dsn_port = "5480" 
    dsn_uid = Variable.get("DW_SECRET_USER")
    dsn_pwd = Variable.get("DW_PASSWORD")
    jdbc_driver_name = "org.netezza.Driver" 
    jdbc_driver_loc = os.path.join('/opt/airflow/include/jdbcdriver/nzjdbc.jar')

    connection_string='jdbc:netezza://'+dsn_hostname+':'+dsn_port+'/'+dsn_database
    
    conn = jaydebeapi.connect(jdbc_driver_name, 
                                connection_string, {'user': dsn_uid, 'password': dsn_pwd},
                                jars=jdbc_driver_loc)

    cur = conn.cursor()
    print(sql_str)
    cur.execute(sql_str)
    df = cur.fetchall()
    cur.close()
    conn.close()

    return df

def funcion_crear_data():
    #
    df_stock_l8_0917 = l8_0917()
    print("se ha cargado stock janis y L8 de la tienda 0917\n")
    df_stock_mfc = stock_mfc()
    print("se ha cargado stock TOM de la tienda 1917\n")
    df_stock_l8_1917 = stock_x_l8()
    df_stock_l8_1917.stock_janis = df_stock_l8_1917.stock_janis.fillna(0)
    print("se ha cargado stock janis y L8 de la tienda 1917\n")




    return

def funcion_subir_s3():

    return

def funcion_subir_postgres():

    return


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_cuadratura_mfc',
    default_args=default_args,
    description="crear y cargar cuadratura del dia para MFC",
    schedule_interval=None,    #preguntar a mati k va por acá
    start_date=pendulum.datetime(2023, 6, 1, tz="America/Santiago"),
    catchup=False,
    tags=["catalogo", "cuadratura", "MFC", "unimarc"],
) as dag:
    
    dag.doc_md = """
    construir y cargar cuadratura mfc. \n
    Delete and INSERT en tabla catalogo.cuadratura_mfc.
    """ 

    t0 = PythonOperator(
        task_id = "funcion_crear_data",
        python_callable = funcion_crear_data,
    )

    t1 = PythonOperator(
        task_id = "funcion_subir_s3",
        python_callable = funcion_subir_s3,
    )

    t2 = PythonOperator(
        task_id = "funcion_subir_postgres",
        python_callable = funcion_subir_postgres
    )

    t0 >> t1 >> t2