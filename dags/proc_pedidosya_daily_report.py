from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from datetime import datetime

def _send_report_to_sftp():
    import jaydebeapi
    import os
    import pandas as pd
    import pysftp

    ## FTP parameters
    ftp_host = Variable.get("PEYA_SFTP_HOST")
    ftp_port = 60
    ftp_user = Variable.get("PEYA_SFTP_USER")
    ftp_pass = Variable.get("PEYA_SFTP_PASSWORD")

    dic_tiendas = {
        "277728":"0028",
        "277738":"0602",
        "138446":"0345",
        "89502":"0469",
        "60848":"0333",
        "277741":"0111",
        "277410":"0962",
        "277730":"0912",
        "92533":"0917",
        "115476":"0086",
        "277425":"0458",
        "59523":"0332",
        "277729":"0759",
        "105034":"0980",
        "277727":"0778",
        "138169":"0324",
        "67660":"0030",
        "288689":"0033",
        "277413":"0336",
        "277736":"0022",
        "105761":"0626",
        "277735":"0011",
        "277739":"0916",
        "132258":"0957",
        "106881":"0923",
        "132275":"0754",
        "277734":"0344",
        "277418":"0710",
        "277424":"0780",
        "277416":"0939",
        "277429":"0375",
        "277430":"0466",
        "303853":"0328",
        "277737":"0402",
        "277422":"0642",
        "277670":"0953",
        "277732":"0903",
        "141036":"0581",
        "140585":"0906",
        "304206":"0445",
        "277726":"0054",
        "138057":"0085"
    }
    data_type = {
        "SKU":"string",
        "PRECIO":"int",
        "STOCK":"int"
    }

    dsn_database = Variable.get("DW_SECRET_DATABASE") 
    dsn_hostname = Variable.get("DW_SECRET_HOSTNAME")
    dsn_port = "5480" 
    dsn_uid = Variable.get("DW_SECRET_USER")
    dsn_pwd = Variable.get("DW_PASSWORD")
    jdbc_driver_name = "org.netezza.Driver" 
    jdbc_driver_loc = os.path.join('/opt/airflow/include/jdbcdriver/nzjdbc.jar')

    connection_string = 'jdbc:netezza://' + dsn_hostname + ':' + dsn_port + '/' + dsn_database
    conn = jaydebeapi.connect(jdbc_driver_name, connection_string, {'user': dsn_uid, 'password': dsn_pwd},jars=jdbc_driver_loc)
    cur = conn.cursor()
    now = datetime.now().strftime('%Y%m%d')
    for tiendapeya in dic_tiendas.keys():
        sql_str = f"""
                        SELECT P.EAN AS SKU
                                , precio.PRECIO_MODAL AS Precio
                                , CASE WHEN FLOOR(NBR_ITM / P.CONT_CONV_UMB) >= 15 THEN 1
                                        WHEN FLOOR(NBR_ITM / P.CONT_CONV_UMB) < 15 THEN 0
                                        ELSE 0 END AS Stock
                        FROM DWC_SMU.SMU.VW_FACT_STOCK S
                        LEFT JOIN DWC_SMU.SMU.VW_DIM_SKU_ATTR SA ON SA.SKU_KEY  = S.SKU_KEY
                        LEFT JOIN DWC_SMU.SMU.VW_DIM_PRODUCT P ON P.SKU_KEY = SA.SKU_KEY 
                        LEFT JOIN DWC_SMU.SMU.VW_DIM_ORGANIZATION_UNIT OU ON OU.OU_KEY = S.OU_KEY
                        LEFT JOIN DWC_SMU.SMU.VW_DIM_ALMACEN A ON A.ALMACEN_KEY =S.ALMACEN_KEY
                        LEFT JOIN DWC_SMU.SMU.VW_DIM_PARTICULARIDAD PART ON S.PARTICULARIDAD_KEY =PART.PARTICULARIDAD_KEY
                        INNER JOIN (SELECT _t.FECHA_CARGA
                                            , LPAD(_t.CODIGO_MATERIAL , 18, 0) AS material
                                            , CASE WHEN _t.UMV = 'UN' THEN 'ST' ELSE _t.UMV END AS UMV 
                                            , _t1.PRECIO_MODAL
                                    FROM (SELECT MAX(FECHA_CARGA) AS FECHA_CARGA
                                            , CODIGO_MATERIAL
                                            , UMV 
                                            FROM NZ_BU.ECOMERCE.VW_POSC_ACT_H_PRECIO_MODAL_UNI
                                            GROUP BY CODIGO_MATERIAL, UMV) _t
                                    INNER JOIN NZ_BU.ECOMERCE.VW_POSC_ACT_H_PRECIO_MODAL_UNI _t1
                                            ON _t.FECHA_CARGA=_t1.FECHA_CARGA
                                            AND _t.CODIGO_MATERIAL=_t1.CODIGO_MATERIAL
                                            AND _t.UMV=_t1.UMV) precio
                                ON precio.MATERIAL = SA.SKU_PRODUCT
                                AND precio.umv = p.UNIDAD_DE_MEDIDA
                        WHERE A.ALMACEN_COD = '0001'
                        AND S.APLICA_STOCK = 'S'
                        AND DATE_VALUE = TO_CHAR(NOW() - INTERVAL '1 days','YYYY-MM-DD')
                        AND OU.OU_ID = '{dic_tiendas[tiendapeya]}'
                        AND P.NLS_PD_DSC IS NOT NULL
                        AND P.UNIDAD_DE_MEDIDA  IS NOT NULL
                        AND PART.PARTICULARIDAD_COD = 'A'
                        AND S.TIPO_STOCK_KEY IN (9161419180, 9145314683);
                    """
        print("Ejecutando tienda:" + dic_tiendas[tiendapeya])
        cur.execute(sql_str)
        results = cur.fetchall()
        columns = [i[0] for i in cur.description]
        df = pd.DataFrame(results, columns=columns)
        df["PRECIO"]=df["PRECIO"].astype("int")
        df.to_csv(tiendapeya + ".csv", header=True, index=False, encoding="utf-8")

        with pysftp.Connection(host=ftp_host, username=ftp_user, password=ftp_pass, port=ftp_port) as sftp:
            localFile = f"{tiendapeya}.csv"
            remotePath = f"/upload/{tiendapeya}.csv"
            sftp.put(localFile, remotePath)

        print(f"Archivo {tiendapeya}.csv cargado")
        os.remove(localFile)
        print("Archivo local eliminado")

    cur.close()
    conn.close()
    print("OK")
    return

default_args = {
    "owner": "ecommerce_ops",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'proc_pedidosya_reporte_diario',
    default_args=default_args,
    description="Reporte diario de precios a SFTP de Pedidos Ya",
    schedule_interval="0 12 * * *",
    start_date=datetime(2022, 2, 1),
    catchup=False,
    tags=["DW", "OPS", "SFTP", "PedidosYa"],
) as dag:

    dag.doc_md = """
    Reporte diario de precios a servidor SFTP de Pedidos Ya.
    """ 
    t0 = PythonOperator(
        task_id = "send_report_to_sftp",
        python_callable = _send_report_to_sftp
    )
