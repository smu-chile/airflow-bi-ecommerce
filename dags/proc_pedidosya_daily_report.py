from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from datetime import datetime

def _send_report_to_sftp():
    import jaydebeapi
    import io
    import os
    import pandas as pd
    import pysftp
    import paramiko

    ## FTP parameters
    ftp_host = Variable.get("PEYA_SFTP_HOST")
    ftp_port = 22
    ftp_user = Variable.get("PEYA_SFTP_USER")
    ftp_rsa_key = Variable.get("PEYA_SFTP_SECRET_RSA_KEY")

    with open("temp_peya_sftp_rsa_key", "w") as key_file:
        key_file.write(ftp_rsa_key)

    dic_tiendas = {
        "277728": "0048",	
        "277738": "0602",	
        "138446": "0345",	
        "89502": "0469",	
        "60848": "0333",	
        "277741": "0111",	
        "277410": "0915",	
        "277730": "0912",	
        "92533": "0331",	
        "115476": "0086",	
        "277425": "0458",	
        "59523": "0332",	
        "277729": "0759",	
        "105034": "0980",	
        "277727": "0778",	
        "138169": "0324",	
        "67660": "0030",	
        "288689": "0033",	
        "277413": "0336",	
        "277736": "0022",	
        "105761": "0006",	
        "277735": "0011",	
        "277739": "0916",	
        "132258": "0957",	
        "106881": "0923",	
        "132275": "0777",	
        "277734": "0344",	
        "277418": "0710",	
        "277416": "0939",	
        "277429": "0375",	
        "277430": "0466",	
        "303853": "0328",	
        "277737": "0402",	
        "277422": "0642",	
        "277670": "0953",	
        "277732": "0903",	
        "141036": "0009",	
        "140585": "0906",	
        "304206": "0445",	
        "277726": "0054",	
        "138057": "0034",	
        "315011": "0961",	
        "329762": "0347",	
        "63108": "0340",	
        "277424": "0058",	
        "105032": "0736"
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
                                , CASE WHEN WF.PRECIO IS NULL THEN precio.PRECIO_MODAL
                                        ELSE WF.PRECIO END AS Precio
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
                        LEFT JOIN (SELECT EAN
                                            , min(PRECIO_PROMOCIONAL) AS PRECIO
                                    FROM NZ_BU.ECOMERCE.VW_WORKFLOW
                                    WHERE FECHA_INICIO_DE_PROMOCION <= TO_CHAR(NOW(),'YYYY-MM-DD')
                                    AND FECHA_FIN_DE_PROMOCION >= TO_CHAR(NOW(),'YYYY-MM-DD')
                                    AND TIPO_PROMOCION IN (1,4)
                                    AND REGISTRO_VALIDO = 'X'
                                    AND ORGANIZACION_VENTAS = '1000'
                                    AND CANAL_DISTRIBUCION = '10'
                                    AND ID_MECANICA NOT IN (25, 26, 27, 36, 37, 50, 51, 53, 67, 72, 77, 93, 99)
                                    AND (ID_MECANICA NOT IN (25, 26, 27, 36, 37, 50, 51, 53, 67, 72, 77, 93, 99)
                                    	OR N_PROMOCION IN (
                                    		5640752022,
											5640762022,
											5640772022,
											5640782022,
											5640792022,
											5640802022,
											5640812022,
											5552412022,
											5552422022,
											5552432022,
											5630882022,
											5630892022,
											5630902022,
											5631152022,
											5631162022,
											5631172022,
											5631182022,
											5631192022,
											5631202022,
											5631212022
                                    	)
                                    )
                                    GROUP BY EAN ) WF ON WF.EAN = P.EAN
                        WHERE A.ALMACEN_COD = '0001'
                        AND S.APLICA_STOCK = 'S'
                        AND DATE_VALUE = TO_CHAR(NOW() - INTERVAL '1 days','YYYY-MM-DD')
                        AND OU.OU_ID = '{dic_tiendas[tiendapeya]}'
                        AND (P.NLS_PD_DSC IS NOT NULL OR P.UNIDAD_DE_MEDIDA IN ('KG', 'KGV'))
                        AND P.UNIDAD_DE_MEDIDA  IS NOT NULL
                        AND PART.PARTICULARIDAD_COD = 'A'
                        AND S.TIPO_STOCK_KEY IN (9161419180, 9145314683)                        
                    """
        print("Ejecutando tienda:" + dic_tiendas[tiendapeya])
        cur.execute(sql_str)
        results = cur.fetchall()
        columns = [i[0] for i in cur.description]
        df = pd.DataFrame(results, columns=columns)
        df["PRECIO"]=df["PRECIO"].astype("int")
        df.to_csv(tiendapeya + ".csv", header=True, index=False, encoding="utf-8")

        with pysftp.Connection(host=ftp_host, 
                                username=ftp_user, 
                                port=ftp_port, 
                                private_key="temp_peya_sftp_rsa_key") as sftp:
            localFile = f"{tiendapeya}.csv"
            remotePath = f"/peya.live.sftp-catalogue/transfer-files/cl_unimarc/upload/{tiendapeya}.csv"
            sftp.put(localFile, remotePath)

        print(f"Archivo {tiendapeya}.csv cargado")
        os.remove(localFile)
        print("Archivo local eliminado")

    cur.close()
    conn.close()

    os.remove("temp_peya_sftp_rsa_key")

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
