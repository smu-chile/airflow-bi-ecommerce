from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime, timedelta, date
import pendulum


def venta_fact_geo(fecha_desde):

    import pandas as pd
    import psycopg2
    # fecha ingresada
    # importa zcobro despacho de AWS
    # credenciales aws
    try:
        conn = psycopg2.connect(
            user=Variable.get('POSTGRESQL_USER'),
            password=Variable.get('POSTGRESQL_PASSWORD'),
            host=Variable.get('POSTGRESQL_HOST'),
            port=5432,
            database=Variable.get('POSTGRESQL_DB')

        )
    except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            if conn is not None:
                conn.close()
    # Get Cursor
    cur = conn.cursor()
    query = f"""
    select to_char(o.fecha_creacion::timestamp, 'yyyy-mm-dd') as fecha_creacion, 
    to_char(timezone('America/Santiago',d.fecha_despacho ::timestamp), 'yyyy-mm-dd') as fecha_compromiso, 
    to_char(o.fecha_picking::timestamp, 'yyyy-mm-dd') as fecha_picking,
    o.id as orden, eo.nombre_estado as estado, a.nombre as firstname, a.apellido as lastname, a.id_empleado as employee_id,
    t2.nombre_tienda_janis as tienda, t.nombre as transportadora, d.lng as lng_pedido, d.lat as lat_pedido, t2.longitud as lng_tienda, t2.latitud as lat_tienda, o.venta_facturada_neta as "sum(venta_neta)",
    o.unidades_solicitadas as "sum(unidades)", o.productos_solicitados as "sum(productos)"
    from ecommdata.ordenes_janis o
    left join ecommdata.despachos d on o.id=d.id_orden 
    LEFT join ecommdata.administradores a on o.id_picker = a.id
    left join ecommdata.estado_orden_janis eo on o.estado_janis=eo.id_estado
    left join ecommdata.transportadoras t on d.id_transportadora =t.id 
    left join ecommdata.tiendas t2 on t.id_tienda = t2.id 
    WHERE o.fecha_creacion >= '{fecha_desde}'
    ;
    """
    cur.execute(query)
    rows = cur.fetchall()
    columns = cur.description
    conn.close()
    result = [{columns[index][0]:column for index, column in enumerate(value)} for value in rows]
    df_venta_fact_geo = pd.DataFrame(result)

    return df_venta_fact_geo

def asistencia_shopper(fecha_desde):
    import pandas as pd
    import psycopg2

    # fecha ingresada
    # importa zcobro despacho de AWS
    # credenciales aws
    try:
        conn = psycopg2.connect(
            user=Variable.get('POSTGRESQL_USER'),
            password=Variable.get('POSTGRESQL_PASSWORD'),
            host=Variable.get('POSTGRESQL_HOST'),
            port=5432,
            database=Variable.get('POSTGRESQL_DB')

        )
    except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            if conn is not None:
                conn.close()
    # Get Cursor
    cur = conn.cursor()
    query = f"""
    select id_empleado as Rut, Tienda, transportadora, fecha_creacion as dia_creacion,  min(a.fecha_hora_cambio) as inicio_jornada,max(a.fecha_hora_cambio) as fin_jornada
    from (select id_empleado,t2.nombre_tienda_janis as Tienda, t.nombre as transportadora,to_char(timezone('UTC',ocde.fecha_creacion::timestamp), 'yyyy-mm-dd') as fecha_creacion, to_char(timezone('UTC',ocde.fecha_creacion::timestamp), 'YYYY-MM-DD" "HH24:MI') as fecha_hora_cambio, ocde.id_orden 
    FROM ecommdata.orden_cambios_de_estado ocde
    left join ecommdata.ordenes_janis o on ocde.id_orden=o.janis_id
    LEFT join ecommdata.administradores a on o.id_picker = a.id
    left join ecommdata.despachos d on o.id=d.id_orden
    left join ecommdata.transportadoras t on d.id_transportadora =t.id 
    left join ecommdata.tiendas t2 on t.id_tienda = t2.id
    where o.fecha_picking is not null
    and to_char(timezone('UTC',ocde.fecha_creacion::timestamp), 'yyyy-mm-dd') >= '{fecha_desde}'
    and ocde.estado_nuevo >= 20 
    and ocde.estado_nuevo <= 90 
    and ocde.estado_nuevo <> 27 
    and ocde.estado_nuevo <> 35 
    and ocde.estado_nuevo <> 50 
    and ocde.estado_nuevo <> 80) as a
    group by id_empleado, fecha_creacion, Tienda, transportadora
    order by fecha_creacion
    ;
    """
    cur.execute(query)
    rows = cur.fetchall()
    columns = cur.description
    conn.close()
    result = [{columns[index][0]:column for index, column in enumerate(value)} for value in rows]
    df_tiempos_shopper = pd.DataFrame(result)

    return df_tiempos_shopper

def funcion_km_real(fecha_desde):

    import pandas as pd 
    import psycopg2
    
    ############## CARGA DE DATOS #######################

    host = Variable.get('POSTGRESQL_HOST')
    database = Variable.get('POSTGRESQL_DB')
    username = Variable.get('POSTGRESQL_USER')
    password = Variable.get('POSTGRESQL_PASSWORD')


    #### VALIDACION DE LA CARGA DE DATOS

    conn = psycopg2.connect(database=database,
                            host=host,
                            user=username,
                            password=password,
                            port=5432)

    cur = conn.cursor()
    cur.execute(f"SELECT fecha_facturacion, id_orden as orden, lat, lng, tienda, transportadora, kms_totales as KMs_Totales FROM forecast_and_planning.tabla_kms WHERE fecha_facturacion >= '{fecha_desde}';")
    rows = cur.fetchall()
    columns = cur.description
    conn.close()
    result = [{columns[index][0]:column for index, column in enumerate(value)} for value in rows]
    df_km_real = pd.DataFrame(result)
    df_km_real.columns = ['fecha_facturacion', 'orden', 'lat', 'lng', 'tienda', 'transportadora', 'KMs_Totales']
    return df_km_real

def funcion_tarifas():

    import pandas as pd 
    import psycopg2
    
    ############## CARGA DE DATOS #######################

    host = Variable.get('POSTGRESQL_HOST')
    database = Variable.get('POSTGRESQL_DB')
    username = Variable.get('POSTGRESQL_USER')
    password = Variable.get('POSTGRESQL_PASSWORD')


    #### VALIDACION DE LA CARGA DE DATOS

    conn = psycopg2.connect(database=database,
                            host=host,
                            user=username,
                            password=password,
                            port=5432)

    cur = conn.cursor()
    cur.execute("SELECT * FROM forecast_and_planning.tarifas_operadores")
    rows = cur.fetchall()
    columns = cur.description
    conn.close()
    result = [{columns[index][0]:column for index, column in enumerate(value)} for value in rows]
    df_tarifa = pd.DataFrame(result)
    return df_tarifa

def _calcular_costos_logisticos(ds):
    import pandas as pd
    import numpy as np
    from geopy import distance
    from calendar import monthrange
    from io import StringIO

    ########################################
    ########### PARÁMETROS #################
    ########################################
    # fecha desde (posteriormente será la ultima fecha cargada en AWS)
    fecha_desde = ((datetime.strptime(ds, '%Y-%m-%d'))) - timedelta(days=90)
    #fecha_desde = "2022-03-26"

    ########################################
    ########### IMPORTA CONSULTAS ##########
    ########################################
    # ubicación local de airflow, después se borran los archivos
    # se incluyen funciones en mismo archivo

    ########################################
    ########### IMPORTA DATOS ##############
    ########################################
    # importa consulta venta con coordenadas
    df_venta_fact_geo = venta_fact_geo(fecha_desde)
    # importa asistencia shoppers
    df_asistencia_shoppers = asistencia_shopper(fecha_desde)
    # importa kilometros recorridos
    df_km_real = funcion_km_real(fecha_desde)
    # importa tarifas costos logísticos (posteriormente será la base cargada en AWS)
    df_tarifas_oper = funcion_tarifas()
    #
    #
    #'fecha_creacion', 'fecha_compromiso', 'fecha_picking', 'orden','estado'
    ########################################
    ########### TRANSFORMACIONES ###########
    ########################################
    ## transformaciones tarifas
    # nombres de columnas
    df_tarifas_oper.columns = ['ID_MES', 'cod_tienda', 'Tienda', 'Operador', 'costo_base',
        'costo_un_picking', 'costo_sku_picking', 'costo_km_despacho',
        'ida_vuelta', 'asegurado', 'recurso_fijo', 'Picker_hr', 'Camion_orden']


    print(df_tarifas_oper)
    df_tarifas_oper['cod_tienda'] = df_tarifas_oper['cod_tienda'].apply(lambda x: int((x.lstrip('0'))) if '-' not in x else x)

    print(df_tarifas_oper)
    ## Transformaciones de df_venta_fact_geo
    print (df_venta_fact_geo)
    # fecha consolidada
    df_venta_fact_geo['fecha_op'] = df_venta_fact_geo.apply(lambda row: str(row['fecha_picking']) if '-' in str(row['fecha_picking']) else (str(row['fecha_compromiso']) if '-' in str(row['fecha_compromiso']) else str(row['fecha_creacion'])), axis=1)
    # calcula ID_MES
    df_venta_fact_geo['ID_MES'] = df_venta_fact_geo['fecha_op'].apply(lambda x: int(pd.to_datetime(x,format='%Y-%m-%d').strftime('%Y%m')))
    # agrega solo operador desde la tabla tarifas (para calcular fecha de corte)
    df_venta_fact_geo['id_tienda'] = df_venta_fact_geo['tienda'].apply(lambda x: str(x).split(' ')[0])
    df_venta_fact_geo = df_venta_fact_geo[(df_venta_fact_geo['id_tienda'].notna())&(df_venta_fact_geo['id_tienda'].astype(str)!="None")]
    df_venta_fact_geo['cod_tienda'] = df_venta_fact_geo.apply(lambda row: str(int(row['id_tienda'].split('-')[0]))+"-0" if '581-' in row['transportadora'] or '445-' in row['transportadora'] else int(row['id_tienda'].split('-')[0]), axis=1)
    lk_operador_del_tarifario = df_tarifas_oper.merge(df_tarifas_oper[['ID_MES','cod_tienda', 'Operador']].groupby('cod_tienda')['ID_MES'].max().reset_index(), on=['ID_MES','cod_tienda'])[['cod_tienda', 'Operador']].drop_duplicates()
    df_venta_fact_geo = df_venta_fact_geo.merge(df_tarifas_oper[['ID_MES','cod_tienda', 'Operador']].drop_duplicates(), on=['ID_MES', 'cod_tienda'], how='left').merge(lk_operador_del_tarifario, on='cod_tienda', how='left')
    df_venta_fact_geo['Operador'] = df_venta_fact_geo.apply(lambda row: row['Operador_x'] if str(row['Operador_x'])!='nan' else row['Operador_y'], axis=1)
    del df_venta_fact_geo['Operador_x'], df_venta_fact_geo['Operador_y']
    # calcula mes de facturación
    df_venta_fact_geo['MES_CORTE25'] = df_venta_fact_geo['fecha_op'].apply(lambda x : pd.to_datetime(x, format="%Y-%m-%d").strftime('%Y%m') 
                                                            if int(pd.to_datetime(x, format="%Y-%m-%d").strftime('%d'))<=25
                                                            else (int(pd.to_datetime(x, format="%Y-%m-%d").strftime('%Y%m'))+1 
                                                                    if int(pd.to_datetime(x, format="%Y-%m-%d").strftime('%m'))<12
                                                                    else int(pd.to_datetime(x, format="%Y-%m-%d").strftime('%Y%m'))+89)).astype(int)
    df_venta_fact_geo['MES_FACTURA'] = df_venta_fact_geo.apply(lambda row: row['MES_CORTE25'] if row['Operador'] in ['Time Jobs', 'Valdishopper'] or row['ID_MES']>=202205 else row['ID_MES'], axis=1)
    del df_venta_fact_geo['MES_CORTE25']
    ## transformaciones de tarifario
    # agrega mes actual si es que no está aun
    meses_venta = list(set(df_venta_fact_geo['MES_FACTURA']))
    meses_falta_tarifa = []
    for mes in meses_venta:
        if mes > max(set(df_tarifas_oper['ID_MES'])):
            meses_falta_tarifa.append(mes)
    for mes in sorted(meses_falta_tarifa):
        df_tarifas_oper_max = df_tarifas_oper[df_tarifas_oper['ID_MES']==df_tarifas_oper['ID_MES'].max()]
        df_tarifas_oper_max['ID_MES'] = mes
        df_tarifas_oper = df_tarifas_oper.append(df_tarifas_oper_max).drop_duplicates()
    df_tarifas_oper['MES_FACTURA'] = df_tarifas_oper['ID_MES']
    # agrega tarifas al dataframe de venta
    df_venta_fact_geo_tarifa = df_venta_fact_geo.merge(df_tarifas_oper[['cod_tienda', 'Tienda', 'costo_base', 'costo_un_picking', 'costo_sku_picking', 'costo_km_despacho',
        'ida_vuelta', 'Camion_orden', 'MES_FACTURA']], on=['cod_tienda', 'MES_FACTURA'], how='left')


    ########################################
    ###########   KILOMETRAJES   ###########
    ########################################
    # Pega los km disponibles en kilometraje real
    df_km_real['KMs_Totales'] = df_km_real['KMs_Totales'].astype(float)
    df_venta_fact_geo_tarifa_km = df_venta_fact_geo_tarifa.merge(df_km_real[['orden', 'KMs_Totales']], on='orden', how='left')
    # calcula KM de vincenty
    df_venta_fact_geo_tarifa_km['KMs_Vincenty'] = df_venta_fact_geo_tarifa_km.apply(lambda row:
        distance.distance((row['lat_tienda'],row['lng_tienda']), (row['lat_pedido'],row['lng_pedido']), ellipsoid='WGS-84').km
        if ~pd.isnull(row['lat_tienda'])
        and ~pd.isnull(row['lat_tienda'])
        and ~pd.isnull(row['lng_pedido']) else np.nan, axis=1)*2
    df_venta_fact_geo_tarifa_km['KMs_Vincenty'] = df_venta_fact_geo_tarifa_km['KMs_Vincenty'].apply(lambda x: 81 if x> 80 else x)
    # proporción o desviación de Vincenty vs Km reales
    desv_vinventy = df_venta_fact_geo_tarifa_km[(df_venta_fact_geo_tarifa_km['KMs_Totales'].notna())].groupby('cod_tienda').sum()[['KMs_Totales', 'KMs_Vincenty']].reset_index()
    desv_vinventy['prop_vinc'] = desv_vinventy.apply(lambda row: row['KMs_Totales']/row['KMs_Vincenty'] if row['KMs_Totales']>0 else 0, axis=1)
    desv_vinventy_sin_cero = desv_vinventy[desv_vinventy['KMs_Totales']>0].sum()[['KMs_Totales', 'KMs_Vincenty']]
    prop_vinc_promedio = desv_vinventy_sin_cero['KMs_Totales']/desv_vinventy_sin_cero['KMs_Vincenty']
    desv_vinventy['prop_vinc'] = desv_vinventy['prop_vinc'].apply(lambda x: x if x>1 else prop_vinc_promedio)
    # agrega la proporción para poner los kilomitros estimados
    df_venta_fact_geo_tarifa_km = df_venta_fact_geo_tarifa_km.merge(desv_vinventy[['cod_tienda', 'prop_vinc']], on='cod_tienda', how='left')
    df_venta_fact_geo_tarifa_km['KMs_estimado'] = df_venta_fact_geo_tarifa_km.apply(lambda row: row['KMs_Vincenty']*row['prop_vinc'] if pd.isnull(row['KMs_Totales']) else 0, axis=1)


    ########################################
    ###########   COSTO ORDENES  ###########
    ########################################
    # Calcula costo Picking
    df_venta_fact_geo_tarifa_km['costo_picking'] = df_venta_fact_geo_tarifa_km.apply(lambda row: int(row['sum(unidades)'])*row['costo_un_picking'] if row['costo_un_picking']>0 else int(row['sum(productos)'])*row['costo_sku_picking'], axis=1)
    # Calcula costo delivery
    df_venta_fact_geo_tarifa_km['KMs_Totales'] = df_venta_fact_geo_tarifa_km['KMs_Totales'].fillna(0)
    df_venta_fact_geo_tarifa_km['KMs_Totales'] = df_venta_fact_geo_tarifa_km.apply(lambda row: row['KMs_Totales'] if row['KMs_Totales']>0
                                                                                else (row['KMs_estimado'] if row['KMs_estimado']>0 else row['KMs_Vincenty']), axis=1)
    del df_venta_fact_geo_tarifa_km['prop_vinc'], df_venta_fact_geo_tarifa_km['KMs_Vincenty']
    df_venta_fact_geo_tarifa_km['costo_delivery'] = df_venta_fact_geo_tarifa_km.apply(lambda row: row['Camion_orden'] if row['Camion_orden'] > 0 
                                                                                    else (row['costo_km_despacho']*(row['KMs_Totales'])*0.5 if row['ida_vuelta']=='ida' 
                                                                                            else row['costo_km_despacho']*(row['KMs_Totales'])), axis=1)
    # calcula el costo total de orden por shopper
    df_venta_fact_geo_tarifa_km['costo_orden_shopper'] = df_venta_fact_geo_tarifa_km.apply(lambda row: row['costo_base']+row['costo_picking']*1.0+row['costo_delivery']-row['Camion_orden']*1.0, axis=1)

    ########################################
    #########  COSTO TIENDA DÍA  ###########
    ########################################
    #
    ######### ASEGURADO SHOPPERS ###########
    # Agrupa para tener el monto juntado por shopper y calcular asegurado
    df_costo_shopper_diario = df_venta_fact_geo_tarifa_km.groupby(['fecha_op','cod_tienda','MES_FACTURA','employee_id', 'Operador'])['costo_orden_shopper', 'Camion_orden'].sum().reset_index()
    # agrega asegurado
    df_costo_shopper_diario_aseg = df_costo_shopper_diario.merge(df_tarifas_oper[['cod_tienda', 'MES_FACTURA', 'asegurado', 'recurso_fijo', 'Picker_hr']], on=['cod_tienda', 'MES_FACTURA'], how='left')
    #arreglo asegurados leslye
    df_costo_shopper_diario_aseg['asegurado'] = df_costo_shopper_diario_aseg.apply(lambda row: 35000 if str(row['cod_tienda'])== '581' and row['fecha_op'] >'2022-09-18' else row['asegurado'], axis=1)
    df_costo_shopper_diario_aseg['asegurado'] = df_costo_shopper_diario_aseg.apply(lambda row: row['asegurado']*0.9 if str(row['Operador'])== 'Touch' and row['fecha_op'] >='2022-10-26' else row['asegurado'], axis=1)
    df_costo_shopper_diario_aseg['asegurado'] = df_costo_shopper_diario_aseg.apply(lambda row: 40000 if str(row['Operador'])== 'Touch' and row['fecha_op'] >'2022-11-03' else row['asegurado'], axis=1)
    #df_costo_shopper_diario_aseg['asegurado'] = df_costo_shopper_diario_aseg.apply(lambda row: 35000 if str(row['cod_tienda'])== '581' and row['fecha_op'] >'2022-09-18' else row['asegurado'], axis=1)
    # calcula dif bono para los días sin venta

    todas_fecha = pd.DataFrame(df_costo_shopper_diario_aseg[['MES_FACTURA','fecha_op']].drop_duplicates(subset='fecha_op'))
    todas_fecha['uno'] = 1
    todas_tienda = pd.DataFrame(df_costo_shopper_diario_aseg['cod_tienda'].drop_duplicates())
    todas_tienda['uno'] = 1
    todas_fecha_tienda = todas_fecha.merge(todas_tienda, on='uno')
    todas_fecha_tienda = todas_fecha_tienda[todas_fecha_tienda['cod_tienda'].apply(lambda x: 1 if '-' in str(x) else 0)==0]
    fecha_tienda_con_vta = df_costo_shopper_diario_aseg[['fecha_op', 'cod_tienda']].drop_duplicates()
    fecha_tienda_con_vta['con_vta'] = 1
    fecha_tienda_sin_vta = todas_fecha_tienda.merge(fecha_tienda_con_vta, on=['fecha_op', 'cod_tienda'], how='left')
    fecha_tienda_sin_vta = fecha_tienda_sin_vta[fecha_tienda_sin_vta['con_vta'].notna()]
    df_tarifas_oper['cod_tienda2'] = df_tarifas_oper['cod_tienda'].apply(lambda x: str(x).zfill(4))

    # pegar despues asegurados_dia_sin_vta con asegurado cambiado a dif bono
    df_costo_shopper_diario_aseg['costo_orden_shopper_para_asegurado'] = df_costo_shopper_diario_aseg.apply(lambda row: row['costo_orden_shopper']*0.8 if row['Operador']=='Time Jobs' else row['costo_orden_shopper'], axis=1)
    df_costo_shopper_diario_aseg['Dif_bono'] = df_costo_shopper_diario_aseg.apply(lambda row: 0 if row['costo_orden_shopper_para_asegurado'] > row['asegurado'] else row['asegurado']-row['costo_orden_shopper_para_asegurado'], axis=1)
    del df_costo_shopper_diario_aseg['costo_orden_shopper_para_asegurado']
    # calcula costo Shopper dia
    df_costo_shopper_diario_aseg['costo_dia_shopper'] = df_costo_shopper_diario_aseg.apply(lambda row: row['Dif_bono']+row['costo_orden_shopper'] if '-' not in str(row['cod_tienda']) else 0, axis=1)
    #
    ##########  PICKERS X HORA  ###########
    # Agrega asistencia shoppers
    df_asistencia_shoppers = df_asistencia_shoppers[['rut', 'tienda', 'transportadora', 'dia_creacion', 'fin_jornada', 'inicio_jornada']]
    df_asistencia_shoppers['horas_en_janis'] = df_asistencia_shoppers.apply(lambda row: pd.to_datetime(row['fin_jornada'], format="%Y-%m-%d %H:%M") - pd.to_datetime(row['inicio_jornada'], format="%Y-%m-%d %H:%M"), axis=1).astype('timedelta64[m]')/60
    df_asistencia_shoppers.columns = ['employee_id', 'Tienda', 'transportadora', 'fecha_op',
        'fin_jornada', 'inicio_jornada', 'horas_en_janis']
    df_asistencia_shoppers = df_asistencia_shoppers[df_asistencia_shoppers['transportadora'].astype(str)!="None"]
    df_asistencia_shoppers = df_asistencia_shoppers[df_asistencia_shoppers['Tienda'].astype(str)!="None"]
    df_asistencia_shoppers['cod_tienda'] =  df_asistencia_shoppers.apply(lambda row: str(int(str(row['Tienda']).split(' ')[0].split('-')[0]))+"-0" if '581-' in row['transportadora'] or '445-' in row['transportadora'] else int(str(row['Tienda']).split(' ')[0].split('-')[0]), axis=1)
    df_asistencia_shoppers_agg = df_asistencia_shoppers.groupby(['employee_id', 'fecha_op', 'cod_tienda']).max('horas_en_janis').reset_index()
    df_costo_shopper_diario_aseg = df_costo_shopper_diario_aseg.merge(df_asistencia_shoppers_agg, on=['employee_id', 'fecha_op', 'cod_tienda'], how='left')
    # calcula costo picker
    # ***
    df_costo_shopper_diario_aseg['costo_dia_picker'] = df_costo_shopper_diario_aseg.apply(lambda row: row['horas_en_janis']*row['Picker_hr'] if pd.notna(row['horas_en_janis'])
                                                                                        else (float(df_costo_shopper_diario_aseg[df_costo_shopper_diario_aseg['cod_tienda']==row['cod_tienda']][['horas_en_janis']].mean()))*row['Picker_hr'], axis=1)
    #
    ##########  recurso fijo ###########
    # calcula en cuantos shopper/tienda/mes prorratear el coordinador external
    cuenta_dia_shopper_mes = df_costo_shopper_diario_aseg.groupby(['MES_FACTURA', 'cod_tienda', 'fecha_op'])['employee_id'].count().reset_index()
    cuenta_dia_shopper_mes.columns = ['MES_FACTURA', 'cod_tienda', 'fecha_op', 'shoppers_dia_tienda_mes']
    df_costo_shopper_diario_aseg = df_costo_shopper_diario_aseg.merge(cuenta_dia_shopper_mes, on=['MES_FACTURA', 'cod_tienda', 'fecha_op'], how='left')
    # calcula cuantos días tiene cada mes
    df_costo_shopper_diario_aseg['dias_del_mes'] = df_costo_shopper_diario_aseg['MES_FACTURA'].apply(lambda x: x-89 if int(str(x)[-2:])==1 else x-1).apply(lambda x: monthrange(int(str(x)[0:4]), int(str(x)[-2:]))[1])
    # calcula el costo por día del coordinador externo
    df_costo_shopper_diario_aseg['costo_coordinador_ext'] = df_costo_shopper_diario_aseg.apply(lambda row: (row['recurso_fijo']/row['dias_del_mes'])/row['shoppers_dia_tienda_mes'], axis=1)
    ##########  TOTAL  ###########
    #
    df_costo_shopper_diario_aseg['costo_logistico_total'] = df_costo_shopper_diario_aseg.fillna(0).apply(lambda row: row['Camion_orden'] + row['costo_dia_shopper'] + row['costo_dia_picker'] + row['costo_coordinador_ext'], axis=1)
    ##########  AGRUPACION  ###########

    # pasa codigo tienda a tienda solo sin diferenciar por op camiones
    df_costo_shopper_diario_aseg['cod_tienda'] = df_costo_shopper_diario_aseg['cod_tienda'].apply(lambda x: str(x).rstrip('-0').zfill(4)
                                                                                                if '-' in str(x)
                                                                                                else str(x).zfill(4))
    # agrupa para obtener por dia tienda: cod Tienda, Fecha, Costo Shoppers, Costo Asegurado, Costo Picker, Monto Camiones,, costo coordinaror Costo total
    df_costo_tienda_diario = df_costo_shopper_diario_aseg.groupby(['fecha_op', 'cod_tienda']).agg(
            costo_shoppers = ('costo_orden_shopper', 'sum'),
            costo_asegurado = ('Dif_bono', 'sum'),
            costo_picker = ('costo_dia_picker', 'sum'),
            costo_camiones = ('Camion_orden', 'sum'),
            costo_coordinador = ('costo_coordinador_ext', 'sum'),
            costo_total = ('costo_logistico_total', 'sum')
        ).reset_index()
    df_costo_tienda_diario["estimado_driver"] = 0
    df_costo_tienda_diario["estimado_gasto_extra"] = 0
    df_costo_tienda_diario["estimado_descuentos"] = 0
    df_costo_tienda_diario.columns = ["fecha","id_tienda","estimado_shoppers", "estimado_asegurado",
    "estimado_picker","estimado_camiones","estimado_coordinador","estimado_total",
    "estimado_driver","estimado_gasto_extra","estimado_descuentos"]
    ##########  EXPORTA  ###########
    # tabla a subir
    buffer = StringIO()
    print("Number of records:")
    print(len(df_costo_tienda_diario.index))
    df_costo_tienda_diario.to_csv(buffer, header=True, index=False, encoding="utf-8", sep = ";")
    buffer.seek(0)

    aws_conn_id="aws_s3_connection"
    file_name = "forecast_and_planning/costos_logisticos/costos_logisticos_diarios_estimacion.csv"
    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    s3_hook.load_string(buffer.getvalue(),
                  key=file_name,
                  bucket_name=s3_bucket,
                  replace=True,
                  encrypt=False)

    return file_name

def _subir_a_bdd(ti, ds):
    import pandas as pd 

    #### IMPORTA CSV
    file_name = ti.xcom_pull(key = "return_value", task_ids = ['calcular_costos_logisticos'])[0]
    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+file_name)
    if not s3_hook.check_for_key(file_name, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % file_name)
    
    costos_object = s3_hook.get_key(file_name, bucket_name = s3_bucket)

    df_costos_estimado = pd.read_csv(costos_object.get()["Body"], decimal=',', sep=';')
    
    # transforma fecha del estimado
    df_costos_estimado = df_costos_estimado[df_costos_estimado['fecha'].notna()]
    df_costos_estimado['fecha'] = df_costos_estimado['fecha'].apply(lambda x: pd.to_datetime(x, format='%Y-%m-%d') if str(x)[:3]=='202' else pd.to_datetime(x, format='%d-%m-%Y')).apply(lambda x: x.strftime('%Y-%m-%d'))
    df_costos_estimado['id_tienda'] = df_costos_estimado['id_tienda'].apply(lambda x: str(int(float(x)))[0:4].rstrip(' ').zfill(4))
    #### SUBE ARCHIVO TRANSFORMADO
    
    for col in df_costos_estimado.columns:
        df_costos_estimado = df_costos_estimado.rename(columns = {col: col.lower()})
    
    df_costos_estimado = df_costos_estimado [["id_tienda","fecha","estimado_shoppers","estimado_asegurado",
                        "estimado_picker","estimado_camiones","estimado_coordinador",
                        "estimado_total","estimado_driver","estimado_gasto_extra","estimado_descuentos"]]


    df_costos_estimado['estimado_shoppers'] = pd.to_numeric(df_costos_estimado['estimado_shoppers'], errors = 'ignore')
    df_costos_estimado['estimado_asegurado'] = pd.to_numeric(df_costos_estimado['estimado_asegurado'], errors = 'ignore')
    df_costos_estimado['estimado_picker'] = pd.to_numeric(df_costos_estimado['estimado_picker'], errors = 'ignore')
    df_costos_estimado['estimado_camiones'] = pd.to_numeric(df_costos_estimado['estimado_camiones'], errors = 'ignore')
    df_costos_estimado['estimado_coordinador'] = pd.to_numeric(df_costos_estimado['estimado_coordinador'], errors = 'ignore')
    df_costos_estimado['estimado_total'] = pd.to_numeric(df_costos_estimado['estimado_total'], errors = 'ignore')
    df_costos_estimado['estimado_driver'] = pd.to_numeric(df_costos_estimado['estimado_driver'], errors = 'ignore')
    df_costos_estimado['estimado_gasto_extra'] = pd.to_numeric(df_costos_estimado['estimado_gasto_extra'], errors = 'ignore')
    df_costos_estimado['estimado_descuentos'] = pd.to_numeric(df_costos_estimado['estimado_descuentos'], errors = 'ignore')
    
    print (df_costos_estimado.dtypes)
    costos_to_sql(df_costos_estimado)

def costos_to_sql(df_costos):

    import pandas as pd 
    import sqlalchemy
    from sqlalchemy import text
    import pandas as pd
    import numpy as np

    
    ############## CARGA DE DATOS #######################

    host = Variable.get('POSTGRESQL_HOST')
    database = Variable.get('POSTGRESQL_DB')
    username = Variable.get('POSTGRESQL_USER')
    password = Variable.get('POSTGRESQL_PASSWORD')

    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    ##### CARGA

    # INSERT
    columns = [
        "id_tienda",
        "fecha",
        "estimado_shoppers", 
        "estimado_asegurado",
        "estimado_picker",
        "estimado_camiones",
        "estimado_coordinador",
        "estimado_total",
        "estimado_driver",
        "estimado_gasto_extra",
        "estimado_descuentos"
    ]
    columns_query = ",".join(columns)
    values_query = ",".join(["%s" for column in columns])
    df_costos = df_costos.fillna("NULL")
    records = list(df_costos.to_records(index=False))
    
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
        INSERT INTO forecast_and_planning.costos_logisticos_diarios_soloestim ("""+columns_query+""") 
        VALUES ("""+values_query+""")
        ON CONFLICT (id_tienda,fecha)
        DO NOTHING; 
    """

    print(incremental_query)
    pg_hook = PostgresHook(conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.executemany(incremental_query, fixed_records)
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    print("Data loaded to Postgres")

    connection = engine.connect()

    engine = sqlalchemy.create_engine(conn_url)
    df_tarifa_test = pd.read_sql("SELECT * FROM forecast_and_planning.costos_logisticos_diarios_soloestim", con=engine)

    try:
        print('EXITOSO: Se ha cargado exitosamente la base costos logísticos')
        print('Datos SQL df_tarifa_test: {}'.format(df_tarifa_test.shape[0]))
        print('Datos df_tarifa_test: {}'.format(df_tarifa_test.shape[0]))
    except Exception as e:
        print('ERROR: No se ha cargado exitosamente la base costos logísticos')
        print (str(e))
        raise Exception('Error, no se pudo cargar a la base de datos')
    connection.close()


default_args = {
    "owner": "capacity_and_planning",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'etl_costos_logisticos_diarios',
    default_args=default_args,
    description="Automatización de calculo de costos logisticos diarios",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2022, 11, 28, tz="America/Santiago"),
    catchup=False,
    tags=["OPS","AWS","ETL", "unimarc", "forecast_and_planning", "costos_logisticos_solo_estim"],
) as dag:

    dag.doc_md = """
    Obtención de costos y kms en base a BDD \n
    para exportar a BDD.
    """ 

    t0 = PythonOperator(
        task_id = "calcular_costos_logisticos",
        python_callable = _calcular_costos_logisticos,
    )

    t1 = PythonOperator(
        task_id = "subir_a_bdd",
        python_callable = _subir_a_bdd,
    )

t0>>t1
