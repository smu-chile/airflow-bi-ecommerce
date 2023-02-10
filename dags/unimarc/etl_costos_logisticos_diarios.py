from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
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
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
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
    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME')
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

    conn_url = "postgresql+psycopg2://"+username+":"+password+"@"+host+":5432/"+database
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
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
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


def _costos_logisticos_mfc():
    import pandas as pd
    import numpy as np
    from calendar import monthrange
    import psycopg2
    from io import StringIO


    ########################################
    ########### PARÁMETROS #################
    ########################################
    # fecha desde (partida mfc)
    fecha_desde = "2022-10-03"
    # costos fijos mensuales
    mantencion = 14906170
    servicios = 19778758 # servicios - Rayo
    arriendo_equipos = 39000 # se quitó traspaleta de $748.000
    varios_patente = 417536
    # tasas
    mantencion_ti = 0.017 + 0.005 + 0.005 #tiene Takeoff (1,7%) + Janis 0,5% y Vtex 0,5%
    fijo_mantencion_ti = 607750+8341667 # TC57+Hotline
    # valores por hc
    moviliza_1_hc = 4500

    ########################################
    ########### FUNCIONES  #################
    ########################################
    # tarifas RayoApp
    def costo_delivery(comuna):
        costo_x_orden = 0
        if comuna.lower() in ['lo barnechea', 'vitacura', 'las condes']:
            costo_x_orden = 5750
        elif comuna.lower() in ['providencia', 'santiago', 'ñuñoa', 'la reina', 'penalolen', 'peñalolen', 'peñalolén', 'huechuraba']:
            costo_x_orden = 6750
        elif comuna.lower() in ['la florida', 'puente alto', 'maipu', 'independencia', 'macul', 'recoleta', 'renca', 'conchalí']:
            costo_x_orden = 9750
        else:
            costo_x_orden = (5750+6750+9750)/3
        return costo_x_orden
    # descuento RayoApp
    def descuento_delivery(ordenes_dia):
        if ordenes_dia >= 401:
            desc = 0.1
        elif ordenes_dia >= 250:
            desc = 0.07
        elif ordenes_dia >= 150:
            desc = 0.05
        else:
            desc = 0
        return desc
    # añadir asegurado RayoApp 70 pedidos
    ## costo remuneración
    def remuneracion_variable(ordenes_dia):
        if ordenes_dia >= 650:
            rem_mes = 92474353 
            hc = 20
        elif ordenes_dia >= 500:
            rem_mes = 72457970 
            hc = 36
        elif ordenes_dia >= 400:
            rem_mes = 68383309 
            hc = 47
        elif ordenes_dia >= 300:
            rem_mes = 59825733 
            hc = 58
        elif ordenes_dia >= 200:
            rem_mes = 52187755 
            hc = 64
        else:
            rem_mes = 37951089 
            hc = 88
        return rem_mes, hc

    
    
    ########################################
    ########### IMPORTA CONSULTAS ##########
    ########################################
    # importa funciones de consulta
    def venta_fact_mfc(fecha_desde):
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
        select to_char(timezone('America/Santiago',o.fecha_creacion::timestamp), 'yyyy-mm-dd') as fecha_creacion, 
        to_char(timezone('America/Santiago',d.fecha_despacho ::timestamp), 'yyyy-mm-dd') as fecha_compromiso, 
        to_char(timezone('America/Santiago',o.fecha_picking::timestamp), 'yyyy-mm-dd') as fecha_picking,
        o.id as orden, eo.nombre_estado as estado, a.nombre as firstname, a.apellido as lastname, a.id_empleado as employee_id,
        t2.nombre_tienda_janis as tienda, t.nombre as transportadora, d.comuna, d.lng as lng_pedido, d.lat as lat_pedido, t2.longitud as lng_tienda, t2.latitud as lat_tienda, o.venta_facturada_neta as "sum(venta_neta)",
        o.unidades_solicitadas as "sum(unidades)", o.productos_solicitados as "sum(productos)"
        from ecommdata.ordenes_janis o
        left join ecommdata.despachos d on o.id=d.id_orden 
        LEFT join ecommdata.administradores a on o.id_picker = a.id
        left join ecommdata.estado_orden_janis eo on o.estado_janis=eo.id_estado
        left join ecommdata.transportadoras t on d.id_transportadora =t.id 
        left join ecommdata.tiendas t2 on t.id_tienda = t2.id 
        WHERE o.fecha_creacion >= '{fecha_desde}' and d.id_transportadora = '1917'
        ;
        """
        cur.execute(query)
        rows = cur.fetchall()
        columns = cur.description
        conn.close()
        result = [{columns[index][0]:column for index, column in enumerate(value)} for value in rows]
        df_venta_fact_geo = pd.DataFrame(result)

        return df_venta_fact_geo

    def calendario():
        conexion = psycopg2.connect(host="bi-ecommerce-postgres-prod-master.cuuchupawrpt.us-east-1.rds.amazonaws.com", database="postgres", user="fmacaya", password="PJfW#36DUPX", port = '5432')
        cur = conexion.cursor()

        # Ejecutamos una consulta
        cur.execute( "SELECT  * FROM ecommdata.calendario" )
        rows = cur.fetchall()
        columns = cur.description
        result = [{columns[index][0]:column for index, column in enumerate(value)} for value in rows]
        df_calendario = pd.DataFrame(result)
        cur.close()
        conexion.close()
        return df_calendario

    def materiales_remuneraciones():
        conexion = psycopg2.connect(host="bi-ecommerce-postgres-prod-master.cuuchupawrpt.us-east-1.rds.amazonaws.com", database="postgres", user="fmacaya", password="PJfW#36DUPX", port = '5432')
        cur = conexion.cursor()
        # Ejecutamos una consulta
        cur.execute( "SELECT  * FROM forecast_and_planning.materiales_remuneraciones" )
        rows = cur.fetchall()
        columns = cur.description
        result = [{columns[index][0]:column for index, column in enumerate(value)} for value in rows]
        df_materiales_remuneraciones = pd.DataFrame(result)
        cur.close()
        conexion.close()
        return df_materiales_remuneraciones

    def mantenciones_servicios():
        conexion = psycopg2.connect(host="bi-ecommerce-postgres-prod-master.cuuchupawrpt.us-east-1.rds.amazonaws.com", database="postgres", user="fmacaya", password="PJfW#36DUPX", port = '5432')
        cur = conexion.cursor()
        # Ejecutamos una consulta
        cur.execute( "SELECT  * FROM forecast_and_planning.mantenciones_y_servicios_mfc" )
        rows = cur.fetchall()
        columns = cur.description
        result = [{columns[index][0]:column for index, column in enumerate(value)} for value in rows]
        df_materiales_remuneraciones = pd.DataFrame(result)
        cur.close()
        conexion.close()
        return df_materiales_remuneraciones

    ########################################
    ########### IMPORTA DATOS ##############
    ########################################
    # importa consulta venta con coordenadas
    df_venta_fact_geo = venta_fact_mfc(fecha_desde)
    # importa calendario
    df_calendario = calendario()
    df_calendario = df_calendario[df_calendario['fecha'].apply(lambda x: pd.to_datetime(x))>=pd.to_datetime(fecha_desde[:-2]+"01")]
    df_calendario = df_calendario[df_calendario['fecha'].apply(lambda x: pd.to_datetime(x))<=pd.to_datetime(date.today())].sort_values(by='fecha')
    # importa remuneraciones y materiales (archivo)
    df_materiales_remuneraciones = materiales_remuneraciones()
    # importa mantenciones y servicios
    df_mantenciones_servicios = mantenciones_servicios()
    df_mantenciones_servicios = df_mantenciones_servicios.rename(columns={'id_mes':'ID_MES'})
    servicios = int(df_mantenciones_servicios[df_mantenciones_servicios['ID_MES']==df_mantenciones_servicios['ID_MES'].max()]['costo_servicios']) # servicios - Rayo

    ########################################
    ########### TRANSFORMACIONES ###########
    ########################################
    ## Transformaciones de df_venta_fact_geo
    # fecha consolidada
    df_venta_fact_geo['fecha_op'] = df_venta_fact_geo.apply(lambda row: str(row['fecha_picking']) if '-' in str(row['fecha_picking']) else (str(row['fecha_compromiso']) if '-' in str(row['fecha_compromiso']) else str(row['fecha_creacion'])), axis=1)
    df_venta_fact_geo = df_venta_fact_geo[df_venta_fact_geo['fecha_op'].apply(lambda x: pd.to_datetime(x))<=pd.to_datetime(date.today())]

    # calcula ID_MES
    df_venta_fact_geo['ID_MES'] = df_venta_fact_geo['fecha_op'].apply(lambda x: int(pd.to_datetime(x,format='%Y-%m-%d').strftime('%Y%m')))
    # agrega solo operador desde la tabla tarifas (para calcular fecha de corte)
    df_venta_fact_geo['id_tienda'] = df_venta_fact_geo['tienda'].apply(lambda x: str(x).split(' ')[0])
    df_venta_fact_geo = df_venta_fact_geo[df_venta_fact_geo['id_tienda'].notna()]
    df_venta_fact_geo['cod_tienda'] = df_venta_fact_geo.apply(lambda row: str(int(row['id_tienda'].split('-')[0]))+"-0" if '581-' in row['transportadora'] or '445-' in row['transportadora'] else int(row['id_tienda'].split('-')[0]), axis=1)
    # calcula mes de facturación
    df_venta_fact_geo['MES_CORTE25'] = df_venta_fact_geo['fecha_op'].apply(lambda x : pd.to_datetime(x, format="%Y-%m-%d").strftime('%Y%m') 
                                                            if int(pd.to_datetime(x, format="%Y-%m-%d").strftime('%d'))<=25
                                                            else (int(pd.to_datetime(x, format="%Y-%m-%d").strftime('%Y%m'))+1 
                                                                    if int(pd.to_datetime(x, format="%Y-%m-%d").strftime('%m'))<12
                                                                    else int(pd.to_datetime(x, format="%Y-%m-%d").strftime('%Y%m'))+89)).astype(int)
    df_venta_fact_geo['MES_FACTURA'] = df_venta_fact_geo.apply(lambda row: row['MES_CORTE25'] if row['ID_MES']>=202205 else row['ID_MES'], axis=1)
    del df_venta_fact_geo['MES_CORTE25']
    ## Transformaciones de remuneraciones
    # agregar meses que aun no se han facturado o actualizado
    df_materiales_remuneraciones = df_materiales_remuneraciones.rename(columns={'id_mes':'ID_MES'})
    meses_vta= df_venta_fact_geo['ID_MES'].unique()
    for mes in meses_vta:
        if int(mes) > df_materiales_remuneraciones['ID_MES'].astype(int).max():
            agrega_mes = df_materiales_remuneraciones[df_materiales_remuneraciones['ID_MES'].astype(int)==df_materiales_remuneraciones['ID_MES'].astype(int).max()]
            agrega_mes['ID_MES'] = mes
            df_materiales_remuneraciones = df_materiales_remuneraciones.append(agrega_mes)
    # agrega id_tienda
    #df_materiales_remuneraciones['id_tienda'] = df_materiales_remuneraciones['tienda'].apply(lambda x: str(x).split(' ')[0])

    ########################################
    ###########   COSTO ORDENES  ###########
    ########################################
    # Calcula costo delivery segun comuna (solo el despacho cada orden, falta descuento y asegurado)
    df_venta_fact_geo['costo_delivery_orden'] = df_venta_fact_geo['comuna'].apply(lambda x: costo_delivery(x))

    ########################################
    #########  COSTO TIENDA DÍA  ###########
    ########################################
    # agrupa dia tienda
    df_venta_fact_geo['ordenes'] = 1
    df_venta_fact_geo['transportadora'] = '1917'
    df_venta_fact_geo_agg = df_venta_fact_geo.groupby(['fecha_op', 'ID_MES', 'id_tienda', 'tienda', 'cod_tienda', 'MES_FACTURA', 'transportadora'])[['ordenes', 'sum(venta_neta)', 'costo_delivery_orden']].sum().reset_index()
    # agrega días entre medio
    df_calendario = df_calendario[['fecha', 'semana_ano_texto']]
    df_calendario.columns = ['fecha_op', 'semana_ano_texto']
    df_calendario['fecha_op'] = df_calendario['fecha_op'].astype(str)
    df_venta_fact_geo_agg['fecha_op'] = df_venta_fact_geo_agg['fecha_op'].astype(str)
    df_venta_fact_geo_agg = df_calendario.merge(df_venta_fact_geo_agg, on='fecha_op', how='left')
    df_venta_fact_geo_agg['ID_MES'] = df_venta_fact_geo_agg['fecha_op'].apply(lambda x: int(pd.to_datetime(x,format='%Y-%m-%d').strftime('%Y%m')))
    df_venta_fact_geo_agg['id_tienda'] = df_venta_fact_geo_agg['id_tienda'].fillna(method='ffill').fillna(method='bfill')
    df_venta_fact_geo_agg['tienda'] = df_venta_fact_geo_agg['tienda'].fillna(method='ffill').fillna(method='bfill')
    df_venta_fact_geo_agg['cod_tienda'] = df_venta_fact_geo_agg['cod_tienda'].fillna(method='ffill').fillna(method='bfill')
    df_venta_fact_geo_agg['transportadora'] = df_venta_fact_geo_agg['transportadora'].fillna(method='ffill').fillna(method='bfill')
    df_venta_fact_geo_agg = df_venta_fact_geo_agg.fillna(0)
    # agrega remuneraciones y materiales
    df_materiales_remuneraciones = df_materiales_remuneraciones.rename(columns={'id_mes': 'ID_MES', 'remuneraciones':'Remuneraciones', 'materiales':'Materiales'})
    df_materiales_remuneraciones = df_materiales_remuneraciones[['ID_MES', 'Remuneraciones', 'Materiales', 'id_tienda']]
    df_materiales_remuneraciones.columns = ['ID_MES', 'Remuneraciones_mes', 'Materiales_mes', 'id_tienda']
    df_venta_fact_geo_agg = df_venta_fact_geo_agg.merge(df_materiales_remuneraciones[['ID_MES', 'Remuneraciones_mes', 'Materiales_mes', 'id_tienda']], on=['ID_MES','id_tienda'], how='left')

    # calcula cantidad de dias del mes en curso
    df_venta_fact_geo_agg['dias_del_mes'] = df_venta_fact_geo_agg['ID_MES'].apply(lambda x: monthrange(int(str(x)[0:4]), int(str(x)[-2:]))[1])

    # Calcula costo remuneraciones
    df_venta_fact_geo_agg['costo_remuneraciones'] = df_venta_fact_geo_agg.apply(lambda row: row['Remuneraciones_mes']/row['dias_del_mes'], axis=1)

    # Calcula costo materiales
    df_venta_fact_geo_agg['costo_materiales'] = df_venta_fact_geo_agg.apply(lambda row: row['Materiales_mes']/row['dias_del_mes'], axis=1)

    # calcula costo mantencion (si es mes pasado reemplazar por tabla mantención y serv)
    df_venta_fact_geo_agg['costo_mantencion'] = df_venta_fact_geo_agg.apply(lambda row: (df_mantenciones_servicios[df_mantenciones_servicios['ID_MES'].notna()]['costo_mantencion'].sum()/row['dias_del_mes']) 
                                                                                if len(df_mantenciones_servicios[df_mantenciones_servicios['ID_MES'].notna()])>0 
                                                                                else mantencion/row['dias_del_mes'], axis=1)

    # calcula costo mantención ti (si es mes pasado reemplazar variable por tabla mantención y serv) * incluye TC57 y Hotline (por sobre el ebitda) 
    df_venta_fact_geo_agg['costo_mantencion_ti'] = df_venta_fact_geo_agg.apply(lambda row: df_mantenciones_servicios[df_mantenciones_servicios['ID_MES'].notna()]['costo_mantencion_ti'].sum()*(row['sum(venta_neta)']/df_venta_fact_geo_agg[df_venta_fact_geo_agg['ID_MES'].notna()]['sum(venta_neta)'].sum()) + (fijo_mantencion_ti/row['dias_del_mes']) 
                                                                                    if df_mantenciones_servicios[df_mantenciones_servicios['ID_MES'].notna()].shape[0]>0 
                                                                                    else (row['sum(venta_neta)']*mantencion_ti) + (fijo_mantencion_ti/row['dias_del_mes']), axis=1)

    # calcula costo servicios  (si es mes pasado reemplazar por tabla mantención y serv)
    df_venta_fact_geo_agg['costo_servicios'] = df_venta_fact_geo_agg.apply(lambda row: df_mantenciones_servicios[df_mantenciones_servicios['ID_MES'].notna()]['costo_servicios'].sum()/row['dias_del_mes'] 
                                                                                if df_mantenciones_servicios[df_mantenciones_servicios['ID_MES'].notna()].shape[0]>0 
                                                                                else servicios/row['dias_del_mes'], axis=1)

    # calcula costo varios  (si es mes pasado, sumar de tabla mantención y serv a lo que se tiene) * incluye movilización de personal y patentes comerciales (por sobre el ebitda)
    df_venta_fact_geo_agg['costo_varios'] = df_venta_fact_geo_agg.apply(lambda row: remuneracion_variable(row['ordenes'])[1]*moviliza_1_hc + varios_patente/row['dias_del_mes'] if row['ordenes']>0 else varios_patente/row['dias_del_mes'], axis=1)
    df_venta_fact_geo_agg['costo_varios'] = df_venta_fact_geo_agg.apply(lambda row: row['costo_varios']+df_mantenciones_servicios[df_mantenciones_servicios['ID_MES'].notna()]['costo_varios'].sum()/row['dias_del_mes'] 
                                                                            if df_mantenciones_servicios[df_mantenciones_servicios['ID_MES'].notna()].shape[0]>0 
                                                                            else row['costo_varios'], axis=1)

    # calcula descuento delivery
    df_venta_fact_geo_agg['descuento_delivery'] = df_venta_fact_geo_agg.apply(lambda row: descuento_delivery(row['ordenes']), axis=1)
    # calcula asegurado delivery
    df_venta_fact_geo_agg['asegurado_delivery'] = df_venta_fact_geo_agg.apply(lambda row: (80*5750+40*5738+40*7234 - row['costo_delivery_orden']) if (row['ordenes'] < 160 and row['costo_delivery_orden'] < 80*5750+40*5738+40*7234 and int(str(row['fecha_op']).replace("-",""))>=20230111)
                                                                            else ((80*5750+40*7234 - row['costo_delivery_orden']) if (row['ordenes'] < 120 and row['costo_delivery_orden'] < 80*5750+40*7234 and int(str(row['fecha_op']).replace("-",""))>=20230101 )
                                                                                    else ((70*5750 - row['costo_delivery_orden']) if (row['ordenes'] < 70 and row['costo_delivery_orden'] < 70*5750) else 0)), axis=1)
    # calcula costo total delivery
    df_venta_fact_geo_agg['costo_delivery_total'] = df_venta_fact_geo_agg.apply(lambda row: row['costo_delivery_orden']*(1 - row['descuento_delivery']) + row['asegurado_delivery'], axis=1)

    # calcula costo arriendo equipos (si es mes pasado reemplazar por tabla mantención y serv) * incluye traspaletas por sobre Ebitda
    df_venta_fact_geo_agg['costo_arriendo_equipos'] = df_venta_fact_geo_agg.apply(lambda row: arriendo_equipos/row['dias_del_mes'], axis=1)
    df_venta_fact_geo_agg['costo_arriendo_equipos'] = df_venta_fact_geo_agg.apply(lambda row: row['costo_arriendo_equipos']+df_mantenciones_servicios[df_mantenciones_servicios['ID_MES'].notna()]['costo_arriendo_equipos'].sum()/row['dias_del_mes'] 
                                                                                if df_mantenciones_servicios[df_mantenciones_servicios['ID_MES'].notna()].shape[0]>0 
                                                                                else row['costo_arriendo_equipos'], axis=1)


    # agrupa para obtener por dia tienda: cod Tienda, Fecha, Costo Shoppers, Costo Asegurado, Costo Picker, Monto Camiones,, costo coordinaror Costo total
    df_costo_mfc_diario = df_venta_fact_geo_agg.groupby(['fecha_op', 'cod_tienda', 'ID_MES']).agg(
            costo_remuneraciones = ('costo_remuneraciones', 'sum'),
            costo_materiales = ('costo_materiales', 'sum'),
            costo_mantencion = ('costo_mantencion', 'sum'),
            costo_mantencion_ti = ('costo_mantencion_ti', 'sum'),
            costo_servicios = ('costo_servicios', 'sum'),
            costo_varios = ('costo_varios', 'sum'),
            costo_delivery_total = ('costo_delivery_total', 'sum'),
            costo_arriendo_equipos = ('costo_arriendo_equipos', 'sum'),
        ).reset_index()

    ## Calcula costo total ##
    df_costo_mfc_diario['costo_logistico_total'] = df_costo_mfc_diario.fillna(0).apply(lambda row: 
            row['costo_remuneraciones']+
            row['costo_mantencion']+
            row['costo_mantencion_ti']+
            row['costo_servicios']+
            row['costo_varios']+
            row['costo_delivery_total']+
            row['costo_arriendo_equipos'], axis=1)
    df_costo_mfc_diario['costo_total_operacion'] = df_costo_mfc_diario.fillna(0).apply(lambda row: 
            row['costo_logistico_total']+
            row['costo_materiales'], axis=1)

    df_costos_mfc = df_costo_mfc_diario 
    # transformaciones mfc formato
    df_costos_mfc['id_tienda'] = df_costos_mfc['cod_tienda']
    df_costos_mfc['fecha'] = df_costos_mfc['fecha_op']
    df_costos_mfc['estimado_camiones'] = df_costos_mfc['costo_delivery_total']
    df_costos_mfc["estimado_gasto_extra"] = df_costos_mfc.fillna(0).apply(lambda row: 
                                                                            #row['costo_remuneraciones']+
                                                                            row['costo_mantencion']+
                                                                            row['costo_mantencion_ti']+
                                                                            row['costo_servicios']+
                                                                            row['costo_varios']+
                                                                            #row['costo_delivery_total']+
                                                                            row['costo_arriendo_equipos']
                                                                        , axis=1)
    df_costos_mfc["estimado_total"] = df_costos_mfc.fillna(0).apply(lambda row: row['estimado_camiones']+
                                                                            row["estimado_gasto_extra"], axis=1)
    df_costos_mfc["estimado_costo_remuneraciones"] = df_costos_mfc['costo_remuneraciones']
    df_costos_mfc["estimado_costo_materiales"] = df_costos_mfc['costo_materiales']
    df_costos_mfc["estimado_total_mas_remuneracion_y_materiales"] = df_costos_mfc.fillna(0).apply(lambda row: row['estimado_total']+
                                                                            row["estimado_costo_remuneraciones"]+
                                                                            row["estimado_costo_materiales"], axis=1)
    # quita mfc del estimado shoppers y appendea costos estimados MFC
    df_costos_estimado = df_costos_estimado[df_costos_estimado['id_tienda']!=1917]
    df_costos_estimado.columns = ['fecha', 'id_tienda', 'estimado_shoppers', 'estimado_asegurado',
        'estimado_picker', 'estimado_camiones', 'estimado_coordinador',
        'estimado_total', 'estimado_driver', 'estimado_gasto_extra',
        'estimado_descuentos', 'fecha_op', 'ID_MES', 'Remuneraciones_mes',
        'Materiales_mes', 'dias_del_mes', 'estimado_costo_remuneraciones',
        'estimado_costo_materiales', 'estimado_total_mas_remuneracion_y_materiales']
    df_costos_estimado = df_costos_estimado.append(df_costos_mfc)

    # transforma fecha del estimado
    df_costos_estimado = df_costos_estimado[df_costos_estimado['fecha'].notna()]
    df_costos_estimado['fecha'] = df_costos_estimado['fecha'].apply(lambda x: pd.to_datetime(x, format='%Y-%m-%d') if str(x)[:3]=='202' else pd.to_datetime(x, format='%d-%m-%Y')).apply(lambda x: x.strftime('%Y-%m-%d'))
    df_costos_estimado['id_tienda'] = df_costos_estimado['id_tienda'].apply(lambda x: str(int(float(x)))[0:4].rstrip(' ').zfill(4))
    #### nombres de columna
    for col in df_costos.columns:
        df_costos = df_costos.rename(columns={col: col.lower()})


    #### columnas y orden
    df_costos = df_costos[["id_tienda","fecha","estimado_shoppers","estimado_asegurado",
                        "estimado_picker","estimado_camiones","estimado_coordinador",
                        "estimado_driver", "estimado_gasto_extra","estimado_descuentos",
                        "estimado_total", "estimado_costo_remuneraciones", "estimado_costo_materiales",
                        "estimado_total_mas_remuneracion_y_materiales",
                        ]]

    #### SUBE ARCHIVO TRANSFORMADO
    df_costos['fecha'] = df_costos['fecha'].apply(lambda x: str(pd.to_datetime(x, format="%Y-%m-%d").strftime("%Y-%m-%d"))).astype(str)
    # for col in df_costos.columns:
    #     if 'estimado' in col:
    #         df_costos[col] = df_costos[col].apply(lambda x: round(x,0) if x.notna() else x).astype(pd.Int64Dtype())

    df_costos_mfc_final = df_costos
    df_costos_mfc_final['estimado_shoppers'] = pd.to_numeric(df_costos_mfc_final['estimado_shoppers'], errors = 'ignore')
    df_costos_mfc_final['estimado_asegurado'] = pd.to_numeric(df_costos_mfc_final['estimado_asegurado'], errors = 'ignore')
    df_costos_mfc_final['estimado_picker'] = pd.to_numeric(df_costos_mfc_final['estimado_picker'], errors = 'ignore')
    df_costos_mfc_final['estimado_camiones'] = pd.to_numeric(df_costos_mfc_final['estimado_camiones'], errors = 'ignore')
    df_costos_mfc_final['estimado_coordinador'] = pd.to_numeric(df_costos_mfc_final['estimado_coordinador'], errors = 'ignore')
    df_costos_mfc_final['estimado_total'] = pd.to_numeric(df_costos_mfc_final['estimado_total'], errors = 'ignore')
    df_costos_mfc_final['estimado_driver'] = pd.to_numeric(df_costos_mfc_final['estimado_driver'], errors = 'ignore')
    df_costos_mfc_final['estimado_gasto_extra'] = pd.to_numeric(df_costos_mfc_final['estimado_gasto_extra'], errors = 'ignore')
    df_costos_mfc_final['estimado_descuentos'] = pd.to_numeric(df_costos_mfc_final['estimado_descuentos'], errors = 'ignore')
    df_costos_mfc_final['estimado_costo_remuneraciones'] = pd.to_numeric(df_costos_mfc_final['estimado_costo_remuneraciones'], errors = 'ignore')
    df_costos_mfc_final['estimado_costo_materiales'] = pd.to_numeric(df_costos_mfc_final['estimado_costo_materiales'], errors = 'ignore')
    df_costos_mfc_final['estimado_total_mas_remuneracion_y_materiales'] = pd.to_numeric(df_costos_mfc_final['estimado_total_mas_remuneracion_y_materiales'], errors = 'ignore')

    buffer = StringIO()
    print("Number of records:")
    print(len(df_costos_mfc_final.index))
    df_costos_mfc_final.to_csv(buffer, header=True, index=False, encoding="utf-8", sep = ";")
    buffer.seek(0)

    aws_conn_id="aws_s3_connection"
    file_name = "forecast_and_planning/costos_logisticos/costos_logisticos_mfc_diarios_estimacion.csv"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    s3_hook.load_string(buffer.getvalue(),
                  key=file_name,
                  bucket_name=s3_bucket,
                  replace=True,
                  encrypt=False)

    return file_name


def _subir_a_bdd_mfc(ti, ds):
    import pandas as pd 

    #### IMPORTA CSV
    file_name = ti.xcom_pull(key = "return_value", task_ids = ['costos_logisticos_mfc'])[0]
    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME')
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+file_name)
    if not s3_hook.check_for_key(file_name, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % file_name)
    
    costos_object = s3_hook.get_key(file_name, bucket_name = s3_bucket)

    df_costos_estimado = pd.read_csv(costos_object.get()["Body"], decimal=',', sep=';')
    
    print (df_costos_estimado.dtypes)
    costos_to_sql_mfc(df_costos_estimado)

def costos_to_sql_mfc(df_costos):

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

    conn_url = "postgresql+psycopg2://"+username+":"+password+"@"+host+":5432/"+database
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
        "estimado_descuentos",
        "estimado_costo_remuneraciones",
        "estimado_costo_materiales",
        "estimado_total_mas_remuneracion_y_materiales"
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
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
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
    schedule_interval="0 7 * * *",
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
    t2 = PythonOperator(
        task_id = "calcular_costos_logisticos_mfc",
        python_callable = _costos_logisticos_mfc,
    )

    t3 = PythonOperator(
        task_id = "subir_a_bdd_mfc",
        python_callable = _subir_a_bdd_mfc,
    )

t0>>t1>>t2>>t3
