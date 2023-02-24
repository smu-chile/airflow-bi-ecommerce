SELECT P.ean AS ean
        , CASE
            WHEN P.CONT_CONV_UMB > 1 THEN CAST(CAST(sa.sku_product AS int) AS varchar(25)) || '_' || P.CONT_CONV_UMB
            ELSE CAST(CAST(sa.sku_product AS int) AS varchar(25))
        END AS id
        ,  WF.PRECIO AS discount_price
        , FLOOR(NBR_ITM / P.CONT_CONV_UMB) AS stock
        , ou.ou_id AS store_id 
        , P.NM AS name
        , P.BRAND_DESC AS trademark
        , CASE 
            WHEN p.unidad_de_medida IN ('KG', 'KGV') THEN 'WW'
            ELSE 'U'
        END AS sale_type
        , sa.sku_product AS material
        , p.unidad_de_medida 
FROM DWC_SMU.SMU.VW_FACT_STOCK S
LEFT JOIN DWC_SMU.SMU.VW_DIM_SKU_ATTR SA ON SA.SKU_KEY  = S.SKU_KEY
LEFT JOIN DWC_SMU.SMU.VW_DIM_PRODUCT P ON P.SKU_KEY = SA.SKU_KEY
LEFT JOIN DWC_SMU.SMU.VW_DIM_ORGANIZATION_UNIT OU ON OU.OU_KEY = S.OU_KEY
LEFT JOIN DWC_SMU.SMU.VW_DIM_ALMACEN A ON A.ALMACEN_KEY =S.ALMACEN_KEY
LEFT JOIN DWC_SMU.SMU.VW_DIM_PARTICULARIDAD PART ON S.PARTICULARIDAD_KEY =PART.PARTICULARIDAD_KEY
LEFT JOIN (SELECT EAN
                    , min(PRECIO_PROMOCIONAL) AS PRECIO
            FROM NZ_BU.ECOMERCE.VW_WORKFLOW
            WHERE FECHA_INICIO_DE_PROMOCION <= TO_CHAR(NOW(),'YYYY-MM-DD')
            AND FECHA_FIN_DE_PROMOCION >= TO_CHAR(NOW(),'YYYY-MM-DD')
            AND TIPO_PROMOCION IN (1,4)
            AND REGISTRO_VALIDO = 'X'
            AND ORGANIZACION_VENTAS = '1000'
            AND CANAL_DISTRIBUCION = '10'
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
AND OU.OU_ID = '{store_id}'
AND (P.NLS_PD_DSC IS NOT NULL OR P.UNIDAD_DE_MEDIDA IN ('KG', 'KGV'))
AND P.UNIDAD_DE_MEDIDA  IS NOT NULL
AND PART.PARTICULARIDAD_COD = 'A'
AND S.TIPO_STOCK_KEY IN (9161419180, 9145314683)
AND FLOOR(NBR_ITM / P.CONT_CONV_UMB) > 0
AND p.indic_ean_ppal = 'X';
