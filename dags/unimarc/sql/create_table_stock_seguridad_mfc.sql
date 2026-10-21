-- ============================================================================
-- SCRIPT DDL: Tabla de Stock de Seguridad MFC (Micro Fulfillment Center)
-- Esquema: ecommdata
-- Tabla: ecommdata.stock_seguridad_mfc
-- Objetivo: Almacenar el stock de seguridad calculado y enviado a MFC (0917)
--           diferenciando por fecha y jornada (AM/PM), con retención de 5 días.
-- Visualización: Metabase BI
-- ============================================================================

CREATE TABLE IF NOT EXISTS ecommdata.stock_seguridad_mfc (
    sku             VARCHAR(50) NOT NULL,
    id_warehouse    VARCHAR(10) NOT NULL,
    security_stock  INTEGER NOT NULL,
    fecha           DATE NOT NULL,
    horario         VARCHAR(2) NOT NULL, -- 'AM' o 'PM'
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_stock_seguridad_mfc PRIMARY KEY (fecha, horario, sku)
);

-- Índices para optimizar consultas en Metabase y filtros de fecha/horario
CREATE INDEX IF NOT EXISTS idx_stock_seguridad_mfc_fecha_horario 
    ON ecommdata.stock_seguridad_mfc (fecha, horario);

CREATE INDEX IF NOT EXISTS idx_stock_seguridad_mfc_sku 
    ON ecommdata.stock_seguridad_mfc (sku);

CREATE INDEX IF NOT EXISTS idx_stock_seguridad_mfc_fecha 
    ON ecommdata.stock_seguridad_mfc (fecha);

-- Comentarios explicativos en catálogo Postgres
COMMENT ON TABLE ecommdata.stock_seguridad_mfc IS 'Histórico de stock de seguridad enviado a MFC (0917) con granularidad diaria y jornada (AM/PM). Retención de 5 días.';
COMMENT ON COLUMN ecommdata.stock_seguridad_mfc.sku IS 'Identificador del SKU/material con UMV (ej: 000000000000007092-UN)';
COMMENT ON COLUMN ecommdata.stock_seguridad_mfc.id_warehouse IS 'Identificador de la tienda / almacén MFC (0917)';
COMMENT ON COLUMN ecommdata.stock_seguridad_mfc.security_stock IS 'Cantidad calculada de stock de seguridad enviada a Transaction Manager';
COMMENT ON COLUMN ecommdata.stock_seguridad_mfc.fecha IS 'Fecha de ejecución del cálculo/envío';
COMMENT ON COLUMN ecommdata.stock_seguridad_mfc.horario IS 'Jornada de ejecución: AM o PM';
COMMENT ON COLUMN ecommdata.stock_seguridad_mfc.created_at IS 'Marca temporal de inserción del registro en base de datos';


-- ============================================================================
-- QUERY DE LIMPIEZA AUTOMÁTICA / MANUAL (RETENCIÓN 5 DÍAS)
-- (Esta lógica se ejecuta automáticamente en cada corrida del DAG)
-- ============================================================================
-- DELETE FROM ecommdata.stock_seguridad_mfc 
-- WHERE fecha < CURRENT_DATE - INTERVAL '5 days';


-- ============================================================================
-- EJEMPLO DE CONSULTA PARA METABASE
-- ============================================================================
-- SELECT 
--     fecha,
--     horario,
--     COUNT(1) AS total_skus,
--     COUNT(CASE WHEN security_stock = 0 THEN 1 END) AS skus_stock_cero,
--     COUNT(CASE WHEN security_stock > 0 THEN 1 END) AS skus_con_stock_seguridad,
--     AVG(security_stock) AS promedio_stock_seguridad,
--     MAX(security_stock) AS max_stock_seguridad
-- FROM ecommdata.stock_seguridad_mfc
-- WHERE fecha >= CURRENT_DATE - INTERVAL '5 days'
-- GROUP BY fecha, horario
-- ORDER BY fecha DESC, horario DESC;
