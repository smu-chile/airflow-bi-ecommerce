-- ============================================================================
-- SCRIPT DDL: Tabla InStock Unimarc
-- Esquema: ecommdata
-- Tabla: ecommdata.instock
-- Objetivo: Foto diaria de InStock por tienda/material proveniente de SAP
--           (datastage/instock_unimarc/YYYY/MM/DD/{tienda}/instock_{tienda}_YYYYMMDD.csv)
-- Carga: DAG etl_instock_unimarc_truncate_and_load (truncate & load diario 08:00)
-- ============================================================================

CREATE TABLE IF NOT EXISTS ecommdata.instock (
    fecha             DATE        NOT NULL,
    tienda            VARCHAR(4)  NOT NULL,
    material          VARCHAR(18) NOT NULL,
    catalogado        BOOLEAN,
    instock           SMALLINT,              -- Se conserva formato 1/0
    clase_abc         VARCHAR(2),            -- Conserva valor original (A, B, C, N o #)
    car_plan          VARCHAR(4),
    estado_operativo  BOOLEAN,
    fecha_carga       TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    CONSTRAINT pk_instock PRIMARY KEY (tienda, material)
);

CREATE INDEX IF NOT EXISTS idx_instock_material ON ecommdata.instock (material);

COMMENT ON TABLE  ecommdata.instock IS 'Foto diaria (último día) de InStock Unimarc por tienda y material. Fuente: S3 datastage/instock_unimarc.';
COMMENT ON COLUMN ecommdata.instock.fecha IS 'Columna Fecha del archivo origen';
COMMENT ON COLUMN ecommdata.instock.tienda IS 'Columna Centro del archivo origen (4 dígitos)';
COMMENT ON COLUMN ecommdata.instock.material IS 'Columna PLU_SAP60 estandarizada a 18 dígitos (sin UMV)';
COMMENT ON COLUMN ecommdata.instock.instock IS 'Indicador InStock 1/0 (formato original)';
COMMENT ON COLUMN ecommdata.instock.clase_abc IS 'Clase ABC original (A, B, C, N o #)';
COMMENT ON COLUMN ecommdata.instock.car_plan IS 'Característica de planificación (CarPLan)';
