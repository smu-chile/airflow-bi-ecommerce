-- ============================================================================
-- SCRIPT DDL: Tabla Publicación Semanal por Tienda y Grupo SAP
-- Esquema: catalogo
-- Tabla: catalogo.publicacion_semana_tienda_grupo_sap
-- Objetivo: Resumen semanal de disponibilidad web (In-Web) y conteo de SKUs
--           (totales, en surtido e-commerce y fuera de surtido) por tienda
--           y jerarquía comercial SAP hasta nivel Grupo SAP.
-- Carga: DAG etl_publicacion_semana_tienda_grupo_sap
-- ============================================================================

CREATE TABLE IF NOT EXISTS catalogo.publicacion_semana_tienda_grupo_sap (
    semana_ano_texto                VARCHAR(15)     NOT NULL,
    id_tienda                       VARCHAR(10)     NOT NULL,
    negocio_sap                     VARCHAR(150)    NULL,
    seccion_sap                     VARCHAR(150)    NULL,
    linea_sap                       VARCHAR(150)    NULL,
    categoria_sap                   VARCHAR(150)    NULL,
    grupo_id                        VARCHAR(50)     NOT NULL,
    grupo_sap                       VARCHAR(150)    NULL,
    inweb                           NUMERIC(11, 10) NULL,
    veces_disponible_web            BIGINT          NOT NULL DEFAULT 0,
    veces_medido                    BIGINT          NOT NULL DEFAULT 0,
    productos_total                 INTEGER         NOT NULL DEFAULT 0,
    productos_surtido_ecommerce     INTEGER         NOT NULL DEFAULT 0,
    productos_no_surtido_ecommerce  INTEGER         NOT NULL DEFAULT 0,
    fecha_actualizacion             TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_publicacion_semana_tienda_grupo_sap
        PRIMARY KEY (semana_ano_texto, id_tienda, grupo_id)
);

-- Filtros directos por tienda o grupo SAP
CREATE INDEX IF NOT EXISTS idx_pub_sem_tda_grp_sap_tienda
    ON catalogo.publicacion_semana_tienda_grupo_sap (id_tienda);

CREATE INDEX IF NOT EXISTS idx_pub_sem_tda_grp_sap_grupo
    ON catalogo.publicacion_semana_tienda_grupo_sap (grupo_id);

-- Filtros y drill-down de jerarquía SAP en Power BI
CREATE INDEX IF NOT EXISTS idx_pub_sem_tda_grp_sap_jerarquia
    ON catalogo.publicacion_semana_tienda_grupo_sap (negocio_sap, seccion_sap, linea_sap, categoria_sap);

CREATE INDEX IF NOT EXISTS idx_pub_sem_tda_grp_sap_linea
    ON catalogo.publicacion_semana_tienda_grupo_sap (linea_sap);

CREATE INDEX IF NOT EXISTS idx_pub_sem_tda_grp_sap_categoria
    ON catalogo.publicacion_semana_tienda_grupo_sap (categoria_sap);

COMMENT ON TABLE catalogo.publicacion_semana_tienda_grupo_sap IS 'Resumen semanal de disponibilidad web (In-Web) y conteo de SKUs por tienda y Grupo SAP, excluyendo productos bloqueados en Lista 8.';
COMMENT ON COLUMN catalogo.publicacion_semana_tienda_grupo_sap.inweb IS 'Promedio de disponibilidad web sobre el surtido activo (veces_disponible_web / veces_medido). NULL si veces_medido = 0.';
COMMENT ON COLUMN catalogo.publicacion_semana_tienda_grupo_sap.veces_disponible_web IS 'Numerador In-Web: cantidad de fotos en la semana donde el surtido e-commerce estuvo disponible para compra.';
COMMENT ON COLUMN catalogo.publicacion_semana_tienda_grupo_sap.veces_medido IS 'Denominador In-Web: total de fotos evaluadas en la semana para productos con surtido_ecommerce = TRUE.';

-- Permisos de lectura para Metabase
GRANT USAGE ON SCHEMA catalogo TO service_metabase;
GRANT SELECT ON TABLE catalogo.publicacion_semana_tienda_grupo_sap TO service_metabase;


