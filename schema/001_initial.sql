-- Schema unificado para gasto-publico-mx
-- Diseñado para rastrear el flujo: PEF → Ejecución → Contratos → Proveedores

BEGIN;

-- ============================================================
-- 1. PRESUPUESTO FEDERAL (Transparencia Presupuestaria)
-- ============================================================

CREATE TABLE IF NOT EXISTS presupuesto_federal (
    id                  BIGSERIAL PRIMARY KEY,
    año                 SMALLINT NOT NULL,
    tipo                TEXT NOT NULL,  -- 'aprobado', 'ejercido', 'proyecto'
    ciclo               TEXT,           -- trimestre si aplica: 'Q1', 'Q2', etc.

    -- Clasificación administrativa
    ramo                TEXT,
    ramo_desc           TEXT,
    unidad_responsable  TEXT,
    unidad_resp_desc    TEXT,

    -- Clasificación funcional
    finalidad           TEXT,
    funcion             TEXT,
    subfuncion          TEXT,
    actividad_inst      TEXT,

    -- Clasificación programática
    programa_presup     TEXT,
    programa_desc       TEXT,

    -- Clasificación económica
    tipo_gasto          TEXT,
    objeto_gasto        TEXT,

    -- Geográfico
    entidad_federativa  TEXT,

    -- Montos (pesos MXN)
    monto_aprobado      NUMERIC(18, 2),
    monto_ejercido      NUMERIC(18, 2),
    monto_pagado        NUMERIC(18, 2),

    -- Metadata
    fuente              TEXT DEFAULT 'transparencia_presupuestaria',
    archivo_origen      TEXT,
    created_at          TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pf_año ON presupuesto_federal(año);
CREATE INDEX IF NOT EXISTS idx_pf_ramo ON presupuesto_federal(ramo);
CREATE INDEX IF NOT EXISTS idx_pf_entidad ON presupuesto_federal(entidad_federativa);
CREATE INDEX IF NOT EXISTS idx_pf_programa ON presupuesto_federal(programa_presup);

-- ============================================================
-- 2. CONTRATOS (CompraNet)
-- ============================================================

CREATE TABLE IF NOT EXISTS contratos (
    id                      BIGSERIAL PRIMARY KEY,
    numero_procedimiento    TEXT,
    tipo_procedimiento      TEXT,   -- licitación pública, invitación, adjudicación directa
    tipo_contratacion       TEXT,   -- obra, adquisiciones, servicios, arrendamientos

    -- Comprador
    unidad_compradora       TEXT,
    unidad_compradora_desc  TEXT,
    dependencia             TEXT,

    -- Proveedor
    proveedor_rfc           TEXT,
    proveedor_nombre        TEXT,
    proveedor_estado        TEXT,

    -- Contrato
    numero_contrato         TEXT,
    titulo_contrato         TEXT,
    fecha_inicio            DATE,
    fecha_fin               DATE,
    fecha_celebracion       DATE,

    -- Montos
    monto_contrato          NUMERIC(18, 2),
    monto_total             NUMERIC(18, 2),
    moneda                  TEXT DEFAULT 'MXN',

    -- Estatus
    estatus_contrato        TEXT,

    -- Metadata
    año                     SMALLINT,
    fuente                  TEXT DEFAULT 'compranet',
    archivo_origen          TEXT,
    created_at              TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_cont_año ON contratos(año);
CREATE INDEX IF NOT EXISTS idx_cont_proveedor ON contratos(proveedor_rfc);
CREATE INDEX IF NOT EXISTS idx_cont_dependencia ON contratos(dependencia);
CREATE INDEX IF NOT EXISTS idx_cont_tipo ON contratos(tipo_procedimiento);
CREATE INDEX IF NOT EXISTS idx_cont_monto ON contratos(monto_contrato);

-- ============================================================
-- 3. PRESUPUESTO CDMX
-- ============================================================

CREATE TABLE IF NOT EXISTS presupuesto_cdmx (
    id                  BIGSERIAL PRIMARY KEY,
    año                 SMALLINT NOT NULL,
    tipo                TEXT NOT NULL,  -- 'aprobado', 'ejercido'

    -- Clasificación administrativa
    dependencia         TEXT,
    dependencia_desc    TEXT,
    unidad_resp         TEXT,
    unidad_resp_desc    TEXT,

    -- Clasificación funcional
    finalidad           TEXT,
    funcion             TEXT,
    subfuncion          TEXT,

    -- Clasificación económica
    capitulo            TEXT,
    concepto            TEXT,
    partida_generica    TEXT,
    partida_especifica  TEXT,

    -- Montos
    monto_aprobado      NUMERIC(18, 2),
    monto_ejercido      NUMERIC(18, 2),

    -- Metadata
    fuente              TEXT DEFAULT 'datos_cdmx',
    archivo_origen      TEXT,
    created_at          TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pcdmx_año ON presupuesto_cdmx(año);
CREATE INDEX IF NOT EXISTS idx_pcdmx_dep ON presupuesto_cdmx(dependencia);

-- ============================================================
-- 4. PROVEEDORES (extraídos de CompraNet)
-- ============================================================

CREATE TABLE IF NOT EXISTS proveedores (
    id                  SERIAL PRIMARY KEY,
    rfc                 TEXT UNIQUE NOT NULL,
    nombre              TEXT,
    estado              TEXT,

    -- Estadísticas agregadas
    total_contratos     INT DEFAULT 0,
    monto_total         NUMERIC(18, 2) DEFAULT 0,
    primer_contrato     DATE,
    ultimo_contrato     DATE,

    -- Metadata
    updated_at          TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_prov_nombre ON proveedores USING gin(to_tsvector('spanish', nombre));
CREATE INDEX IF NOT EXISTS idx_prov_monto ON proveedores(monto_total DESC);

-- ============================================================
-- 5. FLUJO DE DINERO (tabla de linkeo)
-- ============================================================

CREATE TABLE IF NOT EXISTS flujo_gasto (
    id                  BIGSERIAL PRIMARY KEY,

    -- Origen (PEF)
    pf_ramo             TEXT,
    pf_programa         TEXT,
    pf_año              SMALLINT,

    -- Destino (contrato)
    contrato_id         BIGINT REFERENCES contratos(id),

    -- Montos
    monto_asignado      NUMERIC(18, 2),
    monto_contratado    NUMERIC(18, 2),

    -- Calidad del linkeo
    confianza           REAL DEFAULT 0.0,  -- 0-1, qué tan seguro es el match
    metodo_linkeo       TEXT,  -- 'exacto', 'fuzzy', 'manual'

    created_at          TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_flujo_año ON flujo_gasto(pf_año);
CREATE INDEX IF NOT EXISTS idx_flujo_ramo ON flujo_gasto(pf_ramo);

-- ============================================================
-- 6. CRAWL LOG (registro de descargas)
-- ============================================================

CREATE TABLE IF NOT EXISTS crawl_log (
    id          SERIAL PRIMARY KEY,
    fuente      TEXT NOT NULL,
    archivo     TEXT NOT NULL,
    url         TEXT,
    tamaño_mb   REAL,
    filas       INT,
    sha256      TEXT,
    started_at  TIMESTAMPTZ DEFAULT now(),
    finished_at TIMESTAMPTZ,
    status      TEXT DEFAULT 'running'  -- running, ok, error
);

COMMIT;
