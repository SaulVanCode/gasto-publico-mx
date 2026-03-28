-- Schema unificado para gasto-publico-mx (SQLite)
-- Rastreo: PEF → Ejecución → Contratos → Proveedores

-- ============================================================
-- 1. PRESUPUESTO FEDERAL (Transparencia Presupuestaria)
-- ============================================================

CREATE TABLE IF NOT EXISTS presupuesto_federal (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    año                 INTEGER NOT NULL,
    tipo                TEXT NOT NULL,  -- 'aprobado', 'ejercido', 'proyecto'

    -- Clasificación administrativa
    ramo                TEXT,
    ramo_desc           TEXT,
    unidad_responsable  TEXT,
    unidad_resp_desc    TEXT,

    -- Clasificación funcional
    gpo_funcional       TEXT,
    gpo_funcional_desc  TEXT,
    funcion             TEXT,
    funcion_desc        TEXT,
    subfuncion          TEXT,
    subfuncion_desc     TEXT,
    actividad_inst      TEXT,
    actividad_inst_desc TEXT,

    -- Clasificación programática
    modalidad           TEXT,
    modalidad_desc      TEXT,
    programa_presup     TEXT,
    programa_desc       TEXT,

    -- Clasificación económica
    capitulo            TEXT,
    capitulo_desc       TEXT,
    concepto            TEXT,
    concepto_desc       TEXT,
    partida_generica    TEXT,
    partida_gen_desc    TEXT,
    partida_especifica  TEXT,
    partida_esp_desc    TEXT,

    -- Tipo de gasto
    tipo_gasto          TEXT,
    tipo_gasto_desc     TEXT,

    -- Fuente de financiamiento
    fuente_fin          TEXT,
    fuente_fin_desc     TEXT,

    -- Geográfico
    entidad_federativa      TEXT,
    entidad_federativa_desc TEXT,

    -- Clave cartera
    clave_cartera       TEXT,

    -- Montos (pesos MXN)
    monto               REAL NOT NULL DEFAULT 0,

    -- Metadata
    fuente              TEXT DEFAULT 'transparencia_presupuestaria',
    archivo_origen      TEXT,
    created_at          TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_pf_año ON presupuesto_federal(año);
CREATE INDEX IF NOT EXISTS idx_pf_ramo ON presupuesto_federal(ramo);
CREATE INDEX IF NOT EXISTS idx_pf_entidad ON presupuesto_federal(entidad_federativa);
CREATE INDEX IF NOT EXISTS idx_pf_programa ON presupuesto_federal(programa_presup);
CREATE INDEX IF NOT EXISTS idx_pf_tipo ON presupuesto_federal(tipo);

-- ============================================================
-- 2. CONTRATOS (CompraNet)
-- ============================================================

CREATE TABLE IF NOT EXISTS contratos (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    codigo_expediente       TEXT,
    referencia_expediente   TEXT,
    titulo_expediente       TEXT,

    -- Institución compradora
    orden_gobierno          TEXT,
    clave_ramo              TEXT,
    desc_ramo               TEXT,
    tipo_institucion        TEXT,
    clave_institucion       TEXT,
    siglas_institucion      TEXT,
    institucion             TEXT,
    clave_uc                TEXT,
    nombre_uc               TEXT,

    -- Procedimiento
    tipo_procedimiento      TEXT,
    tipo_contratacion       TEXT,
    caracter_procedimiento  TEXT,
    forma_participacion     TEXT,
    numero_procedimiento    TEXT,
    partida_especifica      TEXT,

    -- Programa federal
    clave_programa_federal  TEXT,
    clave_cartera_shcp      TEXT,

    -- Fechas
    fecha_publicacion       TEXT,
    fecha_apertura          TEXT,
    fecha_fallo             TEXT,

    -- Contrato
    codigo_contrato         TEXT,
    numero_contrato         TEXT,
    titulo_contrato         TEXT,
    descripcion_contrato    TEXT,
    estatus_contrato        TEXT,
    fecha_inicio_contrato   TEXT,
    fecha_fin_contrato      TEXT,
    fecha_firma_contrato    TEXT,
    contrato_plurianual     TEXT,

    -- Montos
    importe_drc             REAL,
    monto_min_sin_imp       REAL,
    monto_min_con_imp       REAL,
    monto_max_sin_imp       REAL,
    monto_max_con_imp       REAL,
    moneda                  TEXT DEFAULT 'MXN',

    -- Proveedor
    proveedor_rfc           TEXT,
    proveedor_nombre        TEXT,
    proveedor_pais          TEXT,
    proveedor_nacionalidad  TEXT,
    estratificacion         TEXT,

    -- Metadata
    año                     INTEGER,
    fuente                  TEXT DEFAULT 'compranet',
    archivo_origen          TEXT,
    created_at              TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_cont_año ON contratos(año);
CREATE INDEX IF NOT EXISTS idx_cont_rfc ON contratos(proveedor_rfc);
CREATE INDEX IF NOT EXISTS idx_cont_ramo ON contratos(clave_ramo);
CREATE INDEX IF NOT EXISTS idx_cont_tipo_proc ON contratos(tipo_procedimiento);
CREATE INDEX IF NOT EXISTS idx_cont_importe ON contratos(importe_drc);
CREATE INDEX IF NOT EXISTS idx_cont_institucion ON contratos(institucion);
CREATE INDEX IF NOT EXISTS idx_cont_uc ON contratos(clave_uc);

-- ============================================================
-- 3. PRESUPUESTO CDMX
-- ============================================================

CREATE TABLE IF NOT EXISTS presupuesto_cdmx (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    clave_presupuestaria TEXT,
    año                 INTEGER NOT NULL,
    periodo             TEXT,

    -- Clasificación administrativa
    gobierno_general    TEXT,
    gobierno_desc       TEXT,
    sector              TEXT,
    sector_desc         TEXT,
    subsector           TEXT,
    subsector_desc      TEXT,
    unidad_responsable  TEXT,
    unidad_resp_desc    TEXT,

    -- Clasificación funcional
    finalidad           TEXT,
    finalidad_desc      TEXT,
    funcion             TEXT,
    funcion_desc        TEXT,
    subfuncion          TEXT,
    subfuncion_desc     TEXT,
    area_funcional      TEXT,
    area_funcional_desc TEXT,

    -- Clasificación programática
    modalidad           TEXT,
    modalidad_desc      TEXT,
    programa_presup     TEXT,
    programa_desc       TEXT,

    -- Clasificación económica
    capitulo            TEXT,
    capitulo_desc       TEXT,
    concepto            TEXT,
    concepto_desc       TEXT,
    partida_generica    TEXT,
    partida_gen_desc    TEXT,
    partida_especifica  TEXT,
    partida_esp_desc    TEXT,

    -- Tipo de gasto
    tipo_gasto          TEXT,
    tipo_gasto_desc     TEXT,
    gasto_programable   TEXT,
    gasto_prog_desc     TEXT,

    -- Montos
    monto_aprobado      REAL DEFAULT 0,
    monto_modificado    REAL DEFAULT 0,
    monto_ejercido      REAL DEFAULT 0,

    -- Metadata
    fuente              TEXT DEFAULT 'datos_cdmx',
    archivo_origen      TEXT,
    created_at          TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_pcdmx_año ON presupuesto_cdmx(año);
CREATE INDEX IF NOT EXISTS idx_pcdmx_ur ON presupuesto_cdmx(unidad_responsable);
CREATE INDEX IF NOT EXISTS idx_pcdmx_prog ON presupuesto_cdmx(programa_presup);

-- ============================================================
-- 4. PROVEEDORES (agregado de CompraNet)
-- ============================================================

CREATE TABLE IF NOT EXISTS proveedores (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    rfc                 TEXT UNIQUE NOT NULL,
    nombre              TEXT,
    pais                TEXT,
    nacionalidad        TEXT,
    estratificacion     TEXT,

    -- Estadísticas agregadas
    total_contratos     INTEGER DEFAULT 0,
    monto_total         REAL DEFAULT 0,
    contratos_directos  INTEGER DEFAULT 0,
    contratos_licitacion INTEGER DEFAULT 0,
    dependencias_distintas INTEGER DEFAULT 0,
    primer_contrato     TEXT,
    ultimo_contrato     TEXT,

    updated_at          TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_prov_monto ON proveedores(monto_total DESC);
CREATE INDEX IF NOT EXISTS idx_prov_nombre ON proveedores(nombre);

-- ============================================================
-- 5. FLUJO DE DINERO (linkeo PEF → Contratos)
-- ============================================================

CREATE TABLE IF NOT EXISTS flujo_gasto (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    pf_ramo             TEXT,
    pf_programa         TEXT,
    pf_año              INTEGER,
    contrato_id         INTEGER REFERENCES contratos(id),
    monto_asignado      REAL,
    monto_contratado    REAL,
    confianza           REAL DEFAULT 0.0,
    metodo_linkeo       TEXT,
    created_at          TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_flujo_año ON flujo_gasto(pf_año);
CREATE INDEX IF NOT EXISTS idx_flujo_ramo ON flujo_gasto(pf_ramo);

-- ============================================================
-- 6. ANOMALÍAS DETECTADAS
-- ============================================================

CREATE TABLE IF NOT EXISTS anomalias (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    tipo            TEXT NOT NULL,
    -- tipos: 'fragmentacion', 'proveedor_repetido', 'adjudicacion_sospechosa',
    --        'monto_atipico', 'proveedor_fantasma', 'concentracion'
    severidad       TEXT NOT NULL,  -- 'baja', 'media', 'alta', 'critica'
    titulo          TEXT NOT NULL,
    descripcion     TEXT,
    entidad         TEXT,          -- ramo, dependencia, o UC involucrada
    proveedor_rfc   TEXT,
    monto_involucrado REAL,
    contratos_ids   TEXT,          -- JSON array de IDs
    evidencia       TEXT,          -- JSON con datos de soporte
    created_at      TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_anom_tipo ON anomalias(tipo);
CREATE INDEX IF NOT EXISTS idx_anom_sev ON anomalias(severidad);
CREATE INDEX IF NOT EXISTS idx_anom_rfc ON anomalias(proveedor_rfc);

-- ============================================================
-- 7. CRAWL LOG
-- ============================================================

CREATE TABLE IF NOT EXISTS crawl_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    fuente      TEXT NOT NULL,
    archivo     TEXT NOT NULL,
    url         TEXT,
    tamaño_mb   REAL,
    filas       INTEGER,
    sha256      TEXT,
    started_at  TEXT DEFAULT (datetime('now')),
    finished_at TEXT,
    status      TEXT DEFAULT 'running'
);
