"""Loader: CSV de CompraNet → tablas contratos + proveedores."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from rich.console import Console

from src.db.connection import get_connection

console = Console()

# Mapeo columnas CSV → columnas DB (contratos)
COLUMN_MAP = {
    "Código del expediente": "codigo_expediente",
    "Referencia del expediente": "referencia_expediente",
    "Título del expediente": "titulo_expediente",
    "Orden de gobierno": "orden_gobierno",
    "Clave Ramo": "clave_ramo",
    "Descripción Ramo": "desc_ramo",
    "Tipo de Institución": "tipo_institucion",
    "Clave Institución": "clave_institucion",
    "Siglas de la Institución": "siglas_institucion",
    "Institución": "institucion",
    "Clave de la UC": "clave_uc",
    "Nombre de la UC": "nombre_uc",
    "Tipo Procedimiento": "tipo_procedimiento",
    "Tipo de contratación": "tipo_contratacion",
    "Carácter del procedimiento": "caracter_procedimiento",
    "Forma de participación": "forma_participacion",
    "Número de procedimiento": "numero_procedimiento",
    "Partida específica": "partida_especifica",
    "Clave del programa federal": "clave_programa_federal",
    "Clave de cartera SHCP": "clave_cartera_shcp",
    "Fecha de publicación": "fecha_publicacion",
    "Fecha de apertura": "fecha_apertura",
    "Fecha de fallo": "fecha_fallo",
    "Código del contrato": "codigo_contrato",
    "Núm. del contrato": "numero_contrato",
    "Título del contrato": "titulo_contrato",
    "Descripción del contrato": "descripcion_contrato",
    "Estatus DRC": "estatus_contrato",
    "Fecha de inicio del contrato": "fecha_inicio_contrato",
    "Fecha de fin del contrato": "fecha_fin_contrato",
    "Fecha de firma del contrato": "fecha_firma_contrato",
    "Contrato plurianual": "contrato_plurianual",
    "Importe DRC": "importe_drc",
    "Monto sin imp./mínimo": "monto_min_sin_imp",
    "Monto mínimo con imp.": "monto_min_con_imp",
    "Monto sin imp./máximo": "monto_max_sin_imp",
    "Monto máximo con imp.": "monto_max_con_imp",
    "Moneda": "moneda",
    "rfc": "proveedor_rfc",
    "Proveedor o contratista": "proveedor_nombre",
    "País de la empresa": "proveedor_pais",
    "Nacionalidad proveedor o contratista": "proveedor_nacionalidad",
    "Estratificación": "estratificacion",
}

CONTRATO_COLS = [
    "codigo_expediente", "referencia_expediente", "titulo_expediente",
    "orden_gobierno", "clave_ramo", "desc_ramo", "tipo_institucion",
    "clave_institucion", "siglas_institucion", "institucion",
    "clave_uc", "nombre_uc",
    "tipo_procedimiento", "tipo_contratacion", "caracter_procedimiento",
    "forma_participacion", "numero_procedimiento", "partida_especifica",
    "clave_programa_federal", "clave_cartera_shcp",
    "fecha_publicacion", "fecha_apertura", "fecha_fallo",
    "codigo_contrato", "numero_contrato", "titulo_contrato",
    "descripcion_contrato", "estatus_contrato",
    "fecha_inicio_contrato", "fecha_fin_contrato", "fecha_firma_contrato",
    "contrato_plurianual",
    "importe_drc", "monto_min_sin_imp", "monto_min_con_imp",
    "monto_max_sin_imp", "monto_max_con_imp", "moneda",
    "proveedor_rfc", "proveedor_nombre", "proveedor_pais",
    "proveedor_nacionalidad", "estratificacion",
    "año", "archivo_origen",
]


def _extract_year(filename: str) -> int:
    """Extrae año del nombre: Contratos_CompraNet2025.csv → 2025."""
    import re
    m = re.search(r"(\d{4})", filename)
    return int(m.group(1)) if m else 0


def load_compranet(csv_path: Path, batch_size: int = 5000) -> int:
    """Carga CSV de CompraNet a la tabla contratos."""
    filename = csv_path.name
    año = _extract_year(filename)
    console.print(f"\n[bold]Cargando {filename}[/bold] (año={año})")

    df = pd.read_csv(csv_path, encoding="latin-1", low_memory=False, dtype=str)
    console.print(f"  Filas en CSV: {len(df):,}")

    # Renombrar
    rename = {k: v for k, v in COLUMN_MAP.items() if k in df.columns}
    df = df.rename(columns=rename)
    df["año"] = año
    df["archivo_origen"] = filename

    # Parsear montos
    for col in ["importe_drc", "monto_min_sin_imp", "monto_min_con_imp",
                 "monto_max_sin_imp", "monto_max_con_imp"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Insertar contratos
    cols = [c for c in CONTRATO_COLS if c in df.columns]
    df_insert = df[cols].copy()

    # Reemplazar NaN con None para SQLite
    df_insert = df_insert.where(df_insert.notna(), None)

    conn = get_connection()
    placeholders = ", ".join(["?"] * len(cols))
    col_names = ", ".join(cols)
    sql = f"INSERT INTO contratos ({col_names}) VALUES ({placeholders})"

    total = 0
    for start in range(0, len(df_insert), batch_size):
        batch = df_insert.iloc[start : start + batch_size]
        rows = [tuple(r) for r in batch.itertuples(index=False, name=None)]
        conn.executemany(sql, rows)
        conn.commit()
        total += len(rows)
        if total % 25000 == 0 or total == len(df_insert):
            console.print(f"  Contratos insertados: {total:,}/{len(df_insert):,}")

    # Agregar proveedores
    console.print("  Agregando proveedores ...")
    conn.execute("""
        INSERT OR IGNORE INTO proveedores (rfc, nombre, pais, nacionalidad, estratificacion)
        SELECT DISTINCT proveedor_rfc, proveedor_nombre, proveedor_pais,
               proveedor_nacionalidad, estratificacion
        FROM contratos
        WHERE proveedor_rfc IS NOT NULL AND proveedor_rfc != ''
    """)

    # Actualizar estadísticas de proveedores
    conn.execute("""
        UPDATE proveedores SET
            total_contratos = (
                SELECT COUNT(*) FROM contratos WHERE contratos.proveedor_rfc = proveedores.rfc
            ),
            monto_total = (
                SELECT COALESCE(SUM(importe_drc), 0) FROM contratos
                WHERE contratos.proveedor_rfc = proveedores.rfc
            ),
            contratos_directos = (
                SELECT COUNT(*) FROM contratos
                WHERE contratos.proveedor_rfc = proveedores.rfc
                AND contratos.tipo_procedimiento LIKE '%ADJUDICACIÓN DIRECTA%'
            ),
            contratos_licitacion = (
                SELECT COUNT(*) FROM contratos
                WHERE contratos.proveedor_rfc = proveedores.rfc
                AND contratos.tipo_procedimiento LIKE '%LICITACIÓN%'
            ),
            dependencias_distintas = (
                SELECT COUNT(DISTINCT institucion) FROM contratos
                WHERE contratos.proveedor_rfc = proveedores.rfc
            ),
            primer_contrato = (
                SELECT MIN(fecha_publicacion) FROM contratos
                WHERE contratos.proveedor_rfc = proveedores.rfc
            ),
            ultimo_contrato = (
                SELECT MAX(fecha_publicacion) FROM contratos
                WHERE contratos.proveedor_rfc = proveedores.rfc
            ),
            updated_at = datetime('now')
    """)
    conn.commit()

    n_prov = conn.execute("SELECT COUNT(*) FROM proveedores").fetchone()[0]
    console.print(f"  [green]OK[/green]: {total:,} contratos, {n_prov:,} proveedores")
    conn.close()
    return total


def load_all(data_dir: Path) -> int:
    """Carga todos los CSVs de CompraNet."""
    csv_files = sorted(data_dir.glob("Contratos_CompraNet*.csv"))
    if not csv_files:
        console.print("[yellow]No se encontraron CSVs de CompraNet[/yellow]")
        return 0

    total = 0
    for f in csv_files:
        total += load_compranet(f)

    console.print(f"\n[bold green]Total CompraNet: {total:,} contratos[/bold green]")
    return total
