"""Loader: CSV de Transparencia Presupuestaria → tabla presupuesto_federal."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from rich.console import Console

from src.db.connection import get_connection

console = Console()

# Mapeo columnas CSV → columnas DB
COLUMN_MAP = {
    "CICLO": "año",
    "ID_RAMO": "ramo",
    "DESC_RAMO": "ramo_desc",
    "ID_UR": "unidad_responsable",
    "DESC_UR": "unidad_resp_desc",
    "GPO_FUNCIONAL": "gpo_funcional",
    "DESC_GPO_FUNCIONAL": "gpo_funcional_desc",
    "ID_FUNCION": "funcion",
    "DESC_FUNCION": "funcion_desc",
    "ID_SUBFUNCION": "subfuncion",
    "DESC_SUBFUNCION": "subfuncion_desc",
    "ID_AI": "actividad_inst",
    "DESC_AI": "actividad_inst_desc",
    "ID_MODALIDAD": "modalidad",
    "DESC_MODALIDAD": "modalidad_desc",
    "ID_PP": "programa_presup",
    "DESC_PP": "programa_desc",
    "ID_CAPITULO": "capitulo",
    "DESC_CAPITULO": "capitulo_desc",
    "ID_CONCEPTO": "concepto",
    "DESC_CONCEPTO": "concepto_desc",
    "ID_PARTIDA_GENERICA": "partida_generica",
    "DESC_PARTIDA_GENERICA": "partida_gen_desc",
    "ID_PARTIDA_ESPECIFICA": "partida_especifica",
    "DESC_PARTIDA_ESPECIFICA": "partida_esp_desc",
    "ID_TIPOGASTO": "tipo_gasto",
    "DESC_TIPOGASTO": "tipo_gasto_desc",
    "ID_FF": "fuente_fin",
    "DESC_FF": "fuente_fin_desc",
    "ID_ENTIDAD_FEDERATIVA": "entidad_federativa",
    "DESC_ENTIDAD_FEDERATIVA": "entidad_federativa_desc",
    "ID_CLAVE_CARTERA": "clave_cartera",
}


def _detect_tipo(filename: str) -> str:
    """Detecta tipo de dataset por nombre de archivo."""
    name = filename.upper()
    if name.startswith("PPEF"):
        return "proyecto"
    elif name.startswith("PEF"):
        return "aprobado"
    elif "CUENTA" in name:
        return "ejercido"
    return "aprobado"


def _parse_monto(series: pd.Series) -> pd.Series:
    """Limpia montos con comas y guiones."""
    return (
        series.astype(str)
        .str.replace(",", "", regex=False)
        .str.strip()
        .replace({" -   ": "0", "-": "0", "": "0", "nan": "0"})
        .pipe(pd.to_numeric, errors="coerce")
        .fillna(0)
    )


def load_pef(csv_path: Path, batch_size: int = 5000) -> int:
    """Carga un CSV de PEF a la base de datos.

    Returns: número de filas insertadas.
    """
    filename = csv_path.name
    tipo = _detect_tipo(filename)

    console.print(f"\n[bold]Cargando {filename}[/bold] (tipo={tipo})")

    # Leer CSV
    df = pd.read_csv(csv_path, encoding="latin-1", low_memory=False, dtype=str)
    console.print(f"  Filas en CSV: {len(df):,}")

    # Encontrar columna de monto (varía por año: MONTO_PEF_2025, MONTO_PPEF_2026, etc.)
    monto_cols = [c for c in df.columns if "MONTO" in c.upper()]
    if not monto_cols:
        console.print("[red]No se encontró columna de monto[/red]")
        return 0

    monto_col = monto_cols[0]
    console.print(f"  Columna de monto: {monto_col}")

    # Renombrar columnas
    rename = {k: v for k, v in COLUMN_MAP.items() if k in df.columns}
    df = df.rename(columns=rename)
    df["monto"] = _parse_monto(df[monto_col])
    df["tipo"] = tipo
    df["archivo_origen"] = filename

    # Columnas destino
    db_cols = [
        "año", "tipo", "ramo", "ramo_desc", "unidad_responsable", "unidad_resp_desc",
        "gpo_funcional", "gpo_funcional_desc", "funcion", "funcion_desc",
        "subfuncion", "subfuncion_desc", "actividad_inst", "actividad_inst_desc",
        "modalidad", "modalidad_desc", "programa_presup", "programa_desc",
        "capitulo", "capitulo_desc", "concepto", "concepto_desc",
        "partida_generica", "partida_gen_desc", "partida_especifica", "partida_esp_desc",
        "tipo_gasto", "tipo_gasto_desc", "fuente_fin", "fuente_fin_desc",
        "entidad_federativa", "entidad_federativa_desc", "clave_cartera",
        "monto", "archivo_origen",
    ]

    # Solo columnas que existen
    db_cols = [c for c in db_cols if c in df.columns]
    df_insert = df[db_cols].copy()

    # Insertar por lotes
    conn = get_connection()
    placeholders = ", ".join(["?"] * len(db_cols))
    col_names = ", ".join(db_cols)
    sql = f"INSERT INTO presupuesto_federal ({col_names}) VALUES ({placeholders})"

    total = 0
    for start in range(0, len(df_insert), batch_size):
        batch = df_insert.iloc[start : start + batch_size]
        rows = [tuple(r) for r in batch.itertuples(index=False, name=None)]
        conn.executemany(sql, rows)
        conn.commit()
        total += len(rows)
        if total % 50000 == 0 or total == len(df_insert):
            console.print(f"  Insertadas: {total:,}/{len(df_insert):,}")

    conn.close()
    console.print(f"  [green]OK[/green]: {total:,} filas cargadas")
    return total


def load_all(data_dir: Path) -> int:
    """Carga todos los CSVs de PEF encontrados."""
    csv_files = sorted(data_dir.glob("PEF_*.csv")) + sorted(data_dir.glob("PPEF_*.csv"))
    if not csv_files:
        console.print("[yellow]No se encontraron CSVs de PEF[/yellow]")
        return 0

    total = 0
    for f in csv_files:
        total += load_pef(f)

    console.print(f"\n[bold green]Total PEF: {total:,} filas[/bold green]")
    return total
