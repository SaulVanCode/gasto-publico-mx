"""Loader: CSV de datos.cdmx.gob.mx → tabla presupuesto_cdmx."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from rich.console import Console

from src.db.connection import get_connection

console = Console()

COLUMN_MAP = {
    "clave_presupuestaria": "clave_presupuestaria",
    "ciclo": "año",
    "periodo": "periodo",
    "gobierno_general": "gobierno_general",
    "desc_gobierno_general": "gobierno_desc",
    "sector": "sector",
    "desc_sector": "sector_desc",
    "subsector": "subsector",
    "desc_subsector": "subsector_desc",
    "unidad_responsable": "unidad_responsable",
    "desc_unidad_responsable": "unidad_resp_desc",
    "finalidad": "finalidad",
    "desc_finalidad": "finalidad_desc",
    "funcion": "funcion",
    "desc_funcion": "funcion_desc",
    "subfuncion": "subfuncion",
    "desc_subfuncion": "subfuncion_desc",
    "area_funcional": "area_funcional",
    "desc_area_funcional": "area_funcional_desc",
    "modalidad": "modalidad",
    "desc_modalidad": "modalidad_desc",
    "programa_presupuestario": "programa_presup",
    "desc_programa_presupuestario": "programa_desc",
    "capitulo": "capitulo",
    "desc_capitulo": "capitulo_desc",
    "concepto": "concepto",
    "desc_concepto": "concepto_desc",
    "partida_generica": "partida_generica",
    "desc_partida_generica": "partida_gen_desc",
    "partida_especifica": "partida_especifica",
    "desc_partida_especifica": "partida_esp_desc",
    "tipo_gasto": "tipo_gasto",
    "desc_tipo_gasto": "tipo_gasto_desc",
    "gasto_programable": "gasto_programable",
    "desc_gasto_programable": "gasto_prog_desc",
    "monto_aprobado": "monto_aprobado",
    "monto_modificado": "monto_modificado",
    "monto_ejercido": "monto_ejercido",
}

DB_COLS = [
    "clave_presupuestaria", "año", "periodo",
    "gobierno_general", "gobierno_desc", "sector", "sector_desc",
    "subsector", "subsector_desc", "unidad_responsable", "unidad_resp_desc",
    "finalidad", "finalidad_desc", "funcion", "funcion_desc",
    "subfuncion", "subfuncion_desc", "area_funcional", "area_funcional_desc",
    "modalidad", "modalidad_desc", "programa_presup", "programa_desc",
    "capitulo", "capitulo_desc", "concepto", "concepto_desc",
    "partida_generica", "partida_gen_desc", "partida_especifica", "partida_esp_desc",
    "tipo_gasto", "tipo_gasto_desc", "gasto_programable", "gasto_prog_desc",
    "monto_aprobado", "monto_modificado", "monto_ejercido",
    "archivo_origen",
]


def load_cdmx(csv_path: Path, batch_size: int = 5000) -> int:
    """Carga CSV de presupuesto CDMX."""
    filename = csv_path.name
    console.print(f"\n[bold]Cargando {filename}[/bold]")

    # Intentar diferentes encodings
    for enc in ["utf-8", "latin-1", "cp1252"]:
        try:
            df = pd.read_csv(csv_path, encoding=enc, low_memory=False, dtype=str)
            break
        except UnicodeDecodeError:
            continue
    else:
        console.print(f"  [red]No se pudo leer {filename}[/red]")
        return 0

    console.print(f"  Filas: {len(df):,}")

    # Renombrar columnas
    rename = {k: v for k, v in COLUMN_MAP.items() if k in df.columns}
    df = df.rename(columns=rename)
    df["archivo_origen"] = filename

    # Parsear montos
    for col in ["monto_aprobado", "monto_modificado", "monto_ejercido"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Filtrar columnas existentes
    cols = [c for c in DB_COLS if c in df.columns]
    df_insert = df[cols].copy()
    df_insert = df_insert.where(df_insert.notna(), None)

    conn = get_connection()
    placeholders = ", ".join(["?"] * len(cols))
    col_names = ", ".join(cols)
    sql = f"INSERT INTO presupuesto_cdmx ({col_names}) VALUES ({placeholders})"

    total = 0
    for start in range(0, len(df_insert), batch_size):
        batch = df_insert.iloc[start : start + batch_size]
        rows = [tuple(r) for r in batch.itertuples(index=False, name=None)]
        conn.executemany(sql, rows)
        conn.commit()
        total += len(rows)

    console.print(f"  [green]OK[/green]: {total:,} filas")
    conn.close()
    return total


def load_all(data_dir: Path) -> int:
    """Carga todos los CSVs de presupuesto CDMX."""
    csv_files = sorted(data_dir.glob("Presupuesto_*.csv"))
    if not csv_files:
        console.print("[yellow]No se encontraron CSVs de presupuesto CDMX[/yellow]")
        return 0

    total = 0
    for f in csv_files:
        total += load_cdmx(f)

    console.print(f"\n[bold green]Total CDMX: {total:,} filas[/bold green]")
    return total
