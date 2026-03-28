"""Conexión a base de datos. SQLite por default, PostgreSQL opcional."""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

DB_DIR = Path(__file__).parent.parent.parent / "data"
DEFAULT_SQLITE = DB_DIR / "gasto_publico.db"


def get_sqlite_path() -> Path:
    """Ruta al archivo SQLite."""
    path = Path(os.environ.get("GASTO_DB_PATH", str(DEFAULT_SQLITE)))
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def get_connection() -> sqlite3.Connection:
    """Crea conexión a SQLite."""
    conn = sqlite3.connect(str(get_sqlite_path()))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA cache_size=-64000")  # 64MB cache
    conn.row_factory = sqlite3.Row
    return conn


def run_migrations() -> None:
    """Ejecuta schema SQL."""
    schema_dir = Path(__file__).parent.parent.parent / "schema"
    sql_files = sorted(schema_dir.glob("*.sql"))
    if not sql_files:
        print("No hay archivos de migración.")
        return

    conn = get_connection()
    try:
        for f in sql_files:
            print(f"  Ejecutando {f.name} ...")
            sql = f.read_text(encoding="utf-8")
            conn.executescript(sql)
            print(f"  [OK] {f.name}")
    finally:
        conn.close()
