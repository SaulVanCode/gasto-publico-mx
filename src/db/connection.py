"""Conexión a PostgreSQL."""

from __future__ import annotations

import os

import psycopg


def get_conninfo() -> str:
    """Obtiene connection string desde env var o default local."""
    return os.environ.get(
        "DATABASE_URL",
        "postgresql://gasto:gasto@localhost:5432/gasto_publico",
    )


def get_connection(**kwargs) -> psycopg.Connection:
    """Crea conexión a PostgreSQL."""
    return psycopg.connect(get_conninfo(), **kwargs)


def run_migrations(schema_dir: str = "schema") -> None:
    """Ejecuta archivos SQL de schema/ en orden."""
    from pathlib import Path

    schema_path = Path(schema_dir)
    if not schema_path.exists():
        raise FileNotFoundError(f"No existe directorio de schema: {schema_dir}")

    sql_files = sorted(schema_path.glob("*.sql"))
    if not sql_files:
        print("No hay archivos de migración.")
        return

    conn = get_connection(autocommit=True)
    try:
        for f in sql_files:
            print(f"  Ejecutando {f.name} ...")
            sql = f.read_text(encoding="utf-8")
            conn.execute(sql)
            print(f"  [OK] {f.name}")
    finally:
        conn.close()
