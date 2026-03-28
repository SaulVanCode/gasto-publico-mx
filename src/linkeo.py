"""
Linkeo PEF ↔ Contratos CompraNet.

Conecta presupuesto federal con contratos reales usando:
- Clave de ramo (ramo federal ↔ clave_ramo en CompraNet)
- Clave de cartera SHCP
- Programa federal
"""

from __future__ import annotations

from rich.console import Console
from rich.table import Table

from src.db.connection import get_connection

console = Console()


def linkear_por_ramo() -> int:
    """
    Linkea PEF → contratos por clave de ramo.
    Match: presupuesto_federal.ramo == contratos.clave_ramo
    """
    conn = get_connection()

    # Limpiar linkeos previos
    conn.execute("DELETE FROM flujo_gasto")
    conn.commit()

    # Agregar presupuesto por ramo y linkearlo con totales de contratos
    n = conn.execute("""
        INSERT INTO flujo_gasto (pf_ramo, pf_programa, pf_año, monto_asignado, monto_contratado, confianza, metodo_linkeo)
        SELECT
            pf.ramo,
            pf.programa_presup,
            pf.año,
            pf.monto_pef,
            COALESCE(cn.monto_contratos, 0),
            CASE
                WHEN cn.monto_contratos IS NOT NULL THEN 0.7
                ELSE 0.3
            END,
            'ramo_match'
        FROM (
            SELECT ramo, programa_presup, año, SUM(monto) as monto_pef
            FROM presupuesto_federal
            GROUP BY ramo, programa_presup, año
        ) pf
        LEFT JOIN (
            SELECT clave_ramo, SUM(importe_drc) as monto_contratos
            FROM contratos
            WHERE importe_drc > 0
            GROUP BY clave_ramo
        ) cn ON pf.ramo = cn.clave_ramo
    """).rowcount

    conn.commit()
    console.print(f"  [green]Linkeados {n:,} registros por ramo[/green]")

    conn.close()
    return n


def report_flujo() -> None:
    """Muestra el flujo del dinero: presupuesto → contratos."""
    conn = get_connection()

    rows = conn.execute("""
        SELECT
            pf_ramo,
            pf.ramo_desc,
            SUM(fg.monto_asignado) as presupuesto,
            SUM(fg.monto_contratado) as contratado,
            CASE
                WHEN SUM(fg.monto_asignado) > 0
                THEN SUM(fg.monto_contratado) / SUM(fg.monto_asignado) * 100
                ELSE 0
            END as pct_contratado
        FROM flujo_gasto fg
        LEFT JOIN (
            SELECT DISTINCT ramo, ramo_desc FROM presupuesto_federal
        ) pf ON fg.pf_ramo = pf.ramo
        GROUP BY pf_ramo
        ORDER BY presupuesto DESC
        LIMIT 30
    """).fetchall()

    table = Table(title="Flujo del Dinero: PEF -> Contratos (por Ramo)")
    table.add_column("Ramo", style="cyan", max_width=40)
    table.add_column("Presupuesto", justify="right")
    table.add_column("Contratado", justify="right")
    table.add_column("% Contratado", justify="right")

    for r in rows:
        desc = (r["ramo_desc"] or r["pf_ramo"] or "?")[:40]
        pct = r["pct_contratado"] or 0
        pct_color = "green" if pct < 50 else "yellow" if pct < 100 else "red"
        table.add_row(
            desc,
            f"${r['presupuesto']:,.0f}",
            f"${r['contratado']:,.0f}",
            f"[{pct_color}]{pct:.1f}%[/{pct_color}]",
        )

    console.print(table)

    # Resumen
    totals = conn.execute("""
        SELECT SUM(monto_asignado) as pef, SUM(monto_contratado) as cn
        FROM flujo_gasto
    """).fetchone()

    if totals["pef"] and totals["pef"] > 0:
        pct = totals["cn"] / totals["pef"] * 100
        console.print(f"\n  Total PEF: [bold]${totals['pef']:,.0f}[/bold]")
        console.print(f"  Total contratado (CompraNet): [bold]${totals['cn']:,.0f}[/bold]")
        console.print(f"  Cobertura de rastreo: [bold]{pct:.1f}%[/bold]")

    conn.close()
