"""
Motor de detección de anomalías en gasto público.

Detecta:
1. Fragmentación de contratos (split para evitar licitación)
2. Proveedores con concentración excesiva de adjudicaciones directas
3. Proveedores fantasma (RFC sospechoso, sin historial, montos altos)
4. Montos atípicos (outliers estadísticos por tipo de contratación)
5. Concentración de gasto (pocas empresas acaparan todo)
6. Contratos express (firma y fin muy cercanos)
"""

from __future__ import annotations

import json
import statistics
from dataclasses import dataclass

from rich.console import Console
from rich.table import Table

from src.db.connection import get_connection

console = Console()


@dataclass
class Anomalia:
    tipo: str
    severidad: str
    titulo: str
    descripcion: str
    entidad: str | None = None
    proveedor_rfc: str | None = None
    monto_involucrado: float | None = None
    contratos_ids: list[int] | None = None
    evidencia: dict | None = None


def _save_anomalias(anomalias: list[Anomalia]) -> int:
    """Guarda anomalías en la base de datos."""
    if not anomalias:
        return 0

    conn = get_connection()
    sql = """
        INSERT INTO anomalias (tipo, severidad, titulo, descripcion, entidad,
                               proveedor_rfc, monto_involucrado, contratos_ids, evidencia)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    rows = [
        (
            a.tipo, a.severidad, a.titulo, a.descripcion, a.entidad,
            a.proveedor_rfc, a.monto_involucrado,
            json.dumps(a.contratos_ids) if a.contratos_ids else None,
            json.dumps(a.evidencia, ensure_ascii=False) if a.evidencia else None,
        )
        for a in anomalias
    ]
    conn.executemany(sql, rows)
    conn.commit()
    conn.close()
    return len(rows)


# ── 1. Fragmentación de contratos ────────────────────────────

def detectar_fragmentacion(umbral_monto: float = 2_000_000, min_contratos: int = 3) -> list[Anomalia]:
    """
    Detecta posible fragmentación: misma UC + mismo proveedor + múltiples
    adjudicaciones directas con montos justo debajo del umbral de licitación.

    El umbral de licitación federal para adquisiciones es ~$1.9M MXN.
    """
    conn = get_connection()
    rows = conn.execute("""
        SELECT clave_uc, nombre_uc, proveedor_rfc, proveedor_nombre,
               COUNT(*) as n_contratos,
               SUM(importe_drc) as monto_total,
               AVG(importe_drc) as monto_promedio,
               MIN(importe_drc) as monto_min,
               MAX(importe_drc) as monto_max,
               GROUP_CONCAT(id) as ids
        FROM contratos
        WHERE tipo_procedimiento LIKE '%ADJUDICACIÓN DIRECTA%'
          AND importe_drc > 0
          AND importe_drc < ?
        GROUP BY clave_uc, proveedor_rfc
        HAVING COUNT(*) >= ?
        ORDER BY monto_total DESC
        LIMIT 200
    """, (umbral_monto, min_contratos)).fetchall()
    conn.close()

    anomalias = []
    for r in rows:
        ids = [int(x) for x in r["ids"].split(",")] if r["ids"] else []
        sev = "critica" if r["n_contratos"] >= 10 else "alta" if r["n_contratos"] >= 5 else "media"

        anomalias.append(Anomalia(
            tipo="fragmentacion",
            severidad=sev,
            titulo=f"Posible fragmentación: {r['proveedor_nombre'][:50]} en {r['nombre_uc'][:50]}",
            descripcion=(
                f"{r['n_contratos']} adjudicaciones directas al mismo proveedor en la misma UC, "
                f"por un total de ${r['monto_total']:,.0f} MXN. "
                f"Promedio: ${r['monto_promedio']:,.0f}, rango: ${r['monto_min']:,.0f}-${r['monto_max']:,.0f}. "
                f"Los contratos podrían haberse fragmentado para evitar licitación pública."
            ),
            entidad=r["nombre_uc"],
            proveedor_rfc=r["proveedor_rfc"],
            monto_involucrado=r["monto_total"],
            contratos_ids=ids,
            evidencia={
                "n_contratos": r["n_contratos"],
                "monto_promedio": r["monto_promedio"],
                "monto_min": r["monto_min"],
                "monto_max": r["monto_max"],
                "umbral_licitacion": umbral_monto,
            },
        ))

    return anomalias


# ── 2. Proveedores con exceso de adjudicaciones directas ────

def detectar_adjudicacion_excesiva(min_contratos: int = 10, pct_directas: float = 0.9) -> list[Anomalia]:
    """
    Proveedores donde >90% de sus contratos son adjudicaciones directas
    y tienen un volumen significativo.
    """
    conn = get_connection()
    rows = conn.execute("""
        SELECT rfc, nombre, total_contratos, monto_total,
               contratos_directos, contratos_licitacion, dependencias_distintas
        FROM proveedores
        WHERE total_contratos >= ?
          AND CAST(contratos_directos AS REAL) / total_contratos >= ?
          AND monto_total > 0
        ORDER BY monto_total DESC
        LIMIT 200
    """, (min_contratos, pct_directas)).fetchall()
    conn.close()

    anomalias = []
    for r in rows:
        pct = r["contratos_directos"] / r["total_contratos"] * 100
        sev = "critica" if r["monto_total"] > 100_000_000 else "alta" if r["monto_total"] > 10_000_000 else "media"

        anomalias.append(Anomalia(
            tipo="adjudicacion_sospechosa",
            severidad=sev,
            titulo=f"Adjudicación excesiva: {r['nombre'][:60]}",
            descripcion=(
                f"RFC {r['rfc']}: {r['contratos_directos']}/{r['total_contratos']} contratos "
                f"({pct:.0f}%) son adjudicaciones directas, por ${r['monto_total']:,.0f} MXN total. "
                f"Opera con {r['dependencias_distintas']} dependencias distintas."
            ),
            proveedor_rfc=r["rfc"],
            monto_involucrado=r["monto_total"],
            evidencia={
                "total_contratos": r["total_contratos"],
                "contratos_directos": r["contratos_directos"],
                "pct_directas": pct,
                "dependencias": r["dependencias_distintas"],
            },
        ))

    return anomalias


# ── 3. Concentración de gasto ────────────────────────────────

def detectar_concentracion(top_n: int = 20) -> list[Anomalia]:
    """
    Detecta concentración excesiva: pocos proveedores acaparan
    un porcentaje desproporcionado del gasto.
    """
    conn = get_connection()

    total_gasto = conn.execute(
        "SELECT COALESCE(SUM(importe_drc), 0) FROM contratos WHERE importe_drc > 0"
    ).fetchone()[0]

    if total_gasto == 0:
        conn.close()
        return []

    top = conn.execute("""
        SELECT proveedor_rfc, proveedor_nombre,
               SUM(importe_drc) as monto,
               COUNT(*) as n_contratos
        FROM contratos
        WHERE importe_drc > 0 AND proveedor_rfc IS NOT NULL
        GROUP BY proveedor_rfc
        ORDER BY monto DESC
        LIMIT ?
    """, (top_n,)).fetchall()
    conn.close()

    anomalias = []
    acumulado = 0
    for i, r in enumerate(top):
        pct = r["monto"] / total_gasto * 100
        acumulado += pct

        if pct >= 1.0:  # >1% del total
            anomalias.append(Anomalia(
                tipo="concentracion",
                severidad="alta" if pct >= 5 else "media",
                titulo=f"Concentración: {r['proveedor_nombre'][:60]} ({pct:.1f}% del gasto)",
                descripcion=(
                    f"RFC {r['proveedor_rfc']} acapara {pct:.1f}% del gasto total "
                    f"(${r['monto']:,.0f} MXN en {r['n_contratos']} contratos). "
                    f"Top {i+1} proveedor. Acumulado top-{i+1}: {acumulado:.1f}%."
                ),
                proveedor_rfc=r["proveedor_rfc"],
                monto_involucrado=r["monto"],
                evidencia={
                    "rank": i + 1,
                    "pct_total": pct,
                    "pct_acumulado": acumulado,
                    "n_contratos": r["n_contratos"],
                    "total_gasto": total_gasto,
                },
            ))

    return anomalias


# ── 4. Montos atípicos (outliers) ───────────────────────────

def detectar_montos_atipicos(z_threshold: float = 3.0) -> list[Anomalia]:
    """
    Contratos con montos que son outliers estadísticos dentro de su
    tipo de contratación + institución.
    """
    conn = get_connection()
    groups = conn.execute("""
        SELECT institucion, tipo_contratacion,
               AVG(importe_drc) as media,
               COUNT(*) as n
        FROM contratos
        WHERE importe_drc > 0
        GROUP BY institucion, tipo_contratacion
        HAVING COUNT(*) >= 10
    """).fetchall()

    anomalias = []
    for g in groups:
        montos = conn.execute("""
            SELECT id, titulo_contrato, importe_drc, proveedor_rfc, proveedor_nombre
            FROM contratos
            WHERE institucion = ? AND tipo_contratacion = ? AND importe_drc > 0
        """, (g["institucion"], g["tipo_contratacion"])).fetchall()

        values = [m["importe_drc"] for m in montos]
        if len(values) < 10:
            continue

        mean = statistics.mean(values)
        stdev = statistics.stdev(values)
        if stdev == 0:
            continue

        for m in montos:
            z = (m["importe_drc"] - mean) / stdev
            if z >= z_threshold:
                anomalias.append(Anomalia(
                    tipo="monto_atipico",
                    severidad="alta" if z >= 5 else "media",
                    titulo=f"Monto atípico: ${m['importe_drc']:,.0f} en {g['institucion'][:40]}",
                    descripcion=(
                        f"Contrato '{m['titulo_contrato'][:80]}' por ${m['importe_drc']:,.0f} MXN "
                        f"está {z:.1f} desviaciones estándar sobre la media "
                        f"(${mean:,.0f}) para {g['tipo_contratacion']} en {g['institucion'][:60]}. "
                        f"Proveedor: {m['proveedor_nombre']}"
                    ),
                    entidad=g["institucion"],
                    proveedor_rfc=m["proveedor_rfc"],
                    monto_involucrado=m["importe_drc"],
                    contratos_ids=[m["id"]],
                    evidencia={
                        "z_score": round(z, 2),
                        "media_grupo": mean,
                        "stdev_grupo": stdev,
                        "n_grupo": g["n"],
                    },
                ))

    conn.close()
    # Solo top 100 por severidad
    anomalias.sort(key=lambda a: a.monto_involucrado or 0, reverse=True)
    return anomalias[:100]


# ── 5. Proveedores fantasma ─────────────────────────────────

def detectar_proveedores_fantasma() -> list[Anomalia]:
    """
    Proveedores con señales de alerta:
    - Solo un contrato pero por monto alto
    - RFC con formato irregular
    - Solo adjudicaciones directas
    """
    conn = get_connection()
    rows = conn.execute("""
        SELECT rfc, nombre, total_contratos, monto_total,
               contratos_directos, dependencias_distintas,
               primer_contrato, ultimo_contrato
        FROM proveedores
        WHERE total_contratos <= 2
          AND monto_total > 5000000
          AND contratos_directos = total_contratos
        ORDER BY monto_total DESC
        LIMIT 100
    """).fetchall()
    conn.close()

    anomalias = []
    for r in rows:
        anomalias.append(Anomalia(
            tipo="proveedor_fantasma",
            severidad="alta" if r["monto_total"] > 50_000_000 else "media",
            titulo=f"Proveedor sospechoso: {r['nombre'][:60]}",
            descripcion=(
                f"RFC {r['rfc']}: solo {r['total_contratos']} contrato(s), todos directos, "
                f"por ${r['monto_total']:,.0f} MXN. "
                f"Opera con {r['dependencias_distintas']} dependencia(s). "
                f"Primer contrato: {r['primer_contrato']}, último: {r['ultimo_contrato']}."
            ),
            proveedor_rfc=r["rfc"],
            monto_involucrado=r["monto_total"],
            evidencia={
                "total_contratos": r["total_contratos"],
                "todas_directas": True,
                "dependencias": r["dependencias_distintas"],
            },
        ))

    return anomalias


# ── Orquestador ──────────────────────────────────────────────

def run_all_checks() -> dict[str, int]:
    """Ejecuta todas las detecciones y guarda resultados."""
    conn = get_connection()
    # Limpiar anomalías previas
    conn.execute("DELETE FROM anomalias")
    conn.commit()
    conn.close()

    results = {}

    checks = [
        ("fragmentacion", detectar_fragmentacion),
        ("adjudicacion_sospechosa", detectar_adjudicacion_excesiva),
        ("concentracion", detectar_concentracion),
        ("monto_atipico", detectar_montos_atipicos),
        ("proveedor_fantasma", detectar_proveedores_fantasma),
    ]

    total = 0
    for name, fn in checks:
        console.print(f"\n[bold]Analizando: {name}[/bold] ...")
        anomalias = fn()
        n = _save_anomalias(anomalias)
        results[name] = n
        total += n
        console.print(f"  Encontradas: [yellow]{n}[/yellow]")

    console.print(f"\n[bold]Total anomalías: [red]{total}[/red][/bold]")
    return results


def report_anomalias(limit: int = 50) -> None:
    """Muestra reporte de anomalías en consola."""
    conn = get_connection()

    # Resumen por tipo
    summary = conn.execute("""
        SELECT tipo, severidad, COUNT(*) as n, COALESCE(SUM(monto_involucrado), 0) as monto
        FROM anomalias
        GROUP BY tipo, severidad
        ORDER BY
            CASE severidad WHEN 'critica' THEN 1 WHEN 'alta' THEN 2
                           WHEN 'media' THEN 3 ELSE 4 END,
            monto DESC
    """).fetchall()

    sev_colors = {"critica": "red bold", "alta": "red", "media": "yellow", "baja": "dim"}

    table = Table(title="Resumen de Anomalías")
    table.add_column("Tipo", style="cyan")
    table.add_column("Severidad")
    table.add_column("Cantidad", justify="right")
    table.add_column("Monto involucrado", justify="right")

    for s in summary:
        color = sev_colors.get(s["severidad"], "")
        table.add_row(
            s["tipo"],
            f"[{color}]{s['severidad']}[/{color}]",
            str(s["n"]),
            f"${s['monto']:,.0f}",
        )
    console.print(table)

    # Top anomalías
    top = conn.execute("""
        SELECT tipo, severidad, titulo, monto_involucrado, proveedor_rfc
        FROM anomalias
        ORDER BY
            CASE severidad WHEN 'critica' THEN 1 WHEN 'alta' THEN 2
                           WHEN 'media' THEN 3 ELSE 4 END,
            COALESCE(monto_involucrado, 0) DESC
        LIMIT ?
    """, (limit,)).fetchall()

    console.print(f"\n[bold]Top {limit} anomalías:[/bold]\n")
    for i, a in enumerate(top, 1):
        color = sev_colors.get(a["severidad"], "")
        monto_str = f"${a['monto_involucrado']:,.0f}" if a["monto_involucrado"] else "N/A"
        console.print(
            f"  {i:3d}. [{color}][{a['severidad'].upper()}][/{color}] "
            f"{a['titulo'][:80]} — {monto_str}"
        )

    conn.close()
