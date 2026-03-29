"""
Análisis completo por estado: mapea contratos de CompraNet a entidades
federativas y corre detección de anomalías por estado.
"""

from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path

from rich.console import Console

console = Console()

STATES = [
    "AGUASCALIENTES", "BAJA CALIFORNIA SUR", "BAJA CALIFORNIA",
    "CAMPECHE", "CHIAPAS", "CHIHUAHUA", "CIUDAD DE MEXICO",
    "COAHUILA", "COLIMA", "DURANGO", "GUANAJUATO", "GUERRERO",
    "HIDALGO", "JALISCO", "MEXICO", "MICHOACAN", "MORELOS",
    "NAYARIT", "NUEVO LEON", "OAXACA", "PUEBLA", "QUERETARO",
    "QUINTANA ROO", "SAN LUIS POTOSI", "SINALOA", "SONORA",
    "TABASCO", "TAMAULIPAS", "TLAXCALA", "VERACRUZ", "YUCATAN",
    "ZACATECAS",
]

# Sorted longest-first so "BAJA CALIFORNIA SUR" matches before "BAJA CALIFORNIA"
STATES_SORTED = sorted(STATES, key=len, reverse=True)

# Aliases
ALIASES = {
    "CDMX": "CIUDAD DE MEXICO", "D.F.": "CIUDAD DE MEXICO",
    "EDO. DE MEXICO": "MEXICO", "EDO. MEX": "MEXICO", "EDOMEX": "MEXICO",
    "ESTADO DE MEXICO": "MEXICO",
    "N.L.": "NUEVO LEON", "NL": "NUEVO LEON",
    "SLP": "SAN LUIS POTOSI", "S.L.P.": "SAN LUIS POTOSI",
    "B.C.S.": "BAJA CALIFORNIA SUR", "BCS": "BAJA CALIFORNIA SUR",
    "B.C.": "BAJA CALIFORNIA", "Q. ROO": "QUINTANA ROO",
    "Q.ROO": "QUINTANA ROO", "QROO": "QUINTANA ROO",
}


def _normalize(text: str) -> str:
    """Remove accents and normalize for matching."""
    if not text:
        return ""
    t = text.upper()
    for old, new in [("Á","A"),("É","E"),("Í","I"),("Ó","O"),("Ú","U"),("Ñ","N"),
                      ("\xc1","A"),("\xc9","E"),("\xcd","I"),("\xd3","O"),("\xda","U"),("\xd1","N")]:
        t = t.replace(old, new)
    return t


def detect_state(nombre_uc: str, desc_ramo: str, institucion: str, orden_gob: str) -> str | None:
    """Detecta el estado de un contrato usando nombre_uc, desc_ramo, institucion."""
    # For state governments, desc_ramo IS the state
    if orden_gob in ("GEM", "GEF"):
        norm = _normalize(desc_ramo or "")
        for s in STATES_SORTED:
            if s in norm:
                return s
        for alias, state in ALIASES.items():
            if alias in norm:
                return state

    # For federal, look in nombre_uc and institucion
    combined = _normalize(f"{nombre_uc or ''} {institucion or ''}")

    # Check aliases first
    for alias, state in ALIASES.items():
        if alias in combined:
            return state

    # Check full state names
    for s in STATES_SORTED:
        if s in combined:
            return s

    return None


def mapear_contratos_a_estados(db_path: str = "data/gasto_publico.db") -> dict:
    """Mapea contratos a estados y genera análisis completo por estado."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    console.print("[bold]Mapeando contratos a estados...[/bold]")

    rows = conn.execute("""
        SELECT id, nombre_uc, desc_ramo, institucion, orden_gobierno,
               tipo_procedimiento, importe_drc, proveedor_rfc, proveedor_nombre,
               estratificacion, clave_uc
        FROM contratos
        WHERE importe_drc > 0
    """).fetchall()

    state_data = {}
    mapped = 0
    unmapped = 0

    for r in rows:
        state = detect_state(r["nombre_uc"], r["desc_ramo"], r["institucion"], r["orden_gobierno"])
        if not state:
            unmapped += 1
            continue
        mapped += 1

        if state not in state_data:
            state_data[state] = {
                "contratos": 0, "monto_total": 0,
                "directas_n": 0, "directas_monto": 0,
                "licitaciones_n": 0, "licitaciones_monto": 0,
                "caso_fortuito_n": 0, "caso_fortuito_monto": 0,
                "proveedores": set(),
                "uc_proveedor_pairs": {},  # for fragmentation
                "micro_grandes": [],  # MICRO companies with big contracts
                "proveedor_stats": {},  # for ghost/concentration
            }

        sd = state_data[state]
        sd["contratos"] += 1
        sd["monto_total"] += r["importe_drc"]

        tipo = r["tipo_procedimiento"] or ""
        if "DIRECTA" in tipo.upper():
            sd["directas_n"] += 1
            sd["directas_monto"] += r["importe_drc"]
        if "LICITACI" in tipo.upper():
            sd["licitaciones_n"] += 1
            sd["licitaciones_monto"] += r["importe_drc"]
        if "CASO FORTUITO" in tipo.upper():
            sd["caso_fortuito_n"] += 1
            sd["caso_fortuito_monto"] += r["importe_drc"]

        rfc = r["proveedor_rfc"]
        if rfc:
            sd["proveedores"].add(rfc)

            # Proveedor stats
            if rfc not in sd["proveedor_stats"]:
                sd["proveedor_stats"][rfc] = {
                    "nombre": r["proveedor_nombre"],
                    "n": 0, "monto": 0, "directas": 0
                }
            ps = sd["proveedor_stats"][rfc]
            ps["n"] += 1
            ps["monto"] += r["importe_drc"]
            if "DIRECTA" in tipo.upper():
                ps["directas"] += 1

            # Fragmentation: UC+proveedor pairs
            key = f"{r['clave_uc']}|{rfc}"
            if key not in sd["uc_proveedor_pairs"]:
                sd["uc_proveedor_pairs"][key] = {"n": 0, "monto": 0, "uc": r["nombre_uc"], "prov": r["proveedor_nombre"]}
            sd["uc_proveedor_pairs"][key]["n"] += 1
            sd["uc_proveedor_pairs"][key]["monto"] += r["importe_drc"]

        # MICRO with big contracts
        if r["estratificacion"] == "MICRO" and r["importe_drc"] > 50_000_000:
            sd["micro_grandes"].append({
                "nombre": r["proveedor_nombre"],
                "monto": r["importe_drc"],
                "institucion": r["institucion"],
            })

    conn.close()
    console.print(f"  Mapeados: {mapped:,} | Sin mapear: {unmapped:,}")

    # Now compute anomalies per state
    results = {}
    for state, sd in state_data.items():
        pct_directas = sd["directas_monto"] / sd["monto_total"] * 100 if sd["monto_total"] > 0 else 0

        # Fragmentation: UC+proveedor with 3+ contracts under 2M
        fragmentacion = [
            v for v in sd["uc_proveedor_pairs"].values()
            if v["n"] >= 3
        ]

        # Ghost providers: 1-2 contracts, all direct, >5M
        fantasmas = [
            ps for ps in sd["proveedor_stats"].values()
            if ps["n"] <= 2 and ps["directas"] == ps["n"] and ps["monto"] > 5_000_000
        ]

        # Concentration: top 5 providers share of total
        sorted_provs = sorted(sd["proveedor_stats"].values(), key=lambda x: x["monto"], reverse=True)
        top5_monto = sum(p["monto"] for p in sorted_provs[:5])
        pct_top5 = top5_monto / sd["monto_total"] * 100 if sd["monto_total"] > 0 else 0

        # Top provider
        top_prov = sorted_provs[0] if sorted_provs else None

        results[state] = {
            "contratos": sd["contratos"],
            "monto_total": sd["monto_total"],
            "proveedores": len(sd["proveedores"]),
            "pct_directas": round(pct_directas, 1),
            "directas_monto": sd["directas_monto"],
            "caso_fortuito_n": sd["caso_fortuito_n"],
            "caso_fortuito_monto": sd["caso_fortuito_monto"],
            "fragmentacion_casos": len(fragmentacion),
            "fantasmas": len(fantasmas),
            "fantasmas_monto": sum(f["monto"] for f in fantasmas),
            "micro_grandes": len(sd["micro_grandes"]),
            "micro_monto": sum(m["monto"] for m in sd["micro_grandes"]),
            "pct_top5": round(pct_top5, 1),
            "top_proveedor": {
                "nombre": top_prov["nombre"][:50] if top_prov else "",
                "monto": top_prov["monto"] if top_prov else 0,
                "n": top_prov["n"] if top_prov else 0,
            } if top_prov else None,
            "anomalias_total": len(fragmentacion) + len(fantasmas) + len(sd["micro_grandes"]),
        }

    return results


def generar_json_estados(output: str = "data/estados.json"):
    """Genera JSON con análisis por estado para la infografía."""
    results = mapear_contratos_a_estados()

    # Sort by monto
    sorted_states = sorted(results.items(), key=lambda x: x[1]["monto_total"], reverse=True)

    output_path = Path(output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(dict(sorted_states), f, ensure_ascii=False, indent=2)

    console.print(f"\n[green]JSON generado: {output}[/green]")
    console.print(f"Estados con datos: {len(results)}")

    # Print summary
    console.print(f"\n[bold]Resumen por estado:[/bold]\n")
    for state, d in sorted_states:
        console.print(
            f"  {state:25s} | {d['contratos']:>6,} ctos | ${d['monto_total']/1e9:>8,.1f}B | "
            f"dir:{d['pct_directas']:5.1f}% | anom:{d['anomalias_total']:>3} | "
            f"top5:{d['pct_top5']:5.1f}%"
        )

    return results


if __name__ == "__main__":
    generar_json_estados()
