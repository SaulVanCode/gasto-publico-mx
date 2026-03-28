"""
Crawler para datos.gob.mx (CKAN API federal).

Busca y descarga datasets de presupuesto del catálogo nacional de datos abiertos.
"""

from __future__ import annotations

from pathlib import Path

import httpx
from rich.console import Console
from rich.table import Table

console = Console()

CKAN_BASE = "https://www.datos.gob.mx/api/3/action"

# Datasets conocidos de interés
KNOWN_DATASETS = [
    "presupuesto_egresos_federacion_pef",
    "asignacion_ejecucion_presupuesto_egresos_federacion_2026",
    "asignacion_ejecucion_presupuesto_egresos_federacion_2025",
    "contratos_expedientes_sistema_historico_compranet",
]


def search_datasets(query: str = "presupuesto egresos", rows: int = 20, timeout: float = 30.0) -> list[dict]:
    """Busca datasets en datos.gob.mx via CKAN API."""
    url = f"{CKAN_BASE}/package_search"
    params = {"q": query, "rows": rows}

    try:
        r = httpx.get(url, params=params, timeout=timeout, follow_redirects=True)
        r.raise_for_status()
        data = r.json()
        if not data.get("success"):
            return []
        return data["result"].get("results", [])
    except (httpx.RequestError, httpx.HTTPStatusError) as e:
        console.print(f"[red]Error buscando en datos.gob.mx[/red]: {e}")
        return []


def list_datasets(query: str = "presupuesto egresos") -> None:
    """Lista datasets disponibles en una tabla bonita."""
    datasets = search_datasets(query)
    if not datasets:
        console.print("[yellow]No se encontraron datasets[/yellow]")
        return

    table = Table(title=f"datos.gob.mx — '{query}'")
    table.add_column("ID", style="cyan", max_width=50)
    table.add_column("Título", max_width=60)
    table.add_column("Recursos", justify="right")
    table.add_column("Org", style="dim", max_width=20)

    for ds in datasets:
        org = ds.get("organization", {})
        org_name = org.get("title", "?") if org else "?"
        table.add_row(
            ds.get("name", "?"),
            ds.get("title", "?")[:60],
            str(len(ds.get("resources", []))),
            org_name[:20],
        )

    console.print(table)


def get_dataset_resources(dataset_id: str, timeout: float = 30.0) -> list[dict]:
    """Obtiene recursos (archivos descargables) de un dataset específico."""
    url = f"{CKAN_BASE}/package_show?id={dataset_id}"
    try:
        r = httpx.get(url, timeout=timeout, follow_redirects=True)
        r.raise_for_status()
        data = r.json()
        if not data.get("success"):
            return []
        return data["result"].get("resources", [])
    except (httpx.RequestError, httpx.HTTPStatusError) as e:
        console.print(f"[red]Error[/red]: {e}")
        return []


def download_dataset(
    dataset_id: str,
    dest_dir: Path,
    formats: tuple[str, ...] = ("csv", "xlsx", "json"),
    force: bool = False,
    timeout: float = 300.0,
) -> list[Path]:
    """Descarga todos los recursos CSV/XLSX de un dataset."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    resources = get_dataset_resources(dataset_id)

    if not resources:
        console.print(f"[yellow]No hay recursos para {dataset_id}[/yellow]")
        return []

    console.print(f"\n[bold]datos.gob.mx — {dataset_id}[/bold]")
    results = []

    for res in resources:
        fmt = res.get("format", "").lower()
        if fmt not in formats:
            continue

        url = res.get("url", "")
        if not url:
            continue

        name = res.get("name", "") or url.split("/")[-1]
        safe_name = name.replace(" ", "_").replace("/", "_")
        if not any(safe_name.endswith(f".{f}") for f in formats):
            safe_name = f"{safe_name}.{fmt}"

        dest = dest_dir / safe_name
        if dest.exists() and not force:
            console.print(f"  [dim]Ya existe: {dest.name}[/dim]")
            results.append(dest)
            continue

        console.print(f"  Descargando [bold]{dest.name}[/bold] ...")
        try:
            with httpx.stream("GET", url, timeout=timeout, follow_redirects=True) as r:
                r.raise_for_status()
                with open(dest, "wb") as f:
                    for chunk in r.iter_bytes(chunk_size=65536):
                        f.write(chunk)
            size_mb = dest.stat().st_size / 1e6
            console.print(f"  [green]OK[/green]: {dest.name} ({size_mb:.1f} MB)")
            results.append(dest)
        except (httpx.HTTPStatusError, httpx.RequestError) as e:
            console.print(f"  [red]Error[/red]: {e}")

    console.print(f"\n[bold green]{len(results)}[/bold green] archivos descargados.")
    return results
