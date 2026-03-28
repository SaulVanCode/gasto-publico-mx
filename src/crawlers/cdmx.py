"""
Crawler para datos.cdmx.gob.mx (CKAN API).

Descarga presupuesto de egresos de CDMX y contrataciones abiertas (OCDS).
"""

from __future__ import annotations

from pathlib import Path

import httpx
from rich.console import Console

console = Console()

CKAN_BASE = "https://datos.cdmx.gob.mx/api/3/action"
DATASET_SLUG = "presupuesto-de-egresos"

# OCDS — Contrataciones abiertas CDMX
OCDS_BASE = "https://data.open-contracting.org/en/publication/111/download"
OCDS_YEARS = range(2021, 2027)


def _fetch_ckan_resources(dataset: str, timeout: float = 30.0) -> list[dict]:
    """Consulta CKAN API para obtener URLs de recursos de un dataset."""
    url = f"{CKAN_BASE}/package_show?id={dataset}"
    console.print(f"  Consultando CKAN: [bold]{dataset}[/bold] ...")

    try:
        r = httpx.get(url, timeout=timeout, follow_redirects=True)
        r.raise_for_status()
        data = r.json()
        if not data.get("success"):
            console.print(f"  [red]CKAN error[/red]: {data.get('error', 'unknown')}")
            return []
        return data["result"].get("resources", [])
    except (httpx.RequestError, httpx.HTTPStatusError) as e:
        console.print(f"  [red]Error CKAN[/red]: {e}")
        return []


def _download_file(url: str, dest: Path, force: bool = False, timeout: float = 300.0) -> Path | None:
    """Descarga un archivo genérico."""
    if dest.exists() and not force:
        console.print(f"  [dim]Ya existe: {dest.name} ({dest.stat().st_size / 1e6:.1f} MB)[/dim]")
        return dest

    console.print(f"  Descargando [bold]{dest.name}[/bold] ...")
    try:
        with httpx.stream("GET", url, timeout=timeout, follow_redirects=True) as r:
            if r.status_code == 404:
                console.print(f"  [yellow]No disponible: {dest.name}[/yellow]")
                return None
            r.raise_for_status()
            dest.parent.mkdir(parents=True, exist_ok=True)
            with open(dest, "wb") as f:
                for chunk in r.iter_bytes(chunk_size=65536):
                    f.write(chunk)

        size_mb = dest.stat().st_size / 1e6
        console.print(f"  [green]OK[/green]: {dest.name} ({size_mb:.1f} MB)")
        return dest

    except (httpx.HTTPStatusError, httpx.RequestError) as e:
        console.print(f"  [red]Error[/red]: {e}")
        return None


def crawl_presupuesto(dest_dir: Path, force: bool = False) -> list[Path]:
    """Descarga todos los recursos del dataset de presupuesto de egresos CDMX."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    console.print(f"\n[bold]CDMX — Presupuesto de Egresos[/bold]")
    console.print(f"Destino: {dest_dir}\n")

    resources = _fetch_ckan_resources(DATASET_SLUG)
    if not resources:
        console.print("  [yellow]No se encontraron recursos[/yellow]")
        return []

    results = []
    for res in resources:
        url = res.get("url", "")
        name = res.get("name", "") or url.split("/")[-1]
        fmt = res.get("format", "").lower()

        # Solo descargar CSV y Excel
        if fmt not in ("csv", "xlsx", "xls", "json"):
            continue

        import re
        import unicodedata

        ext = fmt if fmt != "xlsx" else "xlsx"
        # Normalizar unicode y eliminar caracteres no válidos en Windows
        safe_name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
        safe_name = re.sub(r'[^\w\s\-.]', '_', safe_name).replace(" ", "_")
        if not safe_name.endswith(f".{ext}"):
            safe_name = f"{safe_name}.{ext}"

        dest = dest_dir / safe_name
        path = _download_file(url, dest, force=force)
        if path:
            results.append(path)

    console.print(f"\n[bold green]{len(results)}[/bold green] archivos descargados.")
    return results


def crawl_ocds(dest_dir: Path, years: range | list[int] | None = None, force: bool = False) -> list[Path]:
    """Descarga datos OCDS (contrataciones abiertas) de CDMX por año."""
    years = years or OCDS_YEARS
    dest_dir.mkdir(parents=True, exist_ok=True)

    console.print(f"\n[bold]CDMX — Contrataciones Abiertas (OCDS)[/bold]")
    console.print(f"Años: {min(years)}-{max(years)}, destino: {dest_dir}\n")

    results = []
    for year in years:
        url = f"{OCDS_BASE}?name={year}.jsonl.gz"
        dest = dest_dir / f"cdmx_ocds_{year}.jsonl.gz"
        path = _download_file(url, dest, force=force)
        if path:
            results.append(path)

    console.print(f"\n[bold green]{len(results)}[/bold green] archivos descargados.")
    return results
