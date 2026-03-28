"""
Crawler para CompraNet / ComprasMX (upcp-compranet.buengobierno.gob.mx).

Descarga CSVs anuales de contratos y expedientes de licitaciones públicas federales.
"""

from __future__ import annotations

from pathlib import Path

import httpx
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn

console = Console()

BASE_URL = "https://upcp-compranet.buengobierno.gob.mx/cnetassets/datos_abiertos_contratos_expedientes"

DATASET_PATTERNS = {
    "contratos": "{base}/Contratos_CompraNet{year}.csv",
    "expedientes": "{base}/Expedientes_PICompraNet{year}.csv",
}

# CompraNet histórico en datos.gob.mx (2010-2022)
HISTORICO_URL = "https://datos.gob.mx/busca/dataset/contratos-expedientes-sistema-historico-compranet"

YEARS_AVAILABLE = range(2015, 2027)


def download_compranet(
    year: int,
    dest_dir: Path,
    dataset_type: str = "contratos",
    force: bool = False,
    timeout: float = 600.0,
) -> Path | None:
    """Descarga un CSV de CompraNet para un año específico."""
    if dataset_type not in DATASET_PATTERNS:
        raise ValueError(f"Tipo no soportado: {dataset_type}. Usa: {list(DATASET_PATTERNS)}")

    url = DATASET_PATTERNS[dataset_type].format(base=BASE_URL, year=year)
    filename = url.split("/")[-1]
    dest = dest_dir / filename

    if dest.exists() and not force:
        console.print(f"  [dim]Ya existe: {filename} ({dest.stat().st_size / 1e6:.1f} MB)[/dim]")
        return dest

    console.print(f"  Descargando [bold]{filename}[/bold] ...")

    try:
        with httpx.stream("GET", url, timeout=timeout, follow_redirects=True) as r:
            if r.status_code == 404:
                console.print(f"  [yellow]No disponible: {filename}[/yellow]")
                return None
            r.raise_for_status()

            total = int(r.headers.get("content-length", 0))
            dest.parent.mkdir(parents=True, exist_ok=True)

            with open(dest, "wb") as f, Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("{task.completed:.1f}/{task.total:.1f} MB"),
            ) as progress:
                task = progress.add_task(filename, total=total / 1e6 if total else 0)
                downloaded = 0
                for chunk in r.iter_bytes(chunk_size=65536):
                    f.write(chunk)
                    downloaded += len(chunk)
                    progress.update(task, completed=downloaded / 1e6)

        size_mb = dest.stat().st_size / 1e6
        console.print(f"  [green]OK[/green]: {filename} ({size_mb:.1f} MB)")
        return dest

    except httpx.HTTPStatusError as e:
        console.print(f"  [red]Error HTTP {e.response.status_code}[/red]: {url}")
        return None
    except httpx.RequestError as e:
        console.print(f"  [red]Error de conexión[/red]: {e}")
        return None


def crawl_all(
    dest_dir: Path,
    years: range | list[int] | None = None,
    dataset_type: str = "contratos",
    force: bool = False,
) -> list[Path]:
    """Descarga contratos CompraNet para todos los años disponibles."""
    years = years or YEARS_AVAILABLE
    dest_dir.mkdir(parents=True, exist_ok=True)

    console.print(f"\n[bold]CompraNet — {dataset_type.capitalize()}[/bold]")
    console.print(f"Años: {min(years)}-{max(years)}, destino: {dest_dir}\n")

    results = []
    for year in years:
        path = download_compranet(year, dest_dir, dataset_type=dataset_type, force=force)
        if path:
            results.append(path)

    console.print(f"\n[bold green]{len(results)}[/bold green] archivos descargados.")
    return results
