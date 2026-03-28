"""
CLI principal: gasto-publico-mx

Uso:
    gasto crawl pef [--year 2025] [--force]
    gasto crawl compranet [--year 2025] [--type contratos]
    gasto crawl cdmx [--force]
    gasto crawl ocds [--year 2024]
    gasto crawl all [--force]
    gasto search <query>         # busca datasets en datos.gob.mx
    gasto db migrate             # ejecuta migraciones SQL
    gasto stats                  # resumen de datos descargados
"""

from __future__ import annotations

from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

console = Console()

DATA_DIR = Path(__file__).parent.parent / "data" / "raw"


@click.group()
def cli():
    """Rastreador de gasto público en México."""
    pass


# ── Crawl ────────────────────────────────────────────────────

@cli.group()
def crawl():
    """Descarga datos de fuentes públicas."""
    pass


@crawl.command("pef")
@click.option("--year", "-y", type=int, default=None, help="Año específico (default: todos)")
@click.option("--type", "dataset_type", type=click.Choice(["pef", "ppef"]), default="pef")
@click.option("--force", is_flag=True, help="Re-descargar aunque ya exista")
def crawl_pef(year, dataset_type, force):
    """Descarga PEF de Transparencia Presupuestaria."""
    from src.crawlers.transparencia import crawl_all, download_pef

    dest = DATA_DIR / "transparencia"
    if year:
        download_pef(year, dest, dataset_type=dataset_type, force=force)
    else:
        crawl_all(dest, dataset_type=dataset_type, force=force)


@crawl.command("compranet")
@click.option("--year", "-y", type=int, default=None)
@click.option("--type", "dataset_type", type=click.Choice(["contratos", "expedientes"]), default="contratos")
@click.option("--force", is_flag=True)
def crawl_compranet(year, dataset_type, force):
    """Descarga contratos de CompraNet."""
    from src.crawlers.compranet import crawl_all, download_compranet

    dest = DATA_DIR / "compranet"
    if year:
        download_compranet(year, dest, dataset_type=dataset_type, force=force)
    else:
        crawl_all(dest, dataset_type=dataset_type, force=force)


@crawl.command("cdmx")
@click.option("--force", is_flag=True)
def crawl_cdmx(force):
    """Descarga presupuesto de CDMX."""
    from src.crawlers.cdmx import crawl_presupuesto

    dest = DATA_DIR / "cdmx"
    crawl_presupuesto(dest, force=force)


@crawl.command("ocds")
@click.option("--year", "-y", type=int, default=None)
@click.option("--force", is_flag=True)
def crawl_ocds(year, force):
    """Descarga contrataciones abiertas CDMX (OCDS)."""
    from src.crawlers.cdmx import crawl_ocds

    dest = DATA_DIR / "ocds"
    years = [year] if year else None
    crawl_ocds(dest, years=years, force=force)


@crawl.command("all")
@click.option("--force", is_flag=True)
def crawl_all_sources(force):
    """Descarga todo: PEF + CompraNet + CDMX + OCDS."""
    from src.crawlers.transparencia import crawl_all as crawl_tp
    from src.crawlers.compranet import crawl_all as crawl_cn
    from src.crawlers.cdmx import crawl_presupuesto, crawl_ocds

    crawl_tp(DATA_DIR / "transparencia", force=force)
    crawl_cn(DATA_DIR / "compranet", force=force)
    crawl_presupuesto(DATA_DIR / "cdmx", force=force)
    crawl_ocds(DATA_DIR / "ocds", force=force)

    console.print("\n[bold green]Descarga completa.[/bold green]")


# ── Search ───────────────────────────────────────────────────

@cli.command("search")
@click.argument("query")
def search_datasets(query):
    """Busca datasets en datos.gob.mx."""
    from src.crawlers.datos_gob import list_datasets

    list_datasets(query)


# ── DB ───────────────────────────────────────────────────────

@cli.group()
def db():
    """Operaciones de base de datos."""
    pass


@db.command("migrate")
@click.option("--schema-dir", default="schema", help="Directorio con archivos SQL")
def db_migrate(schema_dir):
    """Ejecuta migraciones SQL."""
    from src.db.connection import run_migrations

    run_migrations(schema_dir)
    console.print("[bold green]Migraciones ejecutadas.[/bold green]")


# ── Stats ────────────────────────────────────────────────────

@cli.command("stats")
def stats():
    """Muestra resumen de datos descargados."""
    table = Table(title="Datos descargados")
    table.add_column("Fuente", style="cyan")
    table.add_column("Archivos", justify="right")
    table.add_column("Tamaño total", justify="right")

    for source in ["transparencia", "compranet", "cdmx", "ocds"]:
        source_dir = DATA_DIR / source
        if not source_dir.exists():
            table.add_row(source, "0", "—")
            continue

        files = list(source_dir.iterdir())
        total_size = sum(f.stat().st_size for f in files if f.is_file())
        size_str = f"{total_size / 1e6:.1f} MB" if total_size > 0 else "—"
        table.add_row(source, str(len(files)), size_str)

    console.print(table)


if __name__ == "__main__":
    cli()
