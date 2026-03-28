"""
CLI principal: gasto-publico-mx

Uso:
    gasto crawl pef/compranet/cdmx/ocds/all
    gasto load pef/compranet/cdmx/all
    gasto analyze                    # detecta anomalías
    gasto report                     # reporte de anomalías
    gasto flow                       # flujo PEF → contratos
    gasto search <query>             # busca en datos.gob.mx
    gasto db migrate                 # crea tablas
    gasto stats                      # resumen de datos
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
@click.option("--year", "-y", type=int, default=None)
@click.option("--type", "dataset_type", type=click.Choice(["pef", "ppef"]), default="pef")
@click.option("--force", is_flag=True)
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


# ── Load ─────────────────────────────────────────────────────

@cli.group()
def load():
    """Carga datos descargados a la base de datos."""
    pass


@load.command("pef")
def load_pef_cmd():
    """Carga CSVs de PEF a la base de datos."""
    from src.db.connection import run_migrations
    from src.loaders.pef_loader import load_all

    run_migrations()
    load_all(DATA_DIR / "transparencia")


@load.command("compranet")
def load_compranet_cmd():
    """Carga CSVs de CompraNet a la base de datos."""
    from src.db.connection import run_migrations
    from src.loaders.compranet_loader import load_all

    run_migrations()
    load_all(DATA_DIR / "compranet")


@load.command("cdmx")
def load_cdmx_cmd():
    """Carga CSVs de CDMX a la base de datos."""
    from src.db.connection import run_migrations
    from src.loaders.cdmx_loader import load_all

    run_migrations()
    load_all(DATA_DIR / "cdmx")


@load.command("all")
def load_all_cmd():
    """Carga todos los datos a la base de datos."""
    from src.db.connection import run_migrations
    from src.loaders.pef_loader import load_all as load_pef
    from src.loaders.compranet_loader import load_all as load_cn
    from src.loaders.cdmx_loader import load_all as load_cdmx

    run_migrations()
    load_pef(DATA_DIR / "transparencia")
    load_cn(DATA_DIR / "compranet")
    load_cdmx(DATA_DIR / "cdmx")
    console.print("\n[bold green]Carga completa.[/bold green]")


# ── Analyze ──────────────────────────────────────────────────

@cli.command("analyze")
def analyze():
    """Ejecuta detección de anomalías sobre los datos cargados."""
    from src.anomalias import run_all_checks, report_anomalias

    results = run_all_checks()
    console.print()
    report_anomalias()


# ── Report ───────────────────────────────────────────────────

@cli.command("report")
@click.option("--limit", "-n", default=50, help="Número máximo de anomalías a mostrar")
def report(limit):
    """Muestra reporte de anomalías detectadas."""
    from src.anomalias import report_anomalias

    report_anomalias(limit=limit)


# ── Flow ─────────────────────────────────────────────────────

@cli.command("flow")
def flow():
    """Linkea y muestra flujo PEF → Contratos."""
    from src.linkeo import linkear_por_ramo, report_flujo

    console.print("[bold]Linkeando presupuesto con contratos...[/bold]")
    linkear_por_ramo()
    console.print()
    report_flujo()


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
def db_migrate():
    """Ejecuta migraciones SQL."""
    from src.db.connection import run_migrations

    run_migrations()
    console.print("[bold green]Migraciones ejecutadas.[/bold green]")


@db.command("info")
def db_info():
    """Muestra info de la base de datos."""
    from src.db.connection import get_connection, get_sqlite_path

    console.print(f"DB: {get_sqlite_path()}")
    conn = get_connection()

    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()

    table = Table(title="Tablas en DB")
    table.add_column("Tabla", style="cyan")
    table.add_column("Filas", justify="right")
    table.add_column("Tamaño aprox.", justify="right")

    for t in tables:
        name = t["name"]
        count = conn.execute(f"SELECT COUNT(*) FROM [{name}]").fetchone()[0]
        table.add_row(name, f"{count:,}", "")

    console.print(table)

    # Tamaño del archivo
    db_path = get_sqlite_path()
    if db_path.exists():
        size_mb = db_path.stat().st_size / 1e6
        console.print(f"\nTamaño total DB: {size_mb:.1f} MB")

    conn.close()


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
