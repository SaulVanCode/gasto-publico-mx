from src.loaders.pef_loader import load_pef, load_all as load_all_pef
from src.loaders.compranet_loader import load_compranet, load_all as load_all_compranet
from src.loaders.cdmx_loader import load_cdmx, load_all as load_all_cdmx

__all__ = [
    "load_pef", "load_all_pef",
    "load_compranet", "load_all_compranet",
    "load_cdmx", "load_all_cdmx",
]
