# gasto-publico-mx

AI crawler para rastrear el gasto público en México — del PEF a la última factura.

## Fuentes de datos

| Fuente | Datos | Cobertura |
|--------|-------|-----------|
| Transparencia Presupuestaria | PEF aprobado/ejercido | 2013-2026 |
| CompraNet/ComprasMX | Contratos federales | 2010-2025 |
| datos.cdmx.gob.mx | Presupuesto CDMX | 2023+ |
| OCDS CDMX | Contrataciones abiertas | 2021-2025 |
| datos.gob.mx | Catálogo federal | Varía |

## Instalación

```bash
cd gasto-publico-mx
pip install -e ".[dev]"
```

## Uso

```bash
# Descargar PEF 2025
gasto crawl pef --year 2025

# Descargar contratos CompraNet 2024
gasto crawl compranet --year 2024

# Descargar presupuesto CDMX
gasto crawl cdmx

# Descargar todo
gasto crawl all

# Buscar datasets en datos.gob.mx
gasto search "presupuesto egresos"

# Ver qué se ha descargado
gasto stats

# Migrar base de datos
gasto db migrate
```

## Arquitectura

```
Hacienda (SHCP)  →  PEF aprobado
       ↓
Ramos/Secretarías  →  Ejecución trimestral
       ↓
Transferencias  →  Participaciones a CDMX
       ↓
Presupuesto CDMX  →  Por alcaldía/dependencia
       ↓
Contratos  →  CompraNet: quién ganó, cuánto
       ↓
Auditorías  →  ASF: irregularidades
```
