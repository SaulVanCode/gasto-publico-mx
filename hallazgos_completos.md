# Hallazgos Completos — Gasto Público México 2025
## Con comparativa 2024 y desglose de emergencias

---

## NUEVO: Comparativa 2024 vs 2025

|                          | 2024           | 2025           | Cambio        |
|--------------------------|----------------|----------------|---------------|
| Contratos                | 143,542        | 91,850         | **-36%**      |
| Monto total              | $402.6 mmd     | $707.8 mmd     | **+75.8%**    |
| Adj. directas (%)        | 40.5%          | 41.5%          | +1.0 pp       |
| Adj. directas (monto)    | $163.0 mmd     | $293.9 mmd     | **+80.3%**    |
| Caso fortuito (%)        | 6.9%           | 15.0%          | **+8.1 pp**   |
| Caso fortuito (monto)    | $27.6 mmd      | $106.2 mmd     | **+284.8%**   |
| Proveedores únicos       | 38,139         | 30,703         | **-19.5%**    |
| Top 500 acaparan         | 60.2%          | 79.9%          | **+19.7 pp**  |

### Lectura clave:
- **36% menos contratos pero 76% más dinero** = contratos más grandes, menos granulares
- **"Caso fortuito" se triplicó** de $27.6B a $106.2B (+285%)
- **La concentración subió 20 puntos**: de 60% a 80% en top 500
- **7,400 proveedores menos** participando = mercado más cerrado

---

## NUEVO: Caso Fortuito — ¿Salud o infraestructura?

Total: $106,152 millones MXN en 7,303 contratos

### Por tipo de contratación:
| Tipo              | Contratos | Monto         | %     |
|-------------------|-----------|---------------|-------|
| Adquisiciones     | 8,302     | $77,908M      | 73.4% |
| Servicios         | 1,478     | $25,658M      | 24.2% |
| Obra pública      | 81        | $2,441M       | 2.3%  |

### Por tema (keywords en descripción):
| Tema            | Contratos | Monto         | %     |
|-----------------|-----------|---------------|-------|
| Salud (general) | 2,680     | $52,622M      | 49.6% |
| Medicamentos    | 5,507     | $27,211M      | 25.6% |
| Hospitales      | 841       | $13,574M      | 12.8% |
| Seguridad       | 84        | $3,984M       | 3.8%  |
| Mantenimiento   | 227       | $3,539M       | 3.3%  |
| Limpieza        | 42        | $2,612M       | 2.5%  |
| Alimentación    | 50        | $2,462M       | 2.3%  |
| Transporte      | 56        | $1,887M       | 1.8%  |

### Veredicto:
**~88% es salud** (medicamentos, hospitales, servicios médicos). Solo 2.3% es obra pública.

Esto cambia la narrativa: no es corrupción obvia en carreteras, es un **sistema de salud que funciona en modo emergencia permanente**. El IMSS solo representa el 85% de estos contratos de emergencia.

La pregunta real: ¿por qué el sistema de salud lleva años comprando medicamentos como "emergencia"? ¿Falla la planeación o es un mecanismo para evitar licitaciones?

---

## NUEVO: Empresas MICRO — Fecha de constitución

| Empresa | Constituida | Monto | Tipo | Con quién |
|---------|-------------|-------|------|-----------|
| Comercializadora de Seguridad | 2015 | $4,342M | Licitación | CONAGUA |
| Ingentec Constructora | 2011 | $727M | Adj. directa emergencia | CAPUFE |
| Servicios Bioseguridad | **2019** | $645M | Licitación | IMSS-Bienestar |
| Proquímica Alta Especialidad | **2020** | $332M | Adj. directa emergencia | IMSS |
| Collective Sublackeyes | **2022** | $237M | Licitación | IMSS |
| Grupo Industrial Mayani | **2023** | $205M | Adj. directa urgencia | IMSS-Bienestar |
| Innovación Médica MX | **2021** | $194M | Adj. directa patentes | IMSS |

### Red flags específicos:
- **Collective Sublackeyes** (constituida 2022, MICRO): contrato de $237M con IMSS para "transporte aéreo ala fija, ala rotativa y ambulancias terrestres". Empresa de 3 años clasificada como micro proveyendo transporte aéreo.
- **Grupo Industrial Mayani** (constituida 2023, MICRO): $205M por adj. directa de urgencia con IMSS-Bienestar para "mantenimiento de aires acondicionados". Empresa de 2 años.
- **Proquímica** (constituida 2020, MICRO): $332M por emergencia para compra de sitagliptina (medicamento para diabetes).

---

## Fuentes

Todos los datos son públicos y verificables:

1. **Transparencia Presupuestaria** — transparenciapresupuestaria.gob.mx
   - PEF 2025 (CSV, 87.5 MB, 151,647 partidas)

2. **CompraNet / ComprasMX** — upcp-compranet.buengobierno.gob.mx
   - Contratos 2025 (CSV, 110 MB, 91,850 contratos)
   - Contratos 2024 (CSV, 168 MB, 143,542 contratos)

3. **datos.cdmx.gob.mx** — Portal de datos abiertos CDMX
   - Presupuesto de Egresos 2024 (CKAN API, 19,453 partidas)

4. **datos.gob.mx** — Catálogo Nacional de Datos Abiertos (CKAN API)

5. **OCDS / Open Contracting** — data.open-contracting.org
   - Contrataciones abiertas CDMX (publicación #111)

6. **Marco legal de referencia:**
   - LAASSP Art. 41 (excepciones a licitación)
   - LOPSRM (obra pública)
   - Umbrales de licitación federal 2025

### Código abierto
- GitHub: github.com/SaulVanCode/gasto-publico-mx
- Stack: Python 3.11, SQLite, pandas, httpx, click
- Reproducible: `pip install -e .` → `gasto crawl all` → `gasto load all` → `gasto analyze`
