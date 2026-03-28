# Posts para Reddit
# Subreddits target + posts adaptados

---

## SUBREDDITS RECOMENDADOS

### Tier 1 (Mexico, alto tráfico)
- r/mexico (~1.5M miembros) — post principal
- r/MexicoCity (~200K) — ángulo CDMX

### Tier 2 (nicho pero relevante)
- r/coNtraCorrupcion — si existe/está activo
- r/mexicopolitics
- r/dataisbeautiful — si la infografía queda bien (post en inglés)

### Tier 3 (tech/data)
- r/datasets — compartir las fuentes
- r/OpenData — el ángulo de datos abiertos

---

## POST PRINCIPAL — r/mexico

**Título:**
Analicé 91,850 contratos en CompraNet: 41.5% del gasto público se asigna sin competencia

**Cuerpo:**

Descargué y procesé los datos públicos de CompraNet 2025 (contratos federales), el PEF (presupuesto federal) y el presupuesto de CDMX.

**Lo que encontré:**

**¿Cómo se contrata?**
- Licitaciones públicas: $381 mil millones (53.9%)
- Adjudicaciones directas: $294 mil millones (41.5%)
- Invitación a 3+: $40 mil millones (5.7%)

Casi la mitad del dinero no pasa por proceso competitivo.

**Los top 10 proveedores por adjudicación directa son todos farmacéuticas:**
1. Boehringer Ingelheim — $8,495M (116 contratos, 100% directos)
2. Roche — $8,083M (100% directos)
3. Gilead Sciences — $5,323M (100% directos)

Justificación legal: "patentes, licencias, oferente único". Es legal, pero el volumen es masivo.

**Concentración:**
- Top 500 proveedores (de 30,703) se llevan el 80% del dinero
- Top 20 = 25.9% del gasto total

**Anomalías detectadas: 612**
- 200 casos de posible fragmentación (dividir contratos para evitar licitación)
- 100 "proveedores fantasma" (1-2 contratos, montos millonarios)
- Empresas clasificadas como MICRO con contratos de $4,000+ millones

**"Caso fortuito o fuerza mayor" = $86 mil millones**
7,303 contratos justificados como emergencia. El más grande: AstraZeneca $5,651M con el IMSS.

**CDMX:**
40.3% del presupuesto se va en nómina ($108 mil millones). Seguridad es el mayor rubro: $46 mil millones entre SSC, Policía Auxiliar y Bancaria.

**Instituciones más opacas (% adj. directa):**
- Dos Bocas: 99%
- Banco del Bienestar: 87%
- SAT: 74%
- Marina: 60%
- IMSS: 57%

---

**Nota:** Esto no implica ilegalidad. Señala patrones que merecen escrutinio público. Todos los datos son públicos y descargados de:
- transparenciapresupuestaria.gob.mx
- CompraNet (upcp-compranet.buengobierno.gob.mx)
- datos.cdmx.gob.mx

El código es abierto. Si quieren verificar o contribuir, me dicen.

---

## POST CDMX — r/MexicoCity

**Título:**
¿A dónde van los $268 mil millones del presupuesto de CDMX? Procesé los datos públicos.

**Cuerpo:**

Descargué los datos abiertos del presupuesto de egresos de CDMX 2024 desde datos.cdmx.gob.mx.

**Por capítulo de gasto:**
- 40.3% → Nómina ($108 mil millones)
- 21.3% → Transferencias y subsidios
- 18.9% → Servicios generales
- 6.8% → Inversión pública
- 6.2% → Materiales

**Top dependencias:**
1. Secretaría de Seguridad Ciudadana — $26,179M (9.8%)
2. Metro — $20,551M (7.7%)
3. Sistema de Aguas — $13,266M (5.0%)
4. Secretaría de Salud — $13,128M (4.9%)
5. Policía Auxiliar — $12,866M (4.8%)

Si sumamos todo lo de seguridad (SSC + Policía Auxiliar + Policía Bancaria + Fiscalía) = $54 mil millones. 20% del presupuesto total.

**Alcaldías:**
- Iztapalapa es la que más recibe: $6,646M
- Gustavo A. Madero: $5,420M
- Cuauhtémoc: $3,816M

40 centavos de cada peso se van en sueldos. 7 centavos en inversión real.

Datos: datos.cdmx.gob.mx/dataset/presupuesto-de-egresos

---

## POST — r/dataisbeautiful (inglés)

**Título:**
[OC] I analyzed 91,850 Mexican government contracts ($707B MXN): 41.5% awarded without competition

**Cuerpo:**

I built an automated pipeline to download and analyze Mexico's public procurement data (CompraNet 2025).

Key findings:
- 41.5% of spending ($294B MXN / ~$15B USD) goes through direct awards (no bidding)
- Top 500 suppliers (of 30,703) capture 80% of all spending
- Top 10 direct-award recipients are all pharmaceutical companies (Boehringer $8.5B, Roche $8B, Gilead $5.3B)
- Algorithm detected 612 anomalies: contract splitting, ghost suppliers, statistical outliers
- "Emergency" contracts: $86B MXN across 7,303 contracts

Data sources: transparenciapresupuestaria.gob.mx, CompraNet (compranet.buengobierno.gob.mx)
Tools: Python, SQLite, pandas

---

# TIPS PARA REDDIT CON KARMA BAJO

1. NO postear todo el mismo día — espacia 2-3 días entre subreddits
2. Empieza con r/mexico (más fácil que te acepten)
3. Comenta en otros posts del subreddit ANTES de postear (1-2 días)
4. Si te piden fuentes, contesta rápido y con links directos
5. No pongas links en el post principal — eso baja el ranking
6. Las imágenes/infografías DENTRO del post tienen mejor engagement
7. Postear entre 8-11am hora CDMX (máximo tráfico en r/mexico)
8. Si te borran por karma bajo, espera y vuelve a intentar en 1 semana
9. r/dataisbeautiful requiere tag [OC] y descripción de herramientas
