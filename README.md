# INS Tempo API: Technical integration guide for Transparenta.eu

Romania's National Institute of Statistics (INS) exposes its TEMPO-Online database through a REST JSON API at `http://statistici.insse.ro:8077/tempo-ins/`. **The API requires no authentication, uses SIRUTA codes directly for territorial data, and provides 1,700+ statistical indicators**—making it ideal for enriching Transparenta.eu's 80 million budget records with demographic and economic context at the UAT (local government) level.

The critical discovery for your integration: **INS territorial codes ARE SIRUTA codes**—no transformation or mapping is needed. INS maintains SIRUTA itself, so the statistical database uses the same 6-digit codes as all other Romanian government systems.

---

## OLAP cube structure powers multi-dimensional queries

TEMPO-Online implements a **multi-dimensional OLAP cube model** where each dataset (called a "matrix") contains a measure value (e.g., population count) decomposed across N dimensions (territory, time, age groups, sex, etc.). This hierarchical structure enables flexible slicing and filtering.

**The data hierarchy flows as:**

```
Contexts (8 domains) → Categories (subdomains) → Matrices (datasets) → Dimensions → Options
```

Each matrix stores dimension options with a `nomItemId` (stable numeric identifier), `label` (display text), `offset` (position), and `parentId` (for hierarchical dimensions like age groups or territorial units). The `parentId` field creates tree structures within dimensions—essential for navigating from "Total" aggregates down to specific counties or age brackets.

### The eight statistical domains

| Code | Domain | Relevance to Transparenta.eu |
|------|--------|------------------------------|
| **A** | Social Statistics | Population, demographics, employment—**critical for per-capita calculations** |
| **B** | Economic Statistics | Industry, trade, services, agriculture |
| **C** | Finance | Direct budget/financial indicators |
| **D** | Justice | Court statistics, crime data |
| **E** | Environment | Environmental indicators |
| **F** | Public Utilities & Territorial Administration | Infrastructure, local services—**high UAT relevance** |
| **G** | Sustainable Development (Horizon 2020) | SDG-aligned metrics |
| **H** | Sustainable Development (Targets 2030) | Updated SDG indicators |

---

## API endpoints for programmatic access

The REST API exposes four primary endpoints, all returning JSON:

### Discovery endpoints (GET)

**Root catalog** — `GET /tempo-ins/context/`
Returns all 8 top-level statistical domains with nested children. Response contains arrays of context objects with `id`, `code`, `name`, `level`, `parentCode`, and `children` fields.

**Category details** — `GET /tempo-ins/context/{id}`
Retrieves a specific context node and its children. For example, `/context/1010` returns subcategories under "Population and Demographics."

**Dataset metadata** — `GET /tempo-ins/matrix/{code}`
Returns complete dimension definitions for a dataset. Example response structure for `/matrix/POP105A`:

```json
{
  "matrixName": "POP105A",
  "matrixDescription": "Populatia rezidenta pe grupe de varsta si sexe",
  "lastUpdate": "2024-08-15",
  "startYear": 1990,
  "endYear": 2024,
  "dimensions": [
    {
      "dimensionId": 1,
      "dimensionName": "Varste si grupe de varsta",
      "options": [{"label": "Total", "nomItemId": 1, "offset": 1, "parentId": null}, ...]
    }
  ],
  "matrixDetails": {"nomJud": 0, "nomLoc": 0}
}
```

**Full dataset listing** — `GET /tempo-ins/matrix/matrices?lang=ro`
Returns metadata for all ~1,700 available matrices in a single response.

### Data query endpoint (POST)

**Query dataset** — `POST /tempo-ins/matrix/dataSet/{code}` (JSON) or `POST /tempo-ins/pivot` (CSV)

The query body specifies dimension selections:

```json
{
  "language": "ro",
  "arr": [
    [{"label": "Total", "nomItemId": 1, "offset": 1, "parentId": null}],
    [{"label": "Masculin", "nomItemId": 2, "offset": 1, "parentId": null}],
    [{"label": "Anul 2023", "nomItemId": 34, "offset": 34, "parentId": null}]
  ],
  "matrixName": "POP105A",
  "matrixDetails": {"nomJud": 1, "nomLoc": 0}
}
```

The `arr` array contains one sub-array per dimension, with selected options. The **`matrixDetails` object controls territorial granularity**: `nomJud: 1` includes county-level breakdown; `nomLoc: 1` enables UAT/locality-level data for datasets that support it.

---

## Dimension types and their structure

### Territorial dimensions (SIRUTA-based)

The INS uses **SIRUTA codes directly**—Romania's official 6-digit administrative classification. No mapping or transformation is required.

| Level | NUTS/LAU | Units | Example |
|-------|----------|-------|---------|
| Macroregions | NUTS I | 4 | M1 Nord-Vest |
| Development regions | NUTS II | 8 | Nord-Vest, Centru, etc. |
| Counties (Județe) | NUTS III | 41 + București | Cluj, Timiș, etc. |
| UATs | LAU | **3,222** | Municipalities (103), cities (216), communes (~2,862) |
| Localities | — | ~13,092 | Villages, component localities |

**SIRUTA code structure:**

- 6 digits total: 5-digit unique identifier + 1-digit checksum
- Hierarchical via `SIRSUP` (superior SIRUTA) field linking children to parents
- `JUD` field (1-42) identifies the county
- `TIP` field indicates unit type (1=municipality seat, 3=commune, 40=county, etc.)
- `NIV` field indicates level: 1=county, 2=UAT, 3=locality

### Temporal dimensions

Three granularity patterns exist:

- **Annual**: "Anul 2023", "Anul 2022"
- **Quarterly**: "Trimestrul I 2023"
- **Monthly**: "Ianuarie 2023"

Time options use `nomItemId` for stable identification—preferable to parsing label strings.

### Classification dimensions

Common classification dimensions include:

| Dimension | Example Options |
|-----------|-----------------|
| Sexe (Sex) | Masculin, Feminin, Total |
| Varste si grupe de varsta (Age groups) | 0-4 ani, 5-9 ani, ..., 85+, Total |
| Medii de rezidenta (Area type) | Urban, Rural, Total |
| Activitati CAEN (Economic activities) | NACE Rev.2 classifications |
| Nivel de educatie (Education level) | Primary, secondary, tertiary, etc. |
| UM (Unit of measure) | Numar persoane, Mii lei, etc. |

---

## Identifying UAT-level datasets

**Not all datasets support UAT granularity.** The `matrixDetails` metadata indicates availability:

- `nomJud` = capability for county-level breakdown
- `nomLoc` = capability for locality/UAT-level breakdown

### Confirmed datasets with UAT-level granularity

| Matrix Code | Description | Territorial Depth |
|-------------|-------------|-------------------|
| **POP105A** | Resident population by age, sex | Counties, some UAT |
| **POP105B** | Population by domicile | Localities |
| **POP208C** | Deaths under 1 year | Counties and localities |
| **LOC101B** | Dwellings/housing statistics | Localities |
| **AGR series** | Agricultural data | Often UAT-level |
| **EDU series** | Education facilities | UAT level for some |

### Strategy for discovering UAT datasets

```python
# Pseudocode for finding UAT-capable matrices
for matrix in all_matrices:
    metadata = fetch_matrix_metadata(matrix.code)
    if metadata.matrixDetails.get('nomLoc', 0) > 0:
        print(f"UAT-level available: {matrix.code}")
```

The `gov2-ro/tempo-ins-dump` repository has already catalogued matrices with their territorial capabilities.

---

## Critical implementation constraints

### The 30,000 cell limit

**The API enforces a hard limit of 30,000 cells per query.** Exceeding this returns an error message rather than data. Cell count = product of all selected dimension option counts.

**Mitigation strategies:**

1. **Chunk by time**: Request one year at a time instead of full time series
2. **Chunk by territory**: Request county-by-county when `nomLoc` is enabled
3. **Pre-calculate cell count**: Multiply option counts before querying
4. **Use CSV endpoint**: `/tempo-ins/pivot` sometimes handles larger requests

```python
def estimate_cells(dimension_selections):
    return reduce(lambda a, b: a * len(b), dimension_selections, 1)

if estimate_cells(selections) > 25000:  # Safety margin
    selections = chunk_by_dimension(selections, 'Perioade')
```

### Rate limiting

No explicit rate limit documentation exists, but responsible usage patterns include:

- **0.5-1 second delay** between requests
- **Cache aggressively**: Context hierarchy changes rarely; dataset metadata updates weekly
- **Batch during off-hours** for large exports

### Language parameter

The `language` parameter (`"ro"` or `"en"`) affects label text only—use Romanian (`"ro"`) for consistency with other Romanian government data, or use `nomItemId` values which are language-agnostic.

---

## Mapping INS territorial codes to SIRUTA

**No mapping is required—they are identical.** INS maintains SIRUTA as Romania's official territorial classification. The territorial dimensions in TEMPO datasets use SIRUTA codes directly.

### Obtaining the SIRUTA reference table

Download from `data.gov.ro`:

- **SIRUTA 2024 dataset**: <https://data.gov.ro/dataset/siruta-2024>
- **UAT-specific extract**: <https://data.gov.ro/dataset/unitati-administrativ-teritoriale-coduri-siruta>

The CSV contains these key fields:

| Field | Description | Use Case |
|-------|-------------|----------|
| `SIRUTA` | 6-digit code with checksum | Primary identifier |
| `DENLOC` | Locality name | Display/matching |
| `JUD` | County code (1-42) | County filtering |
| `SIRSUP` | Parent SIRUTA | Hierarchy building |
| `TIP` | Type code | Distinguish UAT types |
| `NIV` | Level (1, 2, 3) | Filter to UAT level (NIV=2) |
| `MED` | Environment (0=rural, 1=urban) | Urban/rural classification |

### For Transparenta.eu integration

Use the `JUD` + `SIRUTA` combination to join INS statistics with your budget records. The 3,222 UATs (NIV=2 in SIRUTA) correspond exactly to Romanian local governments whose budgets you track.

---

## Reference implementations from open-source projects

### tempo.py (Python client)

**Repository**: github.com/mark-veres/tempo.py

Lightweight abstraction providing `Node` and `LeafNode` classes for hierarchy navigation and querying:

```python
from tempo import LeafNode

leaf = LeafNode.by_code('POP105A')
result = leaf.query(
    ('Sexe', ['Total']),
    ('Perioade', ['Anul 2023']),
    ('Macroregiuni, regiuni de dezvoltare si judete', ['Cluj'])
)
```

### QTempo (QGIS plugin)

**Repository**: github.com/alecsandrei/QTempo

Production-ready QGIS plugin that joins TEMPO statistics with geographic boundaries from ANCPI (Romanian Cadaster). Demonstrates spatial matching of SIRUTA codes to administrative polygons—valuable if Transparenta.eu adds geographic visualization.

### tempo-ins-dump (GOV2-RO)

**Repository**: github.com/gov2-ro/tempo-ins-dump

The most comprehensive implementation—a complete ETL pipeline for downloading the entire TEMPO database. Key scripts:

| Script | Output |
|--------|--------|
| `1-fetch-context.py` | `context.csv` (domain hierarchy) |
| `2-fetch-matrices.py` | `matrices.csv` (all dataset codes) |
| `3-fetch-metas.py` | `{code}.json` (dimension definitions) |
| `6-fetch-csv.py` | `{code}.csv` (actual data) |
| `7-data-compactor.py` | Optimized storage format |
| `9-csv-to-parquet.py` | Columnar format for analytics |

This repository documents real-world edge cases including cell limit handling and archived legacy data access.

---

## Priority datasets for civic tech integration

For enriching budget data with demographic and economic context, prioritize:

### Population (essential for per-capita normalization)

| Matrix | Description | Update Frequency |
|--------|-------------|------------------|
| **POP105A** | Resident population by age, sex, territory | Annual (August provisional, December final) |
| **POP105B** | Population by domicile | Annual |
| **Census 2021** | Detailed demographic breakdown | Decennial |

### Economic indicators

| Matrix | Description | Relevance |
|--------|-------------|-----------|
| **FOM116A** | Employment rate by territory | Economic health of UATs |
| **FOM103 series** | Employed persons by sector | Local economic structure |
| **SOM series** | Unemployment statistics | Economic distress indicators |

### Public infrastructure (Domain F)

| Matrix | Description | Relevance |
|--------|-------------|-----------|
| Public utilities | Water, sewerage coverage | Infrastructure investment context |
| Road infrastructure | Road network statistics | Transport spending context |
| Local services | Public service availability | Service delivery context |

### Education and healthcare

| Matrix | Description | Relevance |
|--------|-------------|-----------|
| EDU series | Schools, enrollment, teachers | Education spending context |
| SAN series | Medical facilities, personnel | Healthcare spending context |

---

## Proposed data model for integration

To map the OLAP structure to a relational model suitable for joining with Transparenta.eu's budget data:

```sql
-- Domain/category hierarchy
CREATE TABLE ins_contexts (
    id INTEGER PRIMARY KEY,
    code VARCHAR(10),
    name VARCHAR(255),
    level INTEGER,
    parent_code VARCHAR(10)
);

-- Dataset catalog
CREATE TABLE ins_matrices (
    code VARCHAR(20) PRIMARY KEY,
    name VARCHAR(255),
    description TEXT,
    context_id INTEGER REFERENCES ins_contexts(id),
    start_year INTEGER,
    end_year INTEGER,
    last_update DATE,
    has_county_data BOOLEAN,  -- nomJud
    has_uat_data BOOLEAN      -- nomLoc
);

-- Dimension definitions (per matrix)
CREATE TABLE ins_dimensions (
    id SERIAL PRIMARY KEY,
    matrix_code VARCHAR(20) REFERENCES ins_matrices(code),
    dimension_id INTEGER,
    dimension_name VARCHAR(255),
    is_territorial BOOLEAN,
    is_temporal BOOLEAN
);

-- Dimension option values
CREATE TABLE ins_dimension_options (
    id SERIAL PRIMARY KEY,
    dimension_id INTEGER REFERENCES ins_dimensions(id),
    nom_item_id INTEGER,
    label VARCHAR(255),
    parent_nom_item_id INTEGER  -- For hierarchies
);

-- Fact table (can be per-matrix or unified)
CREATE TABLE ins_statistics (
    id SERIAL PRIMARY KEY,
    matrix_code VARCHAR(20),
    siruta_code VARCHAR(6),   -- Links to UAT
    period_year INTEGER,
    period_quarter INTEGER,   -- NULL for annual
    indicator_value NUMERIC,
    dimension_values JSONB    -- Flexible storage for other dimensions
);

-- Create index for fast UAT lookups
CREATE INDEX idx_stats_siruta ON ins_statistics(siruta_code);
CREATE INDEX idx_stats_period ON ins_statistics(period_year, matrix_code);
```

This schema enables queries like:

```sql
SELECT b.uat_name, b.total_budget, s.indicator_value AS population,
       b.total_budget / s.indicator_value AS budget_per_capita
FROM budget_records b
JOIN ins_statistics s ON b.siruta_code = s.siruta_code
WHERE s.matrix_code = 'POP105A' AND s.period_year = 2023;
```

---

## Conclusion

The INS Tempo API provides a robust, authentication-free pathway to enrich Transparenta.eu with statistical context at the UAT level. **Three key technical facts** enable this integration:

1. **SIRUTA identity**: INS territorial codes equal SIRUTA codes—direct joins with no transformation
2. **nomLoc flag**: The `matrixDetails.nomLoc` parameter identifies which datasets offer UAT granularity
3. **30,000 cell limit**: Query chunking is mandatory for large territorial or time-series requests

The `gov2-ro/tempo-ins-dump` repository provides battle-tested patterns for bulk data ingestion, while `tempo.py` offers a clean abstraction for targeted queries. Begin with POP105A (population) for per-capita calculations, then expand to employment and infrastructure datasets that provide economic context for local government budget analysis.

For implementation, start by fetching the SIRUTA reference table from data.gov.ro and the matrix catalog from `/tempo-ins/matrix/matrices`, then systematically identify and ingest UAT-level datasets that complement your existing budget data.
