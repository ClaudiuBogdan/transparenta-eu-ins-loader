## Overview

Before implementing the INS data integration, we need a complete understanding of INS's data model. The INS Tempo API uses a multi-dimensional OLAP cube structure that must be mapped to our relational entities.

---

## Tech Requirements

### Stack

| Component | Technology | Version |
|-----------|------------|---------|
| Runtime | Node.js | 20+ LTS |
| Language | TypeScript | 5.x (strict) |
| Framework | Fastify | 5.x |
| Database | SQLite | via better-sqlite3 |
| Query Builder | Kysely | 0.27.x |
| HTTP Client | undici | Built-in Node.js |
| CLI | Commander.js | Latest |
| Testing | Vitest | Latest |
| Package Manager | pnpm | Latest |

### Architecture

**Components:**
1. **REST API Server** (Fastify) - Serves data, exposes navigation endpoints
2. **CLI Client** (Commander.js) - Connects to REST API for manual testing
3. **Scraper Module** - INS Tempo API client with rate limiting & chunking
4. **Database Layer** - SQLite + Kysely query builder with type-safe queries

**Design Decisions:**
- Manual scrape triggering (job queue can be added later)
- Generic framework supporting any INS matrix
- Data navigation system for exploring INS catalog before scraping

### API Design

#### Navigation Endpoints
- `GET /api/contexts` - List 8 statistical domains
- `GET /api/contexts/:id` - Context details & children
- `GET /api/matrices` - List matrices (filterable by UAT capability)
- `GET /api/matrices/:code` - Matrix metadata & dimensions

#### Scraping Endpoints
- `POST /api/scrape/matrix/:code` - Trigger matrix scrape

#### Data Endpoints
- `GET /api/data/:matrixCode` - Query scraped statistics
- `GET /api/siruta` - Query SIRUTA reference data

### CLI Commands

```bash
# Navigation
ins-cli contexts                    # List domains
ins-cli matrices --uat-only         # List UAT-capable matrices
ins-cli matrix POP105A              # Show matrix details

# Scraping
ins-cli scrape POP105A --year=2023  # Scrape specific matrix

# Data
ins-cli query POP105A --siruta=123456
```

### Database Schema

- `ins_contexts` - Domain hierarchy (8 domains + subcategories)
- `ins_matrices` - Dataset catalog (~1,700 matrices)
- `ins_dimensions` - Dimension definitions per matrix
- `ins_dimension_options` - Dimension values (nomItemId, label, parentId)
- `ins_statistics` - Scraped fact data (siruta, period, value)
- `siruta` - SIRUTA reference table (3,222 UATs)

### Key Implementation Details

**Rate Limiting:**
- 0.5-1 second delay between INS API requests
- Aggressive caching of context/matrix metadata

**30,000 Cell Limit Handling:**
- Pre-calculate cell count before queries
- Chunk by time dimension (year-by-year)
- Chunk by territory when needed

**SIRUTA Integration:**
- Fetch CSV from data.gov.ro on demand
- Store in SQLite for fast joins
- No mapping needed (INS uses SIRUTA directly)

---

## Background

INS (Institutul Național de Statistică) provides statistical data through their Tempo API. The API exposes a hierarchical catalog of datasets (matrices) with multiple dimensions (territorial, temporal, demographic, etc.).

## Tasks

- [ ] **Analyze INS Tempo API structure** using the discovery endpoints:
  - `http://statistici.insse.ro:8077/tempo-ins/context/` - root catalog
  - `http://statistici.insse.ro:8077/tempo-ins/context/{id}` - category details
  - `http://statistici.insse.ro:8077/tempo-ins/matrix/{code}` - dataset matrix
- [ ] **Document the hierarchical structure**: contexts → categories → matrices → dimensions
- [ ] **Identify all dimension types**:
  - Territorial (SIRUTA codes, counties, regions)
  - Temporal (annual, quarterly, monthly)
  - Indicators (population, employment, etc.)
  - Classifications (gender, age groups, area type)
- [ ] **Map INS territorial codes to SIRUTA codes** (critical for UAT linkage)
- [ ] **Document temporal granularity options** (annual, quarterly, monthly)
- [ ] **Identify datasets with UAT-level data** vs. county/national only
- [ ] **Create a data dictionary** with field descriptions
- [ ] **Study reference projects** for API parsing patterns:
  - [tempo.py](https://github.com/mark-veres/tempo.py) - Python client
  - [QTempo](https://github.com/alecsandrei/QTempo) - QGIS plugin (comprehensive coverage)
  - [Prometeu](https://github.com/gov2-ro/prometeu) - Romanian gov data tools

## Deliverables

1. Technical documentation explaining INS data model
2. Mapping table: INS territorial codes ↔ SIRUTA codes
3. List of datasets with UAT-level granularity
4. List of relevant statistical indicators for Transparenta.eu

## Acceptance Criteria

- [ ] Technical documentation explains cube structure clearly
- [ ] All dimension types identified and documented
- [ ] INS territorial codes mapped to SIRUTA
- [ ] UAT-level datasets identified

## Resources

- INS Tempo Online: <http://statistici.insse.ro:8077/tempo-online/>
- Gov2.ro Tempo Browser: <https://tempo-online.gov2.ro/2/category-browser.html>
