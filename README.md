# Transparenta EU INS Loader

Romanian statistical data loader from INS (Institutul National de Statistica) Tempo API for [Transparenta.eu](https://transparenta.eu). Syncs data from the INS Tempo API and exposes it through a modern, developer-friendly REST interface.

## Overview

This is a **Romanian statistical data loader** that pulls data from the INS Tempo API (Romania's national statistics institute) and exposes it through a REST API. The INS API uses an **OLAP cube model** with ~1,898 matrices (datasets) covering population, economy, education, health, etc.

### The Three Layers

1. **Scraper** (`src/scraper/client.ts`) - Rate-limited client hitting the INS API (750ms delay between requests)
2. **REST API** (`src/server/`) - Fastify server exposing endpoints at `/api/v1/*`
3. **CLI** (`src/cli/`) - Commander.js commands for syncing, exploring, and managing data

### Key Architectural Highlights

**The 30,000 Cell Problem**
The INS API caps queries at 30,000 cells. The chunking system (`src/services/sync/chunking.ts`) adaptively splits queries by time → county → classification to stay under this limit.

**Lease-Based Distributed Sync**
Workers acquire 2-minute auto-expiring leases on chunks, enabling distributed syncing without complex coordination.

**Entity Resolution Pipeline**
The `LabelResolver` maps dimension labels (e.g., "București") to canonical entities (territories, time periods, classifications) with confidence tracking.

**Partitioned Statistics Table**
Pre-created 2,000 partitions by `matrix_id` for the main fact table - optimized for parallel inserts.

## Features

- **1,898 statistical datasets** covering population, economy, education, health, and more
- **Time series data** with year-over-year trends and growth rates
- **Territorial hierarchy** from national to county level (NUTS classification)
- **Analytics endpoints** for rankings, distributions, correlations, and pivot tables
- **Cursor-based pagination** for efficient data retrieval
- **PostgreSQL** with partitioned tables for performance

## Quick Start

### Prerequisites

- Node.js 20+
- pnpm
- PostgreSQL 15+

### Setup

```bash
# Install dependencies
pnpm install

# Configure database (copy and edit .env)
cp .env.example .env
```

### Database Setup

The project uses PostgreSQL 16+ with extensions for hierarchical data and text search. Use Docker for the easiest setup:

```bash
# Start PostgreSQL with Docker (recommended)
docker compose up -d

# Wait for PostgreSQL to be ready
docker compose exec postgres pg_isready -U ins_tempo
```

Or connect to an existing PostgreSQL instance by editing `.env`:

```env
DATABASE_URL=postgresql://user:password@host:5432/database
```

### Database Migration

Run migrations to create the schema (tables, views, indexes, partitions):

```bash
# Run migrations (creates ~2000 partitions, may take 1-2 minutes)
pnpm db:migrate

# To reset the database and run fresh migration
pnpm db:migrate --fresh
```

The migration creates:

- Core tables: `contexts`, `matrices`, `territories`, `time_periods`, `statistics`
- 2000 pre-created partitions for the `statistics` table
- Materialized views for analytics
- Seed data for classification types and units of measure

### Initial Data Sync

After migration, sync metadata from the INS Tempo API:

```bash
# 1. Full sync (contexts, territories, matrices catalog & metadata)
pnpm cli sync all

# OR step-by-step approach:
pnpm cli sync contexts                        # Domain hierarchy (~340)
pnpm cli sync territories                     # NUTS hierarchy (55)
pnpm cli sync matrices                        # Matrix catalog only (~1,898)
pnpm cli sync matrices --full --skip-existing # Detailed metadata (slow)

# 2. Sync statistical data for specific matrices
pnpm cli sync data --matrix POP105A                    # Population by counties
pnpm cli sync data --matrix POP105A --years 2020-2024  # Specific year range
```

**Note:** The INS API has a 750ms rate limit between requests. Full metadata sync (`sync all`) takes ~4 hours.

### Start Development Server

```bash
pnpm dev
```

### Your First API Call

```bash
# Check API health
curl http://localhost:3000/health

# Get Romania's population 2020-2024
curl "http://localhost:3000/api/v1/statistics/POP105A?territoryCode=RO&yearFrom=2020"
```

## Commands

| Command | Description |
|---------|-------------|
| `pnpm dev` | Start server with hot reload |
| `pnpm build` | Compile TypeScript |
| `pnpm start` | Run compiled server |
| `pnpm test` | Run tests (watch mode) |
| `pnpm lint` | Check code style |
| `pnpm typecheck` | TypeScript type checking |
| `pnpm db:migrate` | Run database migrations |
| `pnpm cli` | Run CLI commands |

## CLI Usage

Sync data from INS Tempo API:

```bash
# Sync all contexts (domain hierarchy)
pnpm cli sync contexts

# Sync matrix catalog
pnpm cli sync matrices

# Sync specific matrix with data
pnpm cli sync matrices --code POP105A
pnpm cli sync data --matrix POP105A --years 2020-2024

# Explore available data
pnpm cli matrices list
pnpm cli matrices search "populat"
pnpm cli matrices info POP105A
```

See [CLI_USAGE.md](./docs/CLI_USAGE.md) for complete CLI documentation.

## API Endpoints

### Core Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/v1/contexts` | Browse domain hierarchy |
| `GET /api/v1/matrices` | Search datasets |
| `GET /api/v1/matrices/:code` | Get dataset details |
| `GET /api/v1/territories` | List territories |
| `GET /api/v1/time-periods` | List time periods |
| `GET /api/v1/classifications` | List classification types |

### Statistics & Analytics

| Endpoint | Description |
|----------|-------------|
| `GET /api/v1/statistics/:code` | Query time series data |
| `GET /api/v1/statistics/:code/summary` | Aggregated summary |
| `GET /api/v1/trends/:code` | Year-over-year growth |
| `GET /api/v1/rankings/:code` | Territory rankings |
| `GET /api/v1/distribution/:code` | Statistical distribution |
| `GET /api/v1/pivot/:code` | Pivot table format |
| `GET /api/v1/correlate` | Cross-matrix correlation |

### Example: Population Trends

```bash
curl "http://localhost:3000/api/v1/trends/POP105A?territoryCode=RO&yearFrom=2020"
```

```json
{
  "data": {
    "matrixCode": "POP105A",
    "territory": {"code": "RO", "name": "TOTAL"},
    "trends": [
      {"year": 2020, "value": 19354339, "growthRate": null},
      {"year": 2021, "value": 19229519, "growthRate": -0.65},
      {"year": 2022, "value": 19043098, "growthRate": -0.97},
      {"year": 2023, "value": 19055228, "growthRate": 0.06}
    ]
  }
}
```

See [API_QUERY_SPECIFICATION.md](./docs/API_QUERY_SPECIFICATION.md) for complete API documentation with examples.

## Project Structure

```
src/
  cli/           # CLI commands (sync, explore, query)
  db/            # Database connection, migrations, types
  scraper/       # INS Tempo API client
  server/        # Fastify REST API
    routes/      # API route handlers
    schemas/     # Request/response schemas
    services/    # Business logic
  services/      # Shared services (sync orchestration)
  types/         # TypeScript type definitions
docs/
  API_QUERY_SPECIFICATION.md  # Complete API reference
  CLI_USAGE.md                # CLI documentation
  INS_SPEC/                   # INS API specifications
```

## Data Model

```
contexts (8 domains, ~340 subcategories)
    └── matrices (~1,898 datasets)
            └── statistics (fact data)
                    ├── territory (NUTS hierarchy)
                    ├── time_period (annual/quarterly/monthly)
                    ├── classifications (sex, age, etc.)
                    └── unit_of_measure
```

### Territory Levels

| Level | Description | Example |
|-------|-------------|---------|
| NATIONAL | Romania | RO |
| NUTS1 | Macroregions | 4 regions |
| NUTS2 | Development regions | 8 regions |
| NUTS3 | Counties | 42 counties |
| LAU | Localities | ~3,200 UATs |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | 3000 | Server port |
| `HOST` | 0.0.0.0 | Server host |
| `DATABASE_URL` | - | PostgreSQL connection string |
| `API_URL` | http://localhost:3000 | CLI target API URL |

## Documentation

| Document | Description |
|----------|-------------|
| [API_QUERY_SPECIFICATION.md](./docs/API_QUERY_SPECIFICATION.md) | Complete API reference with cURL examples |
| [CLI_USAGE.md](./docs/CLI_USAGE.md) | CLI commands and data sync guide |
| [SYNC_STRATEGY.md](./docs/SYNC_STRATEGY.md) | Progressive sync phases with size estimation |
| [PARTITIONING.md](./docs/PARTITIONING.md) | Database partitioning strategy |
| [INS_SPEC/](./docs/INS_SPEC/) | Original INS API documentation |

## Tech Stack

- **Runtime:** Node.js 20+
- **Language:** TypeScript
- **Server:** Fastify
- **Database:** PostgreSQL with Kysely ORM
- **CLI:** Commander.js
- **Testing:** Vitest

## INS Tempo API

This project syncs data from the INS Tempo API at `http://statistici.insse.ro:8077/tempo-ins/`.

Key constraints:

- **Rate limit:** 750ms delay between requests (enforced)
- **30,000 cell limit:** Large queries must be chunked
- **No authentication required**

## Data Sync Strategy

### Lazy Loading / On-Demand Sync

Not all ~1,898 matrices have statistical data synced by default. The project uses a **lazy loading** approach:

1. **Metadata is synced for all matrices** - Names, dimensions, classifications
2. **Statistical data is synced on-demand** - Only for matrices you need

This approach is intentional because:

- Full data sync for all matrices takes 2-7 days
- Not all matrices are relevant for every use case
- Storage requirements are significantly reduced (~120GB for full sync)

### Sync Commands

```bash
# 1. Sync metadata for all matrices (required first step)
pnpm cli sync all

# 2. Sync data for individual matrices
pnpm cli sync data --matrix POP105A --years 2020-2024

# 3. Bulk sync all matrices with metadata
pnpm cli sync data --years 2020-2024 --continue-on-error

# 4. Refresh previously synced matrices only
pnpm cli sync data --refresh --years 2020-2024
pnpm cli sync data --older-than 30   # Refresh stale data
pnpm cli sync data --stale-only      # Only STALE status

# 5. Priority matrices script (30 key datasets)
./scripts/sync-priority-matrices.sh 2020-2024
```

### Checking Sync Status

**Via CLI:**

```bash
pnpm cli sync status           # Overview of all matrices
pnpm cli sync status --failed  # Show only failed syncs
```

**Via API:**

```bash
# Get sync status summary
curl http://localhost:3000/api/v1/sync/status
```

### Queue-Based Sync API

Data sync requests go through a job queue to prevent INS API rate limiting and duplicate requests.

**Queue a sync job:**

```bash
# Request data sync (returns job ID immediately)
curl -X POST http://localhost:3000/api/v1/sync/data/POP105A \
  -H "Content-Type: application/json" \
  -d '{"yearFrom": 2020, "yearTo": 2024}'

# Response:
# {"data":{"jobId":1,"status":"PENDING","message":"Sync job queued"}}
```

**Check job status:**

```bash
# Get specific job status
curl http://localhost:3000/api/v1/sync/jobs/1

# List all jobs (with optional filters)
curl "http://localhost:3000/api/v1/sync/jobs?status=PENDING&limit=20"

# Cancel a pending job
curl -X DELETE http://localhost:3000/api/v1/sync/jobs/1
```

**Process the queue (run worker):**

```bash
# Start sync worker (processes jobs until queue is empty)
pnpm cli sync worker

# Process one job and exit
pnpm cli sync worker --once

# Process max 10 jobs
pnpm cli sync worker --limit 10

# List queued jobs
pnpm cli sync jobs
pnpm cli sync jobs --status PENDING
```

**Job states:** `PENDING` → `RUNNING` → `COMPLETED` | `FAILED` | `CANCELLED`

**Duplicate prevention:** If a sync job for the same matrix is already pending or running, the API returns the existing job ID instead of creating a duplicate.

### Resync Strategy

The resync strategy distinguishes between:

- **PENDING** - Metadata not synced, skip during data refresh
- **SYNCED** - Ready for data sync or refresh
- **FAILED** - Needs manual retry
- **STALE** - Needs refresh (data may be outdated)

Use `pnpm cli sync data --refresh` to only resync matrices that already have data, ignoring PENDING matrices that may not be needed.

## Sync Process Deep Dive

This section explains the complete sync process: what data is inserted into which tables, why it's needed, and how the pieces connect.

### Step 1: Database Migration

**Command:** `pnpm db:migrate`

Before any sync, the database schema must be created.

**What gets created:**

| Table | Purpose |
| ----- | ------- |
| `contexts` | Domain hierarchy (8 top-level domains, ~340 total) |
| `matrices` | Dataset catalog (~1,898 matrices) |
| `matrix_dimensions` | Dimension definitions per matrix |
| `matrix_nom_items` | Maps INS `nomItemId` values to canonical entities |
| `territories` | NUTS + LAU territorial hierarchy |
| `time_periods` | Annual, quarterly, monthly time periods |
| `classification_types` | Classification definitions (sex, age, residence, etc.) |
| `classification_values` | Classification values (Male, Female, Urban, Rural, etc.) |
| `units_of_measure` | Units (persons, thousands, percentage, etc.) |
| `statistics` | Main fact table (partitioned into 2,000 partitions) |
| `statistic_classifications` | Junction table linking facts to classifications |
| `sync_checkpoints` | Tracks chunk-level sync progress |
| `sync_coverage` | Tracks matrix-level sync completeness |

**Why partitioning:** The `statistics` table is partitioned by `matrix_id` (2,000 pre-created partitions) because it can contain billions of rows. Partitioning enables parallel inserts and faster queries.

---

### Step 2: Territory Seeding

**Command:** `pnpm cli seed territories`

**Why first:** Territories must exist before metadata sync because dimension labels like "ALBA" or "București" need to resolve to territory records.

**What gets inserted:**

```sql
-- territories table receives:
INSERT INTO territories (code, name, level, path, siruta_code, parent_id)
VALUES
  ('RO', 'Romania', 'NATIONAL', 'RO', NULL, NULL),
  ('RO1', 'Macroregiunea unu', 'NUTS1', 'RO.RO1', NULL, 1),
  ('AB', 'Alba', 'NUTS3', 'RO.RO1.RO12.AB', NULL, 5),
  ('1234', 'Alba Iulia', 'LAU', 'RO.RO1.RO12.AB.1234', '1234', 42);
```

**Data source:** `seed/territories.csv` containing:
- 1 national level (RO)
- 4 macroregions (NUTS1)
- 8 development regions (NUTS2)
- 42 counties (NUTS3)
- ~3,200 localities (LAU) with SIRUTA codes

**Why pre-seeded (not dynamic):**
- Deterministic: Same labels always resolve to same territory
- Auditable: Resolution is traceable
- Correct: SIRUTA codes ensure proper locality matching

---

### Step 3: Metadata Sync

**Command:** `pnpm cli sync all`

This is the longest step (~4 hours) and populates the "catalog" of what data exists.

#### Step 3.1: Sync Contexts

**What happens:** Fetches domain hierarchy from INS API (bilingual RO/EN).

**What gets inserted:**

```sql
-- contexts table receives:
INSERT INTO contexts (ins_code, names, level, path, parent_id)
VALUES
  ('A', '{"ro": "Date statistice", "en": "Statistical data"}', 0, 'A', NULL),
  ('A1', '{"ro": "Populație", "en": "Population"}', 1, 'A.A1', 1),
  ('A1_1', '{"ro": "Recensăminte", "en": "Census"}', 2, 'A.A1.A1_1', 2);
```

**Result:** ~340 context records forming a tree structure.

#### Step 3.2: Sync Matrices Catalog

**What happens:** Fetches list of all matrices (datasets) with basic metadata.

**What gets inserted:**

```sql
-- matrices table receives:
INSERT INTO matrices (ins_code, metadata, dimensions, sync_status, context_id)
VALUES
  ('POP105A', '{"names": {"ro": "Populatia...", "en": "Population..."}}', '[]', 'PENDING', 2);
```

**Result:** ~1,898 matrix records with `sync_status = 'PENDING'`.

#### Step 3.3: Sync Matrix Metadata (Dimensions)

**What happens:** For each matrix, fetches detailed dimension information and resolves labels to canonical entities.

**Tables written:**

**1. `matrix_dimensions`** - Dimension definitions:

```sql
INSERT INTO matrix_dimensions (matrix_id, dim_index, dimension_type, labels, option_count)
VALUES
  (1, 0, 'TERRITORIAL', '{"ro": "Județe", "en": "Counties"}', 43),
  (1, 1, 'TEMPORAL', '{"ro": "Ani", "en": "Years"}', 25),
  (1, 2, 'CLASSIFICATION', '{"ro": "Sexe", "en": "Sex"}', 3);
```

**2. `matrix_nom_items`** - The critical bridge table:

```sql
INSERT INTO matrix_nom_items (
  matrix_id, dim_index, nom_item_id, dimension_type, labels,
  territory_id, time_period_id, classification_value_id, unit_id
) VALUES
  -- Territorial dimension options
  (1, 0, 105, 'TERRITORIAL', '{"ro": "TOTAL"}', 1, NULL, NULL, NULL),
  (1, 0, 106, 'TERRITORIAL', '{"ro": "ALBA"}', 42, NULL, NULL, NULL),
  -- Temporal dimension options
  (1, 1, 4494, 'TEMPORAL', '{"ro": "Anul 2023"}', NULL, 23, NULL, NULL),
  -- Classification dimension options
  (1, 2, 9685, 'CLASSIFICATION', '{"ro": "Total"}', NULL, NULL, 1, NULL),
  (1, 2, 9686, 'CLASSIFICATION', '{"ro": "Masculin"}', NULL, NULL, 2, NULL);
```

**Why `matrix_nom_items` is critical:**
- INS API uses `nomItemId` values (e.g., 105, 4494, 9685) to identify dimension options
- Your database uses foreign keys (`territory_id`, `time_period_id`, etc.)
- This table bridges the two: `nomItemId 106` → `territory_id 42` (Alba county)

**3. `classification_types` and `classification_values`** - Created during resolution:

```sql
-- New classification type
INSERT INTO classification_types (code, names)
VALUES ('SEXE', '{"ro": "Sexe", "en": "Sex"}');

-- Classification values
INSERT INTO classification_values (type_id, code, names, content_hash)
VALUES
  (1, 'TOTAL', '{"ro": "Total", "en": "Total"}', 'sha256...'),
  (1, 'MASCULIN', '{"ro": "Masculin", "en": "Male"}', 'sha256...');
```

**4. `time_periods`** - Auto-created during resolution:

```sql
INSERT INTO time_periods (year, quarter, month, periodicity, period_start, period_end, labels)
VALUES
  (2023, NULL, NULL, 'ANNUAL', '2023-01-01', '2023-12-31', '{"ro": "Anul 2023"}'),
  (2023, 1, NULL, 'QUARTERLY', '2023-01-01', '2023-03-31', '{"ro": "Trim I 2023"}');
```

**After metadata sync completes:**
- Matrix status updated to `sync_status = 'SYNCED'`
- All dimension mappings in place
- Ready for data sync

---

### Step 4: Data Sync

**Command:** `pnpm cli sync data --matrix POP105A --years 2020-2024`

This fetches actual statistical values using the dimension mappings from Step 3.

#### Step 4.1: Load Dimensions (READ only)

Data sync reads from `matrix_nom_items` to understand how to query INS and how to interpret responses:

```sql
-- Load all dimension mappings for this matrix
SELECT dim_index, nom_item_id, dimension_type, territory_id, time_period_id, ...
FROM matrix_nom_items
WHERE matrix_id = 123;
```

**Key point:** Data sync does NOT modify dimension tables. It only reads them.

#### Step 4.2: Generate Chunks

INS API has a 30,000 cell limit. The chunker calculates:

```
cells = dim0_options × dim1_options × dim2_options × ...
```

If over 30k, splits by year → county → classification until each chunk is under the limit.

**What gets inserted:**

```sql
-- sync_checkpoints tracks each chunk
INSERT INTO sync_checkpoints (matrix_id, chunk_hash, status)
VALUES (123, 'sha256...', 'PENDING');
```

#### Step 4.3: Query INS API

For each chunk, builds an `encQuery` from `nomItemId` values:

```
Chunk selections: territory=[105,106], year=[4494], sex=[9685,9686]
encQuery: "105,106:4494:9685,9686"
```

Sends POST request to INS API, receives CSV:

```csv
Judete,Ani,Sexe,Valoare
"TOTAL","Anul 2023","Total",19053815
"ALBA","Anul 2023","Masculin",168432
```

#### Step 4.4: Parse and Insert Rows

For each CSV row:

1. **Parse labels** from columns: "ALBA", "Anul 2023", "Masculin", 168432
2. **Look up entity IDs** from `matrix_nom_items`:
   - "ALBA" → `territory_id = 42`
   - "Anul 2023" → `time_period_id = 23`
   - "Masculin" → `classification_value_id = 2`
3. **Calculate natural key hash** for deduplication
4. **Insert into statistics:**

```sql
INSERT INTO statistics (
  matrix_id, territory_id, time_period_id, unit_id,
  value, value_status, natural_key_hash, source_enc_query
) VALUES (123, 42, 23, 1, 168432, 'NORMAL', 'sha256...', '105,106:4494:9685,9686');
```

5. **Insert classification associations:**

```sql
INSERT INTO statistic_classifications (matrix_id, statistic_id, classification_value_id)
VALUES (123, 999, 2);  -- Links statistic to "Masculin"
```

#### Step 4.5: Update Progress

After each chunk:

```sql
UPDATE sync_checkpoints
SET status = 'COMPLETED', rows_synced = 150
WHERE matrix_id = 123 AND chunk_hash = 'sha256...';
```

After all chunks:

```sql
INSERT INTO sync_coverage (matrix_id, total_territories, synced_territories, ...)
VALUES (123, 43, 43, ...);
```

---

### Summary: Tables by Sync Phase

| Phase | Tables Written | Tables Read |
| ----- | -------------- | ----------- |
| Migration | All tables (schema creation) | - |
| Territory Seed | `territories` | - |
| Metadata Sync | `contexts`, `matrices`, `matrix_dimensions`, `matrix_nom_items`, `classification_types`, `classification_values`, `time_periods` | `territories` |
| Data Sync | `statistics`, `statistic_classifications`, `sync_checkpoints`, `sync_coverage` | `matrices`, `matrix_dimensions`, `matrix_nom_items`, `territories` |

---

### Verifying Sync Quality

**Check for unresolved territory mappings:**

```sql
SELECT m.ins_code, mni.nom_item_id, mni.labels
FROM matrix_nom_items mni
JOIN matrices m ON m.id = mni.matrix_id
WHERE mni.dimension_type = 'TERRITORIAL' AND mni.territory_id IS NULL;
```

**Check sync progress:**

```sql
SELECT status, COUNT(*) FROM sync_checkpoints
WHERE matrix_id = (SELECT id FROM matrices WHERE ins_code = 'POP105A')
GROUP BY status;
```

**Check data coverage:**

```sql
SELECT * FROM sync_coverage WHERE matrix_id = 123;
```

## FAQ

### Sync Workflow

The sync process has **three distinct phases** that must run in order:

#### Phase 1: Metadata Sync (`pnpm cli sync all`)

This fetches the "catalog" from INS - what data exists, not the data itself.

**What gets synced:**

- **Contexts** (~340) - The domain hierarchy (e.g., "A. Population" → "A1. Demographics" → "A1.1 Census")
- **Matrices** (~1,898) - Dataset definitions with names, descriptions, periodicities
- **Dimensions** - Each matrix has 0-8 dimensions (territory, time, sex, age, etc.)
- **Dimension Options** - The `nomItemId` values needed to query data (e.g., `nomItemId=105` for "Total" territory)

**Why it's separate:**

- Takes ~4 hours due to 750ms rate limit
- You can explore what data exists before committing to sync it
- The dimension options build the `matrix_nom_items` mapping table - critical for Phase 3

#### Phase 2: Territory Seeding (`pnpm cli seed territories`)

**Why pre-seeding?**

- Territories must exist *before* data sync
- The INS API returns labels like "București" or "ALBA" - we need to map these to canonical entities
- SIRUTA codes (Romania's locality classification) are loaded from CSV
- This makes entity resolution deterministic and auditable

**What gets loaded:**

- 42 counties (NUTS3)
- 8 development regions (NUTS2)
- 4 macroregions (NUTS1)
- National level (RO)
- ~3,200 localities (LAU) with SIRUTA codes

#### Phase 3: Data Sync (`pnpm cli sync data --matrix POP105A`)

This is where actual statistical values get loaded.

**The flow:**

1. **Chunk generation** - Split the query to stay under 30,000 cells
2. **Lease acquisition** - Grab a 2-minute lock on the chunk
3. **Query INS API** - POST to `/pivot` with `encQuery` string
4. **Parse CSV response** - Extract dimension labels + values
5. **Entity resolution** - Map labels to canonical IDs via `LabelResolver`
6. **Insert facts** - Batch insert into partitioned `statistics` table
7. **Update checkpoint** - Mark chunk as complete for resumability

### Chunking Mechanics

The INS API has a hard **30,000 cell limit** per query. A "cell" is one data point - the intersection of all dimensions.

#### The Problem

Consider a matrix like `POP105A` (Population by counties):

- 42 counties + national = 43 territories
- 25 years (2000-2024) = 25 time periods
- 2 sexes × 5 age groups = 10 classifications

That's `43 × 25 × 10 = 10,750 cells` - under the limit, no problem.

But `FOM104D` (Employment by localities):

- 3,200 localities = 3,200 territories
- 10 years = 10 time periods
- 3 employment types = 3 classifications

That's `3,200 × 10 × 3 = 96,000 cells` - **3x over the limit**.

#### The Adaptive Strategy

The chunking algorithm in `src/services/sync/chunking.ts` splits along dimensions in priority order:

1. **Time** (year) - Split first, most natural boundary
2. **Territory** (county) - Split by county code (AB, AG, AR...)
3. **Classification** - Split by classification value if still too large

**Example split for FOM104D:**

```text
Original: 96,000 cells (too large)
├── Split by year → 10 chunks of 9,600 cells each
│   ├── 2020: 3,200 × 1 × 3 = 9,600 ✓
│   ├── 2021: 9,600 ✓
│   └── ...
```

If a single year still exceeds 30k, it splits by county:

```text
Year 2020 with detailed classifications: 150,000 cells
├── Split by county → 42 chunks
│   ├── AB (Alba): 77 localities × 1 year × 50 classifications = 3,850 ✓
│   ├── AG (Argeș): 102 × 1 × 50 = 5,100 ✓
│   └── ...
```

#### Chunk Anatomy

Each chunk has:

- **`chunkHash`** - Deterministic SHA256 for checkpoint tracking
- **`selections`** - Array of `nomItemId` values per dimension
- **`estimatedCells`** - Predicted cell count
- **`territoryLevel`** - NATIONAL, COUNTY, or UAT
- **`classificationMode`** - `totals-only` or `all`

#### Checkpoint Resumability

If a sync fails mid-way:

1. Completed chunks are recorded in `sync_checkpoints`
2. On restart, the chunker skips already-synced chunks
3. The `chunkHash` ensures deterministic identification

**Example checkpoint query:**

```sql
SELECT chunk_hash, status, synced_at
FROM sync_checkpoints
WHERE matrix_id = 'POP105A' AND status = 'COMPLETED';
```

#### Classification Modes

Two sync modes control granularity:

- **`totals-only`** - Only aggregate values (e.g., "Total" sex, "All ages")
- **`all`** - Full breakdown (e.g., "Male 0-4", "Female 5-9", etc.)

`totals-only` syncs are ~10-50x smaller and useful for initial exploration.

### Entity Resolution

The INS API returns **labels** (text strings), not IDs. The `LabelResolver` maps these labels to canonical database entities.

#### The Challenge

When you query the INS API, you get CSV data like:

```csv
Judete,Ani,Sexe,Valoare
"ALBA",2023,"Masculin",168432
"Bucuresti",2023,"Feminin",1021543
"TOTAL",2023,"Total",19053815
```

But your database needs foreign keys:

- `territory_id` → Which row in `territories` table?
- `time_period_id` → Which row in `time_periods` table?
- `classification_value_ids` → Which rows in `classification_values` table?

#### The Four Resolvers

Located in `src/services/sync/canonical/`:

**1. TerritoryService (`territories.ts`)**

Maps labels like "ALBA", "București", "Municipiul Cluj-Napoca" to territory records.

Resolution strategies:

- **EXACT** - Direct match on `name.ro` or `name.en`
- **PATTERN** - Regex patterns for variations ("Mun. Cluj" → "Cluj-Napoca")
- **SIRUTA** - Fallback to SIRUTA code lookup for localities
- **FUZZY** - Trigram similarity for typos (last resort)

Why it's tricky:

- "București" vs "BUCURESTI" vs "Municipiul București"
- "Județul Alba" vs "ALBA" vs "Alba"
- Localities with same name in different counties

**2. TimePeriodService (`time-periods.ts`)**

Maps labels like "Anul 2023", "2023", "Trim I 2023" to time period records.

Label patterns:

- Annual: "Anul 2023", "2023", "Year 2023"
- Quarterly: "Trim I 2023", "Q1 2023", "Trimestrul I 2023"
- Monthly: "Ianuarie 2023", "Ian 2023", "January 2023"

Auto-creation: Time periods are created on-demand if they don't exist (unlike territories which must be pre-seeded).

**3. ClassificationService (`classifications.ts`)**

Maps labels like "Masculin", "15-19 ani", "Urban" to classification values.

Hierarchical structure:

```text
classification_types (e.g., "Sexe")
└── classification_values (e.g., "Masculin", "Feminin", "Total")
```

Deduplication: Same value text across matrices is deduplicated via content hash.

**4. UnitService (`units.ts`)**

Maps labels like "Numar persoane", "Mii lei", "%" to unit records.

Label patterns:

- "Numar persoane" → PERSONS
- "Mii lei" → THOUSANDS_LEI
- "Procente" / "%" → PERCENTAGE

#### The Resolution Flow

```text
CSV Label → LabelResolver
                ├── Check cache (in-memory during sync)
                ├── Query label_mappings table (previous resolutions)
                ├── Try exact match in canonical table
                ├── Try pattern/fuzzy matching
                ├── Record resolution in label_mappings (audit trail)
                └── Return canonical entity ID
```

#### Audit Trail

Every resolution is recorded in `label_mappings`:

```sql
SELECT
  original_label,
  entity_type,
  resolved_entity_id,
  resolution_method,  -- EXACT, PATTERN, FUZZY, SIRUTA
  confidence,
  created_at
FROM label_mappings
WHERE original_label ILIKE '%bucuresti%';
```

#### Unresolved Labels

When resolution fails:

1. Logged to `unresolved_labels` table with reason
2. Sync continues (doesn't block on one bad label)
3. Can be manually reviewed and fixed later

```sql
SELECT original_label, entity_type, reason, matrix_code
FROM unresolved_labels
WHERE created_at > NOW() - INTERVAL '1 day';
```

### Database Design

The schema uses a **two-layer architecture**: canonical (reference data) and fact (statistics).

#### Canonical Layer (Reference Data)

Normalized, deduplicated reference entities.

**Contexts & Matrices:**

```sql
contexts (
  id, code, name JSONB,     -- {"ro": "Populație", "en": "Population"}
  path ltree,               -- 'A.A1.A1_1' for hierarchy traversal
  parent_id
)

matrices (
  id, code, name JSONB, description JSONB,
  periodicity,              -- ANNUAL, QUARTERLY, MONTHLY
  context_id,
  sync_status,              -- PENDING, SYNCING, SYNCED, FAILED, STALE
  metadata JSONB            -- Flexible storage for INS-specific fields
)
```

**Territories (NUTS + LAU):**

```sql
territories (
  id, code,                 -- 'RO', 'RO1', 'AB', 'SIRUTA:1234'
  name JSONB,
  level,                    -- NATIONAL, NUTS1, NUTS2, NUTS3, LAU
  path ltree,               -- 'RO.RO1.AB.SIRUTA_1234'
  siruta_code,              -- For LAU level only
  parent_id
)
```

**Time Periods:**

```sql
time_periods (
  id, year, quarter, month,
  period_type,              -- ANNUAL, QUARTERLY, MONTHLY
  start_date, end_date,
  label JSONB
)
```

**Classifications (Hierarchical):**

```sql
classification_types (
  id, code, name JSONB      -- "SEXE", {"ro": "Sexe", "en": "Sex"}
)

classification_values (
  id, type_id,
  label JSONB,
  path ltree,               -- 'SEXE.MASCULIN' for nested classifications
  content_hash,             -- Deduplication across matrices
  sort_order
)
```

#### Fact Layer (Statistics)

The main data table, partitioned for performance.

```sql
statistics (
  id,
  matrix_id,
  territory_id,
  time_period_id,
  unit_id,
  value NUMERIC,
  natural_key_hash,         -- SHA256 for deduplication
  version,                  -- For tracking updates
  synced_at
) PARTITION BY LIST (matrix_id);

-- Pre-created 2000 partitions
statistics_1, statistics_2, ... statistics_2000
```

**Junction table for multi-classification facts:**

```sql
statistic_classifications (
  statistic_id,
  classification_value_id,
  dimension_index           -- Which dimension this classification came from
) PARTITION BY LIST (matrix_id);
```

#### Why Partitioning?

The `statistics` table can have **billions of rows**:

- 1,898 matrices × avg 50,000 rows = ~95 million rows (just totals)
- With full classification breakdowns: 1-10 billion rows

Partitioning by `matrix_id`:

- **Parallel inserts** - Different matrices write to different partitions
- **Fast deletes** - Drop partition instead of DELETE
- **Query optimization** - Prune irrelevant partitions
- **Maintenance** - VACUUM/ANALYZE per partition

#### Key Design Patterns

**JSONB for Bilingual Content:**

```sql
-- All user-facing text supports RO/EN
name JSONB  -- {"ro": "Populație", "en": "Population"}

-- Query with language preference
SELECT name->>'ro' AS name_ro FROM matrices;
```

**ltree for Hierarchies:**

```sql
-- Fast ancestor/descendant queries
SELECT * FROM territories WHERE path <@ 'RO.RO1';  -- All in macroregion 1
SELECT * FROM contexts WHERE path ~ '*.A1.*';       -- Pattern matching
```

**Natural Key Hashing:**

```sql
-- Deduplication via dimension combination hash
natural_key_hash = SHA256(
  matrix_id || territory_id || time_period_id ||
  sorted(classification_ids) || unit_id
)

-- Upsert without duplicates
INSERT INTO statistics (...)
ON CONFLICT (natural_key_hash) DO UPDATE SET value = EXCLUDED.value;
```

### INS API Quirks

The INS Tempo API has several peculiarities that the codebase handles.

#### Rate Limiting

**The constraint:** No official rate limit, but aggressive requests get blocked.

**Our solution:** 750ms minimum delay between requests, enforced in `src/scraper/client.ts`:

```typescript
const RATE_LIMIT_MS = 750;

async function rateLimitedFetch(url: string, options?: RequestInit) {
  const now = Date.now();
  const elapsed = now - lastRequestTime;
  if (elapsed < RATE_LIMIT_MS) {
    await sleep(RATE_LIMIT_MS - elapsed);
  }
  lastRequestTime = Date.now();
  return fetch(url, options);
}
```

**Impact:** Full metadata sync takes ~4 hours (20,000+ requests × 750ms).

#### The 30,000 Cell Limit

**The constraint:** Any query returning >30,000 cells fails silently or returns truncated data.

**How to check:** The API doesn't tell you the limit was hit - you just get fewer rows than expected.

**Our solution:** Pre-calculate cell count before querying:

```text
cells = dim1_options × dim2_options × ... × dimN_options
```

If over 30k, chunk the query (see Chunking Mechanics).

#### Bilingual Content (RO/EN)

**The quirk:** Some endpoints return Romanian only, others support `lang` parameter.

**Our solution:** Fetch twice when English is available:

1. First request: Romanian (always available)
2. Second request: English (when supported)

Store both in JSONB: `{"ro": "Populație", "en": "Population"}`

#### CSV Response Format

**The quirk:** Data queries return CSV, not JSON.

**Example response:**

```csv
Judete,Ani,Sexe,Valoare
"TOTAL",2023,"Total",19053815
"ALBA",2023,"Masculin",168432
```

**Parsing challenges:**

- Header row contains dimension labels (not codes)
- Values are quoted strings (need parsing to numbers)
- Romanian characters (diacritics) must be preserved
- Last column is always "Valoare" (the numeric value)

#### The `encQuery` Format

**The quirk:** Queries use a colon-separated string of `nomItemId` values.

**Format:** `nomItemId1:nomItemId2:...:nomItemIdN` (one per dimension)

**Example:**

```text
Matrix POP105A has 3 dimensions:
- Dimension 0: Territory (nomItemId 105 = "Total")
- Dimension 1: Year (nomItemId 4494 = "2023")
- Dimension 2: Sex (nomItemId 9685 = "Total")

encQuery = "105:4494:9685"
```

**Challenge:** You must know the exact `nomItemId` for each dimension option. This is why metadata sync builds the `matrix_nom_items` mapping table first.

#### Missing/Inconsistent Data

**Common issues:**

- Some matrices have no data for certain years
- Locality names change over time (mergers, renames)
- Some dimension options exist in metadata but have no data
- "Total" aggregations sometimes missing

**Our approach:**

- Log but don't fail on missing data
- Track coverage in `sync_coverage` table
- Allow partial syncs to complete

#### HTTP (Not HTTPS)

**The quirk:** The API runs on plain HTTP at `http://statistici.insse.ro:8077/tempo-ins`.

**Implications:**

- No TLS encryption
- Some network environments block non-HTTPS
- Can't use modern fetch features that require secure context

#### Timeout Behavior

**The quirk:** Large queries can take 30+ seconds to respond.

**Our solution:**

- 60-second timeout for data queries
- Retry with exponential backoff on timeout
- Smaller chunks reduce timeout risk

### Distributed Workers

The sync system supports running multiple workers concurrently without conflicts.

#### The Problem

Syncing all ~1,898 matrices with full data takes days. You want to run multiple workers in parallel, but:

- Two workers shouldn't sync the same chunk
- A crashed worker shouldn't leave chunks permanently locked
- Workers need coordination without a separate message queue

#### Lease-Based Locking

Instead of persistent locks, workers acquire **time-limited leases**.

**How it works:**

```sql
-- Worker tries to acquire a lease
INSERT INTO sync_leases (chunk_hash, worker_id, acquired_at, expires_at)
VALUES ('abc123', 'host1-pid-12345', NOW(), NOW() + INTERVAL '2 minutes')
ON CONFLICT (chunk_hash) DO NOTHING
RETURNING *;
```

- If INSERT succeeds → worker owns the chunk
- If INSERT fails (conflict) → another worker has it
- Lease expires after 2 minutes → auto-cleanup

**The `sync_leases` table:**

```sql
sync_leases (
  chunk_hash TEXT PRIMARY KEY,
  worker_id TEXT NOT NULL,
  acquired_at TIMESTAMPTZ,
  expires_at TIMESTAMPTZ
)
```

#### Worker Identification

Each worker has a unique ID: `hostname-pid-randomsuffix`

```typescript
const workerId = `${os.hostname()}-${process.pid}-${randomBytes(4).toString('hex')}`;
// Example: "server1-12345-a1b2c3d4"
```

This helps with:

- Debugging (which worker processed what)
- Identifying stuck workers
- Correlating logs across machines

#### Lease Lifecycle

```text
1. Worker generates chunk list
2. For each chunk:
   ├── Try to acquire lease
   │   ├── Success → Process chunk → Release lease
   │   └── Failure → Skip (another worker has it)
   └── On error → Lease expires automatically
```

#### Automatic Cleanup

Expired leases are cleaned up by active workers:

```sql
-- Before acquiring new leases, clean up expired ones
DELETE FROM sync_leases WHERE expires_at < NOW();
```

This means:

- Crashed workers don't leave permanent locks
- No manual intervention needed
- System self-heals

#### Running Multiple Workers

**On the same machine:**

```bash
# Terminal 1
pnpm cli sync data --matrix POP105A --years 2020-2024

# Terminal 2
pnpm cli sync data --matrix FOM104D --years 2020-2024
```

**On different machines:**

```bash
# Server 1
DATABASE_URL=postgres://... pnpm cli sync data --years 2020-2024

# Server 2
DATABASE_URL=postgres://... pnpm cli sync data --years 2020-2024
```

Both workers share the same PostgreSQL database, so leases coordinate automatically.

#### Checkpoint Resume

If a worker crashes mid-sync:

1. Its leases expire after 2 minutes
2. Another worker (or restart) picks up remaining chunks
3. Completed chunks are skipped via `sync_checkpoints`

```sql
-- Check chunk status before processing
SELECT status FROM sync_checkpoints
WHERE matrix_id = 'POP105A' AND chunk_hash = 'abc123';

-- If COMPLETED, skip. If PENDING/FAILED, process.
```

#### Monitoring Workers

**See active leases:**

```sql
SELECT chunk_hash, worker_id, acquired_at, expires_at
FROM sync_leases
WHERE expires_at > NOW()
ORDER BY acquired_at;
```

**See worker activity:**

```sql
SELECT worker_id, COUNT(*) as chunks_completed, MAX(completed_at) as last_activity
FROM sync_checkpoints
WHERE status = 'COMPLETED'
GROUP BY worker_id
ORDER BY last_activity DESC;
```

#### Trade-offs

**Pros:**

- No external dependencies (Redis, RabbitMQ, etc.)
- Self-healing (expired leases)
- Simple to reason about
- Works across machines

**Cons:**

- 2-minute lease means up to 2 minutes of "wasted" work if worker crashes
- Requires shared PostgreSQL access
- Not as efficient as dedicated job queue for high-concurrency

## License

MIT
