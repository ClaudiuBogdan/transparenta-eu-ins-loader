# Transparenta EU INS Loader

Romanian statistical data loader from INS (Institutul National de Statistica) Tempo API for [Transparenta.eu](https://transparenta.eu). Syncs data from the INS Tempo API and exposes it through a modern, developer-friendly REST interface.

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
pnpm cli sync data POP105A                    # Population by counties
pnpm cli sync data POP105A --years 2020-2024  # Specific year range
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
pnpm cli sync data POP105A --years 2020-2024

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
pnpm cli sync data POP105A --years 2020-2024

# 3. Bulk sync all matrices with metadata
pnpm cli sync data-all --years 2020-2024 --continue-on-error

# 4. Refresh previously synced matrices only
pnpm cli sync data-refresh --years 2020-2024
pnpm cli sync data-refresh --older-than 30   # Refresh stale data
pnpm cli sync data-refresh --stale-only      # Only STALE status

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

Use `data-refresh` to only resync matrices that already have data, ignoring PENDING matrices that may not be needed.

## License

MIT
