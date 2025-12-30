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

# Run migrations
pnpm db:migrate

# Start development server
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

## License

MIT
