# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Transparenta EU INS Loader - a system for loading Romanian statistical data from the INS (Institutul Național de Statistică) Tempo API for the Transparenta.eu platform. The INS API uses a multi-dimensional OLAP cube structure with hierarchical contexts, matrices (datasets), and dimensions.

## Commands

```bash
# Development
pnpm dev              # Start Fastify server with hot reload (tsx watch)
pnpm cli              # Run CLI directly with tsx

# Build and run
pnpm build            # Compile TypeScript to dist/
pnpm start            # Run compiled server

# Testing
pnpm test             # Run vitest in watch mode
pnpm test:run         # Run tests once

# Code quality
pnpm lint             # ESLint check
pnpm lint:fix         # ESLint with auto-fix
pnpm format           # Prettier format all files
pnpm format:check     # Check formatting
pnpm typecheck        # TypeScript type check only

# Database
pnpm db:migrate       # Run migrations
```

## Architecture

**Three-layer system:**
1. **REST API Server** (`src/server/`) - Fastify server exposing navigation and data endpoints
2. **CLI Client** (`src/cli/`) - Commander.js CLI that connects to the REST API
3. **Scraper Module** (`src/scraper/client.ts`) - INS Tempo API client with built-in rate limiting (750ms between requests)

**Data flow:** CLI → REST API → Scraper → INS Tempo API → SQLite (via Kysely)

**Key files:**
- `src/scraper/client.ts` - Rate-limited INS API client, handles 30,000 cell limit
- `src/db/schema.ts` - Kysely schema (contexts, matrices, dimensions, statistics, SIRUTA)
- `src/types/index.ts` - TypeScript types for INS API responses

## Extracting Data

See **[CLI_USAGE.md](./CLI_USAGE.md)** for detailed instructions on:

- Finding and querying matrices (datasets)
- Building `encQuery` strings with `nomItemId` values
- Complete examples for population and education data
- API reference and troubleshooting

## INS API Constraints

- **Rate limit:** 750ms delay between requests (enforced in `rateLimitedFetch`)
- **30,000 cell limit:** Queries exceeding this must be chunked (use `wouldExceedLimit()` helper)
- **Base URL:** `http://statistici.insse.ro:8077/tempo-ins`

## Database

SQLite database at `./data/ins.db` with tables:
- `ins_contexts` - Domain hierarchy (8 domains + subcategories)
- `ins_matrices` - Dataset catalog (~1,700 matrices)
- `ins_dimensions` / `ins_dimension_options` - Matrix dimension metadata
- `ins_statistics` - Scraped fact data
- `siruta` - SIRUTA reference table (3,222 UATs)

## Environment Variables

- `PORT` - Server port (default: 3000)
- `HOST` - Server host (default: 0.0.0.0)
- `API_URL` - CLI target API URL (default: http://localhost:3000)
