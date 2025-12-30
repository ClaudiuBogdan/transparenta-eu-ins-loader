# AGENTS.md

Guidelines for AI coding agents working on this TypeScript project.

## Project Overview

**Transparenta EU INS Loader** - Syncs Romanian statistical data from the INS (Institutul Național de Statistică) Tempo API and exposes it through a modern REST interface for [Transparenta.eu](https://transparenta.eu).

### What It Does

- Syncs **1,898 statistical datasets** (matrices) from INS covering population, economy, education, health
- Stores data in PostgreSQL with partitioned tables for performance
- Exposes a REST API with analytics endpoints (trends, rankings, distributions, pivot tables)
- Provides a CLI for data exploration and sync operations

### Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   CLI       │────▶│  REST API   │────▶│  Scraper    │────▶│  INS Tempo  │
│ (Commander) │     │  (Fastify)  │     │  (client)   │     │    API      │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                           │
                           ▼
                    ┌─────────────┐
                    │ PostgreSQL  │
                    │  (Kysely)   │
                    └─────────────┘
```

| Layer | Location | Description |
|-------|----------|-------------|
| CLI | `src/cli/` | Commander.js commands for sync, explore, query |
| REST API | `src/server/` | Fastify server with routes, schemas, services |
| Scraper | `src/scraper/client.ts` | INS API client with rate limiting |
| Database | `src/db/` | Kysely ORM, migrations, type definitions |
| Services | `src/services/` | Sync orchestration, canonical data resolution |

### INS Tempo API

The upstream data source at `http://statistici.insse.ro:8077/tempo-ins/`.

**Key constraints:**
- **Rate limit:** 750ms minimum delay between requests (enforced in scraper)
- **Cell limit:** 30,000 cells per query (use `wouldExceedLimit()` helper)
- **No authentication** required

**Data model:**
```
contexts (8 domains, ~340 subcategories)
    └── matrices (~1,898 datasets)
            └── statistics (fact data)
                    ├── territory (NUTS hierarchy: national → county)
                    ├── time_period (annual/quarterly/monthly)
                    ├── classifications (sex, age, education level, etc.)
                    └── unit_of_measure
```

### Key Files

| File | Purpose |
|------|---------|
| `src/scraper/client.ts` | INS API client, rate limiting, cell limit checks |
| `src/db/types.ts` | Database schema types (Kysely) |
| `src/types/index.ts` | TypeScript types for INS API responses |
| `src/server/plugins/error-handler.ts` | Custom error classes |
| `src/services/sync/orchestrator.ts` | Data sync coordination |

### Documentation

| Document | Description |
|----------|-------------|
| `docs/API_QUERY_SPECIFICATION.md` | REST API reference with examples |
| `docs/CLI_USAGE.md` | CLI commands and data extraction guide |
| `docs/INS_SPEC/` | Original INS API specifications |

## Quick Reference

```bash
# Run all tests once
pnpm test:run

# Run single test file
pnpm test:run tests/unit/utils/pagination.test.ts

# Run tests matching pattern
pnpm test:run -t "should parse"

# Lint (check only)
pnpm lint

# Lint with auto-fix
pnpm lint:fix

# Type check only (no emit)
pnpm typecheck

# Format all files
pnpm format
```

## Build & Development

```bash
pnpm dev              # Start Fastify server with hot reload
pnpm build            # Compile TypeScript to dist/
pnpm start            # Run compiled server
pnpm cli              # Run CLI directly with tsx
pnpm db:migrate       # Run database migrations
```

## Code Style

### Formatting (Prettier)

- **Semicolons:** Always required
- **Quotes:** Single quotes (`'`)
- **Print width:** 100 characters
- **Trailing commas:** ES5 style
- **Tabs:** 2 spaces (no tabs)
- **Arrow parens:** Always `(x) => x`
- **Line endings:** LF (Unix)

### Naming Conventions

| Element | Convention | Example |
|---------|------------|---------|
| Files | kebab-case | `time-periods.ts` |
| Variables | camelCase, PascalCase, UPPER_CASE | `userId`, `HttpClient`, `MAX_RETRIES` |
| Functions | camelCase | `parseTimePeriod()` |
| Types/Interfaces | PascalCase | `MatrixResponse` |
| Enum members | UPPER_CASE | `STATUS.ACTIVE` |
| Type properties | camelCase, snake_case, PascalCase | `nomItemId`, `item_count` |
| Unused variables | Prefix with underscore | `_unused`, `_err` |

### Import Order

Imports must be ordered with newlines between groups:

```typescript
// 1. Built-in Node.js modules
import { readFile } from 'node:fs/promises';

// 2. External packages (alphabetized)
import { Kysely } from 'kysely';

// 3. Internal project modules
import { logger } from '../logger.js';

// 4. Relative imports (parent, sibling, index)
import { parseDate } from './utils.js';

// 5. Type-only imports (last)
import type { Database } from '../db/types.js';
```

### Type Imports

Always use `import type` for type-only imports:

```typescript
// Correct
import type { FastifyInstance } from 'fastify';
import { someFunction, type SomeType } from './module.js';

// Incorrect
import { FastifyInstance } from 'fastify';  // If only used as type
```

## TypeScript Rules

- **Target:** ES2022
- **Module:** NodeNext (use `.js` extensions in imports)
- **Strict mode:** Enabled
- **No unchecked indexed access:** Enabled
- **No implicit returns:** Enabled
- **No unused locals/parameters:** Enabled

### Async Safety

These are enforced as errors:

- `@typescript-eslint/no-floating-promises` - Must await or void promises
- `@typescript-eslint/no-misused-promises` - No promises in wrong contexts
- `@typescript-eslint/require-await` - Async functions must use await

## Error Handling

Use custom error classes from `src/server/plugins/error-handler.ts`:

```typescript
import { NotFoundError, ValidationError, ConflictError } from '../plugins/error-handler.js';

// Usage
throw new NotFoundError('Matrix not found', { matrixCode: 'POP105A' });
throw new ValidationError('Invalid date format');
throw new ConflictError('Resource already exists');
```

## Testing

### Structure

```
tests/
  fixtures/        # Reusable test data
  mocks/           # Mock implementations
  unit/            # Unit tests mirroring src/ structure
    scraper/
    server/
    services/
    utils/
```

### Critical Rules

1. **Never call real APIs** - Mock all external HTTP requests
2. **Use fixtures** - Import from `tests/fixtures/` for consistent test data
3. **Mock the database** - Use `tests/mocks/db.ts` for Kysely mocks
4. **Test pure functions** - Prefer testing utility functions without side effects

### Test File Example

```typescript
import { describe, it, expect, vi, beforeEach } from 'vitest';

import { myFunction } from '../../../src/utils/my-function.js';

import type { MyType } from '../../../src/types/index.js';

describe('myFunction', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should handle valid input', () => {
    const result = myFunction('input');
    expect(result).toBe('expected');
  });

  it('should throw on invalid input', () => {
    expect(() => myFunction('')).toThrow('Invalid input');
  });
});
```

### Relaxed Rules in Tests

These rules are disabled in test files:

- `@typescript-eslint/no-explicit-any`
- `@typescript-eslint/no-unsafe-assignment`
- `@typescript-eslint/no-unsafe-member-access`
- `@typescript-eslint/require-await`

## Project-Specific Patterns

### INS API Rate Limiting

The scraper has built-in rate limiting (750ms between requests). Never bypass this.

### 30,000 Cell Limit

INS API queries are limited to 30,000 cells. Use `wouldExceedLimit()` helper before querying.

### Database

- ORM: Kysely with PostgreSQL
- Schema: `src/db/types.ts`
- Migrations: `src/db/migrations/`

### Pre-commit Hook

Husky runs lint-staged on commit:

- TypeScript files: ESLint fix + Prettier
- JSON/Markdown: Prettier only

## Common Mistakes to Avoid

1. **Missing `.js` extension** in imports (required for NodeNext)
2. **Forgetting `type` keyword** for type-only imports
3. **Floating promises** - Always await or explicitly void
4. **Using `any`** in source code (allowed in tests only)
5. **PascalCase file names** - Use kebab-case
6. **Calling real APIs in tests** - Always mock external requests
