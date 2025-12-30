# Schema Redesign Proposal

## Executive Summary

This document proposes a redesigned PostgreSQL schema for the INS statistical data system that addresses the current fragility issues while providing a more flexible, maintainable, and API-friendly architecture.

## Problems with Current Schema

### 1. Type Fragility (VARCHAR Limits)
```
Current: VARCHAR(200) for names → Truncation failures
Solution: Use TEXT everywhere for variable-length strings
```

### 2. Tight Coupling to INS API Structure
```
Current: Our schema mirrors INS structure → Changes break sync
Solution: Separate raw layer (INS format) from canonical layer (our format)
```

### 3. Entity Resolution at Sync Time
```
Current: Try to match labels to entities during sync → Complex, error-prone code
Solution: Separate entity resolution with label_mappings table
```

### 4. No Raw Data Preservation
```
Current: Transform and discard → Can't reprocess or debug
Solution: Store raw INS API responses in JSONB
```

### 5. Hierarchical Query Inefficiency
```
Current: String paths with LIKE queries → Slow hierarchical queries
Solution: Use ltree extension for native tree operations
```

---

## Proposed Architecture

### Three-Layer Design

```
┌─────────────────────────────────────────────────────────────────┐
│                        API Layer                                 │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐            │
│  │ api_matrices │ │api_territories│ │api_statistics│            │
│  └──────────────┘ └──────────────┘ └──────────────┘            │
│  Views optimized for REST/GraphQL consumption                    │
└─────────────────────────────────────────────────────────────────┘
                              ▲
┌─────────────────────────────────────────────────────────────────┐
│                      Canonical Layer                             │
│  ┌────────────┐ ┌─────────────┐ ┌─────────────────────┐        │
│  │ territories│ │ time_periods│ │classification_values│        │
│  └────────────┘ └─────────────┘ └─────────────────────┘        │
│  ┌────────────┐ ┌──────────────────┐                           │
│  │  matrices  │ │ matrix_nom_items │ ← Query building          │
│  └────────────┘ └──────────────────┘                           │
│  ┌────────────┐                                                 │
│  │ statistics │ ← Fact table (partitioned)                      │
│  └────────────┘                                                 │
│  Deduplicated, normalized entities with JSONB metadata          │
└─────────────────────────────────────────────────────────────────┘
                              ▲
┌─────────────────────────────────────────────────────────────────┐
│                      Entity Resolution                           │
│  ┌────────────────┐                                             │
│  │ label_mappings │ ← Maps raw labels to canonical entities     │
│  └────────────────┘                                             │
└─────────────────────────────────────────────────────────────────┘
                              ▲
┌─────────────────────────────────────────────────────────────────┐
│                        Raw Layer                                 │
│  ┌──────────────┐ ┌───────────────────────┐                    │
│  │ raw_matrices │ │ raw_dimension_options │                    │
│  └──────────────┘ └───────────────────────┘                    │
│  Exact INS API responses preserved in JSONB                     │
└─────────────────────────────────────────────────────────────────┘
```

---

## Key Design Decisions

### 1. JSONB for Flexible Metadata

**Before:**
```sql
CREATE TABLE matrices (
    name TEXT,
    name_en TEXT,
    definition TEXT,
    definition_en TEXT,
    methodology TEXT,
    methodology_en TEXT,
    -- ... 20 more columns
);
```

**After:**
```sql
CREATE TABLE matrices (
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
    -- {
    --   "names": {"ro": "...", "en": "..."},
    --   "definitions": {"ro": "...", "en": "..."},
    --   "flags": {"has_uat_data": true}
    -- }
);
```

**Benefits:**
- Add new languages without schema changes
- Add new metadata fields without migrations
- Query specific fields: `metadata->'names'->>'ro'`
- GIN index for efficient filtering

### 2. ltree for Hierarchical Data

**Before:**
```sql
path VARCHAR(500)  -- 'RO::MACRO1::NORD_VEST::BH'
-- Query: WHERE path LIKE 'RO::MACRO1::%'
```

**After:**
```sql
path ltree  -- 'RO.MACRO1.NORD_VEST.BH'
-- Query: WHERE path <@ 'RO.MACRO1'  -- Uses GIST index
-- Or: WHERE path ~ 'RO.*.BH'        -- Pattern matching
```

**Benefits:**
- Native tree operators (`<@`, `@>`, `~`, etc.)
- GIST index for fast hierarchical queries
- Built-in ancestor/descendant functions

### 3. Separate Raw and Canonical Layers

```
┌─────────────────────────────────────────────────────────┐
│                    SYNC FLOW                            │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  INS API ──→ raw_matrices (JSONB)                      │
│              raw_dimension_options                      │
│                        │                                │
│                        ▼                                │
│              label_mappings ──→ canonical entities      │
│              (entity resolution)                        │
│                        │                                │
│                        ▼                                │
│              matrices, matrix_nom_items                 │
│              (processed, linked)                        │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

**Benefits:**
- Raw data preserved for reprocessing
- Entity resolution can be improved without re-fetching
- Debugging: compare raw vs. resolved
- Audit trail of source data

### 4. Content-Based Deduplication

**Before:**
```sql
-- Code collision handling in TypeScript
async resolveCodeCollision(typeId, code, normalized) {
    // Complex logic with many edge cases
}
```

**After:**
```sql
CREATE TABLE classification_values (
    type_id INTEGER,
    content_hash TEXT NOT NULL,  -- SHA256 of normalized content
    UNIQUE (type_id, content_hash)
);

-- Simple: same content = same hash = same entity
```

**Benefits:**
- Deterministic deduplication
- No collision handling code needed
- Database enforces uniqueness
- Works regardless of code generation

### 5. Label Mappings for Entity Resolution

```sql
CREATE TABLE label_mappings (
    label_normalized TEXT NOT NULL,
    label_original TEXT NOT NULL,
    context_type TEXT NOT NULL,  -- 'TERRITORY', 'TIME_PERIOD', etc.
    context_hint TEXT,           -- Additional context for disambiguation

    -- Resolved target (exactly one)
    territory_id INTEGER,
    time_period_id INTEGER,
    classification_value_id INTEGER,
    unit_id INTEGER,

    -- Resolution metadata
    resolution_method TEXT,      -- 'EXACT', 'PATTERN', 'FUZZY', 'MANUAL'
    confidence NUMERIC(3,2),

    -- For unresolvable labels
    is_unresolvable BOOLEAN,
    unresolvable_reason TEXT
);
```

**Benefits:**
- Entity resolution is explicit and auditable
- Failed resolutions are tracked, not silently lost
- Manual overrides possible without code changes
- Statistics on resolution quality

---

## API Design

### REST Endpoints (example)

```
GET /api/matrices
GET /api/matrices/{code}
GET /api/matrices/{code}/dimensions
GET /api/matrices/{code}/data?territory=BH&year=2023

GET /api/territories?level=NUTS3
GET /api/territories/{id}/children
GET /api/territories/search?q=Oradea

GET /api/time-periods?year=2023&periodicity=ANNUAL

GET /api/statistics?matrix=POP105A&territory=BH&year=2023
```

### GraphQL Schema (example)

```graphql
type Matrix {
  id: ID!
  insCode: String!
  name(lang: Language = RO): String!
  definition(lang: Language = RO): String
  dimensions: [Dimension!]!
  statistics(
    territory: ID
    timePeriod: ID
    classifications: [ID!]
  ): [Statistic!]!
}

type Territory {
  id: ID!
  nutsCode: String
  sirutaCode: String
  level: TerritoryLevel!
  name(lang: Language = RO): String!
  parent: Territory
  children: [Territory!]!
  ancestors: [Territory!]!
}

type Statistic {
  id: ID!
  value: Float
  valueStatus: String
  territory: Territory
  timePeriod: TimePeriod!
  classifications: [ClassificationValue!]!
}
```

---

## Migration Strategy

### Phase 1: Create New Schema
1. Create new tables alongside existing
2. Set up sync to populate both
3. Validate data integrity

### Phase 2: Migrate Application
1. Update sync code to use new schema
2. Update API to use new views
3. Run parallel for validation

### Phase 3: Cleanup
1. Drop old tables
2. Remove compatibility code
3. Optimize indexes

### Data Migration Script Outline

```sql
-- 1. Migrate raw data
INSERT INTO raw_matrices (ins_code, metadata_ro)
SELECT ins_code, jsonb_build_object(
    'matName', name,
    'matDef', definition,
    -- ...
)
FROM old_matrices;

-- 2. Migrate territories (with ltree paths)
INSERT INTO territories (nuts_code, level, path, names)
SELECT
    code,
    level,
    replace(path, '::', '.')::ltree,
    jsonb_build_object('ro', name, 'normalized', name_normalized)
FROM old_territories;

-- 3. Create label mappings from existing dimension_options
INSERT INTO label_mappings (label_normalized, label_original, context_type, territory_id)
SELECT DISTINCT
    normalize_text(mdo.label),
    mdo.label,
    'TERRITORY',
    mdo.territory_id
FROM old_matrix_dimension_options mdo
WHERE mdo.territory_id IS NOT NULL;
```

---

## Performance Considerations

### Indexes

| Table | Index Type | Column(s) | Purpose |
|-------|------------|-----------|---------|
| territories | GIST | path | Hierarchical queries |
| territories | GIN | names | Full-text search |
| classification_values | GIST | path | Hierarchical queries |
| matrices | GIN | metadata | JSONB filtering |
| label_mappings | BTREE | (label_normalized, context_type) | Entity lookup |
| statistics | BTREE | (matrix_id, territory_id, time_period_id) | Star schema queries |

### Materialized Views (add as needed)

```sql
-- Territory aggregates
CREATE MATERIALIZED VIEW mv_territory_stats AS
SELECT
    t.id,
    t.level,
    COUNT(DISTINCT s.id) as statistic_count,
    COUNT(DISTINCT s.matrix_id) as matrix_count
FROM territories t
LEFT JOIN statistics s ON s.territory_id = t.id
GROUP BY t.id, t.level;

-- Matrix summaries
CREATE MATERIALIZED VIEW mv_matrix_stats AS
SELECT
    m.id,
    COUNT(s.id) as row_count,
    MIN(tp.year) as min_year,
    MAX(tp.year) as max_year
FROM matrices m
LEFT JOIN statistics s ON s.matrix_id = m.id
LEFT JOIN time_periods tp ON s.time_period_id = tp.id
GROUP BY m.id;
```

---

## Comparison: Before vs. After

| Aspect | Before | After |
|--------|--------|-------|
| String lengths | VARCHAR(200) - truncation risk | TEXT - unlimited |
| Hierarchy | String path + LIKE | ltree + GIST index |
| Bilingual | Duplicate columns (_en suffix) | JSONB with language keys |
| Entity resolution | In sync code, error-prone | Separate table, auditable |
| Raw data | Discarded after transform | Preserved in JSONB |
| Deduplication | Code-based, collisions | Content hash, deterministic |
| Schema changes | Migrations required | JSONB fields, flexible |
| API views | Complex JOINs | Pre-built denormalized views |

---

## Files

- `docs/schema-redesign-proposal.sql` - Complete SQL schema
- `docs/SCHEMA_REDESIGN.md` - This design document

---

## Next Steps

1. **Review** - Get feedback on the design
2. **Prototype** - Create a test database with sample data
3. **Benchmark** - Compare query performance
4. **Migration Plan** - Detailed migration scripts
5. **Implementation** - Phase by phase rollout
