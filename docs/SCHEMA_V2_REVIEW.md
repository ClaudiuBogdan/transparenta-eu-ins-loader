# Schema V2 Review (INS statistical data)

## Scope and inputs
- Schema definition: `src/db/schema-v2.sql`.
- Types: `src/db/types-v2.ts`.
- Design intent: `docs/SCHEMA_REDESIGN.md`, `docs/schema-redesign-proposal.sql`.
- API constraints: `INS_API_SPEC.md`.
- Code compatibility scan: sync, API routes, migration, and upsert code under `src/`.

I did not run live INS API calls or Postgres queries for this pass. Findings are based on static review.

## Executive summary
The V2 schema moves in a strong direction (raw + canonical + views, JSONB for metadata, ltree for hierarchy, partitioned facts). The main risks today are integrity gaps and compatibility mismatches with the existing codebase. If you deploy V2 as-is, expect data duplication in statistics, ambiguous label mappings, and runtime errors in sync paths that still target V1 tables/columns.

This review focuses on: correctness constraints, partition/index strategy, and migration compatibility.

## Findings (by severity)

### Critical
1) Missing uniqueness/indexing for fact deduplication
- `statistics.natural_key_hash` is not indexed or constrained, so idempotent upserts and deduplication are not enforced.
- Impact: duplicate facts, slow upserts, and inconsistent query results.
- Location: `src/db/schema-v2.sql:278`.
- Fix: create a per-partition unique index on `natural_key_hash` and common query indexes, similar to `src/db/postgres-schema.sql:529` and `src/db/postgres-schema.sql:583`.

2) Label mappings allow invalid states
- `label_mappings` has no constraint to ensure exactly one target is set (or that a row is flagged unresolvable).
- `context_hint` is nullable, which increases collisions for common labels (for example "TOTAL") across different dimensions.
- Location: `src/db/schema-v2.sql:242`.
- Fix: add a `CHECK` using `num_nonnulls(...)` and require context hints for ambiguous types (or use a composite key on `label_normalized, context_type, context_hint` where `context_hint` is required for certain types).

3) `matrix_nom_items` uniqueness may be wrong
- `UNIQUE (matrix_id, nom_item_id)` assumes nomItemId is unique across all dimensions of a matrix. In INS, nomItemId is only guaranteed unique per dimension.
- Impact: collisions across dimensions will drop or overwrite options.
- Location: `src/db/schema-v2.sql:215`.
- Fix: change unique key to `(matrix_id, dim_index, nom_item_id)`.

4) Partition helpers are incomplete for V2
- `create_statistics_partition` only creates partitions, no FK/index strategy and no separate function for `statistic_classifications`.
- Existing sync code expects `create_stat_classifications_partition` and indexes (see `src/services/sync/data.ts:1468`).
- Location: `src/db/schema-v2.sql:340`.
- Fix: port the partition helper strategy from V1 (`src/db/postgres-schema.sql:583`) or update sync logic to match V2.

### High
5) Checkpoint table naming and columns do not match code
- V2 defines `sync_checkpoints` with `(chunk_hash, chunk_query)`.
- Code expects `data_sync_checkpoints` and `chunk_enc_query_hash`, plus `updated_at`.
- Impact: sync resume and incremental logic will fail at runtime.
- Locations: `src/db/schema-v2.sql:310`, `src/services/sync/checkpoints.ts:59`.
- Fix: either align schema to existing table and column names, or create a compatibility view + update code.

6) `statistics` column names do not match sync code
- V2 uses `unit_id`, while code uses `unit_of_measure_id` and expects `updated_at` in `statistics`.
- Impact: writes will fail unless code is updated.
- Locations: `src/db/schema-v2.sql:278`, `src/services/sync/upsert.ts:93`.
- Fix: align column names or update sync code to use V2 naming.

7) Time period constraints are incomplete
- V2 allows invalid combinations (for example ANNUAL with `month` set).
- Impact: inconsistent temporal data, incorrect queries.
- Location: `src/db/schema-v2.sql:100`.
- Fix: add a periodicity-aware `CHECK` (see `docs/schema-redesign-proposal.sql` for an example).

8) Matrix dimension integrity not enforced
- For `matrix_dimensions`, `dimension_type=CLASSIFICATION` does not require `classification_type_id`, and other types are not validated.
- Impact: inconsistent dimension definitions and resolution errors.
- Location: `src/db/schema-v2.sql:199`.
- Fix: add type-aware `CHECK` constraints (classification requires `classification_type_id`, unit/time/territory should not have it).

### Medium
9) Redundant metadata and dimension summaries can drift
- `matrices.dimensions` JSONB duplicates `matrix_dimensions`.
- Impact: data drift unless maintained via trigger or app logic.
- Location: `src/db/schema-v2.sql:164`.
- Fix: make `dimensions` a generated view or update it with triggers.

10) `contexts.children_type` is free-text
- No constraint to prevent invalid values beyond `context` and `matrix`.
- Impact: dirty data, broken API assumptions.
- Location: `src/db/schema-v2.sql:62`.
- Fix: add `CHECK (children_type IN ('context','matrix'))` or a dedicated enum.

11) ltree path integrity is not enforced
- `path` is not unique and not checked against `parent_id`.
- Impact: tree corruption and ambiguous ancestry queries.
- Locations: `src/db/schema-v2.sql:62`, `src/db/schema-v2.sql:80`.
- Fix: add `UNIQUE (path)` and consider triggers to recompute path when `parent_id` changes.

12) `raw_api_responses` uniqueness blocks history
- `UNIQUE (endpoint, request_params)` prevents multiple snapshots for the same request.
- Impact: you cannot keep history or track changes over time.
- Location: `src/db/schema-v2.sql:30`.
- Fix: include `fetched_at` in uniqueness or store a hash and allow multiple rows.

13) `classification_values.content_hash` has no default
- The hash must be computed in app code, but a DB function exists.
- Impact: inconsistent dedup if any code path forgets to set the hash.
- Location: `src/db/schema-v2.sql:131`, `src/db/schema-v2.sql:334`.
- Fix: use a generated column or a trigger calling `content_hash()`.

14) Missing indexes for common lookups
- Examples: `matrix_nom_items.parent_nom_item_id`, `time_periods.period_start/period_end`, expression indexes for `metadata->'flags'` and `metadata->'periodicity'`.
- Impact: slow API queries and sync lookups.
- Locations: `src/db/schema-v2.sql:215`, `src/db/schema-v2.sql:100`, `src/db/schema-v2.sql:164`.
- Fix: add targeted btree or GIN/trgm indexes based on real query patterns.

### Low
15) `territories.code` is generic
- If you plan to store both NUTS and LAU, a single `code` can be ambiguous. The redesign proposal used `nuts_code` and `siruta_code`.
- Impact: potential ambiguity in joins and API outputs.
- Location: `src/db/schema-v2.sql:80`.
- Fix: split into `nuts_code` and `siruta_code` or enforce uniqueness per level.

16) `value_status` is free text
- V2 uses TEXT, which is flexible but can drift.
- Impact: inconsistent status values; harder to aggregate.
- Location: `src/db/schema-v2.sql:278`.
- Fix: optional enum or constraint on known status markers (".", "-", "*", "<x"), or keep as-is but document.

## Compatibility notes (V2 vs current code)
These are not schema bugs, but they will block adoption unless you migrate code or add compatibility layers.

- Migration still loads V1 SQL: `src/db/migrate.ts:31`.
- API routes read V1 columns like `matrices.name`, `periodicity` array, and `has_uat_data` rather than JSONB metadata or V2 views: `src/server/routes/matrices.ts:110`.
- Sync expects `data_sync_checkpoints` and V1 field names, plus partition helper functions: `src/services/sync/checkpoints.ts:59`, `src/services/sync/data.ts:1468`.

Recommendation: decide whether to fully port sync/API to V2 or to create compatibility views named like V1 (for example a `v_matrices` view that exposes V1 column names).

## Suggestions to improve (actionable)

### 1) Hardening constraints
- Add `CHECK` for `time_periods` periodicity logic.
- Add `CHECK` for `label_mappings` to enforce exactly one target or `is_unresolvable = true`.
- Add `CHECK` for `matrix_dimensions` so `classification_type_id` is only set when `dimension_type = 'CLASSIFICATION'`.
- Add `CHECK` for `contexts.children_type` allowed values.

### 2) Partition and index strategy
- Add per-partition unique index on `statistics.natural_key_hash`.
- Add query indexes on `statistics` (territory, time, unit, matrix) as in V1.
- Add partition function for `statistic_classifications`, or update sync code to avoid needing it.

### 3) Align naming and tables with code
Option A: Update schema to match existing code (fastest path)
- Keep `unit_of_measure_id`, `data_sync_checkpoints`, and `updated_at` in `statistics`.

Option B: Update code to match V2 (cleanest long term)
- Point API routes to V2 views (`v_matrices`, `v_contexts`, etc.).
- Update sync and checkpoint services to V2 table names and columns.

### 4) Reduce JSONB drift
- Replace `matrices.dimensions` with a view that aggregates `matrix_dimensions`.
- Add generated columns for commonly filtered metadata fields (periodicity, yearRange, flags) and index them.

### 5) Raw layer improvements
- If you need matrix-level raw snapshots, add `raw_matrices` or a dedicated table per endpoint to avoid parsing from generic `raw_api_responses`.
- If you want history, relax the unique constraint on `(endpoint, request_params)` or include `fetched_at` in the key.

## Questions / assumptions
- Are there any matrices without time dimensions? If yes, `time_period_id NOT NULL` will block those.
- Do you want historical raw snapshots or only caching? This affects `raw_api_responses` constraints.
- Are nomItemIds unique across dimensions within the same matrix? If not, the `matrix_nom_items` uniqueness needs to change.

## Suggested next steps
1) Decide the migration strategy: update code to V2 or add compatibility views.
2) Add integrity constraints and partition indexes before loading data.
3) Run a small end-to-end sync on a single matrix to validate integrity and performance.

