# Partitioning guide for statistics sync

This document describes how partitioning works in the schema, how partitions are created, and what to watch during sync and maintenance.

## What is partitioned and why

The schema partitions the largest fact tables by matrix_id to keep queries and maintenance fast and scoped:

- `statistics`: list-partitioned by `matrix_id`. Each matrix gets its own partition.
- `statistic_classifications`: list-partitioned by `matrix_id` to align with `statistics`.

Benefits:

- Queries for a single matrix prune to one partition.
- Vacuum/analyze/reindex can run per matrix.
- Dropping a matrix becomes a single partition drop instead of a large delete.

## Partition naming and routing

Partitions are named by convention:

- `statistics_matrix_<matrix_id>`
- `statistic_classifications_matrix_<matrix_id>`

Both parent tables have default partitions:

- `statistics_default`
- `statistic_classifications_default`

If a partition for a matrix_id does not exist, rows will land in the default partition. The default partitions should stay empty in normal operation.

## How partitions are created

Two SQL helper functions are available in `src/db/schema-v2.sql`:

- `create_statistics_partition(p_matrix_id integer)`
  - Creates `statistics_matrix_<id>` if missing.
  - Ensures a per-partition unique index on `natural_key_hash`.
  - Creates the matching `statistic_classifications_matrix_<id>` if missing.

- `create_stat_classifications_partition(p_matrix_id integer)`
  - Creates `statistic_classifications_matrix_<id>` if missing.
  - This is a compatibility helper for existing sync code.

Example usage:

```sql
SELECT create_statistics_partition(123);
SELECT create_stat_classifications_partition(123);
```

Both functions are idempotent and can be called safely multiple times.

## How sync flow uses partitions

The sync flow expects partitions to exist before inserting data:

1) Resolve matrix code to matrix_id.
2) Call `ensurePartitions(matrixId)`.
   - Implementation: `src/services/sync/data.ts:1468`.
3) Batch upsert statistics directly into the partition table:
   - Implementation: `src/services/sync/data.ts:746`.
   - Reason: `ON CONFLICT` uses a per-partition unique index on `natural_key_hash`.
4) Insert related classification mappings into `statistic_classifications` with `matrix_id` set. The partition routing will place rows in the correct partition.

Important detail: the upsert logic relies on a unique index on `natural_key_hash` within each partition. This index does not exist on the parent table, so the partition must be created before any upsert.

## What to watch during creation and sync

### 1) Default partition growth

If partitions are missing, data will route to `*_default`.

Check for unexpected rows:

```sql
SELECT relname, n_live_tup::bigint AS rows
FROM pg_stat_user_tables
WHERE relname IN ('statistics_default', 'statistic_classifications_default');
```

If rows exist in default partitions, create the correct partition and move data:

```sql
-- Example for matrix_id = 123
SELECT create_statistics_partition(123);

INSERT INTO statistics_matrix_123
SELECT * FROM statistics_default WHERE matrix_id = 123;

DELETE FROM statistics_default WHERE matrix_id = 123;
```

Repeat for `statistic_classifications_default` if needed.

### 2) Index presence on partitions

Verify the per-partition unique index exists:

```sql
SELECT indexname
FROM pg_indexes
WHERE tablename = 'statistics_matrix_123'
  AND indexname LIKE 'idx_statistics_matrix_123_natural_key%';
```

If missing, re-run `create_statistics_partition(123)`.

### 3) Consistent natural key hashing

The `natural_key_hash` is computed in the app and used for deduplication. Ensure:

- The same ordering for classification IDs is used every time.
- Nulls are encoded consistently (for example, using a placeholder like "N").
- The code and DB function (if used) produce the same hash format.

## Maintenance and operations

### List existing partitions

```sql
SELECT inhrelid::regclass AS partition_name
FROM pg_inherits
WHERE inhparent = 'statistics'::regclass
ORDER BY 1;
```

### Vacuum and analyze

Run per partition to avoid long locks:

```sql
VACUUM (ANALYZE) statistics_matrix_123;
VACUUM (ANALYZE) statistic_classifications_matrix_123;
```

### Dropping a matrix

Drop both partitions to avoid orphaned classification rows:

```sql
DROP TABLE IF EXISTS statistics_matrix_123;
DROP TABLE IF EXISTS statistic_classifications_matrix_123;
```

### Reindexing a hot partition

```sql
REINDEX TABLE statistics_matrix_123;
```

## Common failure modes

- Missing partition: rows go to default and upserts can fail or be slow.
- Missing unique index: `ON CONFLICT` does not deduplicate.
- Inconsistent hash computation: duplicates appear in the same partition.
- Classification partition missing: inserts go to default or fail if routing is blocked.

## Recommended checklist for new matrices

1) Insert or fetch the matrix row to get matrix_id.
2) Run `create_statistics_partition(matrix_id)`.
3) Run `create_stat_classifications_partition(matrix_id)` (optional but safe).
4) Start sync and verify zero rows in default partitions.
5) After initial load, `VACUUM (ANALYZE)` the new partitions.
