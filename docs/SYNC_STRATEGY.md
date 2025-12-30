# Progressive Sync Strategy

This document outlines a phased approach to syncing INS Tempo data, allowing you to:
- Start with essential datasets
- Monitor storage growth
- Predict full database size before committing to a complete sync

---

## Prerequisites

```bash
# 1. Ensure database is running
docker compose up -d

# 2. Run migrations (creates schema + 2000 partitions)
pnpm db:migrate

# 3. Sync all metadata first (required before any data sync)
pnpm cli sync all
```

**Note:** `sync all` syncs contexts, territories, and matrix metadata (~4 hours). No statistical data is synced yet.

---

## Phase 1: Priority Matrices (Limited Set)

Sync only the most important datasets to validate the pipeline and get initial size estimates.

### 1.1 Sync Priority Matrices (29 key datasets)

```bash
# Sync priority matrices for years 2020-2024
./scripts/sync-priority-matrices.sh 2020-2024
```

Or manually sync specific matrices:

```bash
# Population datasets
pnpm cli sync data POP105A --years 2020-2024   # Population by counties
pnpm cli sync data POP107A --years 2020-2024   # Population by age groups
pnpm cli sync data POP108A --years 2020-2024   # Population by sex

# Economic indicators
pnpm cli sync data SOM101B --years 2020-2024   # Unemployment
pnpm cli sync data SAL101A --years 2020-2024   # Average wages
pnpm cli sync data IPC101I --years 2020-2024   # Consumer price index

# Education
pnpm cli sync data SCL101A --years 2020-2024   # Schools
pnpm cli sync data SCL103D --years 2020-2024   # Students
```

### 1.2 Check Phase 1 Statistics

```bash
# View sync status summary
pnpm cli sync status

# Check database size
PGPASSWORD=ins_tempo psql -h localhost -U ins_tempo -d ins_tempo -c "
SELECT
    pg_size_pretty(pg_database_size('ins_tempo')) AS total_db_size,
    pg_size_pretty(pg_total_relation_size('statistics')) AS statistics_table_size,
    (SELECT COUNT(*) FROM statistics) AS total_rows,
    (SELECT COUNT(DISTINCT matrix_id) FROM statistics) AS matrices_with_data;
"
```

### 1.3 Phase 1 Size Estimation Query

```bash
PGPASSWORD=ins_tempo psql -h localhost -U ins_tempo -d ins_tempo -c "
-- Phase 1 Summary
WITH stats AS (
    SELECT
        COUNT(*) AS total_rows,
        COUNT(DISTINCT matrix_id) AS matrices_synced,
        pg_total_relation_size('statistics') AS table_bytes
    FROM statistics
),
matrices AS (
    SELECT COUNT(*) AS total_matrices FROM matrices WHERE sync_status = 'SYNCED'
)
SELECT
    s.total_rows AS \"Rows Synced\",
    s.matrices_synced AS \"Matrices with Data\",
    m.total_matrices AS \"Total Matrices Available\",
    pg_size_pretty(s.table_bytes) AS \"Current Size\",
    pg_size_pretty((s.table_bytes::numeric / NULLIF(s.matrices_synced, 0)) * m.total_matrices) AS \"Est. Full Size (Same Year Range)\",
    ROUND(s.table_bytes::numeric / NULLIF(s.total_rows, 0), 2) AS \"Bytes per Row\"
FROM stats s, matrices m;
"
```

---

## Phase 2: Full Matrices, Limited Years

Sync all matrices but only for a limited year range (e.g., 2020-2024).

### 2.1 Sync All Matrices (2020-2024 only)

```bash
# Option A: Using CLI bulk command
pnpm cli sync data-all --years 2020-2024 --continue-on-error

# Option B: Using the bash script
./scripts/sync-all-data.sh 2020-2024
```

**Estimated time:** 2-4 days (depends on number of matrices and INS API response times)

### 2.2 Monitor Progress During Sync

```bash
# In a separate terminal, check progress periodically
watch -n 60 'pnpm cli sync status | head -20'

# Or check job queue status
pnpm cli sync jobs --status PENDING
pnpm cli sync jobs --status RUNNING
```

### 2.3 Check Phase 2 Statistics

```bash
# Detailed sync status
pnpm cli sync status

# Failed matrices
pnpm cli sync status --failed

# Database size after Phase 2
PGPASSWORD=ins_tempo psql -h localhost -U ins_tempo -d ins_tempo -c "
SELECT
    pg_size_pretty(pg_database_size('ins_tempo')) AS total_db_size,
    pg_size_pretty(pg_total_relation_size('statistics')) AS statistics_table_size,
    (SELECT COUNT(*) FROM statistics) AS total_rows,
    (SELECT COUNT(DISTINCT matrix_id) FROM statistics) AS matrices_with_data,
    (SELECT MIN(year) FROM statistics) AS min_year,
    (SELECT MAX(year) FROM statistics) AS max_year;
"
```

### 2.4 Phase 2 Size Estimation Query

```bash
PGPASSWORD=ins_tempo psql -h localhost -U ins_tempo -d ins_tempo -c "
-- Phase 2 Summary & Full Sync Prediction
WITH current_stats AS (
    SELECT
        COUNT(*) AS total_rows,
        COUNT(DISTINCT matrix_id) AS matrices_synced,
        MIN(year) AS min_year,
        MAX(year) AS max_year,
        pg_total_relation_size('statistics') AS table_bytes
    FROM statistics
),
year_range AS (
    -- INS data typically goes back to 1990-2000
    SELECT
        5 AS current_years,        -- 2020-2024 = 5 years
        35 AS full_years           -- 1990-2024 = 35 years (estimate)
)
SELECT
    cs.total_rows AS \"Rows (Current)\",
    cs.matrices_synced AS \"Matrices with Data\",
    pg_size_pretty(cs.table_bytes) AS \"Current Size\",
    cs.min_year || '-' || cs.max_year AS \"Year Range\",
    yr.current_years AS \"Years Synced\",
    yr.full_years AS \"Est. Full Years\",
    pg_size_pretty((cs.table_bytes::numeric / yr.current_years) * yr.full_years) AS \"Est. Full Size\",
    ROUND((cs.table_bytes::numeric / yr.current_years) * yr.full_years / 1024 / 1024 / 1024, 1) AS \"Est. Full Size (GB)\"
FROM current_stats cs, year_range yr;
"
```

---

## Phase 3: Full Matrices, Full Years

Expand the year range to include all available historical data.

### 3.1 Refresh with Extended Year Range

```bash
# Refresh all previously synced matrices with full year range
# Note: Only refreshes matrices that already have data
pnpm cli sync data-refresh --years 1990-2024

# Or sync everything from scratch
pnpm cli sync data-all --years 1990-2024 --continue-on-error
```

**Estimated time:** 5-7 days

### 3.2 Check Phase 3 Statistics

```bash
# Final database size
PGPASSWORD=ins_tempo psql -h localhost -U ins_tempo -d ins_tempo -c "
SELECT
    pg_size_pretty(pg_database_size('ins_tempo')) AS total_db_size,
    pg_size_pretty(pg_total_relation_size('statistics')) AS statistics_table_size,
    pg_size_pretty(pg_total_relation_size('matrices')) AS matrices_table_size,
    pg_size_pretty(pg_total_relation_size('contexts')) AS contexts_table_size;
"

# Row counts and year distribution
PGPASSWORD=ins_tempo psql -h localhost -U ins_tempo -d ins_tempo -c "
SELECT
    MIN(year) AS min_year,
    MAX(year) AS max_year,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT matrix_id) AS matrices_with_data,
    ROUND(COUNT(*)::numeric / COUNT(DISTINCT matrix_id), 0) AS avg_rows_per_matrix
FROM statistics;
"
```

---

## Utility Commands

### Database Size Overview

```bash
PGPASSWORD=ins_tempo psql -h localhost -U ins_tempo -d ins_tempo -c "
-- Detailed table sizes
SELECT
    schemaname || '.' || relname AS table_name,
    pg_size_pretty(pg_total_relation_size(relid)) AS total_size,
    pg_size_pretty(pg_relation_size(relid)) AS data_size,
    pg_size_pretty(pg_indexes_size(relid)) AS index_size
FROM pg_catalog.pg_statio_user_tables
ORDER BY pg_total_relation_size(relid) DESC
LIMIT 15;
"
```

### Partition Size Distribution

```bash
PGPASSWORD=ins_tempo psql -h localhost -U ins_tempo -d ins_tempo -c "
-- Top 20 largest partitions
SELECT
    relname AS partition_name,
    pg_size_pretty(pg_total_relation_size(relid)) AS size,
    n_live_tup AS rows
FROM pg_stat_user_tables
WHERE relname LIKE 'statistics_matrix_%'
ORDER BY pg_total_relation_size(relid) DESC
LIMIT 20;
"
```

### Sync Status by Matrix

```bash
PGPASSWORD=ins_tempo psql -h localhost -U ins_tempo -d ins_tempo -c "
-- Matrices with most data
SELECT
    m.ins_code,
    (m.metadata->'names'->>'ro')::text AS name,
    COUNT(s.id) AS rows,
    MIN(s.year) AS min_year,
    MAX(s.year) AS max_year
FROM matrices m
LEFT JOIN statistics s ON m.id = s.matrix_id
GROUP BY m.id, m.ins_code, m.metadata
HAVING COUNT(s.id) > 0
ORDER BY COUNT(s.id) DESC
LIMIT 20;
"
```

### Estimate Remaining Sync Time

```bash
PGPASSWORD=ins_tempo psql -h localhost -U ins_tempo -d ins_tempo -c "
-- Sync progress estimation
WITH synced AS (
    SELECT COUNT(DISTINCT matrix_id) AS count FROM statistics
),
total AS (
    SELECT COUNT(*) AS count FROM matrices WHERE sync_status = 'SYNCED'
),
timing AS (
    -- Assuming ~30 seconds per matrix on average
    SELECT 30 AS seconds_per_matrix
)
SELECT
    s.count AS \"Matrices Synced\",
    t.count AS \"Total Matrices\",
    t.count - s.count AS \"Remaining\",
    ROUND(100.0 * s.count / NULLIF(t.count, 0), 1) AS \"Progress %\",
    ((t.count - s.count) * tm.seconds_per_matrix / 3600) || ' hours' AS \"Est. Time Remaining\"
FROM synced s, total t, timing tm;
"
```

### Check Failed Syncs

```bash
# Via CLI
pnpm cli sync status --failed

# Via SQL
PGPASSWORD=ins_tempo psql -h localhost -U ins_tempo -d ins_tempo -c "
SELECT
    ins_code,
    (metadata->'names'->>'ro')::text AS name,
    sync_status,
    sync_error,
    last_sync_at
FROM matrices
WHERE sync_status = 'FAILED'
ORDER BY last_sync_at DESC
LIMIT 20;
"
```

### Queue Status (when using API-triggered sync)

```bash
# View pending jobs
pnpm cli sync jobs --status PENDING

# Via SQL
PGPASSWORD=ins_tempo psql -h localhost -U ins_tempo -d ins_tempo -c "
SELECT
    status,
    COUNT(*) AS count,
    MIN(created_at) AS oldest,
    MAX(created_at) AS newest
FROM sync_jobs
GROUP BY status
ORDER BY
    CASE status
        WHEN 'RUNNING' THEN 1
        WHEN 'PENDING' THEN 2
        ELSE 3
    END;
"
```

---

## Size Estimates Reference

Based on typical INS data patterns:

| Phase | Year Range | Est. Size | Est. Time |
|-------|------------|-----------|-----------|
| Phase 1 (Priority) | 2020-2024 | 1-5 GB | 1-2 hours |
| Phase 2 (All, Limited) | 2020-2024 | 15-30 GB | 2-4 days |
| Phase 3 (All, Full) | 1990-2024 | 80-120 GB | 5-7 days |

**Note:** Actual sizes depend on which matrices have data and the granularity of dimensions (UAT-level data is much larger than county-level).

---

## Troubleshooting

### Sync Stalled
```bash
# Check for running workers
pnpm cli sync jobs --status RUNNING

# Check INS API availability
curl -s "http://statistici.insse.ro:8077/tempo-ins/context" | head -100
```

### Retry Failed Matrices
```bash
# Retry specific matrix
pnpm cli sync data <MATRIX_CODE> --years 2020-2024

# Retry all failed (via data-refresh which skips PENDING)
pnpm cli sync data-refresh --years 2020-2024
```

### Reset a Matrix Sync Status
```bash
PGPASSWORD=ins_tempo psql -h localhost -U ins_tempo -d ins_tempo -c "
UPDATE matrices SET sync_status = 'SYNCED', sync_error = NULL
WHERE ins_code = 'MATRIX_CODE';
"
```
