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

## Sync Defaults & Resumability

- `sync data` defaults to years 2020-current when `--years` is omitted.
- `--classifications totals` is the default; use `--classifications all` for full breakdowns.
- Chunking is adaptive and capped at 30,000 cells per request.
- Chunk checkpoints are recorded so reruns resume automatically. Use `--force` to re-sync or `--no-resume` to ignore checkpoints.

---

## Phase 1: Priority Matrices (Limited Set)

Sync only the most important datasets to validate the pipeline and get initial size estimates.

### 1.1 Sync Priority Matrices (49 key datasets)

```bash
# Sync priority matrices for years 2020-2024
./scripts/sync-priority-matrices.sh 2020-2024

# Or for full historical range (2016-2024)
./scripts/sync-priority-matrices.sh 2016-2024
```

#### Priority Matrices by Category

| Category | Matrices | Purpose |
|----------|----------|---------|
| **Population** | POP105A, POP107D, POP108D, POP201A, POP202A, POP206A, POP301A | Per capita calculations, demographic trends |
| **Labor & Salary** | SOM101B, SOM103B, AMG110F, AMG1010, FOM104B, FOM104D, FOM104F, FOM105A, FOM105F, FOM106D, FOM106E, FOM107E | Income tax estimation, employment analysis |
| **Economy & Enterprises** | CON103I, CON103H, INT101I, INT101O, INT101R, INT102D, INT104D | Profit tax estimation, GDP analysis |
| **Retail & Commerce** | COM101B, COM104B | TVA/VAT estimation |
| **Transport & Vehicles** | TRN102A, TRN102B, TRN103B, TRN103D | Fuel excise estimation |
| **Consumption** | CLV104A, CLV105A | Alcohol/tobacco excise estimation |
| **Education** | SCL101A, SCL103B, SCL104A, SCL108A | School infrastructure |
| **Health** | SAN101A, SAN103A, SAN104B | Healthcare capacity |
| **Housing** | LOC101B, LOC103A, LOC104A | Construction activity |
| **Agriculture** | AGR101A, AGR201A, AGR301A | Agricultural production |

#### Manual sync examples

```bash
# Population datasets
pnpm cli sync data --matrix POP107D --years 2020-2024   # Population by localities (UAT level)
pnpm cli sync data --matrix POP108D --years 2020-2024   # Mid-year population by localities

# Employment & Salary (for income tax estimation)
pnpm cli sync data --matrix FOM104D --years 2020-2024   # Employees by localities
pnpm cli sync data --matrix FOM106E --years 2020-2024   # Net salary by counties
pnpm cli sync data --matrix FOM107E --years 2020-2024   # Gross salary by counties

# Enterprises (for profit tax estimation)
pnpm cli sync data --matrix INT101O --years 2020-2024   # Active enterprises by counties
pnpm cli sync data --matrix INT104D --years 2020-2024   # Turnover by counties

# Vehicles (for fuel excise estimation)
pnpm cli sync data --matrix TRN103B --years 2020-2024   # Registered vehicles by counties
```

#### UAT-Level Data (Locality Granularity)

**Important:** For matrices with both county AND locality dimensions (like POP107D, POP108D, FOM104D), the INS API requires matching county-locality pairs. This means:

1. **Simple sync** (national totals only - default):
   ```bash
   pnpm cli sync data --matrix POP107D --years 2020-2024
   ```
   Returns: ~5 rows (one per year, national total)

2. **Single county sync** (recommended for testing):
   ```bash
   pnpm cli sync data --matrix POP107D --years 2020-2024 --county AB
   ```
   Returns: ~76 rows for Alba county's localities per year

   Available county codes: AB, AR, AG, BC, BH, BN, BT, BV, BR, BZ, CS, CL, CJ, CT, CV,
   DB, DJ, GL, GR, GJ, HR, HD, IL, IS, IF, MM, MH, MS, NT, OT, PH, SM, SJ, SB, SV, TR,
   TM, TL, VS, VL, VN, B (Bucuresti)

3. **Full UAT sync** (all 42 counties):
   ```bash
   ./scripts/sync-uat-matrix.sh POP107D 2020-2024
   ```
   Iterates through all counties automatically. Takes ~3-5 minutes per matrix per year range.

4. **Example data synced for Alba (2020)**:
   - MUNICIPIUL ALBA IULIA: 74,934
   - MUNICIPIUL AIUD: 25,416
   - ORAS CUGIR: 25,771
   - ~76 localities total

**Full UAT sync** for all ~3,181 localities requires 42 county iterations per year range.
Use the `sync-uat-matrix.sh` script for automated full sync.

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
pnpm cli sync data --years 2020-2024 --continue-on-error

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
pnpm cli sync data --refresh --years 1990-2024

# Or sync everything from scratch
pnpm cli sync data --years 1990-2024 --continue-on-error
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
pnpm cli sync data --matrix <MATRIX_CODE> --years 2020-2024

# Retry all failed (via --refresh which skips PENDING)
pnpm cli sync data --refresh --years 2020-2024
```

### Reset a Matrix Sync Status
```bash
PGPASSWORD=ins_tempo psql -h localhost -U ins_tempo -d ins_tempo -c "
UPDATE matrices SET sync_status = 'SYNCED', sync_error = NULL
WHERE ins_code = 'MATRIX_CODE';
"
```
