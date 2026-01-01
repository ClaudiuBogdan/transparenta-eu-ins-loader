-- Sync Status Queries for INS Tempo Database
-- Run with: PGPASSWORD=ins_tempo psql -h jupiter -U ins_tempo -d ins_tempo -f scripts/sync-status-queries.sql

-- ============================================================
-- 1. Overall Sync Status Summary
-- ============================================================
SELECT 'OVERALL SYNC STATUS' as section;
SELECT sync_status, COUNT(*) as count
FROM matrices
GROUP BY sync_status
ORDER BY count DESC;

-- ============================================================
-- 2. UAT (Localities) Level Matrices
-- ============================================================
SELECT 'UAT LEVEL MATRICES' as section;

-- Count of UAT level matrices by sync status
SELECT
    sync_status,
    COUNT(*) as uat_matrices
FROM matrices
WHERE dimensions::text ILIKE '%localitati%'
GROUP BY sync_status
ORDER BY sync_status;

-- List UAT level matrices
SELECT
    ins_code,
    sync_status,
    metadata->>'name' as name,
    last_sync_at
FROM matrices
WHERE dimensions::text ILIKE '%localitati%'
ORDER BY sync_status, ins_code;

-- ============================================================
-- 3. County (Judete) Level Matrices - Exclusive
-- ============================================================
SELECT 'COUNTY LEVEL MATRICES (Judete only)' as section;

-- Count of county-only level matrices by sync status
SELECT
    sync_status,
    COUNT(*) as county_matrices
FROM matrices
WHERE dimensions::text ILIKE '%"labelRo": "Judete"%'
GROUP BY sync_status
ORDER BY sync_status;

-- List county-only level matrices
SELECT
    ins_code,
    sync_status,
    metadata->>'name' as name,
    last_sync_at
FROM matrices
WHERE dimensions::text ILIKE '%"labelRo": "Judete"%'
ORDER BY sync_status, ins_code;

-- ============================================================
-- 4. County Level Matrices - Including Macro/Region combos
-- ============================================================
SELECT 'COUNTY+ LEVEL MATRICES (includes macro/region/county combos)' as section;

-- Count matrices that include county data (any combo)
SELECT
    sync_status,
    COUNT(*) as county_plus_matrices
FROM matrices
WHERE dimensions::text ILIKE '%judete%'
GROUP BY sync_status
ORDER BY sync_status;

-- ============================================================
-- 5. Pending Matrices by Domain Prefix
-- ============================================================
SELECT 'PENDING MATRICES BY DOMAIN' as section;
SELECT
    LEFT(ins_code, 3) as domain_prefix,
    COUNT(*) as pending_count
FROM matrices
WHERE sync_status = 'PENDING'
GROUP BY LEFT(ins_code, 3)
ORDER BY pending_count DESC;

-- ============================================================
-- 6. Territorial Dimension Types Distribution
-- ============================================================
SELECT 'TERRITORIAL DIMENSION TYPES' as section;
SELECT DISTINCT
    labels->>'ro' as label_ro,
    labels->>'en' as label_en,
    COUNT(*) as matrix_count
FROM matrix_dimensions
WHERE dimension_type = 'TERRITORIAL'
GROUP BY labels->>'ro', labels->>'en'
ORDER BY matrix_count DESC;

-- ============================================================
-- 7. Quick Summary Stats
-- ============================================================
SELECT 'QUICK SUMMARY' as section;
SELECT
    (SELECT COUNT(*) FROM matrices) as total_matrices,
    (SELECT COUNT(*) FROM matrices WHERE sync_status = 'SYNCED') as synced,
    (SELECT COUNT(*) FROM matrices WHERE sync_status = 'PENDING') as pending,
    (SELECT COUNT(*) FROM matrices WHERE dimensions::text ILIKE '%localitati%') as uat_level,
    (SELECT COUNT(*) FROM matrices WHERE dimensions::text ILIKE '%"labelRo": "Judete"%') as county_only,
    (SELECT COUNT(*) FROM matrices WHERE dimensions::text ILIKE '%judete%') as county_plus;
