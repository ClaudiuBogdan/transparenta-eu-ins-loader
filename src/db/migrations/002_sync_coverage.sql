-- Migration: 002_sync_coverage
-- Description: Add sync coverage tracking and enhance sync_checkpoints for county-year chunking
-- Author: Claude Code
-- Date: 2025-01-01

-- ============================================================================
-- ENHANCE SYNC_CHECKPOINTS TABLE
-- ============================================================================

-- Add columns for county-year based chunking
ALTER TABLE sync_checkpoints ADD COLUMN IF NOT EXISTS county_code TEXT;
ALTER TABLE sync_checkpoints ADD COLUMN IF NOT EXISTS year SMALLINT;
ALTER TABLE sync_checkpoints ADD COLUMN IF NOT EXISTS classification_mode TEXT DEFAULT 'all';
ALTER TABLE sync_checkpoints ADD COLUMN IF NOT EXISTS cells_queried INTEGER;
ALTER TABLE sync_checkpoints ADD COLUMN IF NOT EXISTS cells_returned INTEGER;
ALTER TABLE sync_checkpoints ADD COLUMN IF NOT EXISTS error_message TEXT;
ALTER TABLE sync_checkpoints ADD COLUMN IF NOT EXISTS retry_count INTEGER DEFAULT 0;

-- Add index for efficient chunk lookup
CREATE INDEX IF NOT EXISTS idx_sync_checkpoints_chunk_lookup
  ON sync_checkpoints(matrix_id, county_code, year, classification_mode);

-- ============================================================================
-- CREATE SYNC_COVERAGE TABLE
-- ============================================================================

-- Track sync completeness at matrix level
CREATE TABLE IF NOT EXISTS sync_coverage (
    id SERIAL PRIMARY KEY,
    matrix_id INTEGER NOT NULL REFERENCES matrices(id) ON DELETE CASCADE,

    -- Territory coverage
    total_territories INTEGER DEFAULT 0,
    synced_territories INTEGER DEFAULT 0,

    -- Year coverage
    total_years INTEGER DEFAULT 0,
    synced_years INTEGER DEFAULT 0,

    -- Classification coverage
    total_classifications INTEGER DEFAULT 0,
    synced_classifications INTEGER DEFAULT 0,

    -- Data point counts
    expected_data_points BIGINT,
    actual_data_points BIGINT DEFAULT 0,
    null_value_count BIGINT DEFAULT 0,
    missing_value_count BIGINT DEFAULT 0, -- ':' values from INS API

    -- Timestamps
    first_sync_at TIMESTAMPTZ,
    last_sync_at TIMESTAMPTZ,
    last_coverage_update TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (matrix_id)
);

-- Add computed coverage percentage columns (PostgreSQL 12+)
-- Note: These are computed on read, not stored, for flexibility
COMMENT ON TABLE sync_coverage IS 'Tracks sync coverage completeness per matrix.
Coverage percentages can be computed as:
  territory_coverage = synced_territories / total_territories * 100
  year_coverage = synced_years / total_years * 100
  overall_coverage = actual_data_points / expected_data_points * 100';

-- Index for finding incomplete syncs
CREATE INDEX IF NOT EXISTS idx_sync_coverage_matrix ON sync_coverage(matrix_id);

-- ============================================================================
-- CREATE SYNC_DIMENSION_COVERAGE TABLE
-- ============================================================================

-- Track per-dimension completeness for detailed reporting
CREATE TABLE IF NOT EXISTS sync_dimension_coverage (
    id SERIAL PRIMARY KEY,
    matrix_id INTEGER NOT NULL REFERENCES matrices(id) ON DELETE CASCADE,
    dim_index SMALLINT NOT NULL,
    dimension_type dimension_type NOT NULL,

    total_values INTEGER NOT NULL,
    synced_values INTEGER DEFAULT 0,
    missing_value_ids INTEGER[] DEFAULT '{}', -- nom_item_ids not yet synced

    last_updated TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (matrix_id, dim_index)
);

CREATE INDEX IF NOT EXISTS idx_sync_dimension_coverage_matrix ON sync_dimension_coverage(matrix_id);

-- ============================================================================
-- UPDATE SYNC_JOBS FLAGS TYPE (documentation only - JSONB is flexible)
-- ============================================================================

COMMENT ON COLUMN sync_jobs.flags IS 'Sync options as JSONB:
  {
    "skipExisting": boolean,     -- Skip rows that already exist
    "force": boolean,            -- Force re-sync even if checkpoints exist
    "chunkSize": number,         -- Override automatic chunk sizing
    "totalsOnly": boolean,       -- Only sync TOTAL classification values (legacy)
    "fullSync": boolean,         -- Sync ALL dimensions including all territories and classifications
    "includeAllClassifications": boolean,  -- Include all classification breakdowns
    "resume": boolean            -- Resume from last checkpoint
  }';

-- ============================================================================
-- HELPER VIEWS
-- ============================================================================

-- View for sync coverage with computed percentages
CREATE OR REPLACE VIEW sync_coverage_view AS
SELECT
    sc.id,
    sc.matrix_id,
    m.ins_code as matrix_code,
    m.metadata->'names'->>'ro' as matrix_name,
    sc.total_territories,
    sc.synced_territories,
    CASE WHEN sc.total_territories > 0
         THEN ROUND((sc.synced_territories * 100.0 / sc.total_territories)::numeric, 1)
         ELSE 100
    END as territory_coverage_pct,
    sc.total_years,
    sc.synced_years,
    CASE WHEN sc.total_years > 0
         THEN ROUND((sc.synced_years * 100.0 / sc.total_years)::numeric, 1)
         ELSE 100
    END as year_coverage_pct,
    sc.total_classifications,
    sc.synced_classifications,
    CASE WHEN sc.total_classifications > 0
         THEN ROUND((sc.synced_classifications * 100.0 / sc.total_classifications)::numeric, 1)
         ELSE 100
    END as classification_coverage_pct,
    sc.expected_data_points,
    sc.actual_data_points,
    CASE WHEN sc.expected_data_points > 0
         THEN ROUND((sc.actual_data_points * 100.0 / sc.expected_data_points)::numeric, 1)
         ELSE 0
    END as overall_coverage_pct,
    sc.null_value_count,
    sc.missing_value_count,
    sc.first_sync_at,
    sc.last_sync_at,
    sc.last_coverage_update
FROM sync_coverage sc
JOIN matrices m ON sc.matrix_id = m.id;

-- View for checkpoint status by county
CREATE OR REPLACE VIEW sync_checkpoint_summary AS
SELECT
    sc.matrix_id,
    m.ins_code as matrix_code,
    sc.county_code,
    sc.year,
    sc.classification_mode,
    COUNT(*) as chunk_count,
    SUM(sc.row_count) as total_rows,
    MIN(sc.last_synced_at) as first_sync,
    MAX(sc.last_synced_at) as last_sync,
    SUM(CASE WHEN sc.error_message IS NOT NULL THEN 1 ELSE 0 END) as error_count
FROM sync_checkpoints sc
JOIN matrices m ON sc.matrix_id = m.id
GROUP BY sc.matrix_id, m.ins_code, sc.county_code, sc.year, sc.classification_mode;
