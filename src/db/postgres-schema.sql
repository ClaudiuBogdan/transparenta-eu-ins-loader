-- ============================================================================
-- PostgreSQL Schema for INS (Romanian National Institute of Statistics) Data
-- ============================================================================
--
-- A star schema optimized for Romanian statistical data with:
-- - Shared reference tables for territories, time periods, and classifications
-- - Hierarchical path fields using '::' separator for efficient prefix filtering
-- - Parsed temporal components with periodicity enum
-- - Full INS API compatibility through nomItemId mappings
--
-- Prerequisites:
-- - PostgreSQL 14+
-- - pg_trgm extension for path-based filtering
--
-- ============================================================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ============================================================================
-- SECTION 1: ENUM TYPES
-- ============================================================================

-- Periodicity types matching INS data frequencies
CREATE TYPE periodicity_type AS ENUM ('ANNUAL', 'QUARTERLY', 'MONTHLY');

-- Dimension types from INS API
CREATE TYPE dimension_type AS ENUM (
    'TEMPORAL',         -- Years, quarters, months
    'TERRITORIAL',      -- Macroregions, regions, counties, UATs
    'CLASSIFICATION',   -- Sex, age groups, residence, CAEN, etc.
    'UNIT_OF_MEASURE'   -- Measurement units
);

-- Territorial hierarchy levels (NUTS + LAU)
CREATE TYPE territorial_level AS ENUM (
    'NATIONAL',    -- TOTAL (Romania) - 1 entity
    'NUTS1',       -- Macroregions - 4 entities
    'NUTS2',       -- Development regions - 8 entities
    'NUTS3',       -- Counties - 42 entities
    'LAU'          -- UATs/Localities with SIRUTA codes - 3,222 entities
);

-- Matrix (dataset) status
CREATE TYPE matrix_status AS ENUM ('ACTIVE', 'DISCONTINUED');

-- Scrape job status
CREATE TYPE scrape_status AS ENUM ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED');

-- Chunk strategy for large queries
-- BY_YEAR_TERRITORY: Added for locality-level matrices where each year chunk
-- exceeds 30,000 cells (e.g., POP107D has 42M cells per year)
CREATE TYPE chunk_strategy AS ENUM ('BY_YEAR', 'BY_YEAR_TERRITORY', 'BY_TERRITORY', 'BY_CLASSIFICATION');

-- ============================================================================
-- SECTION 2: REFERENCE TABLES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- contexts: Statistical domain hierarchy (8 domains A-H + ~340 subcategories)
-- ----------------------------------------------------------------------------
CREATE TABLE contexts (
    id SERIAL PRIMARY KEY,

    -- INS API fields
    ins_code VARCHAR(10) NOT NULL,
    name TEXT NOT NULL,
    name_en TEXT,  -- English translation from INS API
    level SMALLINT NOT NULL DEFAULT 0,

    -- Hierarchy
    parent_id INTEGER REFERENCES contexts(id) ON DELETE CASCADE,
    path VARCHAR(200) NOT NULL DEFAULT '',

    -- Metadata
    children_type VARCHAR(10) NOT NULL DEFAULT 'context',

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Constraints
    CONSTRAINT uq_contexts_ins_code UNIQUE (ins_code),
    CONSTRAINT chk_contexts_level CHECK (level >= 0),
    CONSTRAINT chk_contexts_children_type CHECK (children_type IN ('context', 'matrix'))
);

COMMENT ON TABLE contexts IS 'Hierarchical statistical domains from INS (8 top-level domains A-H with ~340 subcategories)';
COMMENT ON COLUMN contexts.ins_code IS 'Original INS context code for API compatibility';
COMMENT ON COLUMN contexts.path IS 'Materialized path with :: separator for efficient prefix filtering (e.g., "1::10::1010")';
COMMENT ON COLUMN contexts.children_type IS 'Type of children: "context" for subcategories, "matrix" for datasets';

-- Indexes for contexts
CREATE INDEX idx_contexts_parent ON contexts(parent_id);
CREATE INDEX idx_contexts_path ON contexts USING gin(path gin_trgm_ops);
CREATE INDEX idx_contexts_level ON contexts(level);

-- ----------------------------------------------------------------------------
-- territories: Unified territorial hierarchy (NUTS I-III + LAU/SIRUTA)
-- ----------------------------------------------------------------------------
CREATE TABLE territories (
    id SERIAL PRIMARY KEY,

    -- Identification
    code VARCHAR(20) NOT NULL,
    siruta_code VARCHAR(6),
    name TEXT NOT NULL,
    name_normalized TEXT NOT NULL,

    -- Hierarchy
    level territorial_level NOT NULL,
    parent_id INTEGER REFERENCES territories(id) ON DELETE CASCADE,
    path TEXT NOT NULL DEFAULT '',

    -- SIRUTA metadata (for LAU level)
    siruta_tip SMALLINT,
    siruta_niv SMALLINT,
    siruta_med SMALLINT,

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Constraints
    CONSTRAINT uq_territories_code UNIQUE (code),
    CONSTRAINT uq_territories_siruta UNIQUE (siruta_code)
);

COMMENT ON TABLE territories IS 'Unified territorial hierarchy: NUTS I (4 macroregions), NUTS II (8 regions), NUTS III (42 counties), LAU (3,222 UATs with SIRUTA)';
COMMENT ON COLUMN territories.code IS 'Standardized territory code: "RO" (national), "RO1" (NUTS1), "RO11" (NUTS2), "BH" (county), "26573" (SIRUTA)';
COMMENT ON COLUMN territories.siruta_code IS 'Official SIRUTA code for LAU-level territories (6 digits)';
COMMENT ON COLUMN territories.path IS 'Materialized path for hierarchy queries (e.g., "RO::MACRO1::NORD_VEST::BH::26573")';
COMMENT ON COLUMN territories.name_normalized IS 'Uppercase name without diacritics for matching';
COMMENT ON COLUMN territories.siruta_tip IS 'SIRUTA type: 1=municipality, 2=city, 3=commune, etc.';
COMMENT ON COLUMN territories.siruta_niv IS 'SIRUTA administrative level';
COMMENT ON COLUMN territories.siruta_med IS 'Residential environment: 1=urban, 2=rural';

-- Indexes for territories
CREATE INDEX idx_territories_parent ON territories(parent_id);
CREATE INDEX idx_territories_path ON territories USING gin(path gin_trgm_ops);
CREATE INDEX idx_territories_level ON territories(level);
CREATE INDEX idx_territories_siruta ON territories(siruta_code) WHERE siruta_code IS NOT NULL;
CREATE INDEX idx_territories_name_norm ON territories(name_normalized);

-- ----------------------------------------------------------------------------
-- time_periods: Unified time periods with parsed components
-- ----------------------------------------------------------------------------
CREATE TABLE time_periods (
    id SERIAL PRIMARY KEY,

    -- Parsed components
    year SMALLINT NOT NULL,
    quarter SMALLINT,
    month SMALLINT,
    periodicity periodicity_type NOT NULL,

    -- Display and API compatibility
    ins_label TEXT NOT NULL,

    -- Date representation for ordering and filtering
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Constraints
    CONSTRAINT chk_time_periods_year CHECK (year >= 1900 AND year <= 2100),
    CONSTRAINT chk_time_periods_quarter CHECK (quarter IS NULL OR (quarter >= 1 AND quarter <= 4)),
    CONSTRAINT chk_time_periods_month CHECK (month IS NULL OR (month >= 1 AND month <= 12)),
    CONSTRAINT chk_time_periods_periodicity_match CHECK (
        (periodicity = 'ANNUAL' AND quarter IS NULL AND month IS NULL) OR
        (periodicity = 'QUARTERLY' AND quarter IS NOT NULL AND month IS NULL) OR
        (periodicity = 'MONTHLY' AND month IS NOT NULL)
    ),
    CONSTRAINT uq_time_periods UNIQUE (year, quarter, month, periodicity)
);

COMMENT ON TABLE time_periods IS 'Unified time periods with parsed year/quarter/month components';
COMMENT ON COLUMN time_periods.ins_label IS 'Original INS label for display (e.g., "Anul 2023", "Trimestrul I 2024", "Luna Ianuarie 2024")';
COMMENT ON COLUMN time_periods.period_start IS 'First day of the period';
COMMENT ON COLUMN time_periods.period_end IS 'Last day of the period';

-- Indexes for time_periods
CREATE INDEX idx_time_periods_year ON time_periods(year);
CREATE INDEX idx_time_periods_periodicity ON time_periods(periodicity);
CREATE INDEX idx_time_periods_range ON time_periods(period_start, period_end);

-- ----------------------------------------------------------------------------
-- classification_types: Classification type definitions
-- ----------------------------------------------------------------------------
CREATE TABLE classification_types (
    id SERIAL PRIMARY KEY,

    -- Identification
    code TEXT NOT NULL,
    name TEXT NOT NULL,

    -- INS mapping
    ins_labels TEXT[] NOT NULL DEFAULT '{}',

    -- Structure
    is_hierarchical BOOLEAN NOT NULL DEFAULT FALSE,

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Constraints
    CONSTRAINT uq_classification_types_code UNIQUE (code)
);

COMMENT ON TABLE classification_types IS 'Classification type definitions (sex, residence, age groups, CAEN, etc.)';
COMMENT ON COLUMN classification_types.code IS 'Standardized code: SEX, RESIDENCE, AGE_GROUP, CAEN_REV2, etc.';
COMMENT ON COLUMN classification_types.ins_labels IS 'Array of INS dimension labels that map to this classification type';
COMMENT ON COLUMN classification_types.is_hierarchical IS 'Whether values have parent-child relationships';

-- ----------------------------------------------------------------------------
-- classification_values: Classification option values
-- ----------------------------------------------------------------------------
CREATE TABLE classification_values (
    id SERIAL PRIMARY KEY,

    -- Parent type
    classification_type_id INTEGER NOT NULL REFERENCES classification_types(id) ON DELETE CASCADE,

    -- Value identification
    code TEXT NOT NULL,
    name TEXT NOT NULL,
    name_normalized TEXT NOT NULL,

    -- Hierarchy (for hierarchical classifications like CAEN, age groups)
    parent_id INTEGER REFERENCES classification_values(id) ON DELETE CASCADE,
    path TEXT,
    level SMALLINT NOT NULL DEFAULT 0,
    sort_order INTEGER NOT NULL DEFAULT 0,

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Constraints
    CONSTRAINT uq_classification_values UNIQUE (classification_type_id, code)
);

COMMENT ON TABLE classification_values IS 'Classification values with hierarchy support';
COMMENT ON COLUMN classification_values.code IS 'Standardized value code: M, F, TOTAL, URBAN, RURAL, etc.';
COMMENT ON COLUMN classification_values.path IS 'Hierarchical path for nested classifications (e.g., "CAEN_REV2::A::01::01.1")';
COMMENT ON COLUMN classification_values.name_normalized IS 'Uppercase name without diacritics for matching';
COMMENT ON COLUMN classification_values.sort_order IS 'Display ordering within the classification type';

-- Indexes for classification_values
CREATE INDEX idx_classification_values_type ON classification_values(classification_type_id);
CREATE INDEX idx_classification_values_parent ON classification_values(parent_id);
CREATE INDEX idx_classification_values_path ON classification_values USING gin(path gin_trgm_ops) WHERE path IS NOT NULL;
CREATE INDEX idx_classification_values_name_norm ON classification_values(name_normalized);

-- ----------------------------------------------------------------------------
-- units_of_measure: Measurement units
-- ----------------------------------------------------------------------------
CREATE TABLE units_of_measure (
    id SERIAL PRIMARY KEY,

    -- Identification
    code TEXT NOT NULL,
    name TEXT NOT NULL,
    symbol VARCHAR(20),

    -- INS mapping
    ins_labels VARCHAR(200)[] NOT NULL DEFAULT '{}',

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Constraints
    CONSTRAINT uq_units_code UNIQUE (code)
);

COMMENT ON TABLE units_of_measure IS 'Measurement units (persons, hectares, thousands, etc.)';
COMMENT ON COLUMN units_of_measure.code IS 'Standardized code: PERSONS, HECTARES, THOUSANDS, etc.';
COMMENT ON COLUMN units_of_measure.ins_labels IS 'Array of INS labels: "UM: Numar persoane", etc.';

-- ============================================================================
-- SECTION 3: MATRIX (DATASET) TABLES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- matrices: Statistical datasets (~1,898 matrices)
-- ----------------------------------------------------------------------------
CREATE TABLE matrices (
    id SERIAL PRIMARY KEY,

    -- INS API fields
    ins_code VARCHAR(20) NOT NULL,
    name TEXT NOT NULL,
    name_en TEXT,  -- English translation from INS API

    -- Hierarchy
    context_id INTEGER REFERENCES contexts(id) ON DELETE SET NULL,

    -- Metadata
    periodicity periodicity_type[] NOT NULL DEFAULT '{}',
    definition TEXT,
    definition_en TEXT,  -- English translation from INS API
    methodology TEXT,
    methodology_en TEXT,  -- English translation from INS API
    observations TEXT,
    observations_en TEXT,  -- English translation from INS API
    series_break TEXT,
    series_break_en TEXT,  -- English translation (lastPeriod field)
    series_continuation TEXT,
    series_continuation_en TEXT,  -- English translation (lastPeriod fields)
    responsible_persons TEXT,

    -- Time coverage
    start_year SMALLINT,
    end_year SMALLINT,
    last_update DATE,

    -- Capability flags from INS details
    status matrix_status NOT NULL DEFAULT 'ACTIVE',
    dimension_count SMALLINT NOT NULL DEFAULT 0,
    has_county_data BOOLEAN NOT NULL DEFAULT FALSE,
    has_uat_data BOOLEAN NOT NULL DEFAULT FALSE,
    has_siruta BOOLEAN NOT NULL DEFAULT FALSE,
    has_caen_rev1 BOOLEAN NOT NULL DEFAULT FALSE,
    has_caen_rev2 BOOLEAN NOT NULL DEFAULT FALSE,

    -- Dimension indexes (for API queries)
    territorial_dim_index SMALLINT,
    time_dim_index SMALLINT,
    county_dim_index SMALLINT,
    locality_dim_index SMALLINT,
    um_special BOOLEAN NOT NULL DEFAULT FALSE,

    -- Usage stats
    view_count INTEGER DEFAULT 0,
    download_count INTEGER DEFAULT 0,
    query_complexity INTEGER DEFAULT 0,

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Constraints
    CONSTRAINT uq_matrices_ins_code UNIQUE (ins_code)
);

COMMENT ON TABLE matrices IS 'Statistical datasets (matrices) from INS Tempo API (~1,898 datasets)';
COMMENT ON COLUMN matrices.ins_code IS 'INS matrix code for API queries (e.g., POP105A, SCL103B)';
COMMENT ON COLUMN matrices.dimension_count IS 'Number of dimensions (matMaxDim), typically 3-6';
COMMENT ON COLUMN matrices.has_uat_data IS 'Matrix contains UAT/locality-level data';
COMMENT ON COLUMN matrices.has_county_data IS 'Matrix contains county-level data';
COMMENT ON COLUMN matrices.territorial_dim_index IS 'INS matRegJ: index of territorial dimension';
COMMENT ON COLUMN matrices.time_dim_index IS 'INS matTime: index of time dimension';
COMMENT ON COLUMN matrices.um_special IS 'INS matUMSpec: has special unit of measure dimension';

-- Indexes for matrices
CREATE INDEX idx_matrices_context ON matrices(context_id);
CREATE INDEX idx_matrices_status ON matrices(status);
CREATE INDEX idx_matrices_has_uat ON matrices(has_uat_data) WHERE has_uat_data = TRUE;
CREATE INDEX idx_matrices_has_county ON matrices(has_county_data) WHERE has_county_data = TRUE;
CREATE INDEX idx_matrices_name_trgm ON matrices USING gin(name gin_trgm_ops);
CREATE INDEX idx_matrices_last_update ON matrices(last_update);

-- ----------------------------------------------------------------------------
-- matrix_data_sources: Data sources for matrices
-- ----------------------------------------------------------------------------
CREATE TABLE matrix_data_sources (
    id SERIAL PRIMARY KEY,

    matrix_id INTEGER NOT NULL REFERENCES matrices(id) ON DELETE CASCADE,

    name TEXT NOT NULL,
    name_en TEXT,  -- English translation from INS API
    source_type TEXT,
    link_number INTEGER,
    source_code INTEGER,

    -- Constraints
    CONSTRAINT uq_matrix_data_sources UNIQUE (matrix_id, link_number)
);

COMMENT ON TABLE matrix_data_sources IS 'Data sources associated with matrices';

CREATE INDEX idx_matrix_data_sources_matrix ON matrix_data_sources(matrix_id);

-- ----------------------------------------------------------------------------
-- matrix_dimensions: Dimension definitions per matrix
-- ----------------------------------------------------------------------------
CREATE TABLE matrix_dimensions (
    id SERIAL PRIMARY KEY,

    matrix_id INTEGER NOT NULL REFERENCES matrices(id) ON DELETE CASCADE,

    -- INS API fields
    dim_code SMALLINT NOT NULL,
    label TEXT NOT NULL,
    label_en TEXT,  -- English translation from INS API

    -- Type classification
    dimension_type dimension_type NOT NULL,

    -- Reference to classification type (for CLASSIFICATION dimensions)
    classification_type_id INTEGER REFERENCES classification_types(id),

    -- Metadata
    is_hierarchical BOOLEAN NOT NULL DEFAULT FALSE,
    option_count INTEGER NOT NULL DEFAULT 0,

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Constraints
    CONSTRAINT uq_matrix_dimensions UNIQUE (matrix_id, dim_code)
);

COMMENT ON TABLE matrix_dimensions IS 'Dimension definitions for each matrix (3-6 dimensions per matrix)';
COMMENT ON COLUMN matrix_dimensions.dim_code IS 'Zero-based dimension index, matches INS dimCode';
COMMENT ON COLUMN matrix_dimensions.dimension_type IS 'Type: TEMPORAL, TERRITORIAL, CLASSIFICATION, UNIT_OF_MEASURE';
COMMENT ON COLUMN matrix_dimensions.classification_type_id IS 'Links to classification_types for CLASSIFICATION dimensions';

CREATE INDEX idx_matrix_dimensions_matrix ON matrix_dimensions(matrix_id);
CREATE INDEX idx_matrix_dimensions_type ON matrix_dimensions(dimension_type);
CREATE INDEX idx_matrix_dimensions_class_type ON matrix_dimensions(classification_type_id) WHERE classification_type_id IS NOT NULL;

-- ----------------------------------------------------------------------------
-- matrix_dimension_options: Dimension options with nomItemId mapping
-- ----------------------------------------------------------------------------
CREATE TABLE matrix_dimension_options (
    id SERIAL PRIMARY KEY,

    matrix_dimension_id INTEGER NOT NULL REFERENCES matrix_dimensions(id) ON DELETE CASCADE,

    -- INS API fields (required for query building)
    nom_item_id INTEGER NOT NULL,
    label VARCHAR(500) NOT NULL,
    label_en VARCHAR(500),  -- English translation from INS API
    offset_order INTEGER NOT NULL,
    parent_nom_item_id INTEGER,

    -- References to shared tables (exactly one should be non-null based on dimension type)
    territory_id INTEGER REFERENCES territories(id),
    time_period_id INTEGER REFERENCES time_periods(id),
    classification_value_id INTEGER REFERENCES classification_values(id),
    unit_of_measure_id INTEGER REFERENCES units_of_measure(id),

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Constraints
    CONSTRAINT uq_matrix_dimension_options UNIQUE (matrix_dimension_id, nom_item_id)
);

COMMENT ON TABLE matrix_dimension_options IS 'Maps INS nomItemId values to our shared reference tables';
COMMENT ON COLUMN matrix_dimension_options.nom_item_id IS 'INS unique identifier - required for building encQuery strings';
COMMENT ON COLUMN matrix_dimension_options.offset_order IS 'Display order (1-based), from INS offset field';
COMMENT ON COLUMN matrix_dimension_options.parent_nom_item_id IS 'Parent nomItemId for hierarchical dimensions';

-- Indexes for matrix_dimension_options
CREATE INDEX idx_mdo_dimension ON matrix_dimension_options(matrix_dimension_id);
CREATE INDEX idx_mdo_nom_item_id ON matrix_dimension_options(nom_item_id);
CREATE INDEX idx_mdo_territory ON matrix_dimension_options(territory_id) WHERE territory_id IS NOT NULL;
CREATE INDEX idx_mdo_time_period ON matrix_dimension_options(time_period_id) WHERE time_period_id IS NOT NULL;
CREATE INDEX idx_mdo_classification ON matrix_dimension_options(classification_value_id) WHERE classification_value_id IS NOT NULL;
CREATE INDEX idx_mdo_unit ON matrix_dimension_options(unit_of_measure_id) WHERE unit_of_measure_id IS NOT NULL;
CREATE INDEX idx_mdo_parent ON matrix_dimension_options(parent_nom_item_id) WHERE parent_nom_item_id IS NOT NULL;

-- ============================================================================
-- SECTION 4: SYNC TRACKING
-- ============================================================================

-- ----------------------------------------------------------------------------
-- matrix_sync_status: Track synchronization state per matrix
-- ----------------------------------------------------------------------------
CREATE TABLE matrix_sync_status (
    id SERIAL PRIMARY KEY,

    matrix_id INTEGER NOT NULL REFERENCES matrices(id) ON DELETE CASCADE,

    -- Sync timestamps
    last_full_sync TIMESTAMPTZ,
    last_incremental_sync TIMESTAMPTZ,
    last_metadata_sync TIMESTAMPTZ,

    -- Sync status
    sync_status VARCHAR(20) NOT NULL DEFAULT 'NEVER_SYNCED',

    -- Data coverage
    data_start_year SMALLINT,
    data_end_year SMALLINT,
    row_count BIGINT DEFAULT 0,

    -- Error tracking
    last_error TEXT,
    consecutive_failures INTEGER DEFAULT 0,

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Constraints
    CONSTRAINT uq_matrix_sync_status UNIQUE (matrix_id),
    CONSTRAINT chk_sync_status CHECK (sync_status IN (
        'NEVER_SYNCED', 'SYNCING', 'SYNCED', 'PARTIAL', 'FAILED', 'STALE'
    ))
);

COMMENT ON TABLE matrix_sync_status IS 'Tracks synchronization state and data coverage for each matrix';
COMMENT ON COLUMN matrix_sync_status.sync_status IS 'NEVER_SYNCED, SYNCING, SYNCED, PARTIAL (incomplete), FAILED, STALE (needs refresh)';
COMMENT ON COLUMN matrix_sync_status.data_start_year IS 'Earliest year of data we have';
COMMENT ON COLUMN matrix_sync_status.data_end_year IS 'Latest year of data we have';
COMMENT ON COLUMN matrix_sync_status.row_count IS 'Number of statistics rows for this matrix';

CREATE INDEX idx_matrix_sync_status_status ON matrix_sync_status(sync_status);
CREATE INDEX idx_matrix_sync_status_last_sync ON matrix_sync_status(last_full_sync);

-- ============================================================================
-- SECTION 5: FACT TABLES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- statistics: Main fact table for statistical data values (PARTITIONED)
-- ----------------------------------------------------------------------------
-- Partitioned by matrix_id for performance on large datasets.
-- Each matrix gets its own partition, enabling:
-- - Faster queries scoped to a single matrix
-- - Efficient maintenance (VACUUM, ANALYZE per partition)
-- - Easy data management (drop partition to remove matrix data)
-- ----------------------------------------------------------------------------
CREATE TABLE statistics (
    id BIGSERIAL,

    -- Matrix reference (partition key - must be part of PK)
    matrix_id INTEGER NOT NULL,

    -- Dimension references (denormalized for query performance)
    territory_id INTEGER,
    time_period_id INTEGER NOT NULL,
    unit_of_measure_id INTEGER,

    -- The actual value
    value NUMERIC,
    value_status VARCHAR(10),

    -- Source tracking
    source_enc_query TEXT,
    scraped_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Idempotency support
    natural_key_hash VARCHAR(64),
    version INTEGER NOT NULL DEFAULT 1,

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Primary key includes partition key
    PRIMARY KEY (matrix_id, id)
) PARTITION BY LIST (matrix_id);

COMMENT ON TABLE statistics IS 'Partitioned fact table storing statistical values. Each matrix has its own partition.';
COMMENT ON COLUMN statistics.value IS 'The numeric data value, NULL for missing/unavailable data';
COMMENT ON COLUMN statistics.value_status IS 'Special value markers: ":" (unavailable), "-" (none), "*" (confidential), "<0.5"';
COMMENT ON COLUMN statistics.source_enc_query IS 'Original INS encQuery for traceability and re-scraping';
COMMENT ON COLUMN statistics.natural_key_hash IS 'SHA-256 hash of natural key (matrix_id, territory_id, time_period_id, unit_id, classification_ids) for idempotent upserts';
COMMENT ON COLUMN statistics.version IS 'Row version, incremented on each update';
COMMENT ON COLUMN statistics.updated_at IS 'Timestamp of last update';

-- Note: Indexes are created on partitions automatically when using CREATE INDEX on parent
-- These will be inherited by all partitions
CREATE INDEX idx_statistics_territory ON statistics(territory_id) WHERE territory_id IS NOT NULL;
CREATE INDEX idx_statistics_time ON statistics(time_period_id);
CREATE INDEX idx_statistics_unit ON statistics(unit_of_measure_id) WHERE unit_of_measure_id IS NOT NULL;
CREATE INDEX idx_statistics_star ON statistics(matrix_id, territory_id, time_period_id);
CREATE INDEX idx_statistics_scraped ON statistics(scraped_at);
-- Note: unique index on natural_key_hash is created per partition in create_statistics_partition()

-- Default partition for any matrix_id not yet assigned a specific partition
CREATE TABLE statistics_default PARTITION OF statistics DEFAULT;

-- ----------------------------------------------------------------------------
-- Function: Create partition for a specific matrix
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION create_statistics_partition(p_matrix_id INTEGER)
RETURNS TEXT AS $$
DECLARE
    v_partition_name TEXT;
    v_index_name TEXT;
    v_hash_index_name TEXT;
BEGIN
    v_partition_name := 'statistics_matrix_' || p_matrix_id;
    v_index_name := 'idx_' || v_partition_name || '_time_territory';
    v_hash_index_name := 'idx_' || v_partition_name || '_natural_key';

    -- Check if partition already exists
    IF NOT EXISTS (
        SELECT 1 FROM pg_tables
        WHERE tablename = v_partition_name
        AND schemaname = 'public'
    ) THEN
        -- Create the partition
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF statistics FOR VALUES IN (%s)',
            v_partition_name,
            p_matrix_id
        );

        -- Add foreign key constraints to partition
        EXECUTE format(
            'ALTER TABLE %I ADD CONSTRAINT %I FOREIGN KEY (matrix_id) REFERENCES matrices(id) ON DELETE CASCADE',
            v_partition_name,
            v_partition_name || '_matrix_fk'
        );
        EXECUTE format(
            'ALTER TABLE %I ADD CONSTRAINT %I FOREIGN KEY (territory_id) REFERENCES territories(id)',
            v_partition_name,
            v_partition_name || '_territory_fk'
        );
        EXECUTE format(
            'ALTER TABLE %I ADD CONSTRAINT %I FOREIGN KEY (time_period_id) REFERENCES time_periods(id)',
            v_partition_name,
            v_partition_name || '_time_period_fk'
        );
        EXECUTE format(
            'ALTER TABLE %I ADD CONSTRAINT %I FOREIGN KEY (unit_of_measure_id) REFERENCES units_of_measure(id)',
            v_partition_name,
            v_partition_name || '_unit_fk'
        );

        -- Create composite index for common queries
        EXECUTE format(
            'CREATE INDEX %I ON %I (time_period_id, territory_id)',
            v_index_name,
            v_partition_name
        );

        -- Create unique index on natural_key_hash for upsert support
        EXECUTE format(
            'CREATE UNIQUE INDEX %I ON %I (natural_key_hash) WHERE natural_key_hash IS NOT NULL',
            v_hash_index_name,
            v_partition_name
        );

        RETURN 'Created partition ' || v_partition_name;
    END IF;

    -- Partition exists, ensure hash index exists
    IF NOT EXISTS (
        SELECT 1 FROM pg_class WHERE relname = v_hash_index_name
    ) THEN
        EXECUTE format(
            'CREATE UNIQUE INDEX %I ON %I (natural_key_hash) WHERE natural_key_hash IS NOT NULL',
            v_hash_index_name,
            v_partition_name
        );
        RETURN 'Added hash index to existing partition ' || v_partition_name;
    END IF;

    RETURN 'Partition ' || v_partition_name || ' already exists';
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION create_statistics_partition IS 'Creates a dedicated partition for a matrix with indexes for queries and upsert support.';

-- ----------------------------------------------------------------------------
-- statistic_classifications: Junction table for classification dimensions (PARTITIONED)
-- ----------------------------------------------------------------------------
-- Also partitioned by matrix_id to match statistics table.
-- Note: PostgreSQL doesn't allow FK references to partitioned tables,
-- so we partition this table the same way and handle integrity via application logic.
-- ----------------------------------------------------------------------------
CREATE TABLE statistic_classifications (
    id BIGSERIAL,

    -- Partition key (must match statistics partitioning)
    matrix_id INTEGER NOT NULL,

    -- Reference to statistics (composite: matrix_id + statistic_id)
    statistic_id BIGINT NOT NULL,
    classification_value_id INTEGER NOT NULL REFERENCES classification_values(id),

    -- Primary key includes partition key
    PRIMARY KEY (matrix_id, id),

    -- Unique constraint for deduplication
    CONSTRAINT uq_statistic_classifications UNIQUE (matrix_id, statistic_id, classification_value_id)
) PARTITION BY LIST (matrix_id);

COMMENT ON TABLE statistic_classifications IS 'Partitioned junction table linking statistics to classification values';
COMMENT ON COLUMN statistic_classifications.matrix_id IS 'Partition key - must match the matrix_id of the referenced statistic';
COMMENT ON COLUMN statistic_classifications.statistic_id IS 'Reference to the statistic fact row (integrity via application)';
COMMENT ON COLUMN statistic_classifications.classification_value_id IS 'Reference to the classification value';

-- Indexes (inherited by partitions)
CREATE INDEX idx_stat_class_statistic ON statistic_classifications(matrix_id, statistic_id);
CREATE INDEX idx_stat_class_value ON statistic_classifications(classification_value_id);

-- Default partition
CREATE TABLE statistic_classifications_default PARTITION OF statistic_classifications DEFAULT;

-- ----------------------------------------------------------------------------
-- Function: Create partition for statistic_classifications
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION create_stat_classifications_partition(p_matrix_id INTEGER)
RETURNS VOID AS $$
DECLARE
    v_partition_name TEXT;
BEGIN
    v_partition_name := 'statistic_classifications_matrix_' || p_matrix_id;

    IF NOT EXISTS (
        SELECT 1 FROM pg_tables
        WHERE tablename = v_partition_name
        AND schemaname = 'public'
    ) THEN
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF statistic_classifications FOR VALUES IN (%s)',
            v_partition_name,
            p_matrix_id
        );
    END IF;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION create_stat_classifications_partition IS 'Creates a dedicated partition for statistic_classifications. Call alongside create_statistics_partition.';

-- ============================================================================
-- SECTION 6: SCRAPING INFRASTRUCTURE
-- ============================================================================

-- ----------------------------------------------------------------------------
-- scrape_jobs: Track scraping jobs with chunking support
-- ----------------------------------------------------------------------------
CREATE TABLE scrape_jobs (
    id SERIAL PRIMARY KEY,

    matrix_id INTEGER NOT NULL REFERENCES matrices(id) ON DELETE CASCADE,

    -- Job status
    status scrape_status NOT NULL DEFAULT 'PENDING',

    -- Query parameters
    enc_query TEXT,
    estimated_cells INTEGER,

    -- Chunking info (for queries exceeding 30,000 cells)
    strategy chunk_strategy,
    total_chunks INTEGER DEFAULT 1,
    completed_chunks INTEGER DEFAULT 0,

    -- Timing
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,

    -- Results
    rows_fetched INTEGER DEFAULT 0,
    error_message TEXT,

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE scrape_jobs IS 'Tracks scraping jobs with support for chunked queries (30,000 cell limit)';
COMMENT ON COLUMN scrape_jobs.estimated_cells IS 'Estimated cell count before fetching';
COMMENT ON COLUMN scrape_jobs.strategy IS 'Chunking strategy: BY_YEAR, BY_TERRITORY, BY_CLASSIFICATION';

CREATE INDEX idx_scrape_jobs_matrix ON scrape_jobs(matrix_id);
CREATE INDEX idx_scrape_jobs_status ON scrape_jobs(status);
CREATE INDEX idx_scrape_jobs_created ON scrape_jobs(created_at);

-- ----------------------------------------------------------------------------
-- scrape_chunks: Individual chunks for large queries
-- ----------------------------------------------------------------------------
CREATE TABLE scrape_chunks (
    id SERIAL PRIMARY KEY,

    job_id INTEGER NOT NULL REFERENCES scrape_jobs(id) ON DELETE CASCADE,

    chunk_number INTEGER NOT NULL,
    enc_query TEXT NOT NULL,

    -- Status
    status scrape_status NOT NULL DEFAULT 'PENDING',

    -- Results
    rows_fetched INTEGER DEFAULT 0,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Constraints
    CONSTRAINT uq_scrape_chunks UNIQUE (job_id, chunk_number)
);

COMMENT ON TABLE scrape_chunks IS 'Individual chunks for queries exceeding the 30,000 cell limit';

CREATE INDEX idx_scrape_chunks_job ON scrape_chunks(job_id);
CREATE INDEX idx_scrape_chunks_status ON scrape_chunks(status);

-- ----------------------------------------------------------------------------
-- data_sync_checkpoints: Track sync progress per chunk for incremental sync
-- ----------------------------------------------------------------------------
CREATE TABLE data_sync_checkpoints (
    id SERIAL PRIMARY KEY,

    matrix_id INTEGER NOT NULL REFERENCES matrices(id) ON DELETE CASCADE,

    -- Store hash of enc_query for unique constraint (B-tree has 8KB limit)
    -- The full enc_query can exceed this limit for matrices with many localities
    chunk_enc_query_hash VARCHAR(64) NOT NULL,

    -- Store the full enc_query for reference/debugging
    chunk_enc_query TEXT NOT NULL,

    -- Sync tracking
    last_scraped_at TIMESTAMPTZ NOT NULL,
    row_count INTEGER DEFAULT 0,

    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Constraints: Use hash for unique constraint to avoid B-tree size limit
    CONSTRAINT uq_sync_checkpoints UNIQUE (matrix_id, chunk_enc_query_hash)
);

COMMENT ON TABLE data_sync_checkpoints IS 'Tracks sync progress per chunk (enc_query) for incremental sync support';
COMMENT ON COLUMN data_sync_checkpoints.chunk_enc_query IS 'The encQuery string for this chunk';
COMMENT ON COLUMN data_sync_checkpoints.last_scraped_at IS 'When this chunk was last successfully synced';
COMMENT ON COLUMN data_sync_checkpoints.row_count IS 'Number of rows synced in this chunk';

CREATE INDEX idx_sync_checkpoints_matrix ON data_sync_checkpoints(matrix_id);

-- ============================================================================
-- SECTION 7: FUNCTIONS FOR NATURAL KEY HASH AND PATH COMPUTATION
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Function: Compute natural key hash for statistics
-- ----------------------------------------------------------------------------
-- Used for idempotent upserts. The hash uniquely identifies a data point
-- based on its natural key: matrix + territory + time + unit + classifications
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION compute_statistic_natural_key(
    p_matrix_id INTEGER,
    p_territory_id INTEGER,
    p_time_period_id INTEGER,
    p_unit_of_measure_id INTEGER,
    p_classification_ids INTEGER[]
) RETURNS VARCHAR(64) AS $$
DECLARE
    v_key TEXT;
BEGIN
    -- Build deterministic key: matrix:territory:time:unit:sorted_classifications
    v_key := p_matrix_id::TEXT || ':' ||
             COALESCE(p_territory_id::TEXT, 'N') || ':' ||
             p_time_period_id::TEXT || ':' ||
             COALESCE(p_unit_of_measure_id::TEXT, 'N') || ':' ||
             COALESCE(array_to_string(ARRAY(SELECT unnest(p_classification_ids) ORDER BY 1), ','), '');

    RETURN encode(sha256(v_key::bytea), 'hex');
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION compute_statistic_natural_key IS 'Computes a SHA-256 hash of the natural key for a statistic row. Used for deduplication and upsert operations.';

-- ----------------------------------------------------------------------------
-- Function: Compute context path from parent chain
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION compute_context_path(p_id INTEGER)
RETURNS VARCHAR AS $$
DECLARE
    v_result VARCHAR := '';
    v_current_id INTEGER := p_id;
    v_current_code VARCHAR;
    v_parent_id INTEGER;
BEGIN
    WHILE v_current_id IS NOT NULL LOOP
        SELECT ins_code, parent_id INTO v_current_code, v_parent_id
        FROM contexts WHERE id = v_current_id;

        IF v_result = '' THEN
            v_result := v_current_code;
        ELSE
            v_result := v_current_code || '::' || v_result;
        END IF;

        v_current_id := v_parent_id;
    END LOOP;
    RETURN v_result;
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION compute_context_path IS 'Computes the hierarchical path for a context by traversing parent chain';

-- ----------------------------------------------------------------------------
-- Function: Compute territory path from parent chain
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION compute_territory_path(p_id INTEGER)
RETURNS VARCHAR AS $$
DECLARE
    v_result VARCHAR := '';
    v_current_id INTEGER := p_id;
    v_current_code VARCHAR;
    v_parent_id INTEGER;
BEGIN
    WHILE v_current_id IS NOT NULL LOOP
        SELECT code, parent_id INTO v_current_code, v_parent_id
        FROM territories WHERE id = v_current_id;

        IF v_result = '' THEN
            v_result := v_current_code;
        ELSE
            v_result := v_current_code || '::' || v_result;
        END IF;

        v_current_id := v_parent_id;
    END LOOP;
    RETURN v_result;
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION compute_territory_path IS 'Computes the hierarchical path for a territory by traversing parent chain';

-- ----------------------------------------------------------------------------
-- Function: Compute classification value path from parent chain
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION compute_classification_path(p_id INTEGER)
RETURNS VARCHAR AS $$
DECLARE
    v_result VARCHAR := '';
    v_current_id INTEGER := p_id;
    v_current_code VARCHAR;
    v_parent_id INTEGER;
    v_type_code VARCHAR;
BEGIN
    -- Get the classification type code first
    SELECT ct.code INTO v_type_code
    FROM classification_values cv
    JOIN classification_types ct ON cv.classification_type_id = ct.id
    WHERE cv.id = p_id;

    WHILE v_current_id IS NOT NULL LOOP
        SELECT code, parent_id INTO v_current_code, v_parent_id
        FROM classification_values WHERE id = v_current_id;

        IF v_result = '' THEN
            v_result := v_current_code;
        ELSE
            v_result := v_current_code || '::' || v_result;
        END IF;

        v_current_id := v_parent_id;
    END LOOP;

    -- Prepend the type code
    IF v_type_code IS NOT NULL THEN
        v_result := v_type_code || '::' || v_result;
    END IF;

    RETURN v_result;
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION compute_classification_path IS 'Computes the hierarchical path for a classification value including the type prefix';

-- ============================================================================
-- SECTION 8: TRIGGERS FOR PATH MAINTENANCE
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Trigger function: Update context path on insert/update
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION update_context_path()
RETURNS TRIGGER AS $$
BEGIN
    NEW.path := compute_context_path(NEW.id);
    NEW.updated_at := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- We need a deferred trigger approach because the row must exist first
-- Use AFTER trigger that updates the path
CREATE OR REPLACE FUNCTION set_context_path()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE contexts SET path = compute_context_path(NEW.id) WHERE id = NEW.id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_context_path_insert
AFTER INSERT ON contexts
FOR EACH ROW
EXECUTE FUNCTION set_context_path();

CREATE TRIGGER trg_context_path_update
BEFORE UPDATE OF parent_id ON contexts
FOR EACH ROW
EXECUTE FUNCTION update_context_path();

-- ----------------------------------------------------------------------------
-- Trigger function: Update territory path on insert/update
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION update_territory_path()
RETURNS TRIGGER AS $$
BEGIN
    NEW.path := compute_territory_path(NEW.id);
    NEW.updated_at := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION set_territory_path()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE territories SET path = compute_territory_path(NEW.id) WHERE id = NEW.id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_territory_path_insert
AFTER INSERT ON territories
FOR EACH ROW
EXECUTE FUNCTION set_territory_path();

CREATE TRIGGER trg_territory_path_update
BEFORE UPDATE OF parent_id ON territories
FOR EACH ROW
EXECUTE FUNCTION update_territory_path();

-- ----------------------------------------------------------------------------
-- Trigger function: Update classification value path on insert/update
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION update_classification_path()
RETURNS TRIGGER AS $$
BEGIN
    NEW.path := compute_classification_path(NEW.id);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION set_classification_path()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE classification_values SET path = compute_classification_path(NEW.id) WHERE id = NEW.id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_classification_path_insert
AFTER INSERT ON classification_values
FOR EACH ROW
EXECUTE FUNCTION set_classification_path();

CREATE TRIGGER trg_classification_path_update
BEFORE UPDATE OF parent_id ON classification_values
FOR EACH ROW
EXECUTE FUNCTION update_classification_path();

-- ============================================================================
-- SECTION 9: VIEWS FOR COMMON QUERIES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- View: Matrix summary with dimension info
-- ----------------------------------------------------------------------------
CREATE VIEW v_matrix_summary AS
SELECT
    m.id,
    m.ins_code,
    m.name,
    m.status,
    m.dimension_count,
    m.has_uat_data,
    m.has_county_data,
    m.start_year,
    m.end_year,
    m.last_update,
    c.path AS context_path,
    c.name AS context_name,
    ARRAY_AGG(DISTINCT md.dimension_type ORDER BY md.dimension_type) AS dimension_types
FROM matrices m
LEFT JOIN contexts c ON m.context_id = c.id
LEFT JOIN matrix_dimensions md ON m.id = md.matrix_id
GROUP BY m.id, c.path, c.name;

COMMENT ON VIEW v_matrix_summary IS 'Summary view of matrices with context and dimension type information';

-- ----------------------------------------------------------------------------
-- View: Statistics with denormalized dimensions
-- ----------------------------------------------------------------------------
CREATE VIEW v_statistics_denormalized AS
SELECT
    s.id,
    m.ins_code AS matrix_code,
    m.name AS matrix_name,
    t.name AS territory_name,
    t.level AS territory_level,
    t.path AS territory_path,
    t.siruta_code,
    tp.year,
    tp.quarter,
    tp.month,
    tp.periodicity,
    tp.ins_label AS period_label,
    s.value,
    s.value_status,
    u.name AS unit_name,
    u.symbol AS unit_symbol,
    s.scraped_at
FROM statistics s
JOIN matrices m ON s.matrix_id = m.id
LEFT JOIN territories t ON s.territory_id = t.id
JOIN time_periods tp ON s.time_period_id = tp.id
LEFT JOIN units_of_measure u ON s.unit_of_measure_id = u.id;

COMMENT ON VIEW v_statistics_denormalized IS 'Denormalized view of statistics with all dimension values resolved';

-- ----------------------------------------------------------------------------
-- View: Territory hierarchy
-- ----------------------------------------------------------------------------
CREATE VIEW v_territory_hierarchy AS
SELECT
    t.id,
    t.code,
    t.name,
    t.level,
    t.siruta_code,
    t.path,
    p.id AS parent_id,
    p.name AS parent_name,
    p.level AS parent_level,
    CASE t.level
        WHEN 'NATIONAL' THEN 1
        WHEN 'NUTS1' THEN 2
        WHEN 'NUTS2' THEN 3
        WHEN 'NUTS3' THEN 4
        WHEN 'LAU' THEN 5
    END AS level_order
FROM territories t
LEFT JOIN territories p ON t.parent_id = p.id
ORDER BY t.path;

COMMENT ON VIEW v_territory_hierarchy IS 'Hierarchical view of territories with parent information';

-- ----------------------------------------------------------------------------
-- View: Flat nomItemId lookup (replaces nom_item_mappings table)
-- ----------------------------------------------------------------------------
-- Use this view when you need to look up nomItemIds for query building.
-- Example: Find nomItemId for a specific territory in a matrix.
-- ----------------------------------------------------------------------------
CREATE VIEW v_nom_item_lookup AS
-- Territory mappings
SELECT
    md.matrix_id,
    mdo.nom_item_id,
    mdo.label AS ins_label,
    'TERRITORY' AS ref_type,
    mdo.territory_id AS reference_id,
    t.name AS reference_name,
    t.path AS reference_path
FROM matrix_dimension_options mdo
JOIN matrix_dimensions md ON mdo.matrix_dimension_id = md.id
LEFT JOIN territories t ON mdo.territory_id = t.id
WHERE mdo.territory_id IS NOT NULL

UNION ALL

-- Time period mappings
SELECT
    md.matrix_id,
    mdo.nom_item_id,
    mdo.label AS ins_label,
    'TIME_PERIOD' AS ref_type,
    mdo.time_period_id AS reference_id,
    tp.ins_label AS reference_name,
    NULL AS reference_path
FROM matrix_dimension_options mdo
JOIN matrix_dimensions md ON mdo.matrix_dimension_id = md.id
LEFT JOIN time_periods tp ON mdo.time_period_id = tp.id
WHERE mdo.time_period_id IS NOT NULL

UNION ALL

-- Classification mappings
SELECT
    md.matrix_id,
    mdo.nom_item_id,
    mdo.label AS ins_label,
    'CLASSIFICATION' AS ref_type,
    mdo.classification_value_id AS reference_id,
    cv.name AS reference_name,
    cv.path AS reference_path
FROM matrix_dimension_options mdo
JOIN matrix_dimensions md ON mdo.matrix_dimension_id = md.id
LEFT JOIN classification_values cv ON mdo.classification_value_id = cv.id
WHERE mdo.classification_value_id IS NOT NULL

UNION ALL

-- Unit of measure mappings
SELECT
    md.matrix_id,
    mdo.nom_item_id,
    mdo.label AS ins_label,
    'UNIT' AS ref_type,
    mdo.unit_of_measure_id AS reference_id,
    u.name AS reference_name,
    NULL AS reference_path
FROM matrix_dimension_options mdo
JOIN matrix_dimensions md ON mdo.matrix_dimension_id = md.id
LEFT JOIN units_of_measure u ON mdo.unit_of_measure_id = u.id
WHERE mdo.unit_of_measure_id IS NOT NULL;

COMMENT ON VIEW v_nom_item_lookup IS 'Flat lookup view for nomItemIds. Use to find the nomItemId for a reference entity in a specific matrix.';

-- ----------------------------------------------------------------------------
-- View: Sync status overview
-- ----------------------------------------------------------------------------
CREATE VIEW v_sync_overview AS
SELECT
    m.id AS matrix_id,
    m.ins_code,
    m.name AS matrix_name,
    m.status AS matrix_status,
    COALESCE(ss.sync_status, 'NEVER_SYNCED') AS sync_status,
    ss.last_full_sync,
    ss.last_incremental_sync,
    ss.data_start_year,
    ss.data_end_year,
    ss.row_count,
    ss.consecutive_failures,
    ss.last_error
FROM matrices m
LEFT JOIN matrix_sync_status ss ON m.id = ss.matrix_id;

COMMENT ON VIEW v_sync_overview IS 'Overview of all matrices with their sync status';

-- ============================================================================
-- END OF SCHEMA
-- ============================================================================
