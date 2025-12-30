-- ============================================================================
-- PostgreSQL Schema Redesign for INS Statistical Data
-- ============================================================================
--
-- Design Goals:
-- 1. Prevent type-related failures (use TEXT, not VARCHAR)
-- 2. Preserve raw INS data for reprocessing
-- 3. Clean separation between raw/canonical/api layers
-- 4. Efficient hierarchical queries with ltree
-- 5. Flexible bilingual support with JSONB
-- 6. Content-based deduplication
-- 7. Standard star schema for analytics
--
-- ============================================================================

-- Required extensions
CREATE EXTENSION IF NOT EXISTS ltree;        -- Hierarchical paths
CREATE EXTENSION IF NOT EXISTS pg_trgm;      -- Fuzzy text search
CREATE EXTENSION IF NOT EXISTS pgcrypto;     -- Hashing for deduplication

-- ============================================================================
-- SECTION 1: ENUM TYPES (Minimal, stable enums only)
-- ============================================================================

CREATE TYPE periodicity AS ENUM ('ANNUAL', 'QUARTERLY', 'MONTHLY', 'DAILY');
CREATE TYPE territory_level AS ENUM ('NATIONAL', 'NUTS1', 'NUTS2', 'NUTS3', 'LAU');
CREATE TYPE sync_status AS ENUM ('PENDING', 'SYNCING', 'SYNCED', 'FAILED', 'STALE');

-- ============================================================================
-- SECTION 2: RAW LAYER - Preserve exact INS API data
-- ============================================================================
-- This layer stores INS data exactly as received, enabling:
-- - Reprocessing without re-fetching
-- - Debugging entity resolution issues
-- - Audit trail of source data
-- ============================================================================

-- ----------------------------------------------------------------------------
-- raw_matrices: Complete matrix metadata from INS API
-- ----------------------------------------------------------------------------
CREATE TABLE raw_matrices (
    id SERIAL PRIMARY KEY,
    ins_code TEXT NOT NULL UNIQUE,

    -- Store complete API response as JSONB (bilingual)
    metadata_ro JSONB NOT NULL,
    metadata_en JSONB,

    -- Extracted for indexing (derived from metadata_ro)
    name TEXT GENERATED ALWAYS AS (metadata_ro->>'matName') STORED,

    -- Sync tracking
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    api_version TEXT,  -- Track INS API version if available

    -- Indexing
    CONSTRAINT raw_matrices_valid_json CHECK (jsonb_typeof(metadata_ro) = 'object')
);

CREATE INDEX idx_raw_matrices_name ON raw_matrices USING gin(name gin_trgm_ops);
CREATE INDEX idx_raw_matrices_fetched ON raw_matrices(fetched_at);

COMMENT ON TABLE raw_matrices IS 'Raw INS API responses for matrices, preserved for reprocessing';

-- ----------------------------------------------------------------------------
-- raw_dimension_options: All dimension options exactly as from INS
-- ----------------------------------------------------------------------------
CREATE TABLE raw_dimension_options (
    id SERIAL PRIMARY KEY,

    -- Matrix and dimension reference
    matrix_ins_code TEXT NOT NULL,
    dim_index SMALLINT NOT NULL,
    dim_label TEXT NOT NULL,
    dim_label_en TEXT,

    -- Option data (exactly as from INS)
    nom_item_id INTEGER NOT NULL,
    label TEXT NOT NULL,
    label_en TEXT,
    offset_order INTEGER NOT NULL,
    parent_nom_item_id INTEGER,

    -- Content hash for change detection
    content_hash TEXT GENERATED ALWAYS AS (
        encode(digest(
            matrix_ins_code || ':' || dim_index || ':' || nom_item_id || ':' || label,
            'sha256'
        ), 'hex')
    ) STORED,

    -- Timestamps
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Unique constraint on the natural key
    CONSTRAINT uq_raw_dim_options UNIQUE (matrix_ins_code, dim_index, nom_item_id)
);

CREATE INDEX idx_raw_dim_options_matrix ON raw_dimension_options(matrix_ins_code);
CREATE INDEX idx_raw_dim_options_label ON raw_dimension_options USING gin(label gin_trgm_ops);

COMMENT ON TABLE raw_dimension_options IS 'Raw dimension options from INS, preserving exact labels and nomItemIds';

-- ============================================================================
-- SECTION 3: CANONICAL LAYER - Normalized, deduplicated entities
-- ============================================================================
-- This layer contains our canonical entities, deduplicated and normalized.
-- Each entity type has a clear deduplication strategy.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- territories: Canonical territory hierarchy using ltree
-- ----------------------------------------------------------------------------
-- Deduplication: By SIRUTA code (LAU) or NUTS code (higher levels)
-- ----------------------------------------------------------------------------
CREATE TABLE territories (
    id SERIAL PRIMARY KEY,

    -- Identity (exactly one should be set based on level)
    nuts_code TEXT,           -- For NATIONAL, NUTS1, NUTS2, NUTS3
    siruta_code TEXT,         -- For LAU level (6-digit SIRUTA)

    -- Hierarchy using ltree for efficient queries
    level territory_level NOT NULL,
    path ltree NOT NULL,      -- e.g., 'RO.RO1.RO11.BH.12345'
    parent_id INTEGER REFERENCES territories(id),

    -- Names (JSONB for flexibility and bilingual support)
    names JSONB NOT NULL DEFAULT '{}'::jsonb,
    -- Example: {"ro": "Oradea", "en": "Oradea", "normalized": "ORADEA"}

    -- SIRUTA metadata (for LAU)
    siruta_metadata JSONB,
    -- Example: {"tip": 1, "niv": 3, "med": 1, "rang": "I"}

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Constraints
    CONSTRAINT uq_territories_nuts UNIQUE (nuts_code),
    CONSTRAINT uq_territories_siruta UNIQUE (siruta_code),
    CONSTRAINT chk_territory_identity CHECK (
        (level IN ('NATIONAL', 'NUTS1', 'NUTS2', 'NUTS3') AND nuts_code IS NOT NULL) OR
        (level = 'LAU' AND siruta_code IS NOT NULL)
    )
);

CREATE INDEX idx_territories_path ON territories USING gist(path);
CREATE INDEX idx_territories_level ON territories(level);
CREATE INDEX idx_territories_names ON territories USING gin(names);

COMMENT ON TABLE territories IS 'Canonical territory hierarchy with ltree paths for efficient hierarchical queries';

-- ----------------------------------------------------------------------------
-- time_periods: Canonical time periods
-- ----------------------------------------------------------------------------
-- Deduplication: By (year, quarter, month, periodicity) - natural composite key
-- ----------------------------------------------------------------------------
CREATE TABLE time_periods (
    id SERIAL PRIMARY KEY,

    -- Parsed components (natural key)
    year SMALLINT NOT NULL,
    quarter SMALLINT,  -- 1-4, NULL for non-quarterly
    month SMALLINT,    -- 1-12, NULL for non-monthly
    periodicity periodicity NOT NULL,

    -- Computed date range for filtering
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,

    -- Display labels (JSONB for bilingual)
    labels JSONB NOT NULL DEFAULT '{}'::jsonb,
    -- Example: {"ro": "Anul 2023", "en": "Year 2023", "iso": "2023"}

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Natural key constraint
    CONSTRAINT uq_time_periods UNIQUE (year, quarter, month, periodicity),
    CONSTRAINT chk_time_period_valid CHECK (
        (periodicity = 'ANNUAL' AND quarter IS NULL AND month IS NULL) OR
        (periodicity = 'QUARTERLY' AND quarter BETWEEN 1 AND 4 AND month IS NULL) OR
        (periodicity = 'MONTHLY' AND month BETWEEN 1 AND 12)
    )
);

CREATE INDEX idx_time_periods_range ON time_periods(period_start, period_end);
CREATE INDEX idx_time_periods_year ON time_periods(year);

COMMENT ON TABLE time_periods IS 'Canonical time periods with parsed components and date ranges';

-- ----------------------------------------------------------------------------
-- classification_types: Types of classifications
-- ----------------------------------------------------------------------------
CREATE TABLE classification_types (
    id SERIAL PRIMARY KEY,

    -- Identity
    code TEXT NOT NULL UNIQUE,  -- e.g., 'SEX', 'CAEN_REV2', 'AGE_GROUP'

    -- Metadata (JSONB for flexibility)
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    -- Example: {
    --   "names": {"ro": "Sexe", "en": "Sex"},
    --   "is_hierarchical": true,
    --   "source": "INS",
    --   "version": "2023"
    -- }

    -- Pattern matching for auto-detection
    label_patterns TEXT[] NOT NULL DEFAULT '{}',
    -- Example: ['(?i)^sexe$', '(?i)sex\\s+and\\s+gender']

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE classification_types IS 'Classification type definitions with pattern matching for auto-detection';

-- ----------------------------------------------------------------------------
-- classification_values: Classification values with ltree hierarchy
-- ----------------------------------------------------------------------------
-- Deduplication: By (type_id, content_hash) where content_hash is based on
-- normalized name within the type
-- ----------------------------------------------------------------------------
CREATE TABLE classification_values (
    id SERIAL PRIMARY KEY,

    -- Type reference
    type_id INTEGER NOT NULL REFERENCES classification_types(id) ON DELETE CASCADE,

    -- Identity within type
    code TEXT NOT NULL,  -- Generated or explicit code
    content_hash TEXT NOT NULL,  -- Hash of normalized content for dedup

    -- Hierarchy using ltree
    path ltree,  -- e.g., 'SEX.TOTAL' or 'CAEN_REV2.A.01.01_1'
    parent_id INTEGER REFERENCES classification_values(id),
    level SMALLINT NOT NULL DEFAULT 0,

    -- Names (JSONB for bilingual + normalized)
    names JSONB NOT NULL DEFAULT '{}'::jsonb,
    -- Example: {"ro": "Masculin", "en": "Male", "normalized": "MASCULIN"}

    -- Display
    sort_order INTEGER NOT NULL DEFAULT 0,

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Constraints
    CONSTRAINT uq_classification_values UNIQUE (type_id, content_hash)
);

CREATE INDEX idx_class_values_type ON classification_values(type_id);
CREATE INDEX idx_class_values_path ON classification_values USING gist(path);
CREATE INDEX idx_class_values_names ON classification_values USING gin(names);

COMMENT ON TABLE classification_values IS 'Classification values with content-based deduplication and ltree hierarchy';

-- ----------------------------------------------------------------------------
-- units_of_measure: Measurement units
-- ----------------------------------------------------------------------------
CREATE TABLE units_of_measure (
    id SERIAL PRIMARY KEY,

    code TEXT NOT NULL UNIQUE,
    symbol TEXT,

    -- Names (JSONB for bilingual)
    names JSONB NOT NULL DEFAULT '{}'::jsonb,
    -- Example: {"ro": "Numar persoane", "en": "Number of persons", "normalized": "NUMAR_PERSOANE"}

    -- Pattern matching
    label_patterns TEXT[] NOT NULL DEFAULT '{}',

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE units_of_measure IS 'Units of measure with pattern matching';

-- ============================================================================
-- SECTION 4: ENTITY RESOLUTION LAYER - Links raw to canonical
-- ============================================================================
-- This layer maps INS labels to canonical entities.
-- Keeps the mapping separate from both raw and canonical data.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- label_mappings: Maps INS labels to canonical entities
-- ----------------------------------------------------------------------------
-- This is the key table for entity resolution.
-- When a new label is encountered:
-- 1. Check if mapping exists
-- 2. If not, create new canonical entity OR link to existing
-- 3. Store the mapping for future use
-- ----------------------------------------------------------------------------
CREATE TABLE label_mappings (
    id SERIAL PRIMARY KEY,

    -- The raw label (normalized for matching)
    label_normalized TEXT NOT NULL,
    label_original TEXT NOT NULL,  -- Keep original for display

    -- Context for disambiguation
    context_type TEXT NOT NULL,  -- 'TERRITORY', 'TIME_PERIOD', 'CLASSIFICATION', 'UNIT'
    context_hint TEXT,  -- Additional context (e.g., matrix code, dimension label)

    -- Resolution result (exactly one should be set)
    territory_id INTEGER REFERENCES territories(id),
    time_period_id INTEGER REFERENCES time_periods(id),
    classification_value_id INTEGER REFERENCES classification_values(id),
    unit_id INTEGER REFERENCES units_of_measure(id),

    -- Resolution metadata
    resolution_method TEXT,  -- 'EXACT', 'PATTERN', 'FUZZY', 'MANUAL'
    confidence NUMERIC(3,2),  -- 0.00-1.00

    -- For unresolvable labels
    is_unresolvable BOOLEAN NOT NULL DEFAULT FALSE,
    unresolvable_reason TEXT,

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMPTZ,

    -- Constraints
    CONSTRAINT uq_label_mappings UNIQUE (label_normalized, context_type, context_hint),
    CONSTRAINT chk_label_mapping_target CHECK (
        is_unresolvable OR
        (territory_id IS NOT NULL)::int +
        (time_period_id IS NOT NULL)::int +
        (classification_value_id IS NOT NULL)::int +
        (unit_id IS NOT NULL)::int = 1
    )
);

CREATE INDEX idx_label_mappings_label ON label_mappings(label_normalized);
CREATE INDEX idx_label_mappings_context ON label_mappings(context_type, context_hint);
CREATE INDEX idx_label_mappings_unresolved ON label_mappings(is_unresolvable) WHERE is_unresolvable;

COMMENT ON TABLE label_mappings IS 'Maps raw INS labels to canonical entities, enabling consistent entity resolution';

-- ============================================================================
-- SECTION 5: MATRIX LAYER - Processed matrix metadata
-- ============================================================================

-- ----------------------------------------------------------------------------
-- matrices: Processed matrix metadata
-- ----------------------------------------------------------------------------
CREATE TABLE matrices (
    id SERIAL PRIMARY KEY,

    -- Link to raw data
    raw_matrix_id INTEGER NOT NULL REFERENCES raw_matrices(id),
    ins_code TEXT NOT NULL UNIQUE,

    -- Extracted metadata (JSONB for flexibility)
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    -- Example: {
    --   "names": {"ro": "Populatia...", "en": "Population..."},
    --   "definitions": {"ro": "...", "en": "..."},
    --   "methodology": {"ro": "...", "en": "..."},
    --   "periodicity": ["ANNUAL", "QUARTERLY"],
    --   "year_range": [1990, 2024],
    --   "flags": {
    --     "has_uat_data": true,
    --     "has_county_data": true,
    --     "has_caen": false
    --   }
    -- }

    -- Dimension structure (for query building)
    dimensions JSONB NOT NULL DEFAULT '[]'::jsonb,
    -- Example: [
    --   {"index": 0, "type": "TEMPORAL", "label": {"ro": "Ani", "en": "Years"}},
    --   {"index": 1, "type": "TERRITORIAL", "label": {"ro": "Judete", "en": "Counties"}},
    --   {"index": 2, "type": "CLASSIFICATION", "classification_type": "SEX"}
    -- ]

    -- Sync status
    sync_status sync_status NOT NULL DEFAULT 'PENDING',
    last_sync_at TIMESTAMPTZ,
    sync_error TEXT,

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_matrices_status ON matrices(sync_status);
CREATE INDEX idx_matrices_metadata ON matrices USING gin(metadata);

-- ----------------------------------------------------------------------------
-- matrix_nom_items: nomItemId mappings for query building
-- ----------------------------------------------------------------------------
-- This table enables building INS API queries by mapping our canonical
-- entities back to INS nomItemIds for each matrix.
-- ----------------------------------------------------------------------------
CREATE TABLE matrix_nom_items (
    id SERIAL PRIMARY KEY,

    matrix_id INTEGER NOT NULL REFERENCES matrices(id) ON DELETE CASCADE,
    dim_index SMALLINT NOT NULL,
    nom_item_id INTEGER NOT NULL,

    -- Link to canonical entity (exactly one should be set)
    territory_id INTEGER REFERENCES territories(id),
    time_period_id INTEGER REFERENCES time_periods(id),
    classification_value_id INTEGER REFERENCES classification_values(id),
    unit_id INTEGER REFERENCES units_of_measure(id),

    -- Original label (for debugging)
    label_original TEXT NOT NULL,

    -- Hierarchy within dimension
    parent_nom_item_id INTEGER,
    offset_order INTEGER NOT NULL,

    -- Constraints
    CONSTRAINT uq_matrix_nom_items UNIQUE (matrix_id, nom_item_id),
    CONSTRAINT chk_nom_item_target CHECK (
        (territory_id IS NOT NULL)::int +
        (time_period_id IS NOT NULL)::int +
        (classification_value_id IS NOT NULL)::int +
        (unit_id IS NOT NULL)::int <= 1  -- Can be 0 for unresolved
    )
);

CREATE INDEX idx_mni_matrix ON matrix_nom_items(matrix_id);
CREATE INDEX idx_mni_territory ON matrix_nom_items(territory_id) WHERE territory_id IS NOT NULL;
CREATE INDEX idx_mni_time ON matrix_nom_items(time_period_id) WHERE time_period_id IS NOT NULL;
CREATE INDEX idx_mni_class ON matrix_nom_items(classification_value_id) WHERE classification_value_id IS NOT NULL;

COMMENT ON TABLE matrix_nom_items IS 'Maps canonical entities to INS nomItemIds for each matrix';

-- ============================================================================
-- SECTION 6: FACT TABLE - Statistical data (Partitioned)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- statistics: Main fact table
-- ----------------------------------------------------------------------------
CREATE TABLE statistics (
    id BIGSERIAL,
    matrix_id INTEGER NOT NULL,

    -- Dimension references (denormalized for performance)
    territory_id INTEGER REFERENCES territories(id),
    time_period_id INTEGER NOT NULL REFERENCES time_periods(id),
    unit_id INTEGER REFERENCES units_of_measure(id),

    -- The value
    value NUMERIC,
    value_status TEXT,  -- ':' (unavailable), '-' (none), '*' (confidential)

    -- Natural key hash for upserts
    natural_key_hash TEXT NOT NULL,

    -- Provenance
    source_enc_query TEXT,
    scraped_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Version for updates
    version INTEGER NOT NULL DEFAULT 1,

    PRIMARY KEY (matrix_id, id)
) PARTITION BY LIST (matrix_id);

-- Default partition
CREATE TABLE statistics_default PARTITION OF statistics DEFAULT;

-- Junction table for classifications (also partitioned)
CREATE TABLE statistic_classifications (
    matrix_id INTEGER NOT NULL,
    statistic_id BIGINT NOT NULL,
    classification_value_id INTEGER NOT NULL REFERENCES classification_values(id),

    PRIMARY KEY (matrix_id, statistic_id, classification_value_id)
) PARTITION BY LIST (matrix_id);

CREATE TABLE statistic_classifications_default PARTITION OF statistic_classifications DEFAULT;

-- ============================================================================
-- SECTION 7: HELPER FUNCTIONS
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Function: Normalize text for matching
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION normalize_text(input TEXT)
RETURNS TEXT AS $$
BEGIN
    RETURN upper(
        trim(
            regexp_replace(
                unaccent(input),
                '\s+', ' ', 'g'
            )
        )
    );
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- ----------------------------------------------------------------------------
-- Function: Generate content hash for deduplication
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION content_hash(content TEXT)
RETURNS TEXT AS $$
BEGIN
    RETURN encode(digest(normalize_text(content), 'sha256'), 'hex');
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- ----------------------------------------------------------------------------
-- Function: Parse time period label
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION parse_time_period(label TEXT)
RETURNS TABLE (
    year SMALLINT,
    quarter SMALLINT,
    month SMALLINT,
    periodicity periodicity
) AS $$
DECLARE
    trimmed TEXT := trim(label);
    match_result TEXT[];
BEGIN
    -- Annual: "Anul 2023" or just "2023"
    IF trimmed ~ '^\d{4}$' OR trimmed ~* '^Anul\s+\d{4}$' THEN
        year := (regexp_match(trimmed, '(\d{4})'))[1]::SMALLINT;
        periodicity := 'ANNUAL';
        RETURN NEXT;
        RETURN;
    END IF;

    -- Quarterly: "Trimestrul I 2024" or "T1 2024"
    IF trimmed ~* 'Trimestrul\s+(I{1,3}V?|IV)\s+(\d{4})' THEN
        match_result := regexp_match(trimmed, 'Trimestrul\s+(I{1,3}V?|IV)\s+(\d{4})', 'i');
        year := match_result[2]::SMALLINT;
        quarter := CASE match_result[1]
            WHEN 'I' THEN 1 WHEN 'II' THEN 2
            WHEN 'III' THEN 3 WHEN 'IV' THEN 4
        END;
        periodicity := 'QUARTERLY';
        RETURN NEXT;
        RETURN;
    END IF;

    -- Monthly: "Luna Ianuarie 2024"
    IF trimmed ~* 'Luna\s+(\w+)\s+(\d{4})' THEN
        match_result := regexp_match(trimmed, 'Luna\s+(\w+)\s+(\d{4})', 'i');
        year := match_result[2]::SMALLINT;
        month := CASE lower(match_result[1])
            WHEN 'ianuarie' THEN 1 WHEN 'februarie' THEN 2
            WHEN 'martie' THEN 3 WHEN 'aprilie' THEN 4
            WHEN 'mai' THEN 5 WHEN 'iunie' THEN 6
            WHEN 'iulie' THEN 7 WHEN 'august' THEN 8
            WHEN 'septembrie' THEN 9 WHEN 'octombrie' THEN 10
            WHEN 'noiembrie' THEN 11 WHEN 'decembrie' THEN 12
        END;
        periodicity := 'MONTHLY';
        RETURN NEXT;
        RETURN;
    END IF;

    -- No match - return empty
    RETURN;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- ----------------------------------------------------------------------------
-- Function: Find or create time period
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION find_or_create_time_period(
    p_label TEXT,
    p_lang TEXT DEFAULT 'ro'
)
RETURNS INTEGER AS $$
DECLARE
    v_parsed RECORD;
    v_id INTEGER;
    v_start DATE;
    v_end DATE;
BEGIN
    -- Parse the label
    SELECT * INTO v_parsed FROM parse_time_period(p_label);

    IF v_parsed.year IS NULL THEN
        RETURN NULL;  -- Unparseable
    END IF;

    -- Check for existing
    SELECT id INTO v_id
    FROM time_periods tp
    WHERE tp.year = v_parsed.year
      AND tp.quarter IS NOT DISTINCT FROM v_parsed.quarter
      AND tp.month IS NOT DISTINCT FROM v_parsed.month
      AND tp.periodicity = v_parsed.periodicity;

    IF v_id IS NOT NULL THEN
        -- Update labels if needed
        UPDATE time_periods
        SET labels = labels || jsonb_build_object(p_lang, p_label)
        WHERE id = v_id
          AND NOT (labels ? p_lang);
        RETURN v_id;
    END IF;

    -- Compute date range
    IF v_parsed.periodicity = 'ANNUAL' THEN
        v_start := make_date(v_parsed.year, 1, 1);
        v_end := make_date(v_parsed.year, 12, 31);
    ELSIF v_parsed.periodicity = 'QUARTERLY' THEN
        v_start := make_date(v_parsed.year, (v_parsed.quarter - 1) * 3 + 1, 1);
        v_end := (make_date(v_parsed.year, v_parsed.quarter * 3, 1) + interval '1 month - 1 day')::date;
    ELSIF v_parsed.periodicity = 'MONTHLY' THEN
        v_start := make_date(v_parsed.year, v_parsed.month, 1);
        v_end := (make_date(v_parsed.year, v_parsed.month, 1) + interval '1 month - 1 day')::date;
    END IF;

    -- Insert new
    INSERT INTO time_periods (year, quarter, month, periodicity, period_start, period_end, labels)
    VALUES (v_parsed.year, v_parsed.quarter, v_parsed.month, v_parsed.periodicity, v_start, v_end,
            jsonb_build_object(p_lang, p_label))
    RETURNING id INTO v_id;

    RETURN v_id;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- SECTION 8: API VIEWS - Denormalized for efficient queries
-- ============================================================================

-- ----------------------------------------------------------------------------
-- View: Matrix catalog with full metadata
-- ----------------------------------------------------------------------------
CREATE VIEW api_matrices AS
SELECT
    m.id,
    m.ins_code,
    m.metadata->'names'->>'ro' AS name_ro,
    m.metadata->'names'->>'en' AS name_en,
    m.metadata->'definitions'->>'ro' AS definition_ro,
    m.metadata->'definitions'->>'en' AS definition_en,
    m.metadata->'periodicity' AS periodicity,
    (m.metadata->'year_range'->>0)::int AS start_year,
    (m.metadata->'year_range'->>1)::int AS end_year,
    (m.metadata->'flags'->>'has_uat_data')::boolean AS has_uat_data,
    (m.metadata->'flags'->>'has_county_data')::boolean AS has_county_data,
    m.dimensions,
    m.sync_status,
    m.last_sync_at
FROM matrices m;

-- ----------------------------------------------------------------------------
-- View: Territory hierarchy with translations
-- ----------------------------------------------------------------------------
CREATE VIEW api_territories AS
SELECT
    t.id,
    t.nuts_code,
    t.siruta_code,
    t.level,
    t.path::text AS path,
    t.names->>'ro' AS name_ro,
    t.names->>'en' AS name_en,
    t.names->>'normalized' AS name_normalized,
    p.id AS parent_id,
    p.names->>'ro' AS parent_name_ro,
    t.siruta_metadata
FROM territories t
LEFT JOIN territories p ON t.parent_id = p.id;

-- ----------------------------------------------------------------------------
-- View: Classification values with type info
-- ----------------------------------------------------------------------------
CREATE VIEW api_classification_values AS
SELECT
    cv.id,
    ct.code AS type_code,
    ct.metadata->'names'->>'ro' AS type_name_ro,
    ct.metadata->'names'->>'en' AS type_name_en,
    cv.code AS value_code,
    cv.names->>'ro' AS value_name_ro,
    cv.names->>'en' AS value_name_en,
    cv.path::text AS path,
    cv.level,
    cv.sort_order,
    cv.parent_id
FROM classification_values cv
JOIN classification_types ct ON cv.type_id = ct.id;

-- ----------------------------------------------------------------------------
-- View: Statistics denormalized for API
-- ----------------------------------------------------------------------------
CREATE VIEW api_statistics AS
SELECT
    s.id,
    m.ins_code AS matrix_code,
    m.metadata->'names'->>'ro' AS matrix_name_ro,
    t.names->>'ro' AS territory_name_ro,
    t.nuts_code,
    t.siruta_code,
    t.level AS territory_level,
    tp.year,
    tp.quarter,
    tp.month,
    tp.periodicity,
    tp.labels->>'ro' AS period_label_ro,
    s.value,
    s.value_status,
    u.names->>'ro' AS unit_name_ro,
    u.symbol AS unit_symbol
FROM statistics s
JOIN matrices m ON s.matrix_id = m.id
LEFT JOIN territories t ON s.territory_id = t.id
JOIN time_periods tp ON s.time_period_id = tp.id
LEFT JOIN units_of_measure u ON s.unit_id = u.id;

-- ----------------------------------------------------------------------------
-- View: Unresolved labels for review
-- ----------------------------------------------------------------------------
CREATE VIEW api_unresolved_labels AS
SELECT
    lm.label_original,
    lm.label_normalized,
    lm.context_type,
    lm.context_hint,
    lm.unresolvable_reason,
    COUNT(*) OVER (PARTITION BY lm.label_normalized) AS occurrence_count,
    lm.created_at
FROM label_mappings lm
WHERE lm.is_unresolvable
ORDER BY occurrence_count DESC, lm.created_at DESC;

-- ============================================================================
-- SECTION 9: QUERY BUILDING FUNCTIONS
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Function: Build encQuery for INS API
-- ----------------------------------------------------------------------------
-- Given a matrix and filter criteria, builds the encQuery string
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION build_enc_query(
    p_matrix_code TEXT,
    p_territory_ids INTEGER[] DEFAULT NULL,
    p_time_period_ids INTEGER[] DEFAULT NULL,
    p_classification_value_ids INTEGER[] DEFAULT NULL
)
RETURNS TEXT AS $$
DECLARE
    v_matrix_id INTEGER;
    v_dim_count INTEGER;
    v_query_parts TEXT[];
    v_dim_index INTEGER;
    v_nom_items INTEGER[];
BEGIN
    -- Get matrix
    SELECT id, jsonb_array_length(dimensions)
    INTO v_matrix_id, v_dim_count
    FROM matrices
    WHERE ins_code = p_matrix_code;

    IF v_matrix_id IS NULL THEN
        RAISE EXCEPTION 'Matrix % not found', p_matrix_code;
    END IF;

    -- Initialize query parts array
    v_query_parts := ARRAY[]::TEXT[];

    -- For each dimension, collect nomItemIds
    FOR v_dim_index IN 0..(v_dim_count - 1) LOOP
        SELECT array_agg(nom_item_id ORDER BY offset_order)
        INTO v_nom_items
        FROM matrix_nom_items
        WHERE matrix_id = v_matrix_id
          AND dim_index = v_dim_index
          AND (
              (p_territory_ids IS NOT NULL AND territory_id = ANY(p_territory_ids)) OR
              (p_time_period_ids IS NOT NULL AND time_period_id = ANY(p_time_period_ids)) OR
              (p_classification_value_ids IS NOT NULL AND classification_value_id = ANY(p_classification_value_ids)) OR
              (p_territory_ids IS NULL AND p_time_period_ids IS NULL AND p_classification_value_ids IS NULL)
          );

        IF v_nom_items IS NOT NULL AND array_length(v_nom_items, 1) > 0 THEN
            v_query_parts := v_query_parts ||
                (v_dim_index || '~' || array_to_string(v_nom_items, '-'));
        END IF;
    END LOOP;

    RETURN array_to_string(v_query_parts, ',');
END;
$$ LANGUAGE plpgsql STABLE;

-- ============================================================================
-- END OF SCHEMA
-- ============================================================================

/*
MIGRATION NOTES:
================

1. Data Migration Strategy:
   - Export current data to CSV/JSON
   - Create new schema in parallel
   - Run migration scripts to transform and load
   - Validate data integrity
   - Switch over

2. Key Differences from Current Schema:
   - JSONB for flexible metadata instead of fixed columns
   - ltree for hierarchical paths instead of string paths
   - Separate raw layer preserving INS data
   - Label mappings table for entity resolution
   - Content-based hashing for deduplication
   - All TEXT instead of VARCHAR

3. API Compatibility:
   - Views provide backward-compatible structure
   - Functions encapsulate query building
   - Bilingual support via JSONB fields

4. Performance Considerations:
   - GIN indexes on JSONB fields
   - GIST indexes on ltree paths
   - Partitioning for statistics table
   - Materialized views for heavy queries (add as needed)

5. Future Extensions:
   - Add materialized views for aggregations
   - Add full-text search with tsvector
   - Add audit tables for change tracking
   - Add API rate limiting tables
*/
