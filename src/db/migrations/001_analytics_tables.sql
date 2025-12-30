-- Migration: Add Discovery & Analytics Tables
-- Run this after the base schema is created

-- ============================================================================
-- DISCOVERY & ANALYTICS TABLES
-- ============================================================================

-- Matrix tags for discoverability
CREATE TABLE IF NOT EXISTS matrix_tags (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    name_en VARCHAR(100),
    slug VARCHAR(100) NOT NULL UNIQUE,
    category VARCHAR(50) NOT NULL DEFAULT 'topic',
    description TEXT,
    usage_count INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_matrix_tags_slug ON matrix_tags(slug);
CREATE INDEX IF NOT EXISTS idx_matrix_tags_category ON matrix_tags(category);

-- Junction table for matrix-tag assignments
CREATE TABLE IF NOT EXISTS matrix_tag_assignments (
    matrix_id INTEGER NOT NULL REFERENCES matrices(id) ON DELETE CASCADE,
    tag_id INTEGER NOT NULL REFERENCES matrix_tags(id) ON DELETE CASCADE,
    PRIMARY KEY (matrix_id, tag_id)
);

CREATE INDEX IF NOT EXISTS idx_matrix_tag_assignments_tag ON matrix_tag_assignments(tag_id);

-- Matrix relationships (for related matrices)
CREATE TABLE IF NOT EXISTS matrix_relationships (
    id SERIAL PRIMARY KEY,
    matrix_id INTEGER NOT NULL REFERENCES matrices(id) ON DELETE CASCADE,
    related_matrix_id INTEGER NOT NULL REFERENCES matrices(id) ON DELETE CASCADE,
    relationship_type VARCHAR(50) NOT NULL,
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (matrix_id, related_matrix_id, relationship_type)
);

CREATE INDEX IF NOT EXISTS idx_matrix_relationships_matrix ON matrix_relationships(matrix_id);
CREATE INDEX IF NOT EXISTS idx_matrix_relationships_related ON matrix_relationships(related_matrix_id);
CREATE INDEX IF NOT EXISTS idx_matrix_relationships_type ON matrix_relationships(relationship_type);

-- Data quality metrics
CREATE TABLE IF NOT EXISTS data_quality_metrics (
    id SERIAL PRIMARY KEY,
    matrix_id INTEGER NOT NULL REFERENCES matrices(id) ON DELETE CASCADE,
    territory_id INTEGER REFERENCES territories(id),
    year SMALLINT,
    expected_data_points INTEGER,
    actual_data_points INTEGER,
    null_count INTEGER DEFAULT 0,
    unavailable_count INTEGER DEFAULT 0,
    computed_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (matrix_id, territory_id, year)
);

CREATE INDEX IF NOT EXISTS idx_data_quality_metrics_matrix ON data_quality_metrics(matrix_id);
CREATE INDEX IF NOT EXISTS idx_data_quality_metrics_territory ON data_quality_metrics(territory_id);
CREATE INDEX IF NOT EXISTS idx_data_quality_metrics_year ON data_quality_metrics(year);

-- Saved queries
CREATE TABLE IF NOT EXISTS saved_queries (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    description TEXT,
    matrix_code VARCHAR(20) NOT NULL,
    territory_filter JSONB,
    time_filter JSONB,
    classification_filter JSONB,
    options JSONB,
    is_public BOOLEAN DEFAULT FALSE,
    execution_count INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_saved_queries_matrix ON saved_queries(matrix_code);
CREATE INDEX IF NOT EXISTS idx_saved_queries_public ON saved_queries(is_public) WHERE is_public;
CREATE INDEX IF NOT EXISTS idx_saved_queries_name ON saved_queries USING gin(name gin_trgm_ops);

-- Composite indicators (calculated from multiple matrices)
CREATE TABLE IF NOT EXISTS composite_indicators (
    id SERIAL PRIMARY KEY,
    code VARCHAR(50) NOT NULL UNIQUE,
    name VARCHAR(200) NOT NULL,
    name_en VARCHAR(200),
    formula TEXT NOT NULL,
    unit_code VARCHAR(50),
    config JSONB NOT NULL DEFAULT '{}',
    category VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_composite_indicators_code ON composite_indicators(code);
CREATE INDEX IF NOT EXISTS idx_composite_indicators_category ON composite_indicators(category);
CREATE INDEX IF NOT EXISTS idx_composite_indicators_active ON composite_indicators(is_active) WHERE is_active;

-- ============================================================================
-- MATERIALIZED VIEWS FOR PERFORMANCE
-- ============================================================================

-- Drop existing views if they exist (for idempotency)
DROP MATERIALIZED VIEW IF EXISTS mv_national_timeseries CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_annual_nuts2_totals CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_matrix_stats CASCADE;

-- National time series cache (aggregated at national level per year)
CREATE MATERIALIZED VIEW mv_national_timeseries AS
SELECT
    s.matrix_id,
    tp.year,
    tp.periodicity,
    COUNT(*) AS data_point_count,
    AVG(s.value) AS avg_value,
    SUM(s.value) AS sum_value,
    MIN(s.value) AS min_value,
    MAX(s.value) AS max_value,
    COUNT(*) FILTER (WHERE s.value IS NULL) AS null_count
FROM statistics s
JOIN time_periods tp ON s.time_period_id = tp.id
LEFT JOIN territories t ON s.territory_id = t.id
WHERE t.level = 'NATIONAL' OR s.territory_id IS NULL
GROUP BY s.matrix_id, tp.year, tp.periodicity;

CREATE UNIQUE INDEX idx_mv_national_timeseries_pk ON mv_national_timeseries(matrix_id, year, periodicity);

-- Annual NUTS2 totals (for regional aggregates)
CREATE MATERIALIZED VIEW mv_annual_nuts2_totals AS
SELECT
    s.matrix_id,
    t.id AS territory_id,
    t.code AS territory_code,
    t.names->>'ro' AS territory_name,
    tp.year,
    COUNT(*) AS data_point_count,
    SUM(s.value) AS total_value,
    AVG(s.value) AS avg_value
FROM statistics s
JOIN time_periods tp ON s.time_period_id = tp.id
JOIN territories t ON s.territory_id = t.id
WHERE t.level = 'NUTS2' AND tp.periodicity = 'ANNUAL'
GROUP BY s.matrix_id, t.id, t.code, t.names->>'ro', tp.year;

CREATE UNIQUE INDEX idx_mv_annual_nuts2_totals_pk ON mv_annual_nuts2_totals(matrix_id, territory_id, year);

-- Matrix statistics summary
CREATE MATERIALIZED VIEW mv_matrix_stats AS
SELECT
    m.id AS matrix_id,
    m.ins_code,
    COUNT(DISTINCT s.id) AS total_records,
    COUNT(DISTINCT s.territory_id) AS territory_count,
    COUNT(DISTINCT s.time_period_id) AS time_period_count,
    MIN(tp.year) AS min_year,
    MAX(tp.year) AS max_year,
    COUNT(*) FILTER (WHERE s.value IS NOT NULL) AS non_null_count,
    COUNT(*) FILTER (WHERE s.value IS NULL) AS null_count,
    MAX(s.updated_at) AS last_data_update
FROM matrices m
LEFT JOIN statistics s ON m.id = s.matrix_id
LEFT JOIN time_periods tp ON s.time_period_id = tp.id
GROUP BY m.id, m.ins_code;

CREATE UNIQUE INDEX idx_mv_matrix_stats_pk ON mv_matrix_stats(matrix_id);

-- Function to refresh all materialized views
CREATE OR REPLACE FUNCTION refresh_analytics_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_national_timeseries;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_annual_nuts2_totals;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_matrix_stats;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- SEED DATA - Matrix tags for discoverability
-- ============================================================================

INSERT INTO matrix_tags (name, name_en, slug, category, description) VALUES
    -- Topic tags
    ('Populație', 'Population', 'population', 'topic', 'Date despre populația rezidentă, structura demografică, mișcarea naturală'),
    ('Demografie', 'Demographics', 'demographics', 'topic', 'Natalitate, mortalitate, speranța de viață, migrație'),
    ('Economie', 'Economy', 'economy', 'topic', 'PIB, producție industrială, comerț, servicii'),
    ('Piața muncii', 'Labor Market', 'labor-market', 'topic', 'Ocupare, șomaj, salarii, forța de muncă'),
    ('Educație', 'Education', 'education', 'topic', 'Învățământ, elevi, studenți, personal didactic'),
    ('Sănătate', 'Health', 'health', 'topic', 'Sistemul sanitar, morbiditate, personal medical'),
    ('Construcții', 'Construction', 'construction', 'topic', 'Locuințe, autorizații de construire, clădiri'),
    ('Transport', 'Transport', 'transport', 'topic', 'Infrastructură rutieră, feroviară, trafic'),
    ('Mediu', 'Environment', 'environment', 'topic', 'Calitatea aerului, apei, deșeuri, zone protejate'),
    ('Agricultură', 'Agriculture', 'agriculture', 'topic', 'Producție agricolă, suprafețe cultivate, efectiv animale'),

    -- Audience tags
    ('Cercetători', 'Researchers', 'researchers', 'audience', 'Date pentru cercetare academică și studii științifice'),
    ('Jurnaliști', 'Journalists', 'journalists', 'audience', 'Date pentru analize și reportaje jurnalistice'),
    ('Administrație', 'Public Administration', 'public-admin', 'audience', 'Date pentru politici publice și planificare'),

    -- Use case tags
    ('Serie temporală', 'Time Series', 'time-series', 'use-case', 'Date disponibile pe mai mulți ani pentru analize de tendință'),
    ('Date regionale', 'Regional Data', 'regional-data', 'use-case', 'Date disponibile la nivel de județ sau regiune'),
    ('Date locale', 'Local Data', 'local-data', 'use-case', 'Date disponibile la nivel de UAT sau localitate')
ON CONFLICT (slug) DO NOTHING;

-- ============================================================================
-- SEED DATA - Composite indicators
-- ============================================================================

INSERT INTO composite_indicators (code, name, name_en, formula, unit_code, config, category, is_active) VALUES
    (
        'population-density',
        'Densitatea populației',
        'Population Density',
        'population / area',
        'PERSONS',
        '{"numerator": {"matrixCode": "POP105A", "description": "Populație rezidentă"}, "denominator": {"matrixCode": "GEO101A", "description": "Suprafață în km²"}, "multiplier": 1}'::jsonb,
        'demographics',
        TRUE
    ),
    (
        'birth-rate',
        'Rata natalității',
        'Birth Rate',
        'births * 1000 / population',
        'PROMILE',
        '{"numerator": {"matrixCode": "POP201A", "description": "Născuți vii"}, "denominator": {"matrixCode": "POP105A", "description": "Populație rezidentă"}, "multiplier": 1000}'::jsonb,
        'demographics',
        TRUE
    ),
    (
        'dependency-ratio',
        'Rata de dependență',
        'Dependency Ratio',
        '(young + old) * 100 / working_age',
        'PERCENT',
        '{"numerator": [{"matrixCode": "POP105A", "filter": {"ageGroup": "0-14"}, "description": "Populație 0-14 ani"}, {"matrixCode": "POP105A", "filter": {"ageGroup": "65+"}, "description": "Populație 65+ ani"}], "denominator": {"matrixCode": "POP105A", "filter": {"ageGroup": "15-64"}, "description": "Populație 15-64 ani"}, "multiplier": 100}'::jsonb,
        'demographics',
        TRUE
    )
ON CONFLICT (code) DO NOTHING;
