import type { Generated, Insertable, Selectable, Updateable } from "kysely";

// ============================================================================
// ENUM Types (matching PostgreSQL ENUMs in postgres-schema.sql)
// ============================================================================

export type Periodicity = "ANNUAL" | "QUARTERLY" | "MONTHLY";

export type TerritoryLevel = "NATIONAL" | "NUTS1" | "NUTS2" | "NUTS3" | "LAU";

export type SyncStatus = "PENDING" | "SYNCING" | "SYNCED" | "FAILED" | "STALE";

export type DimensionType =
  | "TEMPORAL"
  | "TERRITORIAL"
  | "CLASSIFICATION"
  | "UNIT_OF_MEASURE";

// ============================================================================
// JSONB Structures (for flexible bilingual metadata)
// ============================================================================

/**
 * Bilingual text structure used throughout the schema
 */
export interface BilingualText {
  ro: string;
  en?: string;
  normalized?: string;
}

/**
 * Matrix metadata stored as JSONB
 */
export interface MatrixMetadata {
  names: BilingualText;
  definitions?: BilingualText;
  methodologies?: BilingualText;
  observations?: BilingualText;
  seriesBreak?: BilingualText;
  seriesContinuation?: BilingualText;
  responsiblePersons?: string;
  lastUpdate?: string;
  periodicity: Periodicity[];
  yearRange?: [number, number];
  flags: {
    hasUatData: boolean;
    hasCountyData: boolean;
    hasSiruta: boolean;
    hasCaenRev1: boolean;
    hasCaenRev2: boolean;
  };
  dataSources?: {
    name: string;
    nameEn?: string;
    sourceType?: string;
    linkNumber?: number;
    sourceCode?: number;
  }[];
  dimensionIndicators?: {
    territorialDimIndex?: number;
    timeDimIndex?: number;
    countyDimIndex?: number;
    localityDimIndex?: number;
    umSpecial?: boolean;
  };
  details?: Record<string, unknown>;
}

/**
 * Matrix dimension summary stored as JSONB
 */
export interface DimensionSummary {
  index: number;
  labelRo: string;
  labelEn?: string;
  type: DimensionType;
  optionCount: number;
  isHierarchical: boolean;
  classificationTypeCode?: string;
}

/**
 * SIRUTA metadata for territories
 */
export interface SirutaMetadata {
  tip?: number;
  niv?: number;
  med?: number;
  rang?: number;
  fsl?: number;
  regiune?: number;
  mnemonic?: string;
}

// ============================================================================
// CANONICAL LAYER Tables
// ============================================================================

/**
 * contexts - Statistical domain hierarchy with bilingual JSONB names
 */
export interface ContextsTable {
  id: Generated<number>;
  ins_code: string;
  names: BilingualText;
  level: number;
  parent_id: number | null;
  path: string; // ltree stored as string
  children_type: "context" | "matrix";
  created_at: Generated<Date>;
  updated_at: Generated<Date>;
}

/**
 * territories - NUTS + LAU hierarchy with ltree path
 */
export interface TerritoriesTable {
  id: Generated<number>;
  code: string;
  siruta_code: string | null;
  level: TerritoryLevel;
  path: string; // ltree stored as string
  parent_id: number | null;
  name: string; // Romanian name (use normalizeText() for comparisons)
  siruta_metadata: SirutaMetadata | null;
  created_at: Generated<Date>;
  updated_at: Generated<Date>;
}

/**
 * time_periods - Unified time periods
 */
export interface TimePeriodsTable {
  id: Generated<number>;
  year: number;
  quarter: number | null;
  month: number | null;
  periodicity: Periodicity;
  period_start: Date;
  period_end: Date;
  labels: BilingualText;
  created_at: Generated<Date>;
}

/**
 * classification_types - Classification type definitions with bilingual names
 */
export interface ClassificationTypesTable {
  id: Generated<number>;
  code: string;
  names: BilingualText;
  is_hierarchical: boolean;
  label_patterns: string[];
  created_at: Generated<Date>;
}

/**
 * classification_values - Classification values with content hash
 */
export interface ClassificationValuesTable {
  id: Generated<number>;
  type_id: number;
  code: string;
  content_hash: string;
  path: string | null; // ltree for hierarchical
  parent_id: number | null;
  level: number;
  names: BilingualText;
  sort_order: number;
  created_at: Generated<Date>;
}

/**
 * units_of_measure - Units with bilingual names
 */
export interface UnitsOfMeasureTable {
  id: Generated<number>;
  code: string;
  symbol: string | null;
  names: BilingualText;
  label_patterns: string[];
  created_at: Generated<Date>;
}

/**
 * matrices - Statistical datasets with JSONB metadata
 */
export interface MatricesTable {
  id: Generated<number>;
  ins_code: string;
  context_id: number | null;
  metadata: MatrixMetadata;
  dimensions: DimensionSummary[];
  sync_status: SyncStatus;
  last_sync_at: Date | null;
  sync_error: string | null;
  created_at: Generated<Date>;
  updated_at: Generated<Date>;
}

/**
 * matrix_dimensions - Dimension definitions per matrix
 */
export interface MatrixDimensionsTable {
  id: Generated<number>;
  matrix_id: number;
  dim_index: number;
  dimension_type: DimensionType;
  labels: BilingualText;
  classification_type_id: number | null;
  is_hierarchical: boolean;
  option_count: number;
  created_at: Generated<Date>;
}

/**
 * matrix_nom_items - nomItemId mappings to canonical entities
 */
export interface MatrixNomItemsTable {
  id: Generated<number>;
  matrix_id: number;
  dim_index: number;
  nom_item_id: number;
  dimension_type: DimensionType; // NOT NULL - we always know the type from the parent dimension
  territory_id: number | null;
  time_period_id: number | null;
  classification_value_id: number | null;
  unit_id: number | null;
  labels: BilingualText;
  parent_nom_item_id: number | null;
  offset_order: number;
  created_at: Generated<Date>;
}

// ============================================================================
// ENTITY RESOLUTION LAYER
// ============================================================================

/**
 * label_mappings - Auditable entity resolution
 */
export interface LabelMappingsTable {
  id: Generated<number>;
  label_normalized: string;
  label_original: string;
  context_type: "TERRITORY" | "TIME_PERIOD" | "CLASSIFICATION" | "UNIT";
  context_hint: Generated<string>; // NOT NULL DEFAULT '' - empty string = no hint

  // Resolved targets (exactly one should be non-null if resolved)
  territory_id: number | null;
  time_period_id: number | null;
  classification_value_id: number | null;
  unit_id: number | null;

  // Resolution metadata
  resolution_method: "EXACT" | "PATTERN" | "FUZZY" | "MANUAL" | "SIRUTA" | null;
  confidence: number | null;

  // Unresolvable tracking
  is_unresolvable: boolean;
  unresolvable_reason: string | null;

  created_at: Generated<Date>;
  resolved_at: Date | null;
}

// ============================================================================
// FACT TABLES
// ============================================================================

/**
 * statistics - Main fact table (partitioned by matrix_id)
 */
export interface StatisticsTable {
  id: Generated<number>;
  matrix_id: number;
  territory_id: number | null;
  time_period_id: number;
  unit_id: number | null;
  value: number | null;
  value_status: string | null;
  natural_key_hash: string;
  source_enc_query: string | null;
  created_at: Generated<Date>;
  updated_at: Generated<Date>;
  version: Generated<number>;
}

/**
 * statistic_classifications - Junction table (partitioned by matrix_id)
 */
export interface StatisticClassificationsTable {
  matrix_id: number;
  statistic_id: number;
  classification_value_id: number;
}

// ============================================================================
// SYNC SYSTEM Types
// ============================================================================

/**
 * Sync task status enum (matrix-level work requests)
 */
export type SyncTaskStatus =
  | "PENDING" // Waiting to be processed
  | "PLANNING" // Generating chunks
  | "RUNNING" // Processing chunks
  | "COMPLETED" // All chunks done
  | "FAILED" // Unrecoverable error or max retries exceeded
  | "CANCELLED"; // Manually cancelled

/**
 * Sync chunk status enum (individual API calls)
 */
export type SyncChunkStatus =
  | "PENDING" // Not yet attempted
  | "RUNNING" // Currently fetching
  | "COMPLETED" // Successfully synced
  | "FAILED" // Failed, will retry
  | "EXHAUSTED" // Max retries exceeded
  | "SKIPPED"; // Already up-to-date (resume mode)

/**
 * Default sync configuration
 */
export const SYNC_DEFAULTS = {
  /** Default start year when not specified */
  yearFrom: 2020,
  /** Default end year when not specified (current year) */
  yearTo: new Date().getFullYear(),
  /** Default classification mode */
  classificationMode: "totals-only" as const,
  /** Retry configuration */
  retry: {
    maxRetries: 3,
    initialBackoffMs: 2000,
    maxBackoffMs: 30000,
    backoffMultiplier: 2,
  },
  /** Lease durations */
  lease: {
    taskDurationMs: 5 * 60 * 1000, // 5 minutes
    chunkDurationMs: 2 * 60 * 1000, // 2 minutes
    apiLockDurationMs: 30 * 1000, // 30 seconds
  },
  /** Rate limit */
  rateLimitMs: 750,
} as const;

/**
 * sync_tasks - Queue table for matrix sync requests (replaces sync_jobs)
 */
export interface SyncTasksTable {
  id: Generated<number>;
  matrix_id: number;

  // Request parameters (immutable after creation)
  year_from: number;
  year_to: number;
  classification_mode: string;
  county_code: string | null;

  // Status
  status: SyncTaskStatus;
  priority: number;

  // Progress (updated as chunks complete)
  chunks_total: number | null;
  chunks_completed: number;
  chunks_failed: number;
  rows_inserted: number;
  rows_updated: number;

  // Timing
  created_at: Generated<Date>;
  started_at: Date | null;
  completed_at: Date | null;

  // Error info
  error_message: string | null;

  // Lease-based locking
  locked_until: Date | null;
  locked_by: string | null;

  // Tracking
  created_by: string;
}

/**
 * sync_checkpoints - Track chunk-level progress within a task
 */
export interface SyncCheckpointsTable {
  id: Generated<number>;
  task_id: number | null; // null for direct CLI syncs without a task
  matrix_id: number;

  // Chunk identification
  chunk_hash: string;
  chunk_index: number;
  chunk_name: string;

  // Chunk parameters
  county_code: string | null;
  year_from: number | null;
  year_to: number | null;
  cells_estimated: number;

  // Status
  status: SyncChunkStatus;

  // Results
  cells_returned: number | null;
  rows_synced: number | null;

  // Timing
  created_at: Generated<Date>;
  started_at: Date | null;
  completed_at: Date | null;

  // Error handling
  error_message: string | null;
  retry_count: number;
  next_retry_at: Date | null;

  // Lease-based locking
  locked_until: Date | null;
  locked_by: string | null;
}

/**
 * sync_rate_limiter - Global INS API lock (single row table)
 */
export interface SyncRateLimiterTable {
  id: number; // Always 1

  // Current lock
  locked_until: Date | null;
  locked_by: string | null;

  // Rate limit config
  min_interval_ms: number;

  // Stats
  last_call_at: Date | null;
  calls_today: number;
  stats_reset_at: Date;
}

// ============================================================================
// DISCOVERY & ANALYTICS Tables
// ============================================================================

/**
 * matrix_tags - Tags for discoverability
 */
export interface MatrixTagsTable {
  id: Generated<number>;
  name: string;
  name_en: string | null;
  slug: string;
  category: string;
  description: string | null;
  usage_count: Generated<number>;
  created_at: Generated<Date>;
}

/**
 * matrix_tag_assignments - Junction table for matrix-tag assignments
 */
export interface MatrixTagAssignmentsTable {
  matrix_id: number;
  tag_id: number;
}

/**
 * matrix_relationships - Related matrices
 */
export interface MatrixRelationshipsTable {
  id: Generated<number>;
  matrix_id: number;
  related_matrix_id: number;
  relationship_type: string;
  notes: string | null;
  created_at: Generated<Date>;
}

/**
 * data_quality_metrics - Data quality metrics per matrix/territory/year
 */
export interface DataQualityMetricsTable {
  id: Generated<number>;
  matrix_id: number;
  territory_id: number | null;
  year: number | null;
  expected_data_points: number | null;
  actual_data_points: number | null;
  null_count: Generated<number>;
  unavailable_count: Generated<number>;
  computed_at: Generated<Date>;
}

/**
 * saved_queries - User-saved queries for reuse
 */
export interface SavedQueriesTable {
  id: Generated<number>;
  name: string;
  description: string | null;
  matrix_code: string;
  territory_filter: Record<string, unknown> | null;
  time_filter: Record<string, unknown> | null;
  classification_filter: Record<string, unknown> | null;
  options: Record<string, unknown> | null;
  is_public: Generated<boolean>;
  execution_count: Generated<number>;
  created_at: Generated<Date>;
  updated_at: Generated<Date>;
}

/**
 * composite_indicators - Calculated indicators from multiple matrices
 */
export interface CompositeIndicatorsTable {
  id: Generated<number>;
  code: string;
  name: string;
  name_en: string | null;
  formula: string;
  unit_code: string | null;
  config: CompositeIndicatorConfig;
  category: string | null;
  is_active: Generated<boolean>;
  created_at: Generated<Date>;
  updated_at: Generated<Date>;
}

/**
 * Config structure for composite indicators
 */
export interface CompositeIndicatorConfig {
  numerator:
    | {
        matrixCode: string;
        description?: string;
        filter?: Record<string, unknown>;
      }
    | {
        matrixCode: string;
        description?: string;
        filter?: Record<string, unknown>;
      }[];
  denominator?: {
    matrixCode: string;
    description?: string;
    filter?: Record<string, unknown>;
  };
  multiplier?: number;
}

// ============================================================================
// Database Interface
// ============================================================================

export interface Database {
  // Canonical layer
  contexts: ContextsTable;
  territories: TerritoriesTable;
  time_periods: TimePeriodsTable;
  classification_types: ClassificationTypesTable;
  classification_values: ClassificationValuesTable;
  units_of_measure: UnitsOfMeasureTable;
  matrices: MatricesTable;
  matrix_dimensions: MatrixDimensionsTable;
  matrix_nom_items: MatrixNomItemsTable;

  // Entity resolution
  label_mappings: LabelMappingsTable;

  // Fact tables
  statistics: StatisticsTable;
  statistic_classifications: StatisticClassificationsTable;

  // Sync system
  sync_tasks: SyncTasksTable;
  sync_checkpoints: SyncCheckpointsTable;
  sync_rate_limiter: SyncRateLimiterTable;

  // Discovery & Analytics
  matrix_tags: MatrixTagsTable;
  matrix_tag_assignments: MatrixTagAssignmentsTable;
  matrix_relationships: MatrixRelationshipsTable;
  data_quality_metrics: DataQualityMetricsTable;
  saved_queries: SavedQueriesTable;
  composite_indicators: CompositeIndicatorsTable;
}

// ============================================================================
// Row Types (for convenience)
// ============================================================================

// Contexts
export type Context = Selectable<ContextsTable>;
export type NewContext = Insertable<ContextsTable>;
export type ContextUpdate = Updateable<ContextsTable>;

// Territories
export type Territory = Selectable<TerritoriesTable>;
export type NewTerritory = Insertable<TerritoriesTable>;
export type TerritoryUpdate = Updateable<TerritoriesTable>;

// Time Periods
export type TimePeriod = Selectable<TimePeriodsTable>;
export type NewTimePeriod = Insertable<TimePeriodsTable>;
export type TimePeriodUpdate = Updateable<TimePeriodsTable>;

// Classification Types
export type ClassificationType = Selectable<ClassificationTypesTable>;
export type NewClassificationType = Insertable<ClassificationTypesTable>;
export type ClassificationTypeUpdate = Updateable<ClassificationTypesTable>;

// Classification Values
export type ClassificationValue = Selectable<ClassificationValuesTable>;
export type NewClassificationValue = Insertable<ClassificationValuesTable>;
export type ClassificationValueUpdate = Updateable<ClassificationValuesTable>;

// Units of Measure
export type UnitOfMeasure = Selectable<UnitsOfMeasureTable>;
export type NewUnitOfMeasure = Insertable<UnitsOfMeasureTable>;
export type UnitOfMeasureUpdate = Updateable<UnitsOfMeasureTable>;

// Matrices
export type Matrix = Selectable<MatricesTable>;
export type NewMatrix = Insertable<MatricesTable>;
export type MatrixUpdate = Updateable<MatricesTable>;

// Matrix Dimensions
export type MatrixDimension = Selectable<MatrixDimensionsTable>;
export type NewMatrixDimension = Insertable<MatrixDimensionsTable>;
export type MatrixDimensionUpdate = Updateable<MatrixDimensionsTable>;

// Matrix Nom Items
export type MatrixNomItem = Selectable<MatrixNomItemsTable>;
export type NewMatrixNomItem = Insertable<MatrixNomItemsTable>;
export type MatrixNomItemUpdate = Updateable<MatrixNomItemsTable>;

// Label Mappings
export type LabelMapping = Selectable<LabelMappingsTable>;
export type NewLabelMapping = Insertable<LabelMappingsTable>;
export type LabelMappingUpdate = Updateable<LabelMappingsTable>;

// Statistics
export type Statistic = Selectable<StatisticsTable>;
export type NewStatistic = Insertable<StatisticsTable>;
export type StatisticUpdate = Updateable<StatisticsTable>;

// Statistic Classifications
export type StatisticClassification = Selectable<StatisticClassificationsTable>;
export type NewStatisticClassification =
  Insertable<StatisticClassificationsTable>;

// Sync Tasks
export type SyncTask = Selectable<SyncTasksTable>;
export type NewSyncTask = Insertable<SyncTasksTable>;
export type SyncTaskUpdate = Updateable<SyncTasksTable>;

// Sync Checkpoints
export type SyncCheckpoint = Selectable<SyncCheckpointsTable>;
export type NewSyncCheckpoint = Insertable<SyncCheckpointsTable>;
export type SyncCheckpointUpdate = Updateable<SyncCheckpointsTable>;

// Sync Rate Limiter
export type SyncRateLimiter = Selectable<SyncRateLimiterTable>;
export type SyncRateLimiterUpdate = Updateable<SyncRateLimiterTable>;

// Matrix Tags
export type MatrixTag = Selectable<MatrixTagsTable>;
export type NewMatrixTag = Insertable<MatrixTagsTable>;
export type MatrixTagUpdate = Updateable<MatrixTagsTable>;

// Matrix Tag Assignments
export type MatrixTagAssignment = Selectable<MatrixTagAssignmentsTable>;
export type NewMatrixTagAssignment = Insertable<MatrixTagAssignmentsTable>;

// Matrix Relationships
export type MatrixRelationship = Selectable<MatrixRelationshipsTable>;
export type NewMatrixRelationship = Insertable<MatrixRelationshipsTable>;
export type MatrixRelationshipUpdate = Updateable<MatrixRelationshipsTable>;

// Data Quality Metrics
export type DataQualityMetric = Selectable<DataQualityMetricsTable>;
export type NewDataQualityMetric = Insertable<DataQualityMetricsTable>;
export type DataQualityMetricUpdate = Updateable<DataQualityMetricsTable>;

// Saved Queries
export type SavedQuery = Selectable<SavedQueriesTable>;
export type NewSavedQuery = Insertable<SavedQueriesTable>;
export type SavedQueryUpdate = Updateable<SavedQueriesTable>;

// Composite Indicators
export type CompositeIndicator = Selectable<CompositeIndicatorsTable>;
export type NewCompositeIndicator = Insertable<CompositeIndicatorsTable>;
export type CompositeIndicatorUpdate = Updateable<CompositeIndicatorsTable>;

// ============================================================================
// Sync Result Types
// ============================================================================

export interface SyncResult {
  inserted: number;
  updated: number;
  deleted?: number;
  errors?: number;
  duration?: number;
}

export interface DataSyncResult {
  rowsInserted: number;
  rowsUpdated: number;
  chunksCompleted?: number;
  totalChunks?: number;
  duration?: number;
}

// ============================================================================
// Helper Types for API responses
// ============================================================================

export interface LocalizedEntity {
  id: number;
  nameRo: string;
  nameEn?: string;
}

export interface TerritoryWithAncestors extends Territory {
  ancestors: {
    id: number;
    code: string;
    level: TerritoryLevel;
    nameRo: string;
  }[];
}

export interface MatrixWithContext extends Matrix {
  contextCode?: string;
  contextNameRo?: string;
  contextNameEn?: string;
  contextPath?: string;
}

export interface StatisticWithDimensions {
  id: number;
  value: number | null;
  valueStatus: string | null;
  territory?: {
    id: number;
    code: string;
    nameRo: string;
    level: TerritoryLevel;
  };
  timePeriod: {
    id: number;
    year: number;
    quarter?: number;
    month?: number;
    periodicity: Periodicity;
    labelRo: string;
  };
  unit?: {
    id: number;
    code: string;
    symbol?: string;
    nameRo: string;
  };
  classifications: {
    typeId: number;
    typeCode: string;
    typeNameRo: string;
    valueId: number;
    valueCode: string;
    valueNameRo: string;
  }[];
}
