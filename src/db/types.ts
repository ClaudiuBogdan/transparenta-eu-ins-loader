import type { Generated, Insertable, Selectable, Updateable } from "kysely";

// ============================================================================
// ENUM Types (matching PostgreSQL ENUMs)
// ============================================================================

export type PeriodicityType = "ANNUAL" | "QUARTERLY" | "MONTHLY";

export type DimensionType =
  | "TEMPORAL"
  | "TERRITORIAL"
  | "CLASSIFICATION"
  | "UNIT_OF_MEASURE";

export type TerritorialLevel = "NATIONAL" | "NUTS1" | "NUTS2" | "NUTS3" | "LAU";

export type MatrixStatus = "ACTIVE" | "DISCONTINUED";

export type ScrapeStatus = "PENDING" | "RUNNING" | "COMPLETED" | "FAILED";

export type ChunkStrategy = "BY_YEAR" | "BY_TERRITORY" | "BY_CLASSIFICATION";

export type SyncStatusType =
  | "NEVER_SYNCED"
  | "SYNCING"
  | "SYNCED"
  | "PARTIAL"
  | "FAILED"
  | "STALE";

// ============================================================================
// Reference Tables
// ============================================================================

/**
 * contexts - Statistical domain hierarchy (8 domains A-H + ~340 subcategories)
 */
export interface ContextsTable {
  id: Generated<number>;
  ins_code: string;
  name: string;
  level: number;
  parent_id: number | null;
  path: string;
  children_type: "context" | "matrix";
  created_at: Generated<Date>;
  updated_at: Generated<Date>;
}

/**
 * territories - Unified territorial hierarchy (NUTS + LAU/SIRUTA)
 */
export interface TerritoriesTable {
  id: Generated<number>;
  code: string;
  siruta_code: string | null;
  name: string;
  name_normalized: string;
  level: TerritorialLevel;
  parent_id: number | null;
  path: string;
  siruta_tip: number | null;
  siruta_niv: number | null;
  siruta_med: number | null;
  created_at: Generated<Date>;
  updated_at: Generated<Date>;
}

/**
 * time_periods - Unified time periods with parsed components
 */
export interface TimePeriodsTable {
  id: Generated<number>;
  year: number;
  quarter: number | null;
  month: number | null;
  periodicity: PeriodicityType;
  ins_label: string;
  period_start: Date;
  period_end: Date;
  created_at: Generated<Date>;
}

/**
 * classification_types - Classification type definitions
 */
export interface ClassificationTypesTable {
  id: Generated<number>;
  code: string;
  name: string;
  ins_labels: string[];
  is_hierarchical: boolean;
  created_at: Generated<Date>;
}

/**
 * classification_values - Classification option values
 */
export interface ClassificationValuesTable {
  id: Generated<number>;
  classification_type_id: number;
  code: string;
  name: string;
  name_normalized: string;
  parent_id: number | null;
  path: string | null;
  level: number;
  sort_order: number;
  created_at: Generated<Date>;
}

/**
 * units_of_measure - Measurement units
 */
export interface UnitsOfMeasureTable {
  id: Generated<number>;
  code: string;
  name: string;
  symbol: string | null;
  ins_labels: string[];
  created_at: Generated<Date>;
}

// ============================================================================
// Matrix (Dataset) Tables
// ============================================================================

/**
 * matrices - Statistical datasets (~1,898 matrices)
 */
export interface MatricesTable {
  id: Generated<number>;
  ins_code: string;
  name: string;
  name_en: string | null;
  context_id: number | null;
  periodicity: PeriodicityType[];
  definition: string | null;
  methodology: string | null;
  observations: string | null;
  series_break: string | null;
  series_continuation: string | null;
  responsible_persons: string | null;
  start_year: number | null;
  end_year: number | null;
  last_update: Date | null;
  status: MatrixStatus;
  dimension_count: number;
  has_county_data: boolean;
  has_uat_data: boolean;
  has_siruta: boolean;
  has_caen_rev1: boolean;
  has_caen_rev2: boolean;
  territorial_dim_index: number | null;
  time_dim_index: number | null;
  county_dim_index: number | null;
  locality_dim_index: number | null;
  um_special: boolean;
  view_count: Generated<number>;
  download_count: Generated<number>;
  query_complexity: Generated<number>;
  created_at: Generated<Date>;
  updated_at: Generated<Date>;
}

/**
 * matrix_data_sources - Data sources for matrices
 */
export interface MatrixDataSourcesTable {
  id: Generated<number>;
  matrix_id: number;
  name: string;
  source_type: string | null;
  link_number: number | null;
  source_code: number | null;
}

/**
 * matrix_dimensions - Dimension definitions per matrix
 */
export interface MatrixDimensionsTable {
  id: Generated<number>;
  matrix_id: number;
  dim_code: number;
  label: string;
  dimension_type: DimensionType;
  classification_type_id: number | null;
  is_hierarchical: boolean;
  option_count: number;
  created_at: Generated<Date>;
}

/**
 * matrix_dimension_options - Dimension options with nomItemId mapping
 */
export interface MatrixDimensionOptionsTable {
  id: Generated<number>;
  matrix_dimension_id: number;
  nom_item_id: number;
  label: string;
  offset_order: number;
  parent_nom_item_id: number | null;
  territory_id: number | null;
  time_period_id: number | null;
  classification_value_id: number | null;
  unit_of_measure_id: number | null;
  created_at: Generated<Date>;
}

// ============================================================================
// Sync Tracking
// ============================================================================

/**
 * matrix_sync_status - Track synchronization state per matrix
 */
export interface MatrixSyncStatusTable {
  id: Generated<number>;
  matrix_id: number;
  last_full_sync: Date | null;
  last_incremental_sync: Date | null;
  last_metadata_sync: Date | null;
  sync_status: SyncStatusType;
  data_start_year: number | null;
  data_end_year: number | null;
  row_count: Generated<number>;
  last_error: string | null;
  consecutive_failures: Generated<number>;
  created_at: Generated<Date>;
  updated_at: Generated<Date>;
}

// ============================================================================
// Fact Tables
// ============================================================================

/**
 * statistics - Main fact table for statistical data values (PARTITIONED)
 */
export interface StatisticsTable {
  id: Generated<number>;
  matrix_id: number;
  territory_id: number | null;
  time_period_id: number;
  unit_of_measure_id: number | null;
  value: number | null;
  value_status: string | null;
  source_enc_query: string | null;
  scraped_at: Generated<Date>;
  created_at: Generated<Date>;
  // Idempotency fields
  natural_key_hash: string | null;
  updated_at: Generated<Date>;
  version: Generated<number>;
}

/**
 * statistic_classifications - Junction table for classification dimensions (PARTITIONED)
 */
export interface StatisticClassificationsTable {
  id: Generated<number>;
  matrix_id: number;
  statistic_id: number;
  classification_value_id: number;
}

// ============================================================================
// Scraping Infrastructure
// ============================================================================

/**
 * scrape_jobs - Track scraping jobs with chunking support
 */
export interface ScrapeJobsTable {
  id: Generated<number>;
  matrix_id: number;
  status: ScrapeStatus;
  enc_query: string | null;
  estimated_cells: number | null;
  strategy: ChunkStrategy | null;
  total_chunks: Generated<number>;
  completed_chunks: Generated<number>;
  started_at: Date | null;
  completed_at: Date | null;
  rows_fetched: Generated<number>;
  error_message: string | null;
  created_at: Generated<Date>;
}

/**
 * scrape_chunks - Individual chunks for large queries
 */
export interface ScrapeChunksTable {
  id: Generated<number>;
  job_id: number;
  chunk_number: number;
  enc_query: string;
  status: ScrapeStatus;
  rows_fetched: Generated<number>;
  started_at: Date | null;
  completed_at: Date | null;
  error_message: string | null;
  retry_count: Generated<number>;
  created_at: Generated<Date>;
}

/**
 * data_sync_checkpoints - Track sync progress per chunk for incremental sync
 */
export interface DataSyncCheckpointsTable {
  id: Generated<number>;
  matrix_id: number;
  chunk_enc_query_hash: string; // SHA-256 hash for unique constraint (B-tree 8KB limit)
  chunk_enc_query: string; // Full enc_query for reference
  last_scraped_at: Date;
  row_count: number;
  created_at: Generated<Date>;
  updated_at: Generated<Date>;
}

// ============================================================================
// Database Interface
// ============================================================================

export interface Database {
  // Reference tables
  contexts: ContextsTable;
  territories: TerritoriesTable;
  time_periods: TimePeriodsTable;
  classification_types: ClassificationTypesTable;
  classification_values: ClassificationValuesTable;
  units_of_measure: UnitsOfMeasureTable;

  // Matrix tables
  matrices: MatricesTable;
  matrix_data_sources: MatrixDataSourcesTable;
  matrix_dimensions: MatrixDimensionsTable;
  matrix_dimension_options: MatrixDimensionOptionsTable;

  // Sync tracking
  matrix_sync_status: MatrixSyncStatusTable;

  // Fact tables
  statistics: StatisticsTable;
  statistic_classifications: StatisticClassificationsTable;

  // Scraping infrastructure
  scrape_jobs: ScrapeJobsTable;
  scrape_chunks: ScrapeChunksTable;

  // Data sync checkpoints
  data_sync_checkpoints: DataSyncCheckpointsTable;
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

// Matrix Data Sources
export type MatrixDataSource = Selectable<MatrixDataSourcesTable>;
export type NewMatrixDataSource = Insertable<MatrixDataSourcesTable>;

// Matrix Dimensions
export type MatrixDimension = Selectable<MatrixDimensionsTable>;
export type NewMatrixDimension = Insertable<MatrixDimensionsTable>;
export type MatrixDimensionUpdate = Updateable<MatrixDimensionsTable>;

// Matrix Dimension Options
export type MatrixDimensionOption = Selectable<MatrixDimensionOptionsTable>;
export type NewMatrixDimensionOption = Insertable<MatrixDimensionOptionsTable>;
export type MatrixDimensionOptionUpdate =
  Updateable<MatrixDimensionOptionsTable>;

// Matrix Sync Status
export type MatrixSyncStatus = Selectable<MatrixSyncStatusTable>;
export type NewMatrixSyncStatus = Insertable<MatrixSyncStatusTable>;
export type MatrixSyncStatusUpdate = Updateable<MatrixSyncStatusTable>;

// Statistics
export type Statistic = Selectable<StatisticsTable>;
export type NewStatistic = Insertable<StatisticsTable>;
export type StatisticUpdate = Updateable<StatisticsTable>;

// Statistic Classifications
export type StatisticClassification = Selectable<StatisticClassificationsTable>;
export type NewStatisticClassification =
  Insertable<StatisticClassificationsTable>;

// Scrape Jobs
export type ScrapeJob = Selectable<ScrapeJobsTable>;
export type NewScrapeJob = Insertable<ScrapeJobsTable>;
export type ScrapeJobUpdate = Updateable<ScrapeJobsTable>;

// Scrape Chunks
export type ScrapeChunk = Selectable<ScrapeChunksTable>;
export type NewScrapeChunk = Insertable<ScrapeChunksTable>;
export type ScrapeChunkUpdate = Updateable<ScrapeChunksTable>;

// Data Sync Checkpoints
export type DataSyncCheckpoint = Selectable<DataSyncCheckpointsTable>;
export type NewDataSyncCheckpoint = Insertable<DataSyncCheckpointsTable>;
export type DataSyncCheckpointUpdate = Updateable<DataSyncCheckpointsTable>;

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
