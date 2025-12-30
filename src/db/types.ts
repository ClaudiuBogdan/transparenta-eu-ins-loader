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
// RAW LAYER Tables
// ============================================================================

/**
 * raw_api_responses - Store raw INS API responses for debugging
 */
export interface RawApiResponsesTable {
  id: Generated<number>;
  endpoint: string;
  request_params: Record<string, unknown> | null;
  response_ro: Record<string, unknown>;
  response_en: Record<string, unknown> | null;
  fetched_at: Generated<Date>;
}

/**
 * raw_dimension_options - Raw dimension options before resolution
 */
export interface RawDimensionOptionsTable {
  id: Generated<number>;
  matrix_code: string;
  dim_index: number;
  dim_label_ro: string;
  dim_label_en: string | null;
  nom_item_id: number;
  label_ro: string;
  label_en: string | null;
  offset_order: number;
  parent_nom_item_id: number | null;
  fetched_at: Generated<Date>;
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
  names: BilingualText;
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

/**
 * sync_checkpoints - Track sync progress per chunk
 */
export interface SyncCheckpointsTable {
  id: Generated<number>;
  matrix_id: number;
  chunk_hash: string;
  chunk_query: string;
  last_synced_at: Date;
  row_count: number;
}

// ============================================================================
// Database Interface
// ============================================================================

export interface Database {
  // Raw layer
  raw_api_responses: RawApiResponsesTable;
  raw_dimension_options: RawDimensionOptionsTable;

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
  sync_checkpoints: SyncCheckpointsTable;
}

// ============================================================================
// Row Types (for convenience)
// ============================================================================

// Raw layer
export type RawApiResponse = Selectable<RawApiResponsesTable>;
export type NewRawApiResponse = Insertable<RawApiResponsesTable>;

export type RawDimensionOption = Selectable<RawDimensionOptionsTable>;
export type NewRawDimensionOption = Insertable<RawDimensionOptionsTable>;

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

// Sync Checkpoints
export type SyncCheckpoint = Selectable<SyncCheckpointsTable>;
export type NewSyncCheckpoint = Insertable<SyncCheckpointsTable>;
export type SyncCheckpointUpdate = Updateable<SyncCheckpointsTable>;

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
