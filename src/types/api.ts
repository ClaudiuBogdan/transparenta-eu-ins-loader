/**
 * API Request/Response Types
 */

import type { PaginationMeta } from "../utils/pagination.js";

// ============================================================================
// Common Response Types
// ============================================================================

export interface ApiResponse<T> {
  data: T;
  meta?: {
    pagination?: PaginationMeta;
    query?: QueryMeta;
  };
}

export interface QueryMeta {
  executionTimeMs: number;
  appliedFilters: Record<string, unknown>;
}

export interface ApiError {
  error: string;
  message: string;
  details?: Record<string, unknown>;
  requestId?: string;
}

// ============================================================================
// Context Types
// ============================================================================

export interface ContextDto {
  id: number;
  insCode: string;
  name: string;
  level: number;
  parentId: number | null;
  path: string;
  childrenType: "context" | "matrix";
  childCount?: number;
}

export interface ContextDetailDto {
  context: ContextDto;
  children: ContextDto[] | MatrixSummaryDto[];
  ancestors: ContextDto[];
}

// ============================================================================
// Matrix Types
// ============================================================================

export type MatrixStatus =
  | "PENDING"
  | "SYNCING"
  | "SYNCED"
  | "FAILED"
  | "STALE";

export interface MatrixSummaryDto {
  id: number;
  insCode: string;
  name: string;
  contextPath: string | null;
  contextName: string | null;
  periodicity: string[];
  hasUatData: boolean;
  hasCountyData: boolean;
  dimensionCount: number;
  startYear: number | null;
  endYear: number | null;
  lastUpdate: string | null;
  status: MatrixStatus;
}

export interface MatrixDetailDto extends MatrixSummaryDto {
  definition: string | null;
  methodology: string | null;
  observations: string | null;
  seriesBreak: string | null;
  seriesContinuation: string | null;
  responsiblePersons: string | null;
  viewCount: number;
  downloadCount: number;
  dataSources: DataSourceDto[];
}

export interface DataSourceDto {
  name: string;
  sourceType: string | null;
}

export interface DimensionInfoDto {
  id: number;
  dimCode: number;
  label: string;
  dimensionType:
    | "TEMPORAL"
    | "TERRITORIAL"
    | "CLASSIFICATION"
    | "UNIT_OF_MEASURE";
  classificationTypeCode?: string;
  isHierarchical: boolean;
  optionCount: number;
}

export interface DimensionOptionDto {
  id: number;
  nomItemId: number;
  label: string;
  offsetOrder: number;
  parentNomItemId: number | null;
  reference: {
    type: "TERRITORY" | "TIME_PERIOD" | "CLASSIFICATION" | "UNIT";
    id: number;
    code?: string;
    name?: string;
    path?: string;
  } | null;
}

export interface SyncStatusDto {
  syncStatus: string;
  lastFullSync: string | null;
  lastMetadataSync: string | null;
  dataStartYear: number | null;
  dataEndYear: number | null;
  rowCount: number;
}

// ============================================================================
// Territory Types
// ============================================================================

export type TerritorialLevel = "NATIONAL" | "NUTS1" | "NUTS2" | "NUTS3" | "LAU";

export interface TerritoryDto {
  id: number;
  code: string;
  sirutaCode: string | null;
  name: string;
  level: TerritorialLevel;
  parentId: number | null;
  path: string;
  children?: TerritoryDto[];
}

// ============================================================================
// Time Period Types
// ============================================================================

export type Periodicity = "ANNUAL" | "QUARTERLY" | "MONTHLY";

export interface TimePeriodDto {
  id: number;
  year: number;
  quarter: number | null;
  month: number | null;
  periodicity: Periodicity;
  insLabel: string;
  periodStart: string;
  periodEnd: string;
}

// ============================================================================
// Classification Types
// ============================================================================

export interface ClassificationTypeDto {
  id: number;
  code: string;
  name: string;
  isHierarchical: boolean;
  valueCount: number;
}

export interface ClassificationValueDto {
  id: number;
  code: string;
  name: string;
  parentId: number | null;
  path: string | null;
  level: number;
  children?: ClassificationValueDto[];
}

// ============================================================================
// Statistics Types
// ============================================================================

export interface StatisticsResponseDto {
  matrix: MatrixSummaryDto;
  series: TimeSeriesDto[];
}

export interface TimeSeriesDto {
  seriesId: string;
  name: string;
  dimensions: SeriesDimensionsDto;
  xAxis: AxisInfoDto;
  yAxis: AxisInfoDto;
  data: DataPointDto[];
}

export interface SeriesDimensionsDto {
  territory?: {
    id: number;
    code: string;
    name: string;
    level: string;
    path: string;
  };
  classifications?: Record<
    string,
    {
      id: number;
      code: string;
      name: string;
    }
  >;
  unit?: {
    id: number;
    code: string;
    name: string;
    symbol: string | null;
  };
}

export interface AxisInfoDto {
  name: string;
  type: "INTEGER" | "FLOAT" | "DATE" | "STRING";
  unit: string;
}

export interface DataPointDto {
  x: string;
  y: number | null;
  status?: string;
  timePeriod?: {
    id: number;
    year: number;
    quarter?: number;
    month?: number;
    periodicity: string;
  };
}

export interface StatisticsSummaryDto {
  matrix: MatrixSummaryDto;
  summary: {
    totalRecords: number;
    timeRange: { from: number; to: number } | null;
    territoryLevels: { level: string; count: number }[];
    valueStats: {
      min: number | null;
      max: number | null;
      avg: number | null;
      sum: number | null;
      nullCount: number;
    };
  };
}
