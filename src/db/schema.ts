import type { Generated, Insertable, Selectable, Updateable } from "kysely";

// ============================================================================
// Table Types
// ============================================================================

export interface InsContextsTable {
  id: number;
  code: string;
  name: string;
  level: number;
  parent_code: string | null;
}

export interface InsMatricesTable {
  code: string;
  name: string;
  description: string | null;
  context_id: number | null;
  start_year: number | null;
  end_year: number | null;
  last_update: string | null;
  has_county_data: number; // SQLite boolean (0/1)
  has_uat_data: number; // SQLite boolean (0/1)
}

export interface InsDimensionsTable {
  id: Generated<number>;
  matrix_code: string;
  dimension_id: number;
  dimension_name: string;
  is_territorial: number; // SQLite boolean (0/1)
  is_temporal: number; // SQLite boolean (0/1)
}

export interface InsDimensionOptionsTable {
  id: Generated<number>;
  dimension_id: number;
  nom_item_id: number;
  label: string;
  offset: number;
  parent_nom_item_id: number | null;
}

export interface InsStatisticsTable {
  id: Generated<number>;
  matrix_code: string;
  siruta_code: string | null;
  period_year: number | null;
  period_quarter: number | null;
  period_month: number | null;
  indicator_value: number | null;
  dimension_values: string | null; // JSON string
}

export interface SirutaTable {
  siruta: string;
  denloc: string;
  jud: number;
  sirsup: string | null;
  tip: number;
  niv: number;
  med: number;
}

// ============================================================================
// Database Interface
// ============================================================================

export interface Database {
  ins_contexts: InsContextsTable;
  ins_matrices: InsMatricesTable;
  ins_dimensions: InsDimensionsTable;
  ins_dimension_options: InsDimensionOptionsTable;
  ins_statistics: InsStatisticsTable;
  siruta: SirutaTable;
}

// ============================================================================
// Row Types (for convenience)
// ============================================================================

export type InsContext = Selectable<InsContextsTable>;
export type NewInsContext = Insertable<InsContextsTable>;
export type InsContextUpdate = Updateable<InsContextsTable>;

export type InsMatrix = Selectable<InsMatricesTable>;
export type NewInsMatrix = Insertable<InsMatricesTable>;
export type InsMatrixUpdate = Updateable<InsMatricesTable>;

export type InsDimension = Selectable<InsDimensionsTable>;
export type NewInsDimension = Insertable<InsDimensionsTable>;

export type InsDimensionOption = Selectable<InsDimensionOptionsTable>;
export type NewInsDimensionOption = Insertable<InsDimensionOptionsTable>;

export type InsStatistic = Selectable<InsStatisticsTable>;
export type NewInsStatistic = Insertable<InsStatisticsTable>;

export type Siruta = Selectable<SirutaTable>;
export type NewSiruta = Insertable<SirutaTable>;
