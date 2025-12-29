// INS Tempo API Types
// Updated 2025-12-27 based on live API exploration

// =====================
// Context Types
// =====================

/**
 * Context item (domain/category) - the inner object in context response
 */
export interface InsContextItem {
  name: string;
  code: string;
  childrenUrl: "context" | "matrix";
  comment: string | null;
  url: string;
}

/**
 * Context response item - wrapper with hierarchy info
 * From GET /context/ and GET /context/{code}
 */
export interface InsContext {
  parentCode: string;
  level: number;
  context: InsContextItem;
}

// =====================
// Matrix Types
// =====================

/**
 * Matrix list item - from GET /matrix/matrices endpoint
 */
export interface InsMatrixListItem {
  name: string;
  code: string;
  childrenUrl: "matrix";
  comment: string | null;
  url: "matrix";
}

/**
 * Data source reference
 */
export interface InsDataSource {
  nume: string;
  tip: string;
  linkNumber: number;
  codTip: number;
}

/**
 * Series break info (when a matrix is discontinued)
 */
export interface InsSeriesBreak {
  lastPeriod: string;
  nextMatrixCode: string;
}

/**
 * Series continuation info (when a matrix continues from another)
 */
export interface InsSeriesContinuation {
  matCode: string;
  lastPeriod: string;
}

/**
 * Complete matrix details flags
 */
export interface InsMatrixDetails {
  /** County dimension presence (0=no, >0=dimension index) */
  nomJud: number;
  /** Locality/UAT dimension presence (0=no, >0=dimension index) */
  nomLoc: number;
  /** Total number of dimensions */
  matMaxDim: number;
  /** Special unit of measure (0/1) */
  matUMSpec: number;
  /** Uses SIRUTA codes (0/1) */
  matSiruta: number;
  /** Uses CAEN Rev.1 classification (0/1) */
  matCaen1: number;
  /** Uses CAEN Rev.2 classification (0/1) */
  matCaen2: number;
  /** Regional dimension index */
  matRegJ: number;
  /** Query complexity cost */
  matCharge: number;
  /** View counter */
  matViews: number;
  /** Download counter */
  matDownloads: number;
  /** Dataset active status (0=discontinued, 1=active) */
  matActive: number;
  /** Time dimension index */
  matTime: number;
}

/**
 * Dimension option/value
 */
export interface InsDimensionOption {
  /** Display label (may include SIRUTA code for localities) */
  label: string;
  /** Unique option identifier (used in queries) */
  nomItemId: number;
  /** Order index (1-based) */
  offset: number;
  /** Parent option ID for hierarchical dimensions */
  parentId: number | null;
}

/**
 * Dimension definition from matrix metadata
 * From GET /matrix/{code} response
 */
export interface InsDimension {
  /** Dimension code/index */
  dimCode: number;
  /** Display label (e.g., "Varste si grupe de varsta", "Sexe") */
  label: string;
  /** Available options for this dimension */
  options: InsDimensionOption[];
}

/**
 * Complete matrix metadata - from GET /matrix/{code} endpoint
 */
export interface InsMatrix {
  /** Full matrix description/name */
  matrixName: string;
  /** Breadcrumb path from root */
  ancestors?: InsContextItem[];
  /** Periodicity: "Anuala", "Trimestriala", "Lunara" */
  periodicitati?: string[];
  /** Data sources */
  surseDeDate?: InsDataSource[];
  /** Definition/description */
  definitie?: string;
  /** Methodology notes */
  metodologie?: string;
  /** Last update date (DD-MM-YYYY format) */
  ultimaActualizare?: string;
  /** Observations/notes */
  observatii?: string;
  /** Responsible persons */
  persoaneResponsabile?: string | null;
  /** Dimension definitions with options */
  dimensionsMap: InsDimension[];
  /** Series interruption info (when discontinued) */
  intrerupere?: InsSeriesBreak | null;
  /** Series continuation info (when continuing from previous matrix) */
  continuareSerie?: InsSeriesContinuation[] | null;
  /** Matrix capability flags */
  details: InsMatrixDetails;
}

// =====================
// Query Types
// =====================

/**
 * Pivot query request - for POST /pivot endpoint
 */
export interface InsQueryRequest {
  /** Colon-separated nomItemId values (one per dimension) */
  encQuery: string;
  /** Language: "ro" or "en" */
  language: "ro" | "en";
  /** Matrix code */
  matCode: string;
  /** From details.matMaxDim */
  matMaxDim: number;
  /** From details.matRegJ */
  matRegJ: number;
  /** From details.matUMSpec */
  matUMSpec: number;
}

/**
 * Parsed data cell from query response
 */
export interface InsDataCell {
  value: number | null;
  dimensions: Record<string, string>;
}

/**
 * Error response from INS API
 */
export interface InsErrorResponse {
  timestamp: number;
  status: number;
  error: string;
  exception?: string;
  message: string;
  path: string;
}

// =====================
// SIRUTA Types
// =====================

/**
 * SIRUTA reference entry for territorial units
 */
export interface SirutaEntry {
  siruta: string;
  denloc: string;
  jud: number;
  sirsup: string;
  tip: number;
  niv: number;
  med: number;
}

// =====================
// Utility Types
// =====================

/**
 * Known dimension types
 */
export type InsDimensionType =
  | "temporal" // Ani, Perioade
  | "territorial" // Judete, Localitati, Macroregiuni
  | "demographic" // Sexe, Varste
  | "residence" // Medii de rezidenta
  | "economic" // CAEN classifications
  | "unit"; // UM: Numar, UM: Ha, etc.

/**
 * Periodicity types
 */
export type InsPeriodicity = "Anuala" | "Trimestriala" | "Lunara";

/**
 * Top-level domain codes
 */
export type InsDomainCode =
  | "1" // A. STATISTICA SOCIALA
  | "2" // B. STATISTICA ECONOMICA
  | "3" // C. FINANTE
  | "4" // D. JUSTITIE
  | "5" // E. MEDIU INCONJURATOR
  | "6" // F. UTILITATI PUBLICE
  | "7" // G. DEZVOLTARE DURABILA - Orizont 2020
  | "8"; // H. DEZVOLTARE DURABILA - Tinte 2030
