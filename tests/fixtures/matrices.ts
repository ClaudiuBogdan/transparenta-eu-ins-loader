/**
 * Matrix fixtures for testing
 */

import type { MatrixMetadata, DimensionSummary } from "../../src/db/types.js";

export const sampleMatrixMetadata: MatrixMetadata = {
  names: {
    ro: "Populatia rezidenta la 1 ianuarie",
    en: "Resident population at January 1st",
  },
  definitions: {
    ro: "Populatia rezidenta reprezinta...",
    en: "Resident population represents...",
  },
  periodicity: ["ANNUAL"],
  yearRange: [2020, 2024],
  flags: {
    hasUatData: false,
    hasCountyData: true,
    hasSiruta: false,
    hasCaenRev1: false,
    hasCaenRev2: false,
  },
  lastUpdate: "2025-02-08T22:00:00.000Z",
};

export const sampleDimensionsSummary: DimensionSummary[] = [
  {
    index: 0,
    labelRo: "Ani",
    labelEn: "Years",
    type: "TEMPORAL",
    optionCount: 5,
    isHierarchical: false,
  },
  {
    index: 1,
    labelRo: "Judete",
    labelEn: "Counties",
    type: "TERRITORIAL",
    optionCount: 43,
    isHierarchical: true,
  },
  {
    index: 2,
    labelRo: "Sexe",
    labelEn: "Sex",
    type: "CLASSIFICATION",
    optionCount: 3,
    isHierarchical: false,
  },
];

export const sampleMatrix = {
  id: 1033,
  ins_code: "POP105A",
  context_id: 10,
  metadata: sampleMatrixMetadata,
  dimensions: sampleDimensionsSummary,
  sync_status: "SYNCED" as const,
  last_sync_at: new Date("2025-01-15T10:30:00.000Z"),
  sync_error: null,
  created_at: new Date("2025-01-01T00:00:00.000Z"),
  updated_at: new Date("2025-01-15T10:30:00.000Z"),
};

export const sampleMatrixWithContext = {
  ...sampleMatrix,
  context_path: "0.1.10.1010",
  context_names: {
    ro: "1. POPULATIA REZIDENTA",
    en: "1. RESIDENT POPULATION",
  },
};

export const sampleMatrixDimension = {
  id: 1,
  matrix_id: 1033,
  dim_index: 0,
  labels: { ro: "Ani", en: "Years" },
  dimension_type: "TEMPORAL" as const,
  classification_type_id: null,
  is_hierarchical: false,
  option_count: 5,
  created_at: new Date(),
  updated_at: new Date(),
};

export const sampleNomItem = {
  id: 101,
  matrix_id: 1033,
  dim_index: 0,
  nom_item_id: 4494,
  dimension_type: "TEMPORAL" as const,
  territory_id: null,
  time_period_id: 2023,
  classification_value_id: null,
  unit_id: null,
  labels: { ro: "Anul 2023", en: "Year 2023" },
  parent_nom_item_id: null,
  offset_order: 34,
  created_at: new Date(),
  updated_at: new Date(),
};

export function createMatrix(overrides: Partial<typeof sampleMatrix> = {}) {
  return { ...sampleMatrix, ...overrides };
}

export function createMatrixMetadata(
  overrides: Partial<MatrixMetadata> = {}
): MatrixMetadata {
  return { ...sampleMatrixMetadata, ...overrides };
}
