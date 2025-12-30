/**
 * Statistics fixtures for testing
 */

export const sampleTimePeriods = [
  {
    id: 2020,
    year: 2020,
    quarter: null,
    month: null,
    periodicity: "ANNUAL" as const,
    period_start: new Date("2020-01-01"),
    period_end: new Date("2020-12-31"),
    labels: { ro: "Anul 2020", en: "Year 2020" },
    created_at: new Date(),
    updated_at: new Date(),
  },
  {
    id: 2021,
    year: 2021,
    quarter: null,
    month: null,
    periodicity: "ANNUAL" as const,
    period_start: new Date("2021-01-01"),
    period_end: new Date("2021-12-31"),
    labels: { ro: "Anul 2021", en: "Year 2021" },
    created_at: new Date(),
    updated_at: new Date(),
  },
  {
    id: 2022,
    year: 2022,
    quarter: null,
    month: null,
    periodicity: "ANNUAL" as const,
    period_start: new Date("2022-01-01"),
    period_end: new Date("2022-12-31"),
    labels: { ro: "Anul 2022", en: "Year 2022" },
    created_at: new Date(),
    updated_at: new Date(),
  },
  {
    id: 2023,
    year: 2023,
    quarter: null,
    month: null,
    periodicity: "ANNUAL" as const,
    period_start: new Date("2023-01-01"),
    period_end: new Date("2023-12-31"),
    labels: { ro: "Anul 2023", en: "Year 2023" },
    created_at: new Date(),
    updated_at: new Date(),
  },
  {
    id: 2024,
    year: 2024,
    quarter: null,
    month: null,
    periodicity: "ANNUAL" as const,
    period_start: new Date("2024-01-01"),
    period_end: new Date("2024-12-31"),
    labels: { ro: "Anul 2024", en: "Year 2024" },
    created_at: new Date(),
    updated_at: new Date(),
  },
];

export const sampleQuarterlyPeriods = [
  {
    id: 202301,
    year: 2023,
    quarter: 1,
    month: null,
    periodicity: "QUARTERLY" as const,
    period_start: new Date("2023-01-01"),
    period_end: new Date("2023-03-31"),
    labels: { ro: "Trimestrul I 2023", en: "Quarter 1 2023" },
    created_at: new Date(),
    updated_at: new Date(),
  },
  {
    id: 202302,
    year: 2023,
    quarter: 2,
    month: null,
    periodicity: "QUARTERLY" as const,
    period_start: new Date("2023-04-01"),
    period_end: new Date("2023-06-30"),
    labels: { ro: "Trimestrul II 2023", en: "Quarter 2 2023" },
    created_at: new Date(),
    updated_at: new Date(),
  },
];

export const sampleStatistics = [
  {
    id: 1,
    matrix_id: 1033,
    territory_id: 1,
    time_period_id: 2020,
    unit_id: 1,
    value: 19354339,
    value_status: null,
    row_hash: "hash1",
    created_at: new Date(),
    updated_at: new Date(),
  },
  {
    id: 2,
    matrix_id: 1033,
    territory_id: 1,
    time_period_id: 2021,
    unit_id: 1,
    value: 19229519,
    value_status: null,
    row_hash: "hash2",
    created_at: new Date(),
    updated_at: new Date(),
  },
  {
    id: 3,
    matrix_id: 1033,
    territory_id: 1,
    time_period_id: 2022,
    unit_id: 1,
    value: 19043098,
    value_status: null,
    row_hash: "hash3",
    created_at: new Date(),
    updated_at: new Date(),
  },
  {
    id: 4,
    matrix_id: 1033,
    territory_id: 1,
    time_period_id: 2023,
    unit_id: 1,
    value: 19055228,
    value_status: null,
    row_hash: "hash4",
    created_at: new Date(),
    updated_at: new Date(),
  },
  {
    id: 5,
    matrix_id: 1033,
    territory_id: 1,
    time_period_id: 2024,
    unit_id: 1,
    value: 19067576,
    value_status: null,
    row_hash: "hash5",
    created_at: new Date(),
    updated_at: new Date(),
  },
];

export const sampleUnits = [
  {
    id: 1,
    code: "PERS",
    names: { ro: "Numar persoane", en: "Number of persons" },
    symbol: null,
    created_at: new Date(),
    updated_at: new Date(),
  },
  {
    id: 2,
    code: "HA",
    names: { ro: "Hectare", en: "Hectares" },
    symbol: "ha",
    created_at: new Date(),
    updated_at: new Date(),
  },
];

export const sampleClassificationTypes = [
  {
    id: 1,
    code: "SEX",
    names: { ro: "Sexe", en: "Sex" },
    is_hierarchical: false,
    created_at: new Date(),
    updated_at: new Date(),
  },
  {
    id: 2,
    code: "VARSTA",
    names: { ro: "Grupe de varsta", en: "Age groups" },
    is_hierarchical: true,
    created_at: new Date(),
    updated_at: new Date(),
  },
];

export const sampleClassificationValues = [
  {
    id: 1,
    type_id: 1,
    code: "TOTAL",
    names: { ro: "Total", en: "Total" },
    parent_id: null,
    path: "TOTAL",
    level: 0,
    sort_order: 0,
    created_at: new Date(),
    updated_at: new Date(),
  },
  {
    id: 2,
    type_id: 1,
    code: "M",
    names: { ro: "Masculin", en: "Male" },
    parent_id: 1,
    path: "TOTAL.M",
    level: 1,
    sort_order: 1,
    created_at: new Date(),
    updated_at: new Date(),
  },
  {
    id: 3,
    type_id: 1,
    code: "F",
    names: { ro: "Feminin", en: "Female" },
    parent_id: 1,
    path: "TOTAL.F",
    level: 1,
    sort_order: 2,
    created_at: new Date(),
    updated_at: new Date(),
  },
];

export const sampleContexts = [
  {
    id: 1,
    ins_code: "1",
    names: { ro: "STATISTICA SOCIALA", en: "SOCIAL STATISTICS" },
    level: 1,
    parent_id: null,
    path: "0.1",
    children_type: "context" as const,
    created_at: new Date(),
    updated_at: new Date(),
  },
  {
    id: 10,
    ins_code: "10",
    names: { ro: "POPULATIE", en: "POPULATION" },
    level: 2,
    parent_id: 1,
    path: "0.1.10",
    children_type: "context" as const,
    created_at: new Date(),
    updated_at: new Date(),
  },
  {
    id: 1010,
    ins_code: "1010",
    names: {
      ro: "1. POPULATIA REZIDENTA",
      en: "1. RESIDENT POPULATION",
    },
    level: 3,
    parent_id: 10,
    path: "0.1.10.1010",
    children_type: "matrix" as const,
    created_at: new Date(),
    updated_at: new Date(),
  },
];

export function createStatistic(
  overrides: Partial<(typeof sampleStatistics)[0]> = {}
) {
  return { ...sampleStatistics[0]!, ...overrides };
}

export function getStatisticsByMatrixId(matrixId: number) {
  return sampleStatistics.filter((s) => s.matrix_id === matrixId);
}

export function getStatisticsByYear(year: number) {
  return sampleStatistics.filter((s) => s.time_period_id === year);
}
