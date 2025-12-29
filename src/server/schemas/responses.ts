/**
 * Response Schemas with Real Examples for OpenAPI Documentation
 */

import { Type, type TSchema } from "@sinclair/typebox";

import { PaginationMetaSchema } from "./common.js";

// ============================================================================
// Context Schemas
// ============================================================================

export const ContextSchema = Type.Object(
  {
    id: Type.Number(),
    insCode: Type.String(),
    name: Type.String(),
    level: Type.Number(),
    parentId: Type.Union([Type.Number(), Type.Null()]),
    path: Type.String(),
    childrenType: Type.Union([Type.Literal("context"), Type.Literal("matrix")]),
    childCount: Type.Optional(Type.Number()),
  },
  {
    examples: [
      {
        id: 1,
        insCode: "A",
        name: "POPULATIE",
        level: 1,
        parentId: null,
        path: "A",
        childrenType: "context",
        childCount: 8,
      },
      {
        id: 15,
        insCode: "A0101",
        name: "Populatia dupa domiciliu",
        level: 3,
        parentId: 3,
        path: "A.A01.A0101",
        childrenType: "matrix",
        childCount: 12,
      },
    ],
  }
);

export const ContextListResponseSchema = Type.Object({
  data: Type.Array(ContextSchema),
  meta: Type.Object({
    pagination: PaginationMetaSchema,
  }),
});

// ContextDetailSchema is defined after MatrixSummarySchema below

// ============================================================================
// Matrix Schemas
// ============================================================================

export const MatrixSummarySchema = Type.Object(
  {
    id: Type.Number(),
    insCode: Type.String({ description: "INS matrix code (e.g., POP105A)" }),
    name: Type.String(),
    contextPath: Type.Union([Type.String(), Type.Null()]),
    contextName: Type.Union([Type.String(), Type.Null()]),
    periodicity: Type.Array(Type.String()),
    hasUatData: Type.Boolean({
      description: "Whether this matrix has data at UAT (locality) level",
    }),
    hasCountyData: Type.Boolean({
      description: "Whether this matrix has data at county (NUTS3) level",
    }),
    dimensionCount: Type.Number(),
    startYear: Type.Union([Type.Number(), Type.Null()]),
    endYear: Type.Union([Type.Number(), Type.Null()]),
    lastUpdate: Type.Union([Type.String({ format: "date-time" }), Type.Null()]),
    status: Type.String(),
  },
  {
    examples: [
      {
        id: 42,
        insCode: "POP105A",
        name: "Populatia pe sexe, pe grupe de varsta si medii",
        contextPath: "A.A01.A0101",
        contextName: "Populatia dupa domiciliu",
        periodicity: ["ANNUAL"],
        hasUatData: true,
        hasCountyData: true,
        dimensionCount: 5,
        startYear: 1990,
        endYear: 2024,
        lastUpdate: "2024-06-15T10:30:00.000Z",
        status: "ACTIVE",
      },
    ],
  }
);

export const DataSourceSchema = Type.Object({
  name: Type.String(),
  sourceType: Type.Union([Type.String(), Type.Null()]),
});

// ContextDetailSchema - defined here after MatrixSummarySchema
export const ContextDetailSchema = Type.Object({
  context: ContextSchema,
  children: Type.Union([
    Type.Array(ContextSchema),
    Type.Array(MatrixSummarySchema),
  ]),
  ancestors: Type.Array(ContextSchema),
});

export const MatrixDetailSchema = Type.Intersect([
  MatrixSummarySchema,
  Type.Object({
    definition: Type.Union([Type.String(), Type.Null()]),
    methodology: Type.Union([Type.String(), Type.Null()]),
    observations: Type.Union([Type.String(), Type.Null()]),
    seriesBreak: Type.Union([Type.String(), Type.Null()]),
    seriesContinuation: Type.Union([Type.String(), Type.Null()]),
    responsiblePersons: Type.Union([Type.String(), Type.Null()]),
    viewCount: Type.Number(),
    downloadCount: Type.Number(),
    dataSources: Type.Array(DataSourceSchema),
  }),
]);

export const DimensionInfoSchema = Type.Object(
  {
    id: Type.Number(),
    dimCode: Type.Number(),
    label: Type.String(),
    dimensionType: Type.String(),
    classificationTypeCode: Type.Optional(Type.String()),
    isHierarchical: Type.Boolean(),
    optionCount: Type.Number(),
  },
  {
    examples: [
      {
        id: 1,
        dimCode: 1,
        label: "Ani",
        dimensionType: "TEMPORAL",
        isHierarchical: false,
        optionCount: 35,
      },
      {
        id: 2,
        dimCode: 2,
        label: "Judete",
        dimensionType: "TERRITORIAL",
        isHierarchical: true,
        optionCount: 43,
      },
      {
        id: 3,
        dimCode: 3,
        label: "Sexe",
        dimensionType: "CLASSIFICATION",
        classificationTypeCode: "SEX",
        isHierarchical: false,
        optionCount: 3,
      },
    ],
  }
);

export const DimensionReferenceSchema = Type.Union([
  Type.Object({
    type: Type.Literal("TERRITORY"),
    id: Type.Number(),
    code: Type.Optional(Type.String()),
    name: Type.Optional(Type.String()),
    path: Type.Optional(Type.String()),
  }),
  Type.Object({
    type: Type.Literal("TIME_PERIOD"),
    id: Type.Number(),
    code: Type.Optional(Type.String()),
    name: Type.Optional(Type.String()),
  }),
  Type.Object({
    type: Type.Literal("CLASSIFICATION"),
    id: Type.Number(),
    code: Type.Optional(Type.String()),
    name: Type.Optional(Type.String()),
  }),
  Type.Object({
    type: Type.Literal("UNIT"),
    id: Type.Number(),
    code: Type.Optional(Type.String()),
    name: Type.Optional(Type.String()),
  }),
  Type.Null(),
]);

export const DimensionOptionSchema = Type.Object(
  {
    id: Type.Number(),
    nomItemId: Type.Number({
      description: "INS nomenclature item ID used in API queries",
    }),
    label: Type.String(),
    offsetOrder: Type.Number(),
    parentNomItemId: Type.Union([Type.Number(), Type.Null()]),
    reference: DimensionReferenceSchema,
  },
  {
    examples: [
      {
        id: 101,
        nomItemId: 4494,
        label: "Anul 2023",
        offsetOrder: 34,
        parentNomItemId: null,
        reference: {
          type: "TIME_PERIOD",
          id: 2023,
          code: "2023",
          name: "2023",
        },
      },
      {
        id: 201,
        nomItemId: 1005,
        label: "Masculin",
        offsetOrder: 1,
        parentNomItemId: 1,
        reference: {
          type: "CLASSIFICATION",
          id: 1,
          code: "M",
          name: "Masculin",
        },
      },
    ],
  }
);

export const SyncStatusSchema = Type.Object({
  syncStatus: Type.String(),
  lastFullSync: Type.Union([Type.String({ format: "date-time" }), Type.Null()]),
  lastMetadataSync: Type.Union([
    Type.String({ format: "date-time" }),
    Type.Null(),
  ]),
  dataStartYear: Type.Union([Type.Number(), Type.Null()]),
  dataEndYear: Type.Union([Type.Number(), Type.Null()]),
  rowCount: Type.Number(),
});

export const MatrixListResponseSchema = Type.Object({
  data: Type.Array(MatrixSummarySchema),
  meta: Type.Object({
    pagination: PaginationMetaSchema,
  }),
});

// ============================================================================
// Territory Schemas
// ============================================================================

// Base territory fields without children (to avoid circular reference)
const TerritoryBaseFields = {
  id: Type.Number(),
  code: Type.String({ description: "NUTS/LAU code" }),
  sirutaCode: Type.Union([
    Type.String({ description: "SIRUTA code for LAU territories" }),
    Type.Null(),
  ]),
  name: Type.String(),
  level: Type.String(),
  parentId: Type.Union([Type.Number(), Type.Null()]),
  path: Type.String(),
};

export const TerritorySchema = Type.Object(
  {
    ...TerritoryBaseFields,
    children: Type.Optional(Type.Array(Type.Object(TerritoryBaseFields))),
  },
  {
    examples: [
      {
        id: 1,
        code: "RO",
        sirutaCode: null,
        name: "ROMANIA",
        level: "NATIONAL",
        parentId: null,
        path: "RO",
      },
      {
        id: 5,
        code: "RO11",
        sirutaCode: null,
        name: "NORD-VEST",
        level: "NUTS2",
        parentId: 2,
        path: "RO.RO1.RO11",
      },
      {
        id: 15,
        code: "BH",
        sirutaCode: "40061",
        name: "Bihor",
        level: "NUTS3",
        parentId: 5,
        path: "RO.RO1.RO11.BH",
      },
    ],
  }
);

export const TerritoryListResponseSchema = Type.Object({
  data: Type.Array(TerritorySchema),
  meta: Type.Object({
    pagination: PaginationMetaSchema,
  }),
});

// ============================================================================
// Time Period Schemas
// ============================================================================

export const TimePeriodSchema = Type.Object(
  {
    id: Type.Number(),
    year: Type.Number(),
    quarter: Type.Union([Type.Number(), Type.Null()]),
    month: Type.Union([Type.Number(), Type.Null()]),
    periodicity: Type.String(),
    insLabel: Type.String({
      description: "INS format label (e.g., 'Anul 2023')",
    }),
    periodStart: Type.String({ format: "date" }),
    periodEnd: Type.String({ format: "date" }),
  },
  {
    examples: [
      {
        id: 2023,
        year: 2023,
        quarter: null,
        month: null,
        periodicity: "ANNUAL",
        insLabel: "Anul 2023",
        periodStart: "2023-01-01",
        periodEnd: "2023-12-31",
      },
      {
        id: 202301,
        year: 2023,
        quarter: 1,
        month: null,
        periodicity: "QUARTERLY",
        insLabel: "Trimestrul I 2023",
        periodStart: "2023-01-01",
        periodEnd: "2023-03-31",
      },
    ],
  }
);

export const TimePeriodListResponseSchema = Type.Object({
  data: Type.Array(TimePeriodSchema),
  meta: Type.Object({
    pagination: PaginationMetaSchema,
  }),
});

// ============================================================================
// Classification Schemas
// ============================================================================

export const ClassificationTypeSchema = Type.Object(
  {
    id: Type.Number(),
    code: Type.String(),
    name: Type.String(),
    isHierarchical: Type.Boolean(),
    valueCount: Type.Number(),
  },
  {
    examples: [
      {
        id: 1,
        code: "SEX",
        name: "Sexe",
        isHierarchical: false,
        valueCount: 3,
      },
      {
        id: 2,
        code: "VARSTA",
        name: "Grupe de varsta",
        isHierarchical: true,
        valueCount: 25,
      },
    ],
  }
);

// Base classification value fields without children (to avoid circular reference)
const ClassificationValueBaseFields = {
  id: Type.Number(),
  code: Type.String(),
  name: Type.String(),
  parentId: Type.Union([Type.Number(), Type.Null()]),
  path: Type.Union([Type.String(), Type.Null()]),
  level: Type.Number(),
};

export const ClassificationValueSchema = Type.Object(
  {
    ...ClassificationValueBaseFields,
    children: Type.Optional(
      Type.Array(Type.Object(ClassificationValueBaseFields))
    ),
  },
  {
    examples: [
      {
        id: 1,
        code: "TOTAL",
        name: "Total",
        parentId: null,
        path: "TOTAL",
        level: 0,
      },
      {
        id: 2,
        code: "M",
        name: "Masculin",
        parentId: 1,
        path: "TOTAL.M",
        level: 1,
      },
      {
        id: 3,
        code: "F",
        name: "Feminin",
        parentId: 1,
        path: "TOTAL.F",
        level: 1,
      },
    ],
  }
);

export const ClassificationTypeListResponseSchema = Type.Object({
  data: Type.Array(ClassificationTypeSchema),
  meta: Type.Object({
    pagination: PaginationMetaSchema,
  }),
});

// ============================================================================
// Statistics Schemas
// ============================================================================

export const DataPointSchema = Type.Object(
  {
    x: Type.String({ description: "X-axis value (usually year or date)" }),
    y: Type.Union([Type.Number(), Type.Null()], {
      description: "Y-axis value (the statistic)",
    }),
    status: Type.Optional(Type.String()),
    timePeriod: Type.Optional(
      Type.Object({
        id: Type.Number(),
        year: Type.Number(),
        quarter: Type.Optional(Type.Number()),
        month: Type.Optional(Type.Number()),
        periodicity: Type.String(),
      })
    ),
  },
  {
    examples: [
      {
        x: "2023",
        y: 19053815,
        timePeriod: { id: 2023, year: 2023, periodicity: "ANNUAL" },
      },
      {
        x: "2022",
        y: 19186201,
        timePeriod: { id: 2022, year: 2022, periodicity: "ANNUAL" },
      },
      {
        x: "2021",
        y: 19328838,
        timePeriod: { id: 2021, year: 2021, periodicity: "ANNUAL" },
      },
    ],
  }
);

export const AxisInfoSchema = Type.Object({
  name: Type.String(),
  type: Type.Union([
    Type.Literal("INTEGER"),
    Type.Literal("FLOAT"),
    Type.Literal("DATE"),
    Type.Literal("STRING"),
  ]),
  unit: Type.String(),
});

export const SeriesDimensionsSchema = Type.Object({
  territory: Type.Optional(
    Type.Object({
      id: Type.Number(),
      code: Type.String(),
      name: Type.String(),
      level: Type.String(),
      path: Type.String(),
    })
  ),
  classifications: Type.Optional(
    Type.Record(
      Type.String(),
      Type.Object({
        id: Type.Number(),
        code: Type.String(),
        name: Type.String(),
      })
    )
  ),
  unit: Type.Optional(
    Type.Object({
      id: Type.Number(),
      code: Type.String(),
      name: Type.String(),
      symbol: Type.Union([Type.String(), Type.Null()]),
    })
  ),
});

export const TimeSeriesSchema = Type.Object(
  {
    seriesId: Type.String({ description: "Unique identifier for this series" }),
    name: Type.String({ description: "Human-readable series name" }),
    dimensions: SeriesDimensionsSchema,
    xAxis: AxisInfoSchema,
    yAxis: AxisInfoSchema,
    data: Type.Array(DataPointSchema),
  },
  {
    examples: [
      {
        seriesId: "POP105A_RO_Total_Total_Total",
        name: "ROMANIA - Total - Total - Total",
        dimensions: {
          territory: {
            id: 1,
            code: "RO",
            name: "ROMANIA",
            level: "NATIONAL",
            path: "RO",
          },
          classifications: {
            SEX: { id: 1, code: "TOTAL", name: "Total" },
            VARSTA: { id: 1, code: "TOTAL", name: "Total" },
            MEDII: { id: 1, code: "TOTAL", name: "Total" },
          },
          unit: {
            id: 1,
            code: "PERS",
            name: "Numar persoane",
            symbol: null,
          },
        },
        xAxis: { name: "Year", type: "INTEGER", unit: "year" },
        yAxis: { name: "Population", type: "INTEGER", unit: "persons" },
        data: [
          { x: "2023", y: 19053815 },
          { x: "2022", y: 19186201 },
          { x: "2021", y: 19328838 },
          { x: "2020", y: 19442038 },
        ],
      },
    ],
  }
);

export const StatisticsResponseSchema = Type.Object(
  {
    matrix: MatrixSummarySchema,
    series: Type.Array(TimeSeriesSchema),
  },
  {
    description: "Statistics query response with time series data",
    examples: [
      {
        matrix: {
          id: 42,
          insCode: "POP105A",
          name: "Populatia pe sexe, pe grupe de varsta si medii",
          contextPath: "A.A01.A0101",
          contextName: "Populatia dupa domiciliu",
          periodicity: ["ANNUAL"],
          hasUatData: true,
          hasCountyData: true,
          dimensionCount: 5,
          startYear: 1990,
          endYear: 2024,
          lastUpdate: "2024-06-15T10:30:00.000Z",
          status: "ACTIVE",
        },
        series: [
          {
            seriesId: "POP105A_RO_Total_Total_Total",
            name: "ROMANIA - Total - Total - Total",
            dimensions: {
              territory: {
                id: 1,
                code: "RO",
                name: "ROMANIA",
                level: "NATIONAL",
                path: "RO",
              },
            },
            xAxis: { name: "Year", type: "INTEGER", unit: "year" },
            yAxis: { name: "Population", type: "INTEGER", unit: "persons" },
            data: [
              { x: "2023", y: 19053815 },
              { x: "2022", y: 19186201 },
              { x: "2021", y: 19328838 },
              { x: "2020", y: 19442038 },
            ],
          },
        ],
      },
    ],
  }
);

export const StatisticsSummaryResponseSchema = Type.Object({
  matrix: MatrixSummarySchema,
  summary: Type.Object({
    totalRecords: Type.Number(),
    timeRange: Type.Union([
      Type.Object({ from: Type.Number(), to: Type.Number() }),
      Type.Null(),
    ]),
    territoryLevels: Type.Array(
      Type.Object({ level: Type.String(), count: Type.Number() })
    ),
    valueStats: Type.Object({
      min: Type.Union([Type.Number(), Type.Null()]),
      max: Type.Union([Type.Number(), Type.Null()]),
      avg: Type.Union([Type.Number(), Type.Null()]),
      sum: Type.Union([Type.Number(), Type.Null()]),
      nullCount: Type.Number(),
    }),
  }),
});

// ============================================================================
// Generic Response Helpers
// ============================================================================

export function createDataResponse<T extends TSchema>(dataSchema: T) {
  return Type.Object({
    data: dataSchema,
  });
}

export function createListResponse<T extends TSchema>(itemSchema: T) {
  return Type.Object({
    data: Type.Array(itemSchema),
    meta: Type.Object({
      pagination: PaginationMetaSchema,
    }),
  });
}
