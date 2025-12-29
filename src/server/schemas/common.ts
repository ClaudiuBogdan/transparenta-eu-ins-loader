/**
 * Common TypeBox schemas for API validation
 */

import { Type, type Static, type TSchema } from "@sinclair/typebox";

// ============================================================================
// Pagination Schemas
// ============================================================================

export const PaginationQuerySchema = Type.Object({
  limit: Type.Optional(Type.Number({ minimum: 1, maximum: 500, default: 50 })),
  cursor: Type.Optional(Type.String()),
});

export type PaginationQuery = Static<typeof PaginationQuerySchema>;

export const PaginationMetaSchema = Type.Object({
  cursor: Type.Union([Type.String(), Type.Null()]),
  hasMore: Type.Boolean(),
  limit: Type.Number(),
  total: Type.Optional(Type.Number()),
});

// ============================================================================
// Error Schemas
// ============================================================================

export const ApiErrorSchema = Type.Object({
  error: Type.String(),
  message: Type.String(),
  details: Type.Optional(Type.Record(Type.String(), Type.Unknown())),
  requestId: Type.Optional(Type.String()),
});

export type ApiErrorType = Static<typeof ApiErrorSchema>;

// ============================================================================
// Response Wrapper Schemas
// ============================================================================

export const QueryMetaSchema = Type.Object({
  executionTimeMs: Type.Number(),
  appliedFilters: Type.Record(Type.String(), Type.Unknown()),
});

export function createResponseSchema<T extends TSchema>(dataSchema: T) {
  return Type.Object({
    data: dataSchema,
    meta: Type.Optional(
      Type.Object({
        pagination: Type.Optional(PaginationMetaSchema),
        query: Type.Optional(QueryMetaSchema),
      })
    ),
  });
}

export function createListResponseSchema<T extends TSchema>(itemSchema: T) {
  return Type.Object({
    data: Type.Array(itemSchema),
    meta: Type.Optional(
      Type.Object({
        pagination: Type.Optional(PaginationMetaSchema),
        query: Type.Optional(QueryMetaSchema),
      })
    ),
  });
}

// ============================================================================
// Common Field Schemas
// ============================================================================

export const TerritorialLevelSchema = Type.Union([
  Type.Literal("NATIONAL"),
  Type.Literal("NUTS1"),
  Type.Literal("NUTS2"),
  Type.Literal("NUTS3"),
  Type.Literal("LAU"),
]);

export const PeriodicitySchema = Type.Union([
  Type.Literal("ANNUAL"),
  Type.Literal("QUARTERLY"),
  Type.Literal("MONTHLY"),
]);

export const MatrixStatusSchema = Type.Union([
  Type.Literal("ACTIVE"),
  Type.Literal("DISCONTINUED"),
]);

export const DimensionTypeSchema = Type.Union([
  Type.Literal("TEMPORAL"),
  Type.Literal("TERRITORIAL"),
  Type.Literal("CLASSIFICATION"),
  Type.Literal("UNIT_OF_MEASURE"),
]);

export const SortOrderSchema = Type.Union([
  Type.Literal("asc"),
  Type.Literal("desc"),
]);

// ============================================================================
// ID Parameter Schemas
// ============================================================================

export const IdParamSchema = Type.Object({
  id: Type.String(),
});

export type IdParam = Static<typeof IdParamSchema>;

export const CodeParamSchema = Type.Object({
  code: Type.String(),
});

export type CodeParam = Static<typeof CodeParamSchema>;

export const MatrixCodeParamSchema = Type.Object({
  matrixCode: Type.String(),
});

export type MatrixCodeParam = Static<typeof MatrixCodeParamSchema>;

// ============================================================================
// Year Range Schema
// ============================================================================

export const YearRangeQuerySchema = Type.Object({
  yearFrom: Type.Optional(Type.Number({ minimum: 1900, maximum: 2100 })),
  yearTo: Type.Optional(Type.Number({ minimum: 1900, maximum: 2100 })),
});

export type YearRangeQuery = Static<typeof YearRangeQuerySchema>;
