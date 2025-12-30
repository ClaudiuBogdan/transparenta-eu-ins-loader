/**
 * Matrix Routes - /api/v1/matrices
 * Updated for V2 schema with JSONB metadata
 */

import { Type, type Static } from "@sinclair/typebox";
import { sql } from "kysely";

import { db } from "../../db/connection.js";
import {
  parseLimit,
  validateCursor,
  createPaginationMeta,
} from "../../utils/pagination.js";
import { NotFoundError } from "../plugins/error-handler.js";
import {
  PaginationQuerySchema,
  CodeParamSchema,
  PeriodicitySchema,
  MatrixStatusSchema,
  SortOrderSchema,
  LocaleSchema,
  type Locale,
} from "../schemas/common.js";
import { MatrixListResponseSchema } from "../schemas/responses.js";

import type {
  MatrixSummaryDto,
  MatrixDetailDto,
  DimensionInfoDto,
  SyncStatusDto,
} from "../../types/api.js";
import type { FastifyInstance } from "fastify";

// ============================================================================
// Schemas
// ============================================================================

const ListMatricesQuerySchema = Type.Intersect([
  PaginationQuerySchema,
  Type.Object({
    locale: Type.Optional(LocaleSchema),
    q: Type.Optional(
      Type.String({ description: "Search query for matrix name" })
    ),
    contextId: Type.Optional(
      Type.Number({ description: "Filter by context ID" })
    ),
    contextPath: Type.Optional(
      Type.String({ description: "Filter by context path prefix" })
    ),
    hasUatData: Type.Optional(
      Type.Boolean({ description: "Filter matrices with UAT-level data" })
    ),
    hasCountyData: Type.Optional(
      Type.Boolean({ description: "Filter matrices with county-level data" })
    ),
    periodicity: Type.Optional(PeriodicitySchema),
    status: Type.Optional(MatrixStatusSchema),
    sortBy: Type.Optional(
      Type.String({ description: "Sort field: name or lastUpdate" })
    ),
    sortOrder: Type.Optional(SortOrderSchema),
  }),
]);

type ListMatricesQuery = Static<typeof ListMatricesQuerySchema>;

const DimIndexParamSchema = Type.Object({
  code: Type.String({ description: "Matrix INS code (e.g., POP105A)" }),
  dimIndex: Type.String({ description: "Dimension index number" }),
});

type DimIndexParam = Static<typeof DimIndexParamSchema>;

// ============================================================================
// Routes
// ============================================================================

export function registerMatrixRoutes(app: FastifyInstance): void {
  /**
   * GET /api/v1/matrices
   * Search and filter matrices
   */
  app.get<{ Querystring: ListMatricesQuery }>(
    "/matrices",
    {
      schema: {
        summary: "List matrices",
        description:
          "Search and filter statistical matrices (datasets). Examples:\n" +
          "- `/matrices?q=populat` - Search for population matrices\n" +
          "- `/matrices?contextPath=A` - All matrices under Population domain\n" +
          "- `/matrices?hasCountyData=true` - Matrices with county-level data",
        tags: ["Matrices"],
        querystring: ListMatricesQuerySchema,
        response: {
          200: MatrixListResponseSchema,
        },
      },
    },
    async (request) => {
      const {
        locale = "ro",
        q,
        contextId,
        contextPath,
        hasUatData,
        hasCountyData,
        periodicity,
        status,
        sortBy = "name",
        sortOrder = "asc",
        limit: rawLimit,
        cursor,
      } = request.query;

      const limit = parseLimit(rawLimit, 50, 100);
      const cursorPayload = validateCursor(cursor);

      let query = db
        .selectFrom("matrices")
        .leftJoin("contexts", "matrices.context_id", "contexts.id")
        .select([
          "matrices.id",
          "matrices.ins_code",
          "matrices.metadata",
          "matrices.dimensions",
          "matrices.sync_status",
          "matrices.last_sync_at",
          "contexts.path as context_path",
          "contexts.names as context_names",
        ]);

      // Apply filters using JSONB operators
      if (q) {
        // Search in the appropriate language, fallback to RO if EN not available
        if (locale === "en") {
          query = query.where((eb) =>
            eb.or([
              eb(sql`metadata->'names'->>'en'`, "ilike", `%${q}%`),
              eb.and([
                eb(sql`metadata->'names'->>'en'`, "is", null),
                eb(sql`metadata->'names'->>'ro'`, "ilike", `%${q}%`),
              ]),
            ])
          );
        } else {
          query = query.where(sql`metadata->'names'->>'ro'`, "ilike", `%${q}%`);
        }
      }

      if (contextId) {
        query = query.where("matrices.context_id", "=", contextId);
      }

      if (contextPath) {
        query = query.where(
          sql`contexts.path::text`,
          "like",
          `${contextPath}%`
        );
      }

      if (hasUatData !== undefined) {
        query = query.where(
          sql`(metadata->'flags'->>'hasUatData')::boolean`,
          "=",
          hasUatData
        );
      }

      if (hasCountyData !== undefined) {
        query = query.where(
          sql`(metadata->'flags'->>'hasCountyData')::boolean`,
          "=",
          hasCountyData
        );
      }

      if (periodicity) {
        query = query.where(
          sql`metadata->'periodicity'`,
          "@>",
          sql`${JSON.stringify([periodicity])}::jsonb`
        );
      }

      if (status) {
        query = query.where("matrices.sync_status", "=", status);
      }

      // Apply cursor-based pagination
      const sortField =
        sortBy === "lastUpdate"
          ? "matrices.last_sync_at"
          : sql`metadata->'names'->>'ro'`;

      if (cursorPayload) {
        const op = sortOrder === "asc" ? ">" : "<";
        const sortValue = String(cursorPayload.sortValue);
        if (sortBy === "lastUpdate") {
          query = query.where((eb) =>
            eb.or([
              eb("matrices.last_sync_at", op, new Date(sortValue)),
              eb.and([
                eb("matrices.last_sync_at", "=", new Date(sortValue)),
                eb("matrices.id", ">", cursorPayload.id),
              ]),
            ])
          );
        } else {
          query = query.where((eb) =>
            eb.or([
              eb(sql`metadata->'names'->>'ro'`, op, sortValue),
              eb.and([
                eb(sql`metadata->'names'->>'ro'`, "=", sortValue),
                eb("matrices.id", ">", cursorPayload.id),
              ]),
            ])
          );
        }
      }

      const rows = await query
        .orderBy(sortField, sortOrder)
        .orderBy("matrices.id", "asc")
        .limit(limit + 1)
        .execute();

      const hasMore = rows.length > limit;
      const items: MatrixSummaryDto[] = rows.slice(0, limit).map((m) => {
        const metadata = m.metadata;
        const dimensions = m.dimensions ?? [];
        const contextNames = m.context_names;

        return {
          id: m.id,
          insCode: m.ins_code,
          name:
            locale === "en" && metadata.names.en
              ? metadata.names.en
              : metadata.names.ro,
          contextPath: m.context_path,
          contextName: contextNames
            ? locale === "en" && contextNames.en
              ? contextNames.en
              : contextNames.ro
            : null,
          periodicity: metadata.periodicity ?? [],
          hasUatData: metadata.flags?.hasUatData ?? false,
          hasCountyData: metadata.flags?.hasCountyData ?? false,
          dimensionCount: dimensions.length,
          startYear: metadata.yearRange?.[0] ?? null,
          endYear: metadata.yearRange?.[1] ?? null,
          lastUpdate: metadata.lastUpdate ?? null,
          status: m.sync_status,
        };
      });

      return {
        data: items,
        meta: {
          pagination: createPaginationMeta(
            items,
            limit,
            sortBy === "lastUpdate" ? "lastUpdate" : "name",
            hasMore
          ),
        },
      };
    }
  );

  /**
   * GET /api/v1/matrices/:code
   * Get matrix details with dimensions
   */
  app.get<{
    Params: Static<typeof CodeParamSchema>;
    Querystring: { locale?: Locale };
  }>(
    "/matrices/:code",
    {
      schema: {
        summary: "Get matrix details",
        description:
          "Get detailed information about a matrix including dimensions, data sources, and sync status. " +
          "Example: `/matrices/POP105A` returns population by sex, age groups, and environment.",
        tags: ["Matrices"],
        params: CodeParamSchema,
        querystring: Type.Object({
          locale: Type.Optional(LocaleSchema),
        }),
        // Response schema disabled - complex nested objects cause serialization issues
      },
    },
    async (request) => {
      const { code } = request.params;
      const locale = request.query.locale ?? "ro";

      // Get matrix with context
      const matrix = await db
        .selectFrom("matrices")
        .leftJoin("contexts", "matrices.context_id", "contexts.id")
        .select([
          "matrices.id",
          "matrices.ins_code",
          "matrices.metadata",
          "matrices.dimensions",
          "matrices.sync_status",
          "matrices.last_sync_at",
          "matrices.sync_error",
          "contexts.path as context_path",
          "contexts.names as context_names",
        ])
        .where("matrices.ins_code", "=", code)
        .executeTakeFirst();

      if (!matrix) {
        throw new NotFoundError(`Matrix with code ${code} not found`);
      }

      const metadata = matrix.metadata;
      const dimensionsSummary = matrix.dimensions ?? [];
      const contextNames = matrix.context_names;

      // Get detailed dimensions from matrix_dimensions table
      const dimensions = await db
        .selectFrom("matrix_dimensions")
        .leftJoin(
          "classification_types",
          "matrix_dimensions.classification_type_id",
          "classification_types.id"
        )
        .select([
          "matrix_dimensions.id",
          "matrix_dimensions.dim_index",
          "matrix_dimensions.labels",
          "matrix_dimensions.dimension_type",
          "matrix_dimensions.is_hierarchical",
          "matrix_dimensions.option_count",
          "classification_types.code as classification_type_code",
        ])
        .where("matrix_dimensions.matrix_id", "=", matrix.id)
        .orderBy("matrix_dimensions.dim_index", "asc")
        .execute();

      const matrixDetail: MatrixDetailDto = {
        id: matrix.id,
        insCode: matrix.ins_code,
        name:
          locale === "en" && metadata.names.en
            ? metadata.names.en
            : metadata.names.ro,
        contextPath: matrix.context_path,
        contextName: contextNames
          ? locale === "en" && contextNames.en
            ? contextNames.en
            : contextNames.ro
          : null,
        periodicity: metadata.periodicity ?? [],
        hasUatData: metadata.flags?.hasUatData ?? false,
        hasCountyData: metadata.flags?.hasCountyData ?? false,
        dimensionCount: dimensionsSummary.length,
        startYear: metadata.yearRange?.[0] ?? null,
        endYear: metadata.yearRange?.[1] ?? null,
        lastUpdate: metadata.lastUpdate ?? null,
        status: matrix.sync_status,
        definition:
          locale === "en" && metadata.definitions?.en
            ? metadata.definitions.en
            : (metadata.definitions?.ro ?? null),
        methodology:
          locale === "en" && metadata.methodologies?.en
            ? metadata.methodologies.en
            : (metadata.methodologies?.ro ?? null),
        observations:
          locale === "en" && metadata.observations?.en
            ? metadata.observations.en
            : (metadata.observations?.ro ?? null),
        seriesBreak:
          locale === "en" && metadata.seriesBreak?.en
            ? metadata.seriesBreak.en
            : (metadata.seriesBreak?.ro ?? null),
        seriesContinuation:
          locale === "en" && metadata.seriesContinuation?.en
            ? metadata.seriesContinuation.en
            : (metadata.seriesContinuation?.ro ?? null),
        responsiblePersons: metadata.responsiblePersons ?? null,
        viewCount: 0,
        downloadCount: 0,
        dataSources: (metadata.dataSources ?? []).map((ds) => ({
          name: locale === "en" && ds.nameEn ? ds.nameEn : ds.name,
          sourceType: ds.sourceType ?? null,
        })),
      };

      const dimensionDtos: DimensionInfoDto[] = dimensions.map((d) => ({
        id: d.id,
        dimCode: d.dim_index,
        label: locale === "en" && d.labels.en ? d.labels.en : d.labels.ro,
        dimensionType: d.dimension_type,
        classificationTypeCode: d.classification_type_code ?? undefined,
        isHierarchical: d.is_hierarchical,
        optionCount: d.option_count,
      }));

      const syncStatusDto: SyncStatusDto | null = {
        syncStatus: matrix.sync_status,
        lastFullSync: matrix.last_sync_at?.toISOString() ?? null,
        lastMetadataSync: matrix.last_sync_at?.toISOString() ?? null,
        dataStartYear: metadata.yearRange?.[0] ?? null,
        dataEndYear: metadata.yearRange?.[1] ?? null,
        rowCount: 0,
      };

      return {
        data: {
          matrix: matrixDetail,
          dimensions: dimensionDtos,
          syncStatus: syncStatusDto,
        },
      };
    }
  );

  const DimensionOptionsQuerySchema = Type.Object({
    locale: Type.Optional(LocaleSchema),
  });

  /**
   * GET /api/v1/matrices/:code/dimensions/:dimIndex
   * Get dimension options (nom items)
   */
  app.get<{
    Params: DimIndexParam;
    Querystring: Static<typeof DimensionOptionsQuerySchema>;
  }>(
    "/matrices/:code/dimensions/:dimIndex",
    {
      schema: {
        summary: "Get dimension options",
        description:
          "Get all available options for a specific dimension. Each option includes its nomItemId " +
          "(used for querying) and reference to territory, time period, or classification value. " +
          "Example: `/matrices/POP105A/dimensions/2` returns available territories.",
        tags: ["Matrices"],
        params: DimIndexParamSchema,
        querystring: DimensionOptionsQuerySchema,
        // Response schema disabled - complex nested objects cause serialization issues
      },
    },
    async (request) => {
      const { code, dimIndex } = request.params;
      const locale = request.query.locale ?? "ro";
      const dimIndexNum = parseInt(dimIndex, 10);

      if (isNaN(dimIndexNum)) {
        throw new NotFoundError("Invalid dimension index");
      }

      // Get matrix
      const matrix = await db
        .selectFrom("matrices")
        .select("id")
        .where("ins_code", "=", code)
        .executeTakeFirst();

      if (!matrix) {
        throw new NotFoundError(`Matrix with code ${code} not found`);
      }

      // Get dimension
      const dimension = await db
        .selectFrom("matrix_dimensions")
        .select([
          "id",
          "dim_index",
          "labels",
          "dimension_type",
          "is_hierarchical",
          "option_count",
        ])
        .where("matrix_id", "=", matrix.id)
        .where("dim_index", "=", dimIndexNum)
        .executeTakeFirst();

      if (!dimension) {
        throw new NotFoundError(
          `Dimension ${dimIndex} not found for matrix ${code}`
        );
      }

      // Get nom items (options) with references
      const options = await db
        .selectFrom("matrix_nom_items")
        .leftJoin(
          "territories",
          "matrix_nom_items.territory_id",
          "territories.id"
        )
        .leftJoin(
          "time_periods",
          "matrix_nom_items.time_period_id",
          "time_periods.id"
        )
        .leftJoin(
          "classification_values",
          "matrix_nom_items.classification_value_id",
          "classification_values.id"
        )
        .leftJoin(
          "units_of_measure",
          "matrix_nom_items.unit_id",
          "units_of_measure.id"
        )
        .select([
          "matrix_nom_items.id",
          "matrix_nom_items.nom_item_id",
          "matrix_nom_items.labels",
          "matrix_nom_items.offset_order",
          "matrix_nom_items.parent_nom_item_id",
          "territories.id as territory_id",
          "territories.code as territory_code",
          "territories.names as territory_names",
          "territories.path as territory_path",
          "time_periods.id as time_period_id",
          "time_periods.year as time_period_year",
          "time_periods.labels as time_period_labels",
          "classification_values.id as class_value_id",
          "classification_values.code as class_value_code",
          "classification_values.names as class_value_names",
          "classification_values.path as class_value_path",
          "units_of_measure.id as unit_id",
          "units_of_measure.code as unit_code",
          "units_of_measure.names as unit_names",
        ])
        .where("matrix_nom_items.matrix_id", "=", matrix.id)
        .where("matrix_nom_items.dim_index", "=", dimIndexNum)
        .orderBy("matrix_nom_items.offset_order", "asc")
        .execute();

      const optionDtos = options.map((opt) => {
        let reference = null;

        if (opt.territory_id) {
          const names = opt.territory_names;
          reference = {
            type: "TERRITORY" as const,
            id: opt.territory_id,
            code: opt.territory_code ?? undefined,
            name: names
              ? locale === "en" && names.en
                ? names.en
                : names.ro
              : undefined,
            path: opt.territory_path ?? undefined,
          };
        } else if (opt.time_period_id) {
          const labels = opt.time_period_labels;
          reference = {
            type: "TIME_PERIOD" as const,
            id: opt.time_period_id,
            name: labels
              ? locale === "en" && labels.en
                ? labels.en
                : labels.ro
              : undefined,
          };
        } else if (opt.class_value_id) {
          const names = opt.class_value_names;
          reference = {
            type: "CLASSIFICATION" as const,
            id: opt.class_value_id,
            code: opt.class_value_code ?? undefined,
            name: names
              ? locale === "en" && names.en
                ? names.en
                : names.ro
              : undefined,
            path: opt.class_value_path ?? undefined,
          };
        } else if (opt.unit_id) {
          const names = opt.unit_names;
          reference = {
            type: "UNIT" as const,
            id: opt.unit_id,
            code: opt.unit_code ?? undefined,
            name: names
              ? locale === "en" && names.en
                ? names.en
                : names.ro
              : undefined,
          };
        }

        const labels = opt.labels;
        return {
          id: opt.id,
          nomItemId: opt.nom_item_id,
          label: labels
            ? locale === "en" && labels.en
              ? labels.en
              : labels.ro
            : "",
          offsetOrder: opt.offset_order,
          parentNomItemId: opt.parent_nom_item_id,
          reference,
        };
      });

      const dimensionLabels = dimension.labels;
      return {
        data: optionDtos,
        meta: {
          dimension: {
            id: dimension.id,
            dimCode: dimension.dim_index,
            label: dimensionLabels
              ? locale === "en" && dimensionLabels.en
                ? dimensionLabels.en
                : dimensionLabels.ro
              : "",
            dimensionType: dimension.dimension_type,
            isHierarchical: dimension.is_hierarchical,
            optionCount: dimension.option_count,
          },
        },
      };
    }
  );
}
