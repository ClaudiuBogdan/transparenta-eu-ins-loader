/**
 * Matrix Routes - /api/v1/matrices
 */

import { Type, type Static } from "@sinclair/typebox";

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
// Helpers
// ============================================================================

/**
 * Parse PostgreSQL array string format (e.g., "{ANNUAL,QUARTERLY}") to JS array
 */
function parsePgArray(value: unknown): string[] {
  if (Array.isArray(value)) {
    return value.map(String);
  }
  if (typeof value === "string") {
    // Handle PostgreSQL array format: {value1,value2}
    if (value.startsWith("{") && value.endsWith("}")) {
      const inner = value.slice(1, -1);
      if (inner === "") return [];
      return inner.split(",").map((s) => s.trim());
    }
    return [value];
  }
  return [];
}

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

const DimCodeParamSchema = Type.Object({
  code: Type.String({ description: "Matrix INS code (e.g., POP105A)" }),
  dimCode: Type.String({ description: "Dimension code number" }),
});

type DimCodeParam = Static<typeof DimCodeParamSchema>;

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
          "matrices.name",
          "matrices.name_en",
          "matrices.periodicity",
          "matrices.has_uat_data",
          "matrices.has_county_data",
          "matrices.dimension_count",
          "matrices.start_year",
          "matrices.end_year",
          "matrices.last_update",
          "matrices.status",
          "contexts.path as context_path",
          "contexts.name as context_name",
        ]);

      // Apply filters
      if (q) {
        // Search in the appropriate language, fallback to RO if EN not available
        if (locale === "en") {
          query = query.where((eb) =>
            eb.or([
              eb("matrices.name_en", "ilike", `%${q}%`),
              eb.and([
                eb("matrices.name_en", "is", null),
                eb("matrices.name", "ilike", `%${q}%`),
              ]),
            ])
          );
        } else {
          query = query.where("matrices.name", "ilike", `%${q}%`);
        }
      }

      if (contextId) {
        query = query.where("matrices.context_id", "=", contextId);
      }

      if (contextPath) {
        query = query.where("contexts.path", "like", `${contextPath}%`);
      }

      if (hasUatData !== undefined) {
        query = query.where("matrices.has_uat_data", "=", hasUatData);
      }

      if (hasCountyData !== undefined) {
        query = query.where("matrices.has_county_data", "=", hasCountyData);
      }

      if (periodicity) {
        query = query.where("matrices.periodicity", "@>", [periodicity]);
      }

      if (status) {
        query = query.where("matrices.status", "=", status);
      } else {
        query = query.where("matrices.status", "=", "ACTIVE");
      }

      // Apply cursor-based pagination
      const sortField =
        sortBy === "lastUpdate" ? "matrices.last_update" : "matrices.name";
      if (cursorPayload) {
        const op = sortOrder === "asc" ? ">" : "<";
        const sortValue = String(cursorPayload.sortValue);
        query = query.where((eb) =>
          eb.or([
            eb(sortField, op, sortValue),
            eb.and([
              eb(sortField, "=", sortValue),
              eb("matrices.id", ">", cursorPayload.id),
            ]),
          ])
        );
      }

      const rows = await query
        .orderBy(sortField, sortOrder)
        .orderBy("matrices.id", "asc")
        .limit(limit + 1)
        .execute();

      const hasMore = rows.length > limit;
      const items: MatrixSummaryDto[] = rows.slice(0, limit).map((m) => ({
        id: m.id,
        insCode: m.ins_code,
        // Use locale-appropriate name, fallback to RO if EN not available
        name: locale === "en" && m.name_en ? m.name_en : m.name,
        contextPath: m.context_path,
        contextName: m.context_name,
        periodicity: parsePgArray(m.periodicity),
        hasUatData: m.has_uat_data,
        hasCountyData: m.has_county_data,
        dimensionCount: m.dimension_count,
        startYear: m.start_year,
        endYear: m.end_year,
        lastUpdate: m.last_update?.toISOString() ?? null,
        status: m.status,
      }));

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

      // Get matrix
      const matrix = await db
        .selectFrom("matrices")
        .leftJoin("contexts", "matrices.context_id", "contexts.id")
        .select([
          "matrices.id",
          "matrices.ins_code",
          "matrices.name",
          "matrices.name_en",
          "matrices.periodicity",
          "matrices.has_uat_data",
          "matrices.has_county_data",
          "matrices.dimension_count",
          "matrices.start_year",
          "matrices.end_year",
          "matrices.last_update",
          "matrices.status",
          "matrices.definition",
          "matrices.definition_en",
          "matrices.methodology",
          "matrices.methodology_en",
          "matrices.observations",
          "matrices.observations_en",
          "matrices.series_break",
          "matrices.series_break_en",
          "matrices.series_continuation",
          "matrices.series_continuation_en",
          "matrices.responsible_persons",
          "matrices.view_count",
          "matrices.download_count",
          "contexts.path as context_path",
          "contexts.name as context_name",
        ])
        .where("matrices.ins_code", "=", code)
        .executeTakeFirst();

      if (!matrix) {
        throw new NotFoundError(`Matrix with code ${code} not found`);
      }

      // Get dimensions
      const dimensions = await db
        .selectFrom("matrix_dimensions")
        .leftJoin(
          "classification_types",
          "matrix_dimensions.classification_type_id",
          "classification_types.id"
        )
        .select([
          "matrix_dimensions.id",
          "matrix_dimensions.dim_code",
          "matrix_dimensions.label",
          "matrix_dimensions.label_en",
          "matrix_dimensions.dimension_type",
          "matrix_dimensions.is_hierarchical",
          "matrix_dimensions.option_count",
          "classification_types.code as classification_type_code",
        ])
        .where("matrix_dimensions.matrix_id", "=", matrix.id)
        .orderBy("matrix_dimensions.dim_code", "asc")
        .execute();

      // Get data sources
      const dataSources = await db
        .selectFrom("matrix_data_sources")
        .select(["name", "name_en", "source_type"])
        .where("matrix_id", "=", matrix.id)
        .execute();

      // Get sync status
      const syncStatus = await db
        .selectFrom("matrix_sync_status")
        .select([
          "sync_status",
          "last_full_sync",
          "last_metadata_sync",
          "data_start_year",
          "data_end_year",
          "row_count",
        ])
        .where("matrix_id", "=", matrix.id)
        .executeTakeFirst();

      const matrixDetail: MatrixDetailDto = {
        id: matrix.id,
        insCode: matrix.ins_code,
        // Use locale-appropriate values, fallback to RO if EN not available
        name: locale === "en" && matrix.name_en ? matrix.name_en : matrix.name,
        contextPath: matrix.context_path,
        contextName: matrix.context_name,
        periodicity: parsePgArray(matrix.periodicity),
        hasUatData: matrix.has_uat_data,
        hasCountyData: matrix.has_county_data,
        dimensionCount: matrix.dimension_count,
        startYear: matrix.start_year,
        endYear: matrix.end_year,
        lastUpdate: matrix.last_update?.toISOString() ?? null,
        status: matrix.status,
        definition:
          locale === "en" && matrix.definition_en
            ? matrix.definition_en
            : matrix.definition,
        methodology:
          locale === "en" && matrix.methodology_en
            ? matrix.methodology_en
            : matrix.methodology,
        observations:
          locale === "en" && matrix.observations_en
            ? matrix.observations_en
            : matrix.observations,
        seriesBreak:
          locale === "en" && matrix.series_break_en
            ? matrix.series_break_en
            : matrix.series_break,
        seriesContinuation:
          locale === "en" && matrix.series_continuation_en
            ? matrix.series_continuation_en
            : matrix.series_continuation,
        responsiblePersons: matrix.responsible_persons,
        viewCount: matrix.view_count ?? 0,
        downloadCount: matrix.download_count ?? 0,
        dataSources: dataSources.map((ds) => ({
          name: locale === "en" && ds.name_en ? ds.name_en : ds.name,
          sourceType: ds.source_type,
        })),
      };

      const dimensionDtos: DimensionInfoDto[] = dimensions.map((d) => ({
        id: d.id,
        dimCode: d.dim_code,
        label: locale === "en" && d.label_en ? d.label_en : d.label,
        dimensionType: d.dimension_type,
        classificationTypeCode: d.classification_type_code ?? undefined,
        isHierarchical: d.is_hierarchical,
        optionCount: d.option_count,
      }));

      const syncStatusDto: SyncStatusDto | null = syncStatus
        ? {
            syncStatus: syncStatus.sync_status,
            lastFullSync: syncStatus.last_full_sync?.toISOString() ?? null,
            lastMetadataSync:
              syncStatus.last_metadata_sync?.toISOString() ?? null,
            dataStartYear: syncStatus.data_start_year,
            dataEndYear: syncStatus.data_end_year,
            rowCount: syncStatus.row_count ?? 0,
          }
        : null;

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
   * GET /api/v1/matrices/:code/dimensions/:dimCode
   * Get dimension options
   */
  app.get<{
    Params: DimCodeParam;
    Querystring: Static<typeof DimensionOptionsQuerySchema>;
  }>(
    "/matrices/:code/dimensions/:dimCode",
    {
      schema: {
        summary: "Get dimension options",
        description:
          "Get all available options for a specific dimension. Each option includes its nomItemId " +
          "(used for querying) and reference to territory, time period, or classification value. " +
          "Example: `/matrices/POP105A/dimensions/2` returns available territories.",
        tags: ["Matrices"],
        params: DimCodeParamSchema,
        querystring: DimensionOptionsQuerySchema,
        // Response schema disabled - complex nested objects cause serialization issues
      },
    },
    async (request) => {
      const { code, dimCode } = request.params;
      const locale = request.query.locale ?? "ro";
      const dimCodeNum = parseInt(dimCode, 10);

      if (isNaN(dimCodeNum)) {
        throw new NotFoundError("Invalid dimension code");
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
          "dim_code",
          "label",
          "label_en",
          "dimension_type",
          "is_hierarchical",
          "option_count",
        ])
        .where("matrix_id", "=", matrix.id)
        .where("dim_code", "=", dimCodeNum)
        .executeTakeFirst();

      if (!dimension) {
        throw new NotFoundError(
          `Dimension ${dimCode} not found for matrix ${code}`
        );
      }

      // Get options with references
      const options = await db
        .selectFrom("matrix_dimension_options")
        .leftJoin(
          "territories",
          "matrix_dimension_options.territory_id",
          "territories.id"
        )
        .leftJoin(
          "time_periods",
          "matrix_dimension_options.time_period_id",
          "time_periods.id"
        )
        .leftJoin(
          "classification_values",
          "matrix_dimension_options.classification_value_id",
          "classification_values.id"
        )
        .leftJoin(
          "units_of_measure",
          "matrix_dimension_options.unit_of_measure_id",
          "units_of_measure.id"
        )
        .select([
          "matrix_dimension_options.id",
          "matrix_dimension_options.nom_item_id",
          "matrix_dimension_options.label",
          "matrix_dimension_options.label_en",
          "matrix_dimension_options.offset_order",
          "matrix_dimension_options.parent_nom_item_id",
          "territories.id as territory_id",
          "territories.code as territory_code",
          "territories.name as territory_name",
          "territories.path as territory_path",
          "time_periods.id as time_period_id",
          "time_periods.year as time_period_year",
          "time_periods.ins_label as time_period_label",
          "classification_values.id as class_value_id",
          "classification_values.code as class_value_code",
          "classification_values.name as class_value_name",
          "classification_values.path as class_value_path",
          "units_of_measure.id as unit_id",
          "units_of_measure.code as unit_code",
          "units_of_measure.name as unit_name",
        ])
        .where(
          "matrix_dimension_options.matrix_dimension_id",
          "=",
          dimension.id
        )
        .orderBy("matrix_dimension_options.offset_order", "asc")
        .execute();

      const optionDtos = options.map((opt) => {
        let reference = null;

        if (opt.territory_id) {
          reference = {
            type: "TERRITORY" as const,
            id: opt.territory_id,
            code: opt.territory_code ?? undefined,
            name: opt.territory_name ?? undefined,
            path: opt.territory_path ?? undefined,
          };
        } else if (opt.time_period_id) {
          reference = {
            type: "TIME_PERIOD" as const,
            id: opt.time_period_id,
            name: opt.time_period_label ?? undefined,
          };
        } else if (opt.class_value_id) {
          reference = {
            type: "CLASSIFICATION" as const,
            id: opt.class_value_id,
            code: opt.class_value_code ?? undefined,
            name: opt.class_value_name ?? undefined,
            path: opt.class_value_path ?? undefined,
          };
        } else if (opt.unit_id) {
          reference = {
            type: "UNIT" as const,
            id: opt.unit_id,
            code: opt.unit_code ?? undefined,
            name: opt.unit_name ?? undefined,
          };
        }

        return {
          id: opt.id,
          nomItemId: opt.nom_item_id,
          label: locale === "en" && opt.label_en ? opt.label_en : opt.label,
          offsetOrder: opt.offset_order,
          parentNomItemId: opt.parent_nom_item_id,
          reference,
        };
      });

      return {
        data: optionDtos,
        meta: {
          dimension: {
            id: dimension.id,
            dimCode: dimension.dim_code,
            label:
              locale === "en" && dimension.label_en
                ? dimension.label_en
                : dimension.label,
            dimensionType: dimension.dimension_type,
            isHierarchical: dimension.is_hierarchical,
            optionCount: dimension.option_count,
          },
        },
      };
    }
  );
}
