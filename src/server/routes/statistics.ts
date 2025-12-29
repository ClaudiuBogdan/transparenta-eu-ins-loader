/**
 * Statistics Routes - /api/v1/statistics
 * Main data query endpoint with time series format
 */

import { Type, type Static } from "@sinclair/typebox";

import { db } from "../../db/connection.js";
import {
  parseLimit,
  validateCursor,
  createPaginationMeta,
} from "../../utils/pagination.js";
import { NotFoundError, NoDataError } from "../plugins/error-handler.js";
import {
  PaginationQuerySchema,
  PeriodicitySchema,
  TerritorialLevelSchema,
} from "../schemas/common.js";
import { StatisticsSummaryResponseSchema } from "../schemas/responses.js";

import type {
  TimeSeriesDto,
  DataPointDto,
  MatrixSummaryDto,
  StatisticsSummaryDto,
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

const StatisticsQuerySchema = Type.Intersect([
  PaginationQuerySchema,
  Type.Object({
    // Territory filters
    territoryId: Type.Optional(
      Type.Number({ description: "Filter by territory ID" })
    ),
    territoryCode: Type.Optional(
      Type.String({
        description: "Filter by territory code (e.g., 'RO', 'BH')",
      })
    ),
    territoryPath: Type.Optional(
      Type.String({ description: "Filter by territory path prefix" })
    ),
    territoryLevel: Type.Optional(TerritorialLevelSchema),

    // Time filters
    yearFrom: Type.Optional(
      Type.Number({ description: "Start year filter (inclusive)" })
    ),
    yearTo: Type.Optional(
      Type.Number({ description: "End year filter (inclusive)" })
    ),
    periodicity: Type.Optional(PeriodicitySchema),

    // Classification filters (JSON string)
    classificationFilters: Type.Optional(
      Type.String({
        description:
          'JSON object mapping classification type codes to arrays of value codes. Example: {"SEX":["M","F"]}',
      })
    ),

    // Grouping
    groupBy: Type.Optional(
      Type.Union(
        [
          Type.Literal("territory"),
          Type.Literal("classification"),
          Type.Literal("none"),
        ],
        { description: "How to group the time series" }
      )
    ),
  }),
]);

type StatisticsQuery = Static<typeof StatisticsQuerySchema>;

const MatrixCodeParamSchema = Type.Object({
  matrixCode: Type.String({ description: "Matrix INS code (e.g., POP105A)" }),
});

type MatrixCodeParam = Static<typeof MatrixCodeParamSchema>;

const SummaryDataResponseSchema = Type.Object({
  data: StatisticsSummaryResponseSchema,
});

// ============================================================================
// Routes
// ============================================================================

export function registerStatisticsRoutes(app: FastifyInstance): void {
  /**
   * GET /api/v1/statistics/:matrixCode
   * Query statistics as time series
   */
  app.get<{ Params: MatrixCodeParam; Querystring: StatisticsQuery }>(
    "/statistics/:matrixCode",
    {
      schema: {
        summary: "Query statistics",
        description:
          "Query statistical data from a matrix and get results as time series. " +
          "Examples:\n" +
          "- `/statistics/POP105A?territoryCode=RO&yearFrom=2020` - Romania population from 2020\n" +
          "- `/statistics/POP105A?territoryLevel=NUTS3&groupBy=territory` - By county\n" +
          '- `/statistics/POP105A?classificationFilters={"SEX":["M"]}` - Male population only',
        tags: ["Statistics"],
        params: MatrixCodeParamSchema,
        querystring: StatisticsQuerySchema,
        // Response schema disabled - complex nested objects cause serialization issues
      },
    },
    async (request) => {
      const { matrixCode } = request.params;
      const {
        territoryId,
        territoryCode,
        territoryPath,
        territoryLevel,
        yearFrom,
        yearTo,
        periodicity,
        classificationFilters: classFiltersJson,
        groupBy = "none",
        limit: rawLimit,
        cursor,
      } = request.query;

      const limit = parseLimit(rawLimit, 100, 1000);
      const cursorPayload = validateCursor(cursor);

      // Get matrix
      const matrix = await db
        .selectFrom("matrices")
        .leftJoin("contexts", "matrices.context_id", "contexts.id")
        .select([
          "matrices.id",
          "matrices.ins_code",
          "matrices.name",
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
        ])
        .where("matrices.ins_code", "=", matrixCode)
        .executeTakeFirst();

      if (!matrix) {
        throw new NotFoundError(`Matrix ${matrixCode} not found`);
      }

      // Build statistics query
      let query = db
        .selectFrom("statistics")
        .innerJoin(
          "time_periods",
          "statistics.time_period_id",
          "time_periods.id"
        )
        .leftJoin("territories", "statistics.territory_id", "territories.id")
        .leftJoin(
          "units_of_measure",
          "statistics.unit_of_measure_id",
          "units_of_measure.id"
        )
        .select([
          "statistics.id",
          "statistics.value",
          "statistics.value_status",
          "time_periods.id as tp_id",
          "time_periods.year",
          "time_periods.quarter",
          "time_periods.month",
          "time_periods.periodicity",
          "time_periods.ins_label",
          "territories.id as terr_id",
          "territories.code as terr_code",
          "territories.name as terr_name",
          "territories.level as terr_level",
          "territories.path as terr_path",
          "units_of_measure.id as unit_id",
          "units_of_measure.code as unit_code",
          "units_of_measure.name as unit_name",
          "units_of_measure.symbol as unit_symbol",
        ])
        .where("statistics.matrix_id", "=", matrix.id);

      // Apply territory filters
      if (territoryId !== undefined) {
        query = query.where("statistics.territory_id", "=", territoryId);
      }

      if (territoryCode) {
        query = query.where("territories.code", "=", territoryCode);
      }

      if (territoryPath) {
        query = query.where("territories.path", "like", `${territoryPath}%`);
      }

      if (territoryLevel) {
        query = query.where("territories.level", "=", territoryLevel);
      }

      // Apply time filters
      if (yearFrom !== undefined) {
        query = query.where("time_periods.year", ">=", yearFrom);
      }

      if (yearTo !== undefined) {
        query = query.where("time_periods.year", "<=", yearTo);
      }

      if (periodicity) {
        query = query.where("time_periods.periodicity", "=", periodicity);
      }

      // Parse and apply classification filters
      let classFilters: Record<string, string[]> = {};
      if (classFiltersJson) {
        try {
          classFilters = JSON.parse(classFiltersJson) as Record<
            string,
            string[]
          >;
        } catch {
          // Ignore invalid JSON
        }
      }

      // If we have classification filters, join the statistic_classifications table
      const classFilterEntries = Object.entries(classFilters);
      if (classFilterEntries.length > 0) {
        // Get classification value IDs for the filters
        for (const [typeCode, valueCodes] of classFilterEntries) {
          if (valueCodes.length === 0) continue;

          const classValueIds = await db
            .selectFrom("classification_values")
            .innerJoin(
              "classification_types",
              "classification_values.classification_type_id",
              "classification_types.id"
            )
            .select("classification_values.id")
            .where("classification_types.code", "=", typeCode)
            .where("classification_values.code", "in", valueCodes)
            .execute();

          if (classValueIds.length > 0) {
            const ids = classValueIds.map((cv) => cv.id);
            query = query.where(
              "statistics.id",
              "in",
              db
                .selectFrom("statistic_classifications")
                .select("statistic_id")
                .where("matrix_id", "=", matrix.id)
                .where("classification_value_id", "in", ids)
            );
          }
        }
      }

      // Apply cursor pagination
      if (cursorPayload) {
        query = query.where((eb) =>
          eb.or([
            eb("time_periods.year", ">", cursorPayload.sortValue as number),
            eb.and([
              eb("time_periods.year", "=", cursorPayload.sortValue as number),
              eb("statistics.id", ">", cursorPayload.id),
            ]),
          ])
        );
      }

      // Fetch data
      const rows = await query
        .orderBy("time_periods.year", "asc")
        .orderBy("time_periods.quarter", "asc")
        .orderBy("time_periods.month", "asc")
        .orderBy("statistics.id", "asc")
        .limit(limit + 1)
        .execute();

      if (rows.length === 0) {
        throw new NoDataError("No statistics found for the given query");
      }

      const hasMore = rows.length > limit;
      const dataRows = rows.slice(0, limit);

      // Build time series based on grouping
      const series: TimeSeriesDto[] = [];

      if (groupBy === "none") {
        // Single series with all data points
        const dataPoints: DataPointDto[] = dataRows.map((row) => ({
          x: row.ins_label,
          y: row.value ?? null,
          status: row.value_status ?? undefined,
          timePeriod: {
            id: row.tp_id,
            year: row.year,
            quarter: row.quarter ?? undefined,
            month: row.month ?? undefined,
            periodicity: row.periodicity,
          },
        }));

        // Use first row for dimensions (they should all be the same for groupBy=none)
        // We know dataRows has elements because we throw NoDataError if rows.length === 0
        const firstRow = dataRows[0]!;
        series.push({
          seriesId: `${matrixCode}_series`,
          name: matrix.name,
          dimensions: {
            territory: firstRow.terr_id
              ? {
                  id: firstRow.terr_id,
                  code: firstRow.terr_code ?? "",
                  name: firstRow.terr_name ?? "",
                  level: firstRow.terr_level ?? "",
                  path: firstRow.terr_path ?? "",
                }
              : undefined,
            unit: firstRow.unit_id
              ? {
                  id: firstRow.unit_id,
                  code: firstRow.unit_code ?? "",
                  name: firstRow.unit_name ?? "",
                  symbol: firstRow.unit_symbol ?? null,
                }
              : undefined,
          },
          xAxis: { name: "Period", type: "STRING", unit: "" },
          yAxis: {
            name: "Value",
            type: "FLOAT",
            unit: firstRow.unit_symbol ?? firstRow.unit_name ?? "",
          },
          data: dataPoints,
        });
      } else if (groupBy === "territory") {
        // Group by territory
        const territoryGroups = new Map<
          number,
          { info: (typeof dataRows)[0]; points: DataPointDto[] }
        >();

        for (const row of dataRows) {
          const terrId = row.terr_id ?? 0;
          if (!territoryGroups.has(terrId)) {
            territoryGroups.set(terrId, { info: row, points: [] });
          }
          territoryGroups.get(terrId)!.points.push({
            x: row.ins_label,
            y: row.value ?? null,
            status: row.value_status ?? undefined,
            timePeriod: {
              id: row.tp_id,
              year: row.year,
              quarter: row.quarter ?? undefined,
              month: row.month ?? undefined,
              periodicity: row.periodicity,
            },
          });
        }

        for (const [terrId, group] of territoryGroups) {
          const { info, points } = group;
          series.push({
            seriesId: `${matrixCode}_${info.terr_code ?? String(terrId)}`,
            name: info.terr_name ?? "Unknown",
            dimensions: {
              territory: info.terr_id
                ? {
                    id: info.terr_id,
                    code: info.terr_code ?? "",
                    name: info.terr_name ?? "",
                    level: info.terr_level ?? "",
                    path: info.terr_path ?? "",
                  }
                : undefined,
              unit: info.unit_id
                ? {
                    id: info.unit_id,
                    code: info.unit_code ?? "",
                    name: info.unit_name ?? "",
                    symbol: info.unit_symbol ?? null,
                  }
                : undefined,
            },
            xAxis: { name: "Period", type: "STRING", unit: "" },
            yAxis: {
              name: "Value",
              type: "FLOAT",
              unit: info.unit_symbol ?? info.unit_name ?? "",
            },
            data: points,
          });
        }
      } else {
        // groupBy === "classification"
        // For classification grouping, we need to fetch classification values
        const statIds = dataRows.map((r) => r.id);
        const classValues =
          statIds.length > 0
            ? await db
                .selectFrom("statistic_classifications")
                .innerJoin(
                  "classification_values",
                  "statistic_classifications.classification_value_id",
                  "classification_values.id"
                )
                .innerJoin(
                  "classification_types",
                  "classification_values.classification_type_id",
                  "classification_types.id"
                )
                .select([
                  "statistic_classifications.statistic_id",
                  "classification_types.code as type_code",
                  "classification_values.id as value_id",
                  "classification_values.code as value_code",
                  "classification_values.name as value_name",
                ])
                .where("statistic_classifications.matrix_id", "=", matrix.id)
                .where("statistic_classifications.statistic_id", "in", statIds)
                .execute()
            : [];

        // Build map of statistic_id -> classification info
        const classMap = new Map<
          number,
          Record<string, { id: number; code: string; name: string }>
        >();
        for (const cv of classValues) {
          if (!classMap.has(cv.statistic_id)) {
            classMap.set(cv.statistic_id, {});
          }
          classMap.get(cv.statistic_id)![cv.type_code] = {
            id: cv.value_id,
            code: cv.value_code,
            name: cv.value_name,
          };
        }

        // Group by classification combination
        const classGroups = new Map<
          string,
          {
            info: (typeof dataRows)[0];
            classifications: Record<
              string,
              { id: number; code: string; name: string }
            >;
            points: DataPointDto[];
          }
        >();

        for (const row of dataRows) {
          const classifications = classMap.get(row.id) ?? {};
          const key = Object.entries(classifications)
            .sort(([a], [b]) => a.localeCompare(b))
            .map(([k, v]) => `${k}:${v.code}`)
            .join("|");

          if (!classGroups.has(key)) {
            classGroups.set(key, { info: row, classifications, points: [] });
          }
          classGroups.get(key)!.points.push({
            x: row.ins_label,
            y: row.value ?? null,
            status: row.value_status ?? undefined,
            timePeriod: {
              id: row.tp_id,
              year: row.year,
              quarter: row.quarter ?? undefined,
              month: row.month ?? undefined,
              periodicity: row.periodicity,
            },
          });
        }

        for (const [key, group] of classGroups) {
          const { info, classifications, points } = group;
          const classNames = Object.values(classifications)
            .map((c) => c.name)
            .join(", ");

          series.push({
            seriesId: `${matrixCode}_${key || "all"}`,
            name: classNames || matrix.name,
            dimensions: {
              territory: info.terr_id
                ? {
                    id: info.terr_id,
                    code: info.terr_code ?? "",
                    name: info.terr_name ?? "",
                    level: info.terr_level ?? "",
                    path: info.terr_path ?? "",
                  }
                : undefined,
              classifications:
                Object.keys(classifications).length > 0
                  ? classifications
                  : undefined,
              unit: info.unit_id
                ? {
                    id: info.unit_id,
                    code: info.unit_code ?? "",
                    name: info.unit_name ?? "",
                    symbol: info.unit_symbol ?? null,
                  }
                : undefined,
            },
            xAxis: { name: "Period", type: "STRING", unit: "" },
            yAxis: {
              name: "Value",
              type: "FLOAT",
              unit: info.unit_symbol ?? info.unit_name ?? "",
            },
            data: points,
          });
        }
      }

      const matrixDto: MatrixSummaryDto = {
        id: matrix.id,
        insCode: matrix.ins_code,
        name: matrix.name,
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
      };

      return {
        data: {
          matrix: matrixDto,
          series,
        },
        meta: {
          query: {
            executionTimeMs: 0, // Would need timing logic
            appliedFilters: {
              territoryId,
              territoryCode,
              territoryPath,
              territoryLevel,
              yearFrom,
              yearTo,
              periodicity,
              classificationFilters: classFilters,
              groupBy,
            },
          },
          pagination: createPaginationMeta(dataRows, limit, "year", hasMore),
        },
      };
    }
  );

  /**
   * GET /api/v1/statistics/:matrixCode/summary
   * Get aggregated summary statistics
   */
  app.get<{ Params: MatrixCodeParam }>(
    "/statistics/:matrixCode/summary",
    {
      schema: {
        summary: "Get statistics summary",
        description:
          "Get an aggregated summary of statistics for a matrix including total records, " +
          "time range, territory distribution, and value statistics (min, max, avg, sum).",
        tags: ["Statistics"],
        params: MatrixCodeParamSchema,
        response: {
          200: SummaryDataResponseSchema,
        },
      },
    },
    async (request) => {
      const { matrixCode } = request.params;

      // Get matrix
      const matrix = await db
        .selectFrom("matrices")
        .leftJoin("contexts", "matrices.context_id", "contexts.id")
        .select([
          "matrices.id",
          "matrices.ins_code",
          "matrices.name",
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
        ])
        .where("matrices.ins_code", "=", matrixCode)
        .executeTakeFirst();

      if (!matrix) {
        throw new NotFoundError(`Matrix ${matrixCode} not found`);
      }

      // Get statistics aggregates
      const stats = await db
        .selectFrom("statistics")
        .select([
          db.fn.count("id").as("total_count"),
          db.fn.min("value").as("min_value"),
          db.fn.max("value").as("max_value"),
          db.fn.avg("value").as("avg_value"),
          db.fn.sum("value").as("sum_value"),
        ])
        .where("matrix_id", "=", matrix.id)
        .executeTakeFirst();

      // Get time range
      const timeRange = await db
        .selectFrom("statistics")
        .innerJoin(
          "time_periods",
          "statistics.time_period_id",
          "time_periods.id"
        )
        .select([
          db.fn.min("time_periods.year").as("min_year"),
          db.fn.max("time_periods.year").as("max_year"),
        ])
        .where("statistics.matrix_id", "=", matrix.id)
        .executeTakeFirst();

      // Get territory level distribution
      const territoryLevels = await db
        .selectFrom("statistics")
        .innerJoin("territories", "statistics.territory_id", "territories.id")
        .select(["territories.level", db.fn.count("statistics.id").as("count")])
        .where("statistics.matrix_id", "=", matrix.id)
        .groupBy("territories.level")
        .execute();

      // Get null count
      const nullCount = await db
        .selectFrom("statistics")
        .select(db.fn.count("id").as("count"))
        .where("matrix_id", "=", matrix.id)
        .where("value", "is", null)
        .executeTakeFirst();

      const matrixDto: MatrixSummaryDto = {
        id: matrix.id,
        insCode: matrix.ins_code,
        name: matrix.name,
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
      };

      const summary: StatisticsSummaryDto = {
        matrix: matrixDto,
        summary: {
          totalRecords: Number(stats?.total_count ?? 0),
          timeRange:
            timeRange?.min_year && timeRange?.max_year
              ? {
                  from: timeRange.min_year,
                  to: timeRange.max_year,
                }
              : null,
          territoryLevels: territoryLevels.map((tl) => ({
            level: tl.level,
            count: Number(tl.count),
          })),
          valueStats: {
            min: stats?.min_value !== null ? Number(stats?.min_value) : null,
            max: stats?.max_value !== null ? Number(stats?.max_value) : null,
            avg: stats?.avg_value !== null ? Number(stats?.avg_value) : null,
            sum: stats?.sum_value !== null ? Number(stats?.sum_value) : null,
            nullCount: Number(nullCount?.count ?? 0),
          },
        },
      };

      return {
        data: summary,
      };
    }
  );
}
