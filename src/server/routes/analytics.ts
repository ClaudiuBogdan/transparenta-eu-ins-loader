/**
 * Analytics Routes - /api/v1/analytics
 * Advanced analytical endpoints using PostgreSQL window functions
 */

import { Type, type Static } from "@sinclair/typebox";
import { sql, type SqlBool } from "kysely";

import { db } from "../../db/connection.js";
import {
  parseLimit,
  validateCursor,
  createPaginationMeta,
} from "../../utils/pagination.js";
import { NotFoundError } from "../plugins/error-handler.js";
import {
  PaginationQuerySchema,
  LocaleSchema,
  type Locale,
} from "../schemas/common.js";

import type { FastifyInstance } from "fastify";

// ============================================================================
// Schemas
// ============================================================================

const CompareTerritoriesQuerySchema = Type.Object({
  locale: Type.Optional(LocaleSchema),
  matrixCode: Type.String({ description: "Matrix code to compare" }),
  territoryIds: Type.Array(Type.Number(), {
    description: "Territory IDs to compare (max 10)",
    maxItems: 10,
    minItems: 2,
  }),
  year: Type.Optional(Type.Number({ description: "Year to compare" })),
  classificationValueId: Type.Optional(
    Type.Number({ description: "Optional classification value filter" })
  ),
});

type CompareTerritoriesQuery = Static<typeof CompareTerritoriesQuerySchema>;

const TrendsParamSchema = Type.Object({
  matrixCode: Type.String({ description: "Matrix INS code" }),
});

const TrendsQuerySchema = Type.Object({
  locale: Type.Optional(LocaleSchema),
  territoryId: Type.Optional(Type.Number({ description: "Territory ID" })),
  yearFrom: Type.Optional(Type.Number({ description: "Start year" })),
  yearTo: Type.Optional(Type.Number({ description: "End year" })),
  classificationValueId: Type.Optional(
    Type.Number({ description: "Classification value filter" })
  ),
});

type TrendsQuery = Static<typeof TrendsQuerySchema>;

const RankingsParamSchema = Type.Object({
  matrixCode: Type.String({ description: "Matrix INS code" }),
});

const RankingsQuerySchema = Type.Intersect([
  PaginationQuerySchema,
  Type.Object({
    locale: Type.Optional(LocaleSchema),
    year: Type.Number({ description: "Year for ranking" }),
    territoryLevel: Type.Optional(
      Type.Union(
        [
          Type.Literal("NATIONAL"),
          Type.Literal("NUTS1"),
          Type.Literal("NUTS2"),
          Type.Literal("NUTS3"),
          Type.Literal("LAU"),
        ],
        { description: "Territory level to rank" }
      )
    ),
    classificationValueId: Type.Optional(
      Type.Number({ description: "Classification value filter" })
    ),
    sortOrder: Type.Optional(
      Type.Union([Type.Literal("asc"), Type.Literal("desc")])
    ),
  }),
]);

type RankingsQuery = Static<typeof RankingsQuerySchema>;

const DistributionParamSchema = Type.Object({
  matrixCode: Type.String({ description: "Matrix INS code" }),
});

const DistributionQuerySchema = Type.Object({
  locale: Type.Optional(LocaleSchema),
  year: Type.Number({ description: "Year for distribution" }),
  territoryLevel: Type.Optional(
    Type.Union(
      [
        Type.Literal("NATIONAL"),
        Type.Literal("NUTS1"),
        Type.Literal("NUTS2"),
        Type.Literal("NUTS3"),
        Type.Literal("LAU"),
      ],
      { description: "Territory level for distribution" }
    )
  ),
  classificationValueId: Type.Optional(
    Type.Number({ description: "Classification value filter" })
  ),
});

type DistributionQuery = Static<typeof DistributionQuerySchema>;

const AggregateParamSchema = Type.Object({
  matrixCode: Type.String({ description: "Matrix INS code" }),
});

const AggregateQuerySchema = Type.Object({
  locale: Type.Optional(LocaleSchema),
  year: Type.Number({ description: "Year to aggregate" }),
  parentTerritoryId: Type.Number({
    description: "Parent territory ID to aggregate children",
  }),
  aggregateFunction: Type.Optional(
    Type.Union(
      [
        Type.Literal("SUM"),
        Type.Literal("AVG"),
        Type.Literal("MIN"),
        Type.Literal("MAX"),
        Type.Literal("COUNT"),
      ],
      { description: "Aggregation function" }
    )
  ),
  classificationValueId: Type.Optional(
    Type.Number({ description: "Classification value filter" })
  ),
});

type AggregateQuery = Static<typeof AggregateQuerySchema>;

const CorrelateQuerySchema = Type.Object({
  locale: Type.Optional(LocaleSchema),
  matrixCodeX: Type.String({ description: "First matrix code" }),
  matrixCodeY: Type.String({ description: "Second matrix code" }),
  year: Type.Number({ description: "Year for correlation" }),
  territoryLevel: Type.Optional(
    Type.Union(
      [Type.Literal("NUTS2"), Type.Literal("NUTS3"), Type.Literal("LAU")],
      { description: "Territory level for correlation" }
    )
  ),
});

type CorrelateQuery = Static<typeof CorrelateQuerySchema>;

const PivotParamSchema = Type.Object({
  matrixCode: Type.String({ description: "Matrix INS code" }),
});

const PivotQuerySchema = Type.Object({
  locale: Type.Optional(LocaleSchema),
  rowDimension: Type.Union(
    [
      Type.Literal("territory"),
      Type.Literal("time"),
      Type.Literal("classification"),
    ],
    { description: "Dimension for rows" }
  ),
  colDimension: Type.Union(
    [
      Type.Literal("territory"),
      Type.Literal("time"),
      Type.Literal("classification"),
    ],
    { description: "Dimension for columns" }
  ),
  territoryId: Type.Optional(Type.Number()),
  yearFrom: Type.Optional(Type.Number()),
  yearTo: Type.Optional(Type.Number()),
  classificationTypeId: Type.Optional(Type.Number()),
});

type PivotQuery = Static<typeof PivotQuerySchema>;

// ============================================================================
// Helper Functions
// ============================================================================

function getLocalizedName(
  names: { ro: string; en?: string } | null | undefined,
  locale: Locale
): string {
  if (!names) return "";
  return locale === "en" && names.en ? names.en : names.ro;
}

async function getMatrix(code: string) {
  const matrix = await db
    .selectFrom("matrices")
    .select(["id", "ins_code", "metadata"])
    .where("ins_code", "=", code)
    .executeTakeFirst();

  if (!matrix) {
    throw new NotFoundError(`Matrix with code ${code} not found`);
  }
  return matrix;
}

// ============================================================================
// Routes
// ============================================================================

export function registerAnalyticsRoutes(app: FastifyInstance): void {
  /**
   * GET /api/v1/compare/territories
   * Side-by-side comparison of territories for a matrix
   */
  app.get<{ Querystring: CompareTerritoriesQuery }>(
    "/compare/territories",
    {
      schema: {
        summary: "Compare territories",
        description:
          "Compare statistics across multiple territories for a matrix. " +
          "Returns side-by-side values with ranking. " +
          "Example: `/compare/territories?matrixCode=POP105A&territoryIds=1,2,3&year=2023`",
        tags: ["Analytics"],
        querystring: CompareTerritoriesQuerySchema,
      },
    },
    async (request) => {
      const {
        locale = "ro",
        matrixCode,
        territoryIds,
        year,
        classificationValueId,
      } = request.query;

      const matrix = await getMatrix(matrixCode);

      // Get the most recent year if not specified
      let targetYear = year;
      if (!targetYear) {
        const latestYear = await db
          .selectFrom("statistics")
          .innerJoin(
            "time_periods",
            "statistics.time_period_id",
            "time_periods.id"
          )
          .select(sql<number>`MAX(time_periods.year)`.as("max_year"))
          .where("statistics.matrix_id", "=", matrix.id)
          .where("statistics.territory_id", "in", territoryIds)
          .executeTakeFirst();
        targetYear = latestYear?.max_year ?? new Date().getFullYear();
      }

      // Build query with window function for ranking
      let query = db
        .selectFrom("statistics")
        .innerJoin("territories", "statistics.territory_id", "territories.id")
        .innerJoin(
          "time_periods",
          "statistics.time_period_id",
          "time_periods.id"
        )
        .select([
          "territories.id as territory_id",
          "territories.code as territory_code",
          "territories.names as territory_names",
          "territories.level as territory_level",
          "statistics.value",
          sql<number>`ROW_NUMBER() OVER (ORDER BY statistics.value DESC NULLS LAST)`.as(
            "rank"
          ),
        ])
        .where("statistics.matrix_id", "=", matrix.id)
        .where("statistics.territory_id", "in", territoryIds)
        .where("time_periods.year", "=", targetYear)
        .where("time_periods.periodicity", "=", "ANNUAL");

      if (classificationValueId) {
        query = query.where(
          sql<SqlBool>`EXISTS (
            SELECT 1 FROM statistic_classifications sc
            WHERE sc.matrix_id = statistics.matrix_id
            AND sc.statistic_id = statistics.id
            AND sc.classification_value_id = ${classificationValueId}
          )`
        );
      }

      const rows = await query.execute();

      const territories = rows.map((r) => ({
        id: r.territory_id,
        code: r.territory_code,
        name: getLocalizedName(r.territory_names, locale),
        level: r.territory_level,
        value: r.value,
        rank: r.rank,
      }));

      // Calculate summary statistics
      const values = territories
        .map((t) => t.value)
        .filter((v): v is number => v !== null);
      const summary =
        values.length > 0
          ? {
              min: Math.min(...values),
              max: Math.max(...values),
              avg: values.reduce((a, b) => a + b, 0) / values.length,
              range: Math.max(...values) - Math.min(...values),
            }
          : null;

      return {
        data: {
          matrixCode,
          year: targetYear,
          territories,
          summary,
        },
      };
    }
  );

  /**
   * GET /api/v1/trends/:matrixCode
   * Time series with year-over-year growth rates
   */
  app.get<{
    Params: Static<typeof TrendsParamSchema>;
    Querystring: TrendsQuery;
  }>(
    "/trends/:matrixCode",
    {
      schema: {
        summary: "Get trends",
        description:
          "Get time series data with year-over-year growth rates using LAG window function. " +
          "Example: `/trends/POP105A?territoryId=1&yearFrom=2015&yearTo=2023`",
        tags: ["Analytics"],
        params: TrendsParamSchema,
        querystring: TrendsQuerySchema,
      },
    },
    async (request) => {
      const { matrixCode } = request.params;
      const { territoryId, yearFrom, yearTo, classificationValueId } =
        request.query;

      const matrix = await getMatrix(matrixCode);

      // Build query with LAG for year-over-year calculation
      let baseQuery = db
        .selectFrom("statistics")
        .innerJoin(
          "time_periods",
          "statistics.time_period_id",
          "time_periods.id"
        )
        .select([
          "time_periods.year",
          "time_periods.periodicity",
          "statistics.value",
          sql<number>`LAG(statistics.value) OVER (ORDER BY time_periods.year)`.as(
            "prev_value"
          ),
        ])
        .where("statistics.matrix_id", "=", matrix.id)
        .where("time_periods.periodicity", "=", "ANNUAL");

      if (territoryId) {
        baseQuery = baseQuery.where(
          "statistics.territory_id",
          "=",
          territoryId
        );
      }

      if (yearFrom) {
        baseQuery = baseQuery.where("time_periods.year", ">=", yearFrom);
      }

      if (yearTo) {
        baseQuery = baseQuery.where("time_periods.year", "<=", yearTo);
      }

      if (classificationValueId) {
        baseQuery = baseQuery.where(
          sql<SqlBool>`EXISTS (
            SELECT 1 FROM statistic_classifications sc
            WHERE sc.matrix_id = statistics.matrix_id
            AND sc.statistic_id = statistics.id
            AND sc.classification_value_id = ${classificationValueId}
          )`
        );
      }

      const rows = await baseQuery
        .orderBy("time_periods.year", "asc")
        .execute();

      const dataPoints = rows.map((r) => {
        const value = r.value;
        const prevValue = r.prev_value;

        let yoyChange = null;
        let yoyChangePercent = null;

        if (value !== null && prevValue !== null && prevValue !== 0) {
          yoyChange = value - prevValue;
          yoyChangePercent = ((value - prevValue) / Math.abs(prevValue)) * 100;
        }

        return {
          year: r.year,
          periodicity: r.periodicity,
          value,
          previousValue: prevValue,
          yoyChange,
          yoyChangePercent:
            yoyChangePercent !== null
              ? Math.round(yoyChangePercent * 100) / 100
              : null,
        };
      });

      // Calculate overall trend
      const valuesWithYears = dataPoints
        .filter((d) => d.value !== null)
        .map((d) => ({ year: d.year, value: d.value! }));

      let trend = null;
      if (valuesWithYears.length >= 2) {
        const firstValue = valuesWithYears[0]!.value;
        const lastValue = valuesWithYears[valuesWithYears.length - 1]!.value;
        const firstYear = valuesWithYears[0]!.year;
        const lastYear = valuesWithYears[valuesWithYears.length - 1]!.year;

        trend = {
          startYear: firstYear,
          endYear: lastYear,
          startValue: firstValue,
          endValue: lastValue,
          totalChange: lastValue - firstValue,
          totalChangePercent:
            firstValue !== 0
              ? Math.round(
                  ((lastValue - firstValue) / Math.abs(firstValue)) * 10000
                ) / 100
              : null,
          direction:
            lastValue > firstValue
              ? "up"
              : lastValue < firstValue
                ? "down"
                : "stable",
        };
      }

      return {
        data: {
          matrixCode,
          territoryId: territoryId ?? null,
          dataPoints,
          trend,
        },
      };
    }
  );

  /**
   * GET /api/v1/rankings/:matrixCode
   * Territory rankings with percentile and quartile
   */
  app.get<{
    Params: Static<typeof RankingsParamSchema>;
    Querystring: RankingsQuery;
  }>(
    "/rankings/:matrixCode",
    {
      schema: {
        summary: "Get rankings",
        description:
          "Rank territories by value with percentile and quartile positions. " +
          "Uses ROW_NUMBER, PERCENT_RANK, and NTILE window functions. " +
          "Example: `/rankings/POP105A?year=2023&territoryLevel=NUTS3`",
        tags: ["Analytics"],
        params: RankingsParamSchema,
        querystring: RankingsQuerySchema,
      },
    },
    async (request) => {
      const { matrixCode } = request.params;
      const {
        locale = "ro",
        year,
        territoryLevel,
        classificationValueId,
        sortOrder = "desc",
        limit: rawLimit,
        cursor,
      } = request.query;

      const limit = parseLimit(rawLimit, 50, 100);
      const cursorPayload = validateCursor(cursor);

      const matrix = await getMatrix(matrixCode);

      // Build ranking query with window functions
      const orderDirection = sortOrder === "asc" ? "ASC" : "DESC";
      const nullsPosition = sortOrder === "asc" ? "LAST" : "LAST";

      let query = db
        .selectFrom("statistics")
        .innerJoin("territories", "statistics.territory_id", "territories.id")
        .innerJoin(
          "time_periods",
          "statistics.time_period_id",
          "time_periods.id"
        )
        .select([
          "territories.id as territory_id",
          "territories.code as territory_code",
          "territories.names as territory_names",
          "territories.level as territory_level",
          "statistics.value",
          sql<number>`ROW_NUMBER() OVER (ORDER BY statistics.value ${sql.raw(orderDirection)} NULLS ${sql.raw(nullsPosition)})`.as(
            "rank"
          ),
          sql<number>`PERCENT_RANK() OVER (ORDER BY statistics.value ${sql.raw(orderDirection)} NULLS ${sql.raw(nullsPosition)})`.as(
            "percentile"
          ),
          sql<number>`NTILE(4) OVER (ORDER BY statistics.value ${sql.raw(orderDirection)} NULLS ${sql.raw(nullsPosition)})`.as(
            "quartile"
          ),
        ])
        .where("statistics.matrix_id", "=", matrix.id)
        .where("time_periods.year", "=", year)
        .where("time_periods.periodicity", "=", "ANNUAL");

      if (territoryLevel) {
        query = query.where("territories.level", "=", territoryLevel);
      }

      if (classificationValueId) {
        query = query.where(
          sql<SqlBool>`EXISTS (
            SELECT 1 FROM statistic_classifications sc
            WHERE sc.matrix_id = statistics.matrix_id
            AND sc.statistic_id = statistics.id
            AND sc.classification_value_id = ${classificationValueId}
          )`
        );
      }

      // Apply cursor pagination on rank
      if (cursorPayload) {
        query = query.where("territories.id", ">", cursorPayload.id);
      }

      const rows = await query
        .orderBy("territories.id", "asc")
        .limit(limit + 1)
        .execute();

      const hasMore = rows.length > limit;
      const items = rows.slice(0, limit).map((r) => ({
        id: r.territory_id,
        rank: r.rank,
        territory: {
          id: r.territory_id,
          code: r.territory_code,
          name: getLocalizedName(r.territory_names, locale),
          level: r.territory_level,
        },
        value: r.value,
        percentile: Math.round(r.percentile * 10000) / 100,
        quartile: r.quartile,
      }));

      // Sort by rank for output
      items.sort((a, b) => a.rank - b.rank);

      return {
        data: items,
        meta: {
          matrixCode,
          year,
          territoryLevel: territoryLevel ?? "all",
          sortOrder,
          pagination: createPaginationMeta(items, limit, "id", hasMore),
        },
      };
    }
  );

  /**
   * GET /api/v1/distribution/:matrixCode
   * Statistical distribution (percentiles, std dev, etc.)
   */
  app.get<{
    Params: Static<typeof DistributionParamSchema>;
    Querystring: DistributionQuery;
  }>(
    "/distribution/:matrixCode",
    {
      schema: {
        summary: "Get distribution",
        description:
          "Get statistical distribution of values including percentiles and standard deviation. " +
          "Example: `/distribution/POP105A?year=2023&territoryLevel=NUTS3`",
        tags: ["Analytics"],
        params: DistributionParamSchema,
        querystring: DistributionQuerySchema,
      },
    },
    async (request) => {
      const { matrixCode } = request.params;
      const { year, territoryLevel, classificationValueId } = request.query;

      const matrix = await getMatrix(matrixCode);

      // Build query for distribution statistics
      let query = db
        .selectFrom("statistics")
        .innerJoin("territories", "statistics.territory_id", "territories.id")
        .innerJoin(
          "time_periods",
          "statistics.time_period_id",
          "time_periods.id"
        )
        .select([
          sql<number>`COUNT(*)`.as("count"),
          sql<number>`COUNT(statistics.value)`.as("non_null_count"),
          sql<number>`AVG(statistics.value)`.as("mean"),
          sql<number>`STDDEV(statistics.value)`.as("stddev"),
          sql<number>`MIN(statistics.value)`.as("min"),
          sql<number>`MAX(statistics.value)`.as("max"),
          sql<number>`PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY statistics.value)`.as(
            "p25"
          ),
          sql<number>`PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY statistics.value)`.as(
            "median"
          ),
          sql<number>`PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY statistics.value)`.as(
            "p75"
          ),
          sql<number>`PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY statistics.value)`.as(
            "p10"
          ),
          sql<number>`PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY statistics.value)`.as(
            "p90"
          ),
        ])
        .where("statistics.matrix_id", "=", matrix.id)
        .where("time_periods.year", "=", year)
        .where("time_periods.periodicity", "=", "ANNUAL");

      if (territoryLevel) {
        query = query.where("territories.level", "=", territoryLevel);
      }

      if (classificationValueId) {
        query = query.where(
          sql<SqlBool>`EXISTS (
            SELECT 1 FROM statistic_classifications sc
            WHERE sc.matrix_id = statistics.matrix_id
            AND sc.statistic_id = statistics.id
            AND sc.classification_value_id = ${classificationValueId}
          )`
        );
      }

      const result = await query.executeTakeFirst();

      if (!result || result.count === 0) {
        return {
          data: null,
          meta: { matrixCode, year, message: "No data found for criteria" },
        };
      }

      const distribution = {
        count: result.count,
        nonNullCount: result.non_null_count,
        nullCount: result.count - result.non_null_count,
        mean: result.mean !== null ? Math.round(result.mean * 100) / 100 : null,
        stddev:
          result.stddev !== null ? Math.round(result.stddev * 100) / 100 : null,
        min: result.min,
        max: result.max,
        range:
          result.min !== null && result.max !== null
            ? result.max - result.min
            : null,
        percentiles: {
          p10: result.p10 !== null ? Math.round(result.p10 * 100) / 100 : null,
          p25: result.p25 !== null ? Math.round(result.p25 * 100) / 100 : null,
          p50:
            result.median !== null
              ? Math.round(result.median * 100) / 100
              : null,
          p75: result.p75 !== null ? Math.round(result.p75 * 100) / 100 : null,
          p90: result.p90 !== null ? Math.round(result.p90 * 100) / 100 : null,
        },
        iqr:
          result.p25 !== null && result.p75 !== null
            ? Math.round((result.p75 - result.p25) * 100) / 100
            : null,
      };

      return {
        data: {
          matrixCode,
          year,
          territoryLevel: territoryLevel ?? "all",
          distribution,
        },
      };
    }
  );

  /**
   * GET /api/v1/aggregate/:matrixCode
   * Hierarchical aggregation using ltree
   */
  app.get<{
    Params: Static<typeof AggregateParamSchema>;
    Querystring: AggregateQuery;
  }>(
    "/aggregate/:matrixCode",
    {
      schema: {
        summary: "Aggregate data",
        description:
          "Aggregate data from child territories to parent using ltree path matching. " +
          "Example: `/aggregate/POP105A?year=2023&parentTerritoryId=1&aggregateFunction=SUM`",
        tags: ["Analytics"],
        params: AggregateParamSchema,
        querystring: AggregateQuerySchema,
      },
    },
    async (request) => {
      const { matrixCode } = request.params;
      const {
        locale = "ro",
        year,
        parentTerritoryId,
        aggregateFunction = "SUM",
        classificationValueId,
      } = request.query;

      const matrix = await getMatrix(matrixCode);

      // Get parent territory path
      const parent = await db
        .selectFrom("territories")
        .select(["id", "code", "path", "names", "level"])
        .where("id", "=", parentTerritoryId)
        .executeTakeFirst();

      if (!parent) {
        throw new NotFoundError(
          `Territory with ID ${String(parentTerritoryId)} not found`
        );
      }

      // Build aggregation query
      const aggFunc = aggregateFunction.toUpperCase();
      const aggSql =
        aggFunc === "COUNT"
          ? sql<number>`COUNT(*)`
          : aggFunc === "AVG"
            ? sql<number>`AVG(statistics.value)`
            : aggFunc === "MIN"
              ? sql<number>`MIN(statistics.value)`
              : aggFunc === "MAX"
                ? sql<number>`MAX(statistics.value)`
                : sql<number>`SUM(statistics.value)`;

      let query = db
        .selectFrom("statistics")
        .innerJoin("territories", "statistics.territory_id", "territories.id")
        .innerJoin(
          "time_periods",
          "statistics.time_period_id",
          "time_periods.id"
        )
        .select([
          "territories.level as territory_level",
          aggSql.as("aggregated_value"),
          sql<number>`COUNT(*)`.as("child_count"),
        ])
        .where("statistics.matrix_id", "=", matrix.id)
        .where("time_periods.year", "=", year)
        .where("time_periods.periodicity", "=", "ANNUAL")
        .where(sql<SqlBool>`territories.path <@ ${parent.path}::ltree`)
        .where("territories.id", "!=", parentTerritoryId) // Exclude parent itself
        .groupBy("territories.level");

      if (classificationValueId) {
        query = query.where(
          sql<SqlBool>`EXISTS (
            SELECT 1 FROM statistic_classifications sc
            WHERE sc.matrix_id = statistics.matrix_id
            AND sc.statistic_id = statistics.id
            AND sc.classification_value_id = ${classificationValueId}
          )`
        );
      }

      const rows = await query.execute();

      // Also get direct children breakdown
      const childrenQuery = await db
        .selectFrom("statistics")
        .innerJoin("territories", "statistics.territory_id", "territories.id")
        .innerJoin(
          "time_periods",
          "statistics.time_period_id",
          "time_periods.id"
        )
        .select([
          "territories.id as territory_id",
          "territories.code as territory_code",
          "territories.names as territory_names",
          "territories.level as territory_level",
          "statistics.value",
        ])
        .where("statistics.matrix_id", "=", matrix.id)
        .where("time_periods.year", "=", year)
        .where("time_periods.periodicity", "=", "ANNUAL")
        .where("territories.parent_id", "=", parentTerritoryId)
        .execute();

      const children = childrenQuery.map((c) => ({
        id: c.territory_id,
        code: c.territory_code,
        name: getLocalizedName(c.territory_names, locale),
        level: c.territory_level,
        value: c.value,
      }));

      const byLevel = rows.reduce<
        Record<string, { aggregatedValue: number | null; childCount: number }>
      >((acc, r) => {
        acc[r.territory_level] = {
          aggregatedValue:
            r.aggregated_value !== null
              ? Math.round(r.aggregated_value * 100) / 100
              : null,
          childCount: r.child_count,
        };
        return acc;
      }, {});

      return {
        data: {
          matrixCode,
          year,
          aggregateFunction,
          parent: {
            id: parent.id,
            code: parent.code,
            name: getLocalizedName(parent.names, locale),
            level: parent.level,
          },
          aggregateByLevel: byLevel,
          directChildren: children,
        },
      };
    }
  );

  /**
   * GET /api/v1/correlate
   * Cross-matrix correlation using PostgreSQL CORR function
   */
  app.get<{ Querystring: CorrelateQuery }>(
    "/correlate",
    {
      schema: {
        summary: "Calculate correlation",
        description:
          "Calculate Pearson correlation between two matrices across territories. " +
          "Example: `/correlate?matrixCodeX=POP105A&matrixCodeY=ECO123B&year=2023&territoryLevel=NUTS3`",
        tags: ["Analytics"],
        querystring: CorrelateQuerySchema,
      },
    },
    async (request) => {
      const {
        locale = "ro",
        matrixCodeX,
        matrixCodeY,
        year,
        territoryLevel = "NUTS3",
      } = request.query;

      const matrixX = await getMatrix(matrixCodeX);
      const matrixY = await getMatrix(matrixCodeY);

      // Build correlation query joining both matrices on territory and year
      const result = await db
        .selectFrom("statistics as sx")
        .innerJoin("statistics as sy", (join) =>
          join
            .onRef("sx.territory_id", "=", "sy.territory_id")
            .onRef("sx.time_period_id", "=", "sy.time_period_id")
        )
        .innerJoin("territories", "sx.territory_id", "territories.id")
        .innerJoin("time_periods", "sx.time_period_id", "time_periods.id")
        .select([
          sql<number>`CORR(sx.value, sy.value)`.as("correlation"),
          sql<number>`REGR_SLOPE(sy.value, sx.value)`.as("slope"),
          sql<number>`REGR_INTERCEPT(sy.value, sx.value)`.as("intercept"),
          sql<number>`REGR_R2(sy.value, sx.value)`.as("r_squared"),
          sql<number>`COUNT(*)`.as("sample_size"),
        ])
        .where("sx.matrix_id", "=", matrixX.id)
        .where("sy.matrix_id", "=", matrixY.id)
        .where("time_periods.year", "=", year)
        .where("time_periods.periodicity", "=", "ANNUAL")
        .where("territories.level", "=", territoryLevel)
        .where("sx.value", "is not", null)
        .where("sy.value", "is not", null)
        .executeTakeFirst();

      if (!result || result.sample_size === 0) {
        return {
          data: null,
          meta: {
            matrixCodeX,
            matrixCodeY,
            year,
            territoryLevel,
            message: "Insufficient data for correlation",
          },
        };
      }

      const correlation = {
        pearsonR:
          result.correlation !== null
            ? Math.round(result.correlation * 10000) / 10000
            : null,
        rSquared:
          result.r_squared !== null
            ? Math.round(result.r_squared * 10000) / 10000
            : null,
        regression: {
          slope:
            result.slope !== null
              ? Math.round(result.slope * 10000) / 10000
              : null,
          intercept:
            result.intercept !== null
              ? Math.round(result.intercept * 100) / 100
              : null,
        },
        sampleSize: result.sample_size,
        interpretation:
          result.correlation !== null
            ? Math.abs(result.correlation) >= 0.7
              ? "strong"
              : Math.abs(result.correlation) >= 0.4
                ? "moderate"
                : Math.abs(result.correlation) >= 0.2
                  ? "weak"
                  : "negligible"
            : null,
      };

      return {
        data: {
          matrixX: {
            code: matrixCodeX,
            name: getLocalizedName(matrixX.metadata.names, locale),
          },
          matrixY: {
            code: matrixCodeY,
            name: getLocalizedName(matrixY.metadata.names, locale),
          },
          year,
          territoryLevel,
          correlation,
        },
      };
    }
  );

  /**
   * GET /api/v1/pivot/:matrixCode
   * Pivot table format
   */
  app.get<{
    Params: Static<typeof PivotParamSchema>;
    Querystring: PivotQuery;
  }>(
    "/pivot/:matrixCode",
    {
      schema: {
        summary: "Get pivot table",
        description:
          "Get data in pivot table format with customizable row and column dimensions. " +
          "Example: `/pivot/POP105A?rowDimension=territory&colDimension=time&yearFrom=2020&yearTo=2023`",
        tags: ["Analytics"],
        params: PivotParamSchema,
        querystring: PivotQuerySchema,
      },
    },
    async (request) => {
      const { matrixCode } = request.params;
      const {
        locale = "ro",
        rowDimension,
        colDimension,
        territoryId,
        yearFrom,
        yearTo,
      } = request.query;

      if (rowDimension === colDimension) {
        throw new NotFoundError("Row and column dimensions must be different");
      }

      const matrix = await getMatrix(matrixCode);

      // Build query based on selected dimensions
      let query = db
        .selectFrom("statistics")
        .innerJoin("territories", "statistics.territory_id", "territories.id")
        .innerJoin(
          "time_periods",
          "statistics.time_period_id",
          "time_periods.id"
        )
        .select([
          "territories.id as territory_id",
          "territories.code as territory_code",
          "territories.names as territory_names",
          "time_periods.year",
          "statistics.value",
        ])
        .where("statistics.matrix_id", "=", matrix.id)
        .where("time_periods.periodicity", "=", "ANNUAL");

      if (territoryId) {
        // Get children of this territory
        const parent = await db
          .selectFrom("territories")
          .select("path")
          .where("id", "=", territoryId)
          .executeTakeFirst();

        if (parent) {
          query = query.where(
            sql<SqlBool>`territories.path <@ ${parent.path}::ltree`
          );
        }
      }

      if (yearFrom) {
        query = query.where("time_periods.year", ">=", yearFrom);
      }

      if (yearTo) {
        query = query.where("time_periods.year", "<=", yearTo);
      }

      const rows = await query
        .orderBy("territories.code", "asc")
        .orderBy("time_periods.year", "asc")
        .execute();

      // Build pivot table based on row/col dimensions
      interface PivotCell {
        rowKey: string;
        rowLabel: string;
        colKey: string;
        colLabel: string;
        value: number | null;
      }

      const cells: PivotCell[] = rows.map((r) => {
        let rowKey: string, rowLabel: string, colKey: string, colLabel: string;

        if (rowDimension === "territory") {
          rowKey = r.territory_code;
          rowLabel = getLocalizedName(r.territory_names, locale);
        } else {
          rowKey = String(r.year);
          rowLabel = String(r.year);
        }

        if (colDimension === "time") {
          colKey = String(r.year);
          colLabel = String(r.year);
        } else {
          colKey = r.territory_code;
          colLabel = getLocalizedName(r.territory_names, locale);
        }

        return {
          rowKey,
          rowLabel,
          colKey,
          colLabel,
          value: r.value,
        };
      });

      // Extract unique rows and columns
      const rowsMap = new Map<string, string>();
      const colsMap = new Map<string, string>();

      for (const cell of cells) {
        rowsMap.set(cell.rowKey, cell.rowLabel);
        colsMap.set(cell.colKey, cell.colLabel);
      }

      const pivotRows = Array.from(rowsMap.entries()).map(([key, label]) => ({
        key,
        label,
      }));
      const pivotCols = Array.from(colsMap.entries()).map(([key, label]) => ({
        key,
        label,
      }));

      // Build pivot data matrix
      const pivotData: Record<string, Record<string, number | null>> = {};
      for (const cell of cells) {
        pivotData[cell.rowKey] ??= {};
        pivotData[cell.rowKey]![cell.colKey] = cell.value;
      }

      return {
        data: {
          matrixCode,
          rowDimension,
          colDimension,
          rows: pivotRows,
          columns: pivotCols,
          values: pivotData,
        },
      };
    }
  );
}
