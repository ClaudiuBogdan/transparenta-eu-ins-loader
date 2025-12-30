/**
 * Saved Queries Routes - /api/v1/queries
 * Save and execute reusable queries
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

const ListQueriesQuerySchema = Type.Intersect([
  PaginationQuerySchema,
  Type.Object({
    locale: Type.Optional(LocaleSchema),
    matrixCode: Type.Optional(
      Type.String({ description: "Filter by matrix code" })
    ),
    publicOnly: Type.Optional(
      Type.Boolean({ description: "Only public queries", default: false })
    ),
  }),
]);

type ListQueriesQuery = Static<typeof ListQueriesQuerySchema>;

const CreateQueryBodySchema = Type.Object({
  name: Type.String({ minLength: 1, maxLength: 200 }),
  description: Type.Optional(Type.String()),
  matrixCode: Type.String({ minLength: 1, maxLength: 20 }),
  territoryFilter: Type.Optional(
    Type.Object({
      territoryIds: Type.Optional(Type.Array(Type.Number())),
      level: Type.Optional(Type.String()),
      parentId: Type.Optional(Type.Number()),
    })
  ),
  timeFilter: Type.Optional(
    Type.Object({
      yearFrom: Type.Optional(Type.Number()),
      yearTo: Type.Optional(Type.Number()),
      periodicity: Type.Optional(Type.String()),
    })
  ),
  classificationFilter: Type.Optional(
    Type.Object({
      classificationValueIds: Type.Optional(Type.Array(Type.Number())),
      classificationTypeId: Type.Optional(Type.Number()),
    })
  ),
  options: Type.Optional(
    Type.Object({
      sortBy: Type.Optional(Type.String()),
      sortOrder: Type.Optional(Type.String()),
      limit: Type.Optional(Type.Number()),
      aggregateFunction: Type.Optional(Type.String()),
    })
  ),
  isPublic: Type.Optional(Type.Boolean({ default: false })),
});

type CreateQueryBody = Static<typeof CreateQueryBodySchema>;

const QueryIdParamSchema = Type.Object({
  id: Type.Number({ description: "Query ID" }),
});

const ExecuteQueryQuerySchema = Type.Object({
  locale: Type.Optional(LocaleSchema),
  limit: Type.Optional(Type.Number({ default: 100 })),
});

type ExecuteQueryQuery = Static<typeof ExecuteQueryQuerySchema>;

// ============================================================================
// Helper Functions
// ============================================================================

function getLocalizedName(
  names: { ro: string; en?: string },
  locale: Locale
): string {
  return locale === "en" && names.en ? names.en : names.ro;
}

// ============================================================================
// Routes
// ============================================================================

export function registerQueryRoutes(app: FastifyInstance): void {
  /**
   * GET /api/v1/queries
   * List saved queries
   */
  app.get<{ Querystring: ListQueriesQuery }>(
    "/queries",
    {
      schema: {
        summary: "List saved queries",
        description: "Get a list of saved queries with pagination.",
        tags: ["Saved Queries"],
        querystring: ListQueriesQuerySchema,
      },
    },
    async (request) => {
      const {
        matrixCode,
        publicOnly = false,
        limit: rawLimit,
        cursor,
      } = request.query;

      const limit = parseLimit(rawLimit, 50, 100);
      const cursorPayload = validateCursor(cursor);

      let query = db
        .selectFrom("saved_queries")
        .select([
          "id",
          "name",
          "description",
          "matrix_code",
          "territory_filter",
          "time_filter",
          "classification_filter",
          "options",
          "is_public",
          "execution_count",
          "created_at",
          "updated_at",
        ]);

      if (publicOnly) {
        query = query.where("is_public", "=", true);
      }

      if (matrixCode) {
        query = query.where("matrix_code", "=", matrixCode);
      }

      if (cursorPayload) {
        query = query.where("id", ">", cursorPayload.id);
      }

      const rows = await query
        .orderBy("id", "asc")
        .limit(limit + 1)
        .execute();

      const hasMore = rows.length > limit;
      const items = rows.slice(0, limit).map((q) => ({
        id: q.id,
        name: q.name,
        description: q.description,
        matrixCode: q.matrix_code,
        isPublic: q.is_public,
        executionCount: q.execution_count,
        createdAt: q.created_at.toISOString(),
        updatedAt: q.updated_at.toISOString(),
      }));

      return {
        data: items,
        meta: {
          pagination: createPaginationMeta(items, limit, "id", hasMore),
        },
      };
    }
  );

  /**
   * POST /api/v1/queries
   * Create a new saved query
   */
  app.post<{ Body: CreateQueryBody }>(
    "/queries",
    {
      schema: {
        summary: "Create saved query",
        description:
          "Save a query configuration for later reuse. " +
          "Example: Create a query for population by NUTS3 regions for 2020-2023.",
        tags: ["Saved Queries"],
        body: CreateQueryBodySchema,
      },
    },
    async (request) => {
      const {
        name,
        description,
        matrixCode,
        territoryFilter,
        timeFilter,
        classificationFilter,
        options,
        isPublic = false,
      } = request.body;

      // Verify matrix exists
      const matrix = await db
        .selectFrom("matrices")
        .select("id")
        .where("ins_code", "=", matrixCode)
        .executeTakeFirst();

      if (!matrix) {
        throw new NotFoundError(`Matrix with code ${matrixCode} not found`);
      }

      const result = await db
        .insertInto("saved_queries")
        .values({
          name,
          description: description ?? null,
          matrix_code: matrixCode,
          territory_filter: territoryFilter ?? null,
          time_filter: timeFilter ?? null,
          classification_filter: classificationFilter ?? null,
          options: options ?? null,
          is_public: isPublic,
        })
        .returning([
          "id",
          "name",
          "description",
          "matrix_code",
          "is_public",
          "created_at",
        ])
        .executeTakeFirst();

      return {
        data: {
          id: result!.id,
          name: result!.name,
          description: result!.description,
          matrixCode: result!.matrix_code,
          isPublic: result!.is_public,
          createdAt: result!.created_at.toISOString(),
        },
        message: "Query saved successfully",
      };
    }
  );

  /**
   * GET /api/v1/queries/:id
   * Get saved query details
   */
  app.get<{
    Params: Static<typeof QueryIdParamSchema>;
    Querystring: { locale?: Locale };
  }>(
    "/queries/:id",
    {
      schema: {
        summary: "Get saved query",
        description: "Get details of a saved query including all filters.",
        tags: ["Saved Queries"],
        params: QueryIdParamSchema,
        querystring: Type.Object({
          locale: Type.Optional(LocaleSchema),
        }),
      },
    },
    async (request) => {
      const { id } = request.params;
      const locale = request.query.locale ?? "ro";

      const query = await db
        .selectFrom("saved_queries")
        .select([
          "id",
          "name",
          "description",
          "matrix_code",
          "territory_filter",
          "time_filter",
          "classification_filter",
          "options",
          "is_public",
          "execution_count",
          "created_at",
          "updated_at",
        ])
        .where("id", "=", id)
        .executeTakeFirst();

      if (!query) {
        throw new NotFoundError(`Query with ID ${String(id)} not found`);
      }

      // Get matrix info
      const matrix = await db
        .selectFrom("matrices")
        .select(["ins_code", "metadata"])
        .where("ins_code", "=", query.matrix_code)
        .executeTakeFirst();

      return {
        data: {
          id: query.id,
          name: query.name,
          description: query.description,
          matrix: matrix
            ? {
                code: matrix.ins_code,
                name: getLocalizedName(matrix.metadata.names, locale),
              }
            : null,
          filters: {
            territory: query.territory_filter,
            time: query.time_filter,
            classification: query.classification_filter,
          },
          options: query.options,
          isPublic: query.is_public,
          executionCount: query.execution_count,
          createdAt: query.created_at.toISOString(),
          updatedAt: query.updated_at.toISOString(),
        },
      };
    }
  );

  /**
   * GET /api/v1/queries/:id/execute
   * Execute a saved query
   */
  app.get<{
    Params: Static<typeof QueryIdParamSchema>;
    Querystring: ExecuteQueryQuery;
  }>(
    "/queries/:id/execute",
    {
      schema: {
        summary: "Execute saved query",
        description:
          "Execute a saved query and return results. Increments execution count.",
        tags: ["Saved Queries"],
        params: QueryIdParamSchema,
        querystring: ExecuteQueryQuerySchema,
      },
    },
    async (request) => {
      const { id } = request.params;
      const { locale = "ro", limit = 100 } = request.query;

      // Get query
      const savedQuery = await db
        .selectFrom("saved_queries")
        .select([
          "id",
          "name",
          "matrix_code",
          "territory_filter",
          "time_filter",
          "classification_filter",
          "options",
        ])
        .where("id", "=", id)
        .executeTakeFirst();

      if (!savedQuery) {
        throw new NotFoundError(`Query with ID ${String(id)} not found`);
      }

      // Get matrix
      const matrix = await db
        .selectFrom("matrices")
        .select(["id", "ins_code", "metadata"])
        .where("ins_code", "=", savedQuery.matrix_code)
        .executeTakeFirst();

      if (!matrix) {
        throw new NotFoundError(`Matrix ${savedQuery.matrix_code} not found`);
      }

      // Build and execute query
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
          "statistics.unit_id",
          "units_of_measure.id"
        )
        .select([
          "statistics.id",
          "statistics.value",
          "statistics.value_status",
          "territories.id as territory_id",
          "territories.code as territory_code",
          "territories.names as territory_names",
          "territories.level as territory_level",
          "time_periods.year",
          "time_periods.periodicity",
          "time_periods.labels as time_labels",
          "units_of_measure.code as unit_code",
          "units_of_measure.names as unit_names",
        ])
        .where("statistics.matrix_id", "=", matrix.id);

      // Apply territory filter
      const territoryFilter = savedQuery.territory_filter as {
        territoryIds?: number[];
        level?: string;
        parentId?: number;
      } | null;

      if (territoryFilter) {
        if (territoryFilter.territoryIds?.length) {
          query = query.where(
            "statistics.territory_id",
            "in",
            territoryFilter.territoryIds
          );
        }
        if (territoryFilter.level) {
          query = query.where(
            "territories.level",
            "=",
            territoryFilter.level as
              | "NATIONAL"
              | "NUTS1"
              | "NUTS2"
              | "NUTS3"
              | "LAU"
          );
        }
        if (territoryFilter.parentId) {
          const parent = await db
            .selectFrom("territories")
            .select("path")
            .where("id", "=", territoryFilter.parentId)
            .executeTakeFirst();

          if (parent) {
            query = query.where(
              sql<SqlBool>`territories.path <@ ${parent.path}::ltree`
            );
          }
        }
      }

      // Apply time filter
      const timeFilter = savedQuery.time_filter as {
        yearFrom?: number;
        yearTo?: number;
        periodicity?: string;
      } | null;

      if (timeFilter) {
        if (timeFilter.yearFrom) {
          query = query.where("time_periods.year", ">=", timeFilter.yearFrom);
        }
        if (timeFilter.yearTo) {
          query = query.where("time_periods.year", "<=", timeFilter.yearTo);
        }
        if (timeFilter.periodicity) {
          query = query.where(
            "time_periods.periodicity",
            "=",
            timeFilter.periodicity as "ANNUAL" | "QUARTERLY" | "MONTHLY"
          );
        }
      }

      // Apply classification filter
      const classificationFilter = savedQuery.classification_filter as {
        classificationValueIds?: number[];
      } | null;

      if (classificationFilter?.classificationValueIds?.length) {
        query = query.where(
          sql<SqlBool>`EXISTS (
            SELECT 1 FROM statistic_classifications sc
            WHERE sc.matrix_id = statistics.matrix_id
            AND sc.statistic_id = statistics.id
            AND sc.classification_value_id = ANY(${classificationFilter.classificationValueIds})
          )`
        );
      }

      // Apply options
      const options = savedQuery.options as {
        sortBy?: string;
        sortOrder?: string;
      } | null;

      const sortOrder = options?.sortOrder === "desc" ? "desc" : "asc";
      query = query
        .orderBy("time_periods.year", sortOrder)
        .orderBy("territories.code", "asc")
        .limit(limit);

      const rows = await query.execute();

      // Update execution count
      await db
        .updateTable("saved_queries")
        .set({
          execution_count: sql`execution_count + 1`,
          updated_at: new Date(),
        })
        .where("id", "=", id)
        .execute();

      // Format results
      const results = rows.map((r) => ({
        id: r.id,
        value: r.value,
        valueStatus: r.value_status,
        territory: r.territory_id
          ? {
              id: r.territory_id,
              code: r.territory_code,
              name: r.territory_names
                ? getLocalizedName(r.territory_names, locale)
                : null,
              level: r.territory_level,
            }
          : null,
        timePeriod: {
          year: r.year,
          periodicity: r.periodicity,
          label: r.time_labels
            ? getLocalizedName(r.time_labels, locale)
            : String(r.year),
        },
        unit: r.unit_code
          ? {
              code: r.unit_code,
              name: r.unit_names
                ? getLocalizedName(r.unit_names, locale)
                : r.unit_code,
            }
          : null,
      }));

      return {
        data: {
          query: {
            id: savedQuery.id,
            name: savedQuery.name,
            matrixCode: savedQuery.matrix_code,
          },
          results,
          resultCount: results.length,
        },
      };
    }
  );

  /**
   * DELETE /api/v1/queries/:id
   * Delete a saved query
   */
  app.delete<{ Params: Static<typeof QueryIdParamSchema> }>(
    "/queries/:id",
    {
      schema: {
        summary: "Delete saved query",
        description: "Delete a saved query by ID.",
        tags: ["Saved Queries"],
        params: QueryIdParamSchema,
      },
    },
    async (request) => {
      const { id } = request.params;

      const result = await db
        .deleteFrom("saved_queries")
        .where("id", "=", id)
        .returning("id")
        .executeTakeFirst();

      if (!result) {
        throw new NotFoundError(`Query with ID ${String(id)} not found`);
      }

      return {
        message: "Query deleted successfully",
        data: { id: result.id },
      };
    }
  );
}
