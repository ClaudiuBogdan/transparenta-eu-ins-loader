/**
 * Discovery Routes - /api/v1/discover
 * Endpoints for discovering matrices by topic, territory, time range, and breakdowns
 */

import { Type, type Static } from "@sinclair/typebox";
import { sql, type SqlBool } from "kysely";

import { db } from "../../db/connection.js";
import {
  parseLimit,
  validateCursor,
  createPaginationMeta,
} from "../../utils/pagination.js";
import {
  PaginationQuerySchema,
  LocaleSchema,
  type Locale,
} from "../schemas/common.js";

import type { FastifyInstance } from "fastify";

// ============================================================================
// Schemas
// ============================================================================

const TopicsQuerySchema = Type.Object({
  locale: Type.Optional(LocaleSchema),
  category: Type.Optional(
    Type.String({
      description: "Filter tags by category (topic, audience, use-case)",
    })
  ),
});

type TopicsQuery = Static<typeof TopicsQuerySchema>;

const ByTerritoryQuerySchema = Type.Intersect([
  PaginationQuerySchema,
  Type.Object({
    locale: Type.Optional(LocaleSchema),
    territoryId: Type.Number({
      description: "Territory ID to find matrices for",
    }),
    level: Type.Optional(
      Type.Union(
        [
          Type.Literal("NATIONAL"),
          Type.Literal("NUTS1"),
          Type.Literal("NUTS2"),
          Type.Literal("NUTS3"),
          Type.Literal("LAU"),
        ],
        { description: "Minimum territory level" }
      )
    ),
    includeChildren: Type.Optional(
      Type.Boolean({ description: "Include child territories in search" })
    ),
  }),
]);

type ByTerritoryQuery = Static<typeof ByTerritoryQuerySchema>;

const ByTimeRangeQuerySchema = Type.Intersect([
  PaginationQuerySchema,
  Type.Object({
    locale: Type.Optional(LocaleSchema),
    yearFrom: Type.Optional(Type.Number({ description: "Start year" })),
    yearTo: Type.Optional(Type.Number({ description: "End year" })),
    periodicity: Type.Optional(
      Type.Union(
        [
          Type.Literal("ANNUAL"),
          Type.Literal("QUARTERLY"),
          Type.Literal("MONTHLY"),
        ],
        { description: "Filter by periodicity" }
      )
    ),
  }),
]);

type ByTimeRangeQuery = Static<typeof ByTimeRangeQuerySchema>;

const ByBreakdownsQuerySchema = Type.Intersect([
  PaginationQuerySchema,
  Type.Object({
    locale: Type.Optional(LocaleSchema),
    hasTerritory: Type.Optional(
      Type.Boolean({ description: "Matrices with territorial dimension" })
    ),
    hasTemporal: Type.Optional(
      Type.Boolean({ description: "Matrices with temporal dimension" })
    ),
    classificationType: Type.Optional(
      Type.String({
        description: "Classification type code (e.g., SEX, AGE_GROUP)",
      })
    ),
    minDimensions: Type.Optional(
      Type.Number({ description: "Minimum number of dimensions" })
    ),
    maxDimensions: Type.Optional(
      Type.Number({ description: "Maximum number of dimensions" })
    ),
  }),
]);

type ByBreakdownsQuery = Static<typeof ByBreakdownsQuerySchema>;

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

export function registerDiscoverRoutes(app: FastifyInstance): void {
  /**
   * GET /api/v1/discover/topics
   * Browse matrices by subject area (tags) with matrix counts
   */
  app.get<{ Querystring: TopicsQuery }>(
    "/discover/topics",
    {
      schema: {
        summary: "Browse by topic",
        description:
          "Get all available topics (tags) with matrix counts. " +
          "Use this to build a topic-based navigation. " +
          "Examples:\n" +
          "- `/discover/topics` - All tags\n" +
          "- `/discover/topics?category=topic` - Topic tags only",
        tags: ["Discovery"],
        querystring: TopicsQuerySchema,
      },
    },
    async (request) => {
      const { locale = "ro", category } = request.query;

      let query = db
        .selectFrom("matrix_tags")
        .leftJoin(
          "matrix_tag_assignments",
          "matrix_tags.id",
          "matrix_tag_assignments.tag_id"
        )
        .select([
          "matrix_tags.id",
          "matrix_tags.name",
          "matrix_tags.name_en",
          "matrix_tags.slug",
          "matrix_tags.category",
          "matrix_tags.description",
          sql<number>`COUNT(DISTINCT matrix_tag_assignments.matrix_id)`.as(
            "matrix_count"
          ),
        ])
        .groupBy([
          "matrix_tags.id",
          "matrix_tags.name",
          "matrix_tags.name_en",
          "matrix_tags.slug",
          "matrix_tags.category",
          "matrix_tags.description",
        ]);

      if (category) {
        query = query.where("matrix_tags.category", "=", category);
      }

      const rows = await query
        .orderBy("matrix_tags.category", "asc")
        .orderBy("matrix_tags.name", "asc")
        .execute();

      // Group by category
      const byCategory: Record<string, typeof rows> = {};
      for (const row of rows) {
        byCategory[row.category] ??= [];
        byCategory[row.category]!.push(row);
      }

      const categories = Object.entries(byCategory).map(([cat, tags]) => ({
        category: cat,
        tags: tags.map((t) => ({
          id: t.id,
          name: locale === "en" && t.name_en ? t.name_en : t.name,
          slug: t.slug,
          description: t.description,
          matrixCount: t.matrix_count,
        })),
      }));

      return {
        data: {
          categories,
          totalTags: rows.length,
        },
      };
    }
  );

  /**
   * GET /api/v1/discover/by-territory
   * Find matrices that have data for a specific territory
   */
  app.get<{ Querystring: ByTerritoryQuery }>(
    "/discover/by-territory",
    {
      schema: {
        summary: "Find matrices by territory",
        description:
          "Find matrices that have statistical data for a specific territory. " +
          "Use includeChildren=true to also find matrices with data for child territories. " +
          "Example: `/discover/by-territory?territoryId=1&includeChildren=true`",
        tags: ["Discovery"],
        querystring: ByTerritoryQuerySchema,
      },
    },
    async (request) => {
      const {
        locale = "ro",
        territoryId,
        level,
        includeChildren = false,
        limit: rawLimit,
        cursor,
      } = request.query;

      const limit = parseLimit(rawLimit, 50, 100);
      const cursorPayload = validateCursor(cursor);

      // Get territory path for hierarchical query
      const territory = await db
        .selectFrom("territories")
        .select(["id", "path", "level"])
        .where("id", "=", territoryId)
        .executeTakeFirst();

      if (!territory) {
        return { data: [], meta: { pagination: { limit, hasMore: false } } };
      }

      // Find matrices that have statistics for this territory (or children)
      let query = db
        .selectFrom("statistics")
        .innerJoin("matrices", "statistics.matrix_id", "matrices.id")
        .innerJoin("territories", "statistics.territory_id", "territories.id")
        .leftJoin("contexts", "matrices.context_id", "contexts.id")
        .select([
          "matrices.id",
          "matrices.ins_code",
          "matrices.metadata",
          "matrices.dimensions",
          "matrices.sync_status",
          "contexts.names as context_names",
          sql<number>`COUNT(DISTINCT statistics.id)`.as("data_point_count"),
          sql<number>`COUNT(DISTINCT statistics.time_period_id)`.as(
            "year_count"
          ),
        ])
        .groupBy([
          "matrices.id",
          "matrices.ins_code",
          "matrices.metadata",
          "matrices.dimensions",
          "matrices.sync_status",
          "contexts.names",
        ]);

      if (includeChildren) {
        // Include all territories under this path
        query = query.where(
          sql<SqlBool>`territories.path <@ ${territory.path}::ltree`
        );
      } else {
        query = query.where("territories.id", "=", territoryId);
      }

      if (level) {
        query = query.where("territories.level", "=", level);
      }

      // Apply cursor pagination
      if (cursorPayload) {
        query = query.where("matrices.id", ">", cursorPayload.id);
      }

      const rows = await query
        .orderBy("matrices.id", "asc")
        .limit(limit + 1)
        .execute();

      const hasMore = rows.length > limit;
      const items = rows.slice(0, limit).map((m) => {
        const metadata = m.metadata;
        const contextNames = m.context_names;
        return {
          id: m.id,
          insCode: m.ins_code,
          name: getLocalizedName(metadata.names, locale),
          contextName: contextNames
            ? getLocalizedName(contextNames, locale)
            : null,
          periodicity: metadata.periodicity ?? [],
          dataPointCount: m.data_point_count,
          yearCount: m.year_count,
          startYear: metadata.yearRange?.[0] ?? null,
          endYear: metadata.yearRange?.[1] ?? null,
        };
      });

      return {
        data: items,
        meta: {
          territory: {
            id: territory.id,
            level: territory.level,
          },
          pagination: createPaginationMeta(items, limit, "id", hasMore),
        },
      };
    }
  );

  /**
   * GET /api/v1/discover/by-time-range
   * Find matrices with data covering a specific time range
   */
  app.get<{ Querystring: ByTimeRangeQuery }>(
    "/discover/by-time-range",
    {
      schema: {
        summary: "Find matrices by time range",
        description:
          "Find matrices that have data within a specific year range. " +
          "Example: `/discover/by-time-range?yearFrom=2015&yearTo=2023&periodicity=ANNUAL`",
        tags: ["Discovery"],
        querystring: ByTimeRangeQuerySchema,
      },
    },
    async (request) => {
      const {
        locale = "ro",
        yearFrom,
        yearTo,
        periodicity,
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
          "contexts.names as context_names",
        ]);

      // Filter by year range from metadata
      if (yearFrom) {
        query = query.where(
          sql`(matrices.metadata->'yearRange'->>1)::int`,
          ">=",
          yearFrom
        );
      }

      if (yearTo) {
        query = query.where(
          sql`(matrices.metadata->'yearRange'->>0)::int`,
          "<=",
          yearTo
        );
      }

      if (periodicity) {
        query = query.where(
          sql`matrices.metadata->'periodicity'`,
          "@>",
          sql`${JSON.stringify([periodicity])}::jsonb`
        );
      }

      // Apply cursor pagination
      if (cursorPayload) {
        query = query.where("matrices.id", ">", cursorPayload.id);
      }

      const rows = await query
        .orderBy("matrices.id", "asc")
        .limit(limit + 1)
        .execute();

      const hasMore = rows.length > limit;
      const items = rows.slice(0, limit).map((m) => {
        const metadata = m.metadata;
        const contextNames = m.context_names;
        const startYear = metadata.yearRange?.[0] ?? null;
        const endYear = metadata.yearRange?.[1] ?? null;

        // Calculate overlap with requested range
        let coverageYears = 0;
        if (startYear && endYear) {
          const rangeStart = yearFrom ?? startYear;
          const rangeEnd = yearTo ?? endYear;
          const overlapStart = Math.max(startYear, rangeStart);
          const overlapEnd = Math.min(endYear, rangeEnd);
          coverageYears = Math.max(0, overlapEnd - overlapStart + 1);
        }

        return {
          id: m.id,
          insCode: m.ins_code,
          name: getLocalizedName(metadata.names, locale),
          contextName: contextNames
            ? getLocalizedName(contextNames, locale)
            : null,
          periodicity: metadata.periodicity ?? [],
          startYear,
          endYear,
          coverageYears,
          dimensionCount: (m.dimensions ?? []).length,
        };
      });

      return {
        data: items,
        meta: {
          filter: {
            yearFrom: yearFrom ?? null,
            yearTo: yearTo ?? null,
            periodicity: periodicity ?? null,
          },
          pagination: createPaginationMeta(items, limit, "id", hasMore),
        },
      };
    }
  );

  /**
   * GET /api/v1/discover/by-breakdowns
   * Find matrices with specific dimension types (breakdowns)
   */
  app.get<{ Querystring: ByBreakdownsQuery }>(
    "/discover/by-breakdowns",
    {
      schema: {
        summary: "Find matrices by breakdowns",
        description:
          "Find matrices that have specific dimension types for data breakdowns. " +
          "Examples:\n" +
          "- `/discover/by-breakdowns?hasTerritory=true` - Matrices with regional data\n" +
          "- `/discover/by-breakdowns?classificationType=SEX` - Matrices broken down by sex",
        tags: ["Discovery"],
        querystring: ByBreakdownsQuerySchema,
      },
    },
    async (request) => {
      const {
        locale = "ro",
        hasTerritory,
        hasTemporal,
        classificationType,
        minDimensions,
        maxDimensions,
        limit: rawLimit,
        cursor,
      } = request.query;

      const limit = parseLimit(rawLimit, 50, 100);
      const cursorPayload = validateCursor(cursor);

      // Build subquery for dimension filtering
      let query = db
        .selectFrom("matrices")
        .leftJoin("contexts", "matrices.context_id", "contexts.id")
        .select([
          "matrices.id",
          "matrices.ins_code",
          "matrices.metadata",
          "matrices.dimensions",
          "matrices.sync_status",
          "contexts.names as context_names",
        ]);

      // Filter by dimension count
      if (minDimensions !== undefined) {
        query = query.where(
          sql`jsonb_array_length(matrices.dimensions)`,
          ">=",
          minDimensions
        );
      }

      if (maxDimensions !== undefined) {
        query = query.where(
          sql`jsonb_array_length(matrices.dimensions)`,
          "<=",
          maxDimensions
        );
      }

      // Filter by dimension types using EXISTS subqueries
      if (hasTerritory !== undefined) {
        if (hasTerritory) {
          query = query.where(
            sql<SqlBool>`EXISTS (
              SELECT 1 FROM matrix_dimensions md
              WHERE md.matrix_id = matrices.id
              AND md.dimension_type = 'TERRITORIAL'
            )`
          );
        } else {
          query = query.where(
            sql<SqlBool>`NOT EXISTS (
              SELECT 1 FROM matrix_dimensions md
              WHERE md.matrix_id = matrices.id
              AND md.dimension_type = 'TERRITORIAL'
            )`
          );
        }
      }

      if (hasTemporal !== undefined) {
        if (hasTemporal) {
          query = query.where(
            sql<SqlBool>`EXISTS (
              SELECT 1 FROM matrix_dimensions md
              WHERE md.matrix_id = matrices.id
              AND md.dimension_type = 'TEMPORAL'
            )`
          );
        } else {
          query = query.where(
            sql<SqlBool>`NOT EXISTS (
              SELECT 1 FROM matrix_dimensions md
              WHERE md.matrix_id = matrices.id
              AND md.dimension_type = 'TEMPORAL'
            )`
          );
        }
      }

      if (classificationType) {
        query = query.where(
          sql<SqlBool>`EXISTS (
            SELECT 1 FROM matrix_dimensions md
            INNER JOIN classification_types ct ON md.classification_type_id = ct.id
            WHERE md.matrix_id = matrices.id
            AND ct.code = ${classificationType}
          )`
        );
      }

      // Apply cursor pagination
      if (cursorPayload) {
        query = query.where("matrices.id", ">", cursorPayload.id);
      }

      const rows = await query
        .orderBy("matrices.id", "asc")
        .limit(limit + 1)
        .execute();

      const hasMore = rows.length > limit;
      const matrixIds = rows.slice(0, limit).map((m) => m.id);

      // Get dimension types for each matrix
      const dimensions =
        matrixIds.length > 0
          ? await db
              .selectFrom("matrix_dimensions")
              .leftJoin(
                "classification_types",
                "matrix_dimensions.classification_type_id",
                "classification_types.id"
              )
              .select([
                "matrix_dimensions.matrix_id",
                "matrix_dimensions.dimension_type",
                "matrix_dimensions.labels",
                "classification_types.code as classification_code",
              ])
              .where("matrix_dimensions.matrix_id", "in", matrixIds)
              .execute()
          : [];

      // Group dimensions by matrix
      const dimensionsByMatrix = new Map<
        number,
        { type: string; label: string; classificationCode?: string }[]
      >();
      for (const d of dimensions) {
        if (!dimensionsByMatrix.has(d.matrix_id)) {
          dimensionsByMatrix.set(d.matrix_id, []);
        }
        dimensionsByMatrix.get(d.matrix_id)!.push({
          type: d.dimension_type,
          label: getLocalizedName(d.labels, locale),
          classificationCode: d.classification_code ?? undefined,
        });
      }

      const items = rows.slice(0, limit).map((m) => {
        const metadata = m.metadata;
        const contextNames = m.context_names;
        return {
          id: m.id,
          insCode: m.ins_code,
          name: getLocalizedName(metadata.names, locale),
          contextName: contextNames
            ? getLocalizedName(contextNames, locale)
            : null,
          dimensions: dimensionsByMatrix.get(m.id) ?? [],
          startYear: metadata.yearRange?.[0] ?? null,
          endYear: metadata.yearRange?.[1] ?? null,
        };
      });

      return {
        data: items,
        meta: {
          filter: {
            hasTerritory: hasTerritory ?? null,
            hasTemporal: hasTemporal ?? null,
            classificationType: classificationType ?? null,
            minDimensions: minDimensions ?? null,
            maxDimensions: maxDimensions ?? null,
          },
          pagination: createPaginationMeta(items, limit, "id", hasMore),
        },
      };
    }
  );
}
