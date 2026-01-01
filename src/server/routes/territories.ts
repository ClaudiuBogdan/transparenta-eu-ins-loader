/**
 * Territory Routes - /api/v1/territories
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
  IdParamSchema,
  TerritorialLevelSchema,
  LocaleSchema,
} from "../schemas/common.js";
import {
  TerritoryListResponseSchema,
  TerritorySchema,
} from "../schemas/responses.js";

import type { TerritoryDto } from "../../types/api.js";
import type { FastifyInstance } from "fastify";

// ============================================================================
// Schemas
// ============================================================================

const ListTerritoriesQuerySchema = Type.Intersect([
  PaginationQuerySchema,
  Type.Object({
    locale: Type.Optional(LocaleSchema),
    level: Type.Optional(TerritorialLevelSchema),
    parentId: Type.Optional(
      Type.Number({ description: "Filter by parent territory ID" })
    ),
    pathPrefix: Type.Optional(
      Type.String({ description: "Filter by path prefix (e.g., 'RO.RO1')" })
    ),
    search: Type.Optional(
      Type.String({ description: "Search by territory name" })
    ),
    sirutaCode: Type.Optional(
      Type.String({ description: "Filter by SIRUTA code" })
    ),
    includeChildren: Type.Optional(Type.Boolean()),
  }),
]);

type ListTerritoriesQuery = Static<typeof ListTerritoriesQuerySchema>;

// Response schemas
const TerritoryDetailResponseSchema = Type.Object({
  data: Type.Intersect([
    TerritorySchema,
    Type.Object({
      children: Type.Array(TerritorySchema),
    }),
  ]),
});

// ============================================================================
// Routes
// ============================================================================

export function registerTerritoryRoutes(app: FastifyInstance): void {
  /**
   * GET /api/v1/territories
   * List territories with filtering
   */
  app.get<{ Querystring: ListTerritoriesQuery }>(
    "/territories",
    {
      schema: {
        summary: "List territories",
        description:
          "Browse the NUTS/LAU territorial hierarchy. Levels: NATIONAL (Romania), NUTS1 (macro-regions), " +
          "NUTS2 (development regions), NUTS3 (counties), LAU (localities/UATs). " +
          "Examples:\n- `/territories?level=NUTS3` - All counties\n- `/territories?search=bucur` - Search for Bucharest",
        tags: ["Territories"],
        querystring: ListTerritoriesQuerySchema,
        response: {
          200: TerritoryListResponseSchema,
        },
      },
    },
    async (request) => {
      const {
        level,
        parentId,
        pathPrefix,
        search,
        sirutaCode,
        limit: rawLimit,
        cursor,
      } = request.query;

      const limit = parseLimit(rawLimit, 50, 500);
      const cursorPayload = validateCursor(cursor);

      let query = db
        .selectFrom("territories")
        .select([
          "id",
          "code",
          "siruta_code",
          "name",
          "level",
          "parent_id",
          "path",
        ]);

      // Apply filters
      if (level) {
        query = query.where("level", "=", level);
      }

      if (parentId !== undefined) {
        query = query.where("parent_id", "=", parentId);
      }

      if (pathPrefix) {
        query = query.where(sql`path::text`, "like", `${pathPrefix}%`);
      }

      if (search) {
        // Search in name using case-insensitive normalized comparison
        query = query.where(
          sql`UPPER(name)`,
          "like",
          `%${search.toUpperCase()}%`
        );
      }

      if (sirutaCode) {
        query = query.where("siruta_code", "=", sirutaCode);
      }

      // Apply cursor using name
      if (cursorPayload) {
        query = query.where((eb) =>
          eb.or([
            eb("name", ">", cursorPayload.sortValue as string),
            eb.and([
              eb("name", "=", cursorPayload.sortValue as string),
              eb("id", ">", cursorPayload.id),
            ]),
          ])
        );
      }

      const rows = await query
        .orderBy("name", "asc")
        .orderBy("id", "asc")
        .limit(limit + 1)
        .execute();

      const hasMore = rows.length > limit;
      const items: TerritoryDto[] = rows.slice(0, limit).map((t) => ({
        id: t.id,
        code: t.code,
        sirutaCode: t.siruta_code,
        name: t.name,
        level: t.level,
        parentId: t.parent_id,
        path: t.path,
      }));

      return {
        data: items,
        meta: {
          pagination: createPaginationMeta(items, limit, "name", hasMore),
        },
      };
    }
  );

  /**
   * GET /api/v1/territories/:id
   * Get territory details
   */
  app.get<{
    Params: Static<typeof IdParamSchema>;
    Querystring: { locale?: "ro" | "en" };
  }>(
    "/territories/:id",
    {
      schema: {
        summary: "Get territory details",
        description:
          "Get a territory with its direct children. Use this to navigate down the hierarchy. " +
          "Example: Get a county to see its localities.",
        tags: ["Territories"],
        params: IdParamSchema,
        querystring: Type.Object({
          locale: Type.Optional(LocaleSchema),
        }),
        response: {
          200: TerritoryDetailResponseSchema,
        },
      },
    },
    async (request) => {
      const id = Number.parseInt(request.params.id, 10);

      if (Number.isNaN(id)) {
        throw new NotFoundError("Invalid territory ID");
      }

      const territory = await db
        .selectFrom("territories")
        .select([
          "id",
          "code",
          "siruta_code",
          "name",
          "level",
          "parent_id",
          "path",
        ])
        .where("id", "=", id)
        .executeTakeFirst();

      if (!territory) {
        throw new NotFoundError(`Territory with ID ${String(id)} not found`);
      }

      // Get children
      const children = await db
        .selectFrom("territories")
        .select([
          "id",
          "code",
          "siruta_code",
          "name",
          "level",
          "parent_id",
          "path",
        ])
        .where("parent_id", "=", id)
        .orderBy("name", "asc")
        .execute();

      const territoryDto: TerritoryDto = {
        id: territory.id,
        code: territory.code,
        sirutaCode: territory.siruta_code,
        name: territory.name,
        level: territory.level,
        parentId: territory.parent_id,
        path: territory.path,
        children: children.map((c) => ({
          id: c.id,
          code: c.code,
          sirutaCode: c.siruta_code,
          name: c.name,
          level: c.level,
          parentId: c.parent_id,
          path: c.path,
        })),
      };

      return {
        data: territoryDto,
      };
    }
  );

  const AvailableDataQuerySchema = Type.Intersect([
    PaginationQuerySchema,
    Type.Object({
      locale: Type.Optional(LocaleSchema),
    }),
  ]);

  type AvailableDataQuery = Static<typeof AvailableDataQuerySchema>;

  /**
   * GET /api/v1/territories/:id/available-data
   * Get matrices with data available for a territory
   */
  app.get<{
    Params: Static<typeof IdParamSchema>;
    Querystring: AvailableDataQuery;
  }>(
    "/territories/:id/available-data",
    {
      schema: {
        summary: "Get available data for territory",
        description:
          "Get all matrices that have statistical data available for a specific territory. " +
          "Includes data point counts and year ranges. " +
          "Example: Get all matrices with data for Bucharest.",
        tags: ["Territories"],
        params: IdParamSchema,
        querystring: AvailableDataQuerySchema,
      },
    },
    async (request) => {
      const id = Number.parseInt(request.params.id, 10);
      const locale = request.query.locale ?? "ro";
      const limit = parseLimit(request.query.limit, 50, 100);
      const cursorPayload = validateCursor(request.query.cursor);

      if (Number.isNaN(id)) {
        throw new NotFoundError("Invalid territory ID");
      }

      // Get territory
      const territory = await db
        .selectFrom("territories")
        .select(["id", "code", "name", "level", "path"])
        .where("id", "=", id)
        .executeTakeFirst();

      if (!territory) {
        throw new NotFoundError(`Territory with ID ${String(id)} not found`);
      }

      // Find all matrices with data for this territory
      let query = db
        .selectFrom("statistics")
        .innerJoin("matrices", "statistics.matrix_id", "matrices.id")
        .innerJoin(
          "time_periods",
          "statistics.time_period_id",
          "time_periods.id"
        )
        .leftJoin("contexts", "matrices.context_id", "contexts.id")
        .select([
          "matrices.id",
          "matrices.ins_code",
          "matrices.metadata",
          "contexts.names as context_names",
          sql<number>`COUNT(DISTINCT statistics.id)`.as("data_point_count"),
          sql<number>`COUNT(DISTINCT time_periods.year)`.as("year_count"),
          sql<number>`MIN(time_periods.year)`.as("min_year"),
          sql<number>`MAX(time_periods.year)`.as("max_year"),
        ])
        .where("statistics.territory_id", "=", id)
        .groupBy([
          "matrices.id",
          "matrices.ins_code",
          "matrices.metadata",
          "contexts.names",
        ]);

      // Apply cursor pagination
      if (cursorPayload) {
        query = query.having("matrices.id", ">", cursorPayload.id);
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
          name:
            locale === "en" && metadata.names.en
              ? metadata.names.en
              : metadata.names.ro,
          contextName: contextNames
            ? locale === "en" && contextNames.en
              ? contextNames.en
              : contextNames.ro
            : null,
          periodicity: metadata.periodicity ?? [],
          dataPointCount: m.data_point_count,
          yearCount: m.year_count,
          yearRange: {
            min: m.min_year,
            max: m.max_year,
          },
        };
      });

      return {
        data: items,
        meta: {
          territory: {
            id: territory.id,
            code: territory.code,
            name: territory.name,
            level: territory.level,
          },
          pagination: createPaginationMeta(items, limit, "id", hasMore),
        },
      };
    }
  );
}
