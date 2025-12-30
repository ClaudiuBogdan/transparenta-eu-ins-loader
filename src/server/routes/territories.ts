/**
 * Territory Routes - /api/v1/territories
 * Updated for V2 schema with JSONB names
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
        locale = "ro",
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
          "names",
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
        // Search in normalized name (stored in JSONB)
        query = query.where(
          sql`names->>'normalized'`,
          "ilike",
          `%${search.toUpperCase()}%`
        );
      }

      if (sirutaCode) {
        query = query.where("siruta_code", "=", sirutaCode);
      }

      // Apply cursor using JSONB name
      if (cursorPayload) {
        query = query.where((eb) =>
          eb.or([
            eb(sql`names->>'ro'`, ">", cursorPayload.sortValue as string),
            eb.and([
              eb(sql`names->>'ro'`, "=", cursorPayload.sortValue as string),
              eb("id", ">", cursorPayload.id),
            ]),
          ])
        );
      }

      const rows = await query
        .orderBy(sql`names->>'ro'`, "asc")
        .orderBy("id", "asc")
        .limit(limit + 1)
        .execute();

      const hasMore = rows.length > limit;
      const items: TerritoryDto[] = rows.slice(0, limit).map((t) => ({
        id: t.id,
        code: t.code,
        sirutaCode: t.siruta_code,
        name: locale === "en" && t.names.en ? t.names.en : t.names.ro,
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
      const locale = request.query.locale ?? "ro";

      if (Number.isNaN(id)) {
        throw new NotFoundError("Invalid territory ID");
      }

      const territory = await db
        .selectFrom("territories")
        .select([
          "id",
          "code",
          "siruta_code",
          "names",
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
          "names",
          "level",
          "parent_id",
          "path",
        ])
        .where("parent_id", "=", id)
        .orderBy(sql`names->>'ro'`, "asc")
        .execute();

      const territoryDto: TerritoryDto = {
        id: territory.id,
        code: territory.code,
        sirutaCode: territory.siruta_code,
        name:
          locale === "en" && territory.names.en
            ? territory.names.en
            : territory.names.ro,
        level: territory.level,
        parentId: territory.parent_id,
        path: territory.path,
        children: children.map((c) => ({
          id: c.id,
          code: c.code,
          sirutaCode: c.siruta_code,
          name: locale === "en" && c.names.en ? c.names.en : c.names.ro,
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
}
