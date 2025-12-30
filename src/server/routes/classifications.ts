/**
 * Classification Routes - /api/v1/classifications
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
import { PaginationQuerySchema } from "../schemas/common.js";
import {
  ClassificationTypeListResponseSchema,
  ClassificationTypeSchema,
  ClassificationValueSchema,
} from "../schemas/responses.js";

import type {
  ClassificationTypeDto,
  ClassificationValueDto,
} from "../../types/api.js";
import type { FastifyInstance } from "fastify";

// ============================================================================
// Schemas
// ============================================================================

const ListClassificationTypesQuerySchema = Type.Intersect([
  PaginationQuerySchema,
  Type.Object({
    search: Type.Optional(Type.String()),
    isHierarchical: Type.Optional(Type.Boolean()),
  }),
]);

type ListClassificationTypesQuery = Static<
  typeof ListClassificationTypesQuerySchema
>;

const CodeParamSchema = Type.Object({
  code: Type.String(),
});

type CodeParam = Static<typeof CodeParamSchema>;

const ListClassificationValuesQuerySchema = Type.Intersect([
  PaginationQuerySchema,
  Type.Object({
    search: Type.Optional(
      Type.String({ description: "Search by value name or code" })
    ),
    parentId: Type.Optional(
      Type.Number({ description: "Filter by parent value ID" })
    ),
    level: Type.Optional(
      Type.Number({ description: "Filter by hierarchy level" })
    ),
    rootsOnly: Type.Optional(
      Type.Boolean({ description: "Return only root values" })
    ),
  }),
]);

type ListClassificationValuesQuery = Static<
  typeof ListClassificationValuesQuerySchema
>;

// Response schemas
const ClassificationValuesResponseSchema = Type.Object({
  data: Type.Array(ClassificationValueSchema),
  meta: Type.Object({
    classificationType: ClassificationTypeSchema,
    pagination: Type.Object({
      cursor: Type.Union([Type.String(), Type.Null()]),
      hasMore: Type.Boolean(),
      limit: Type.Number(),
    }),
  }),
});

const ClassificationValueDetailResponseSchema = Type.Object({
  data: Type.Intersect([
    ClassificationValueSchema,
    Type.Object({
      children: Type.Optional(Type.Array(ClassificationValueSchema)),
    }),
  ]),
});

// ============================================================================
// Routes
// ============================================================================

export function registerClassificationRoutes(app: FastifyInstance): void {
  /**
   * GET /api/v1/classifications
   * List classification types
   */
  app.get<{ Querystring: ListClassificationTypesQuery }>(
    "/classifications",
    {
      schema: {
        summary: "List classification types",
        description:
          "Get available classification systems used to categorize statistical data. " +
          "Examples: SEX (sex/gender), AGE_GROUP (age groups), RESIDENCE (urban/rural environment).",
        tags: ["Classifications"],
        querystring: ListClassificationTypesQuerySchema,
        response: {
          200: ClassificationTypeListResponseSchema,
        },
      },
    },
    async (request) => {
      const { search, isHierarchical, limit: rawLimit, cursor } = request.query;

      const limit = parseLimit(rawLimit, 50, 100);
      const cursorPayload = validateCursor(cursor);

      let query = db
        .selectFrom("classification_types")
        .select(["id", "code", "names", "is_hierarchical"]);

      // Apply filters - search in JSONB names
      if (search) {
        query = query.where((eb) =>
          eb.or([
            eb(sql`names->>'ro'`, "ilike", `%${search}%`),
            eb("code", "ilike", `%${search}%`),
          ])
        );
      }

      if (isHierarchical !== undefined) {
        query = query.where("is_hierarchical", "=", isHierarchical);
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

      // Get value counts for each type
      const typeIds = rows.slice(0, limit).map((r) => r.id);

      let valueCounts: {
        type_id: number;
        count: string | number | bigint;
      }[] = [];
      if (typeIds.length > 0) {
        valueCounts = await db
          .selectFrom("classification_values")
          .select(["type_id", db.fn.count("id").as("count")])
          .where("type_id", "in", typeIds)
          .groupBy("type_id")
          .execute();
      }

      const countMap = new Map(
        valueCounts.map((vc) => [vc.type_id, Number(vc.count)])
      );

      const hasMore = rows.length > limit;
      const items: ClassificationTypeDto[] = rows.slice(0, limit).map((ct) => ({
        id: ct.id,
        code: ct.code,
        name: ct.names.ro,
        isHierarchical: ct.is_hierarchical,
        valueCount: countMap.get(ct.id) ?? 0,
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
   * GET /api/v1/classifications/:code
   * Get classification type with values
   */
  app.get<{
    Params: CodeParam;
    Querystring: ListClassificationValuesQuery;
  }>(
    "/classifications/:code",
    {
      schema: {
        summary: "Get classification values",
        description:
          "Get all values for a classification type. For hierarchical classifications, " +
          "use rootsOnly=true to get top-level values, then drill down using parentId.",
        tags: ["Classifications"],
        params: CodeParamSchema,
        querystring: ListClassificationValuesQuerySchema,
        response: {
          200: ClassificationValuesResponseSchema,
        },
      },
    },
    async (request) => {
      const { code } = request.params;
      const {
        search,
        parentId,
        level,
        rootsOnly,
        limit: rawLimit,
        cursor,
      } = request.query;

      // Get classification type
      const classificationType = await db
        .selectFrom("classification_types")
        .select(["id", "code", "names", "is_hierarchical"])
        .where("code", "=", code)
        .executeTakeFirst();

      if (!classificationType) {
        throw new NotFoundError(`Classification type ${code} not found`);
      }

      const limit = parseLimit(rawLimit, 100, 500);
      const cursorPayload = validateCursor(cursor);

      let query = db
        .selectFrom("classification_values")
        .select([
          "id",
          "code",
          "names",
          "parent_id",
          "path",
          "level",
          "sort_order",
        ])
        .where("type_id", "=", classificationType.id);

      // Apply filters - search in JSONB names
      if (search) {
        query = query.where((eb) =>
          eb.or([
            eb(sql`names->>'ro'`, "ilike", `%${search}%`),
            eb("code", "ilike", `%${search}%`),
            eb(sql`names->>'normalized'`, "ilike", `%${search.toUpperCase()}%`),
          ])
        );
      }

      if (parentId !== undefined) {
        query = query.where("parent_id", "=", parentId);
      }

      if (level !== undefined) {
        query = query.where("level", "=", level);
      }

      if (rootsOnly) {
        query = query.where("parent_id", "is", null);
      }

      // Apply cursor
      if (cursorPayload) {
        query = query.where((eb) =>
          eb.or([
            eb("sort_order", ">", cursorPayload.sortValue as number),
            eb.and([
              eb("sort_order", "=", cursorPayload.sortValue as number),
              eb("id", ">", cursorPayload.id),
            ]),
          ])
        );
      }

      const rows = await query
        .orderBy("sort_order", "asc")
        .orderBy("id", "asc")
        .limit(limit + 1)
        .execute();

      const hasMore = rows.length > limit;
      const values: ClassificationValueDto[] = rows
        .slice(0, limit)
        .map((cv) => ({
          id: cv.id,
          code: cv.code,
          name: cv.names.ro,
          parentId: cv.parent_id,
          path: cv.path,
          level: cv.level,
        }));

      // Get total count
      const totalResult = await db
        .selectFrom("classification_values")
        .select(db.fn.count("id").as("count"))
        .where("type_id", "=", classificationType.id)
        .executeTakeFirst();

      const typeDto: ClassificationTypeDto = {
        id: classificationType.id,
        code: classificationType.code,
        name: classificationType.names.ro,
        isHierarchical: classificationType.is_hierarchical,
        valueCount: Number(totalResult?.count ?? 0),
      };

      return {
        data: values,
        meta: {
          classificationType: typeDto,
          pagination: createPaginationMeta(values, limit, "id", hasMore),
        },
      };
    }
  );

  /**
   * GET /api/v1/classifications/:code/values/:valueId
   * Get single classification value with children
   */
  app.get<{
    Params: { code: string; valueId: string };
  }>(
    "/classifications/:code/values/:valueId",
    {
      schema: {
        summary: "Get classification value detail",
        description:
          "Get a single classification value with its children. Use for drilling down hierarchical classifications.",
        tags: ["Classifications"],
        params: Type.Object({
          code: Type.String({ description: "Classification type code" }),
          valueId: Type.String({ description: "Classification value ID" }),
        }),
        response: {
          200: ClassificationValueDetailResponseSchema,
        },
      },
    },
    async (request) => {
      const { code, valueId } = request.params;
      const valueIdNum = Number.parseInt(valueId, 10);

      if (Number.isNaN(valueIdNum)) {
        throw new NotFoundError("Invalid classification value ID");
      }

      // Get classification type
      const classificationType = await db
        .selectFrom("classification_types")
        .select(["id", "code", "names", "is_hierarchical"])
        .where("code", "=", code)
        .executeTakeFirst();

      if (!classificationType) {
        throw new NotFoundError(`Classification type ${code} not found`);
      }

      // Get the value
      const value = await db
        .selectFrom("classification_values")
        .select([
          "id",
          "code",
          "names",
          "parent_id",
          "path",
          "level",
          "sort_order",
        ])
        .where("type_id", "=", classificationType.id)
        .where("id", "=", valueIdNum)
        .executeTakeFirst();

      if (!value) {
        throw new NotFoundError(
          `Classification value ${valueId} not found in type ${code}`
        );
      }

      // Get children if hierarchical
      let children: ClassificationValueDto[] = [];
      if (classificationType.is_hierarchical) {
        const childRows = await db
          .selectFrom("classification_values")
          .select([
            "id",
            "code",
            "names",
            "parent_id",
            "path",
            "level",
            "sort_order",
          ])
          .where("type_id", "=", classificationType.id)
          .where("parent_id", "=", valueIdNum)
          .orderBy("sort_order", "asc")
          .execute();

        children = childRows.map((cv) => ({
          id: cv.id,
          code: cv.code,
          name: cv.names.ro,
          parentId: cv.parent_id,
          path: cv.path,
          level: cv.level,
        }));
      }

      const valueDto: ClassificationValueDto = {
        id: value.id,
        code: value.code,
        name: value.names.ro,
        parentId: value.parent_id,
        path: value.path,
        level: value.level,
        children: children.length > 0 ? children : undefined,
      };

      return {
        data: valueDto,
      };
    }
  );
}
