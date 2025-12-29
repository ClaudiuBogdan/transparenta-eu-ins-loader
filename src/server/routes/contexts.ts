/**
 * Context Routes - /api/v1/contexts
 */

import { Type, type Static } from "@sinclair/typebox";

import {
  PaginationQuerySchema,
  IdParamSchema,
  LocaleSchema,
} from "../schemas/common.js";
import {
  ContextListResponseSchema,
  ContextSchema,
  MatrixSummarySchema,
} from "../schemas/responses.js";
import { listContexts, getContextById } from "../services/context.service.js";

import type { FastifyInstance } from "fastify";

// ============================================================================
// Schemas
// ============================================================================

const ListContextsQuerySchema = Type.Intersect([
  PaginationQuerySchema,
  Type.Object({
    locale: Type.Optional(LocaleSchema),
    level: Type.Optional(
      Type.Number({
        minimum: 0,
        description: "Filter by hierarchy level (0=root)",
      })
    ),
    parentId: Type.Optional(
      Type.Number({ description: "Filter by parent context ID" })
    ),
    pathPrefix: Type.Optional(
      Type.String({ description: "Filter by path prefix (e.g., 'A.A01')" })
    ),
  }),
]);

type ListContextsQuery = Static<typeof ListContextsQuerySchema>;

const ContextDetailResponseSchema = Type.Object({
  data: Type.Object({
    context: ContextSchema,
    children: Type.Union([
      Type.Array(ContextSchema),
      Type.Array(MatrixSummarySchema),
    ]),
    ancestors: Type.Array(ContextSchema),
  }),
});

// ============================================================================
// Routes
// ============================================================================

export function registerContextRoutes(app: FastifyInstance): void {
  /**
   * GET /api/v1/contexts
   * List all contexts with optional filtering
   */
  app.get<{ Querystring: ListContextsQuery }>(
    "/contexts",
    {
      schema: {
        summary: "List contexts",
        description:
          "Browse the hierarchical context structure. Contexts organize statistical matrices into domains " +
          "(e.g., Population, Economy). Use level=1 for top-level domains, or parentId to get children of a specific context.",
        tags: ["Discovery"],
        querystring: ListContextsQuerySchema,
        response: {
          200: ContextListResponseSchema,
        },
      },
    },
    async (request) => {
      const { locale, level, parentId, pathPrefix, limit, cursor } =
        request.query;

      const result = await listContexts({
        locale,
        level,
        parentId,
        pathPrefix,
        limit,
        cursor,
      });

      return {
        data: result.items,
        meta: {
          pagination: result.pagination,
        },
      };
    }
  );

  const ContextDetailQuerySchema = Type.Object({
    locale: Type.Optional(LocaleSchema),
  });

  /**
   * GET /api/v1/contexts/:id
   * Get context details with children and ancestors
   */
  app.get<{
    Params: Static<typeof IdParamSchema>;
    Querystring: Static<typeof ContextDetailQuerySchema>;
  }>(
    "/contexts/:id",
    {
      schema: {
        summary: "Get context details",
        description:
          "Get a specific context with its children (sub-contexts or matrices) and ancestor path. " +
          "If childrenType is 'matrix', children contains matrices; otherwise sub-contexts.",
        tags: ["Discovery"],
        params: IdParamSchema,
        querystring: ContextDetailQuerySchema,
        response: {
          200: ContextDetailResponseSchema,
        },
      },
    },
    async (request) => {
      const id = parseInt(request.params.id, 10);
      const locale = request.query.locale ?? "ro";

      if (isNaN(id)) {
        return { error: "VALIDATION_ERROR", message: "Invalid context ID" };
      }

      const result = await getContextById(id, locale);

      return {
        data: result,
      };
    }
  );
}
