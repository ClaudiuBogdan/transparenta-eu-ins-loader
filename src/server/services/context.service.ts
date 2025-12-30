/**
 * Context Service - Business logic for context operations
 * Updated for V2 schema with JSONB names
 */

import { sql } from "kysely";

import { db } from "../../db/connection.js";
import {
  createPaginationMeta,
  parseLimit,
  validateCursor,
  type PaginatedResult,
} from "../../utils/pagination.js";
import { NotFoundError } from "../plugins/error-handler.js";

import type { BilingualText } from "../../db/types-v2.js";
import type {
  ContextDto,
  ContextDetailDto,
  MatrixSummaryDto,
} from "../../types/api.js";

// ============================================================================
// Query Options
// ============================================================================

export interface ListContextsOptions {
  level?: number;
  parentId?: number;
  pathPrefix?: string;
  limit?: number;
  cursor?: string;
  locale?: "ro" | "en";
}

// ============================================================================
// Service Functions
// ============================================================================

/**
 * List contexts with optional filtering and pagination
 */
export async function listContexts(
  options: ListContextsOptions = {}
): Promise<PaginatedResult<ContextDto>> {
  const limit = parseLimit(options.limit, 50, 100);
  const cursorPayload = validateCursor(options.cursor);
  const locale = options.locale ?? "ro";

  let query = db
    .selectFrom("contexts")
    .select([
      "id",
      "ins_code",
      "names",
      "level",
      "parent_id",
      "path",
      "children_type",
    ]);

  // Apply filters
  if (options.level !== undefined) {
    query = query.where("level", "=", options.level);
  }

  if (options.parentId !== undefined) {
    query = query.where("parent_id", "=", options.parentId);
  }

  if (options.pathPrefix) {
    query = query.where(sql`path::text`, "like", `${options.pathPrefix}%`);
  }

  // Apply cursor-based pagination using JSONB name
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

  // Fetch one extra to check for more
  const rows = await query
    .orderBy(sql`names->>'ro'`, "asc")
    .orderBy("id", "asc")
    .limit(limit + 1)
    .execute();

  const hasMore = rows.length > limit;
  const items = rows.slice(0, limit).map((row) => mapContextRow(row, locale));

  return {
    items,
    pagination: createPaginationMeta(
      items,
      limit,
      "name" as keyof ContextDto,
      hasMore
    ),
  };
}

/**
 * Get context by ID with children and ancestors
 */
export async function getContextById(
  id: number,
  locale: "ro" | "en" = "ro"
): Promise<ContextDetailDto> {
  // Get the context
  const context = await db
    .selectFrom("contexts")
    .select([
      "id",
      "ins_code",
      "names",
      "level",
      "parent_id",
      "path",
      "children_type",
    ])
    .where("id", "=", id)
    .executeTakeFirst();

  if (!context) {
    throw new NotFoundError(`Context with ID ${String(id)} not found`);
  }

  const contextDto = mapContextRow(context, locale);

  // Get ancestors by traversing up the hierarchy
  const ancestors: ContextDto[] = [];
  let currentParentId = context.parent_id;

  while (currentParentId !== null) {
    const parent = await db
      .selectFrom("contexts")
      .select([
        "id",
        "ins_code",
        "names",
        "level",
        "parent_id",
        "path",
        "children_type",
      ])
      .where("id", "=", currentParentId)
      .executeTakeFirst();

    if (parent) {
      ancestors.unshift(mapContextRow(parent, locale));
      currentParentId = parent.parent_id;
    } else {
      break;
    }
  }

  // Get children based on children_type
  if (context.children_type === "context") {
    const childContexts = await db
      .selectFrom("contexts")
      .select([
        "id",
        "ins_code",
        "names",
        "level",
        "parent_id",
        "path",
        "children_type",
      ])
      .where("parent_id", "=", id)
      .orderBy(sql`names->>'ro'`, "asc")
      .execute();

    return {
      context: contextDto,
      children: childContexts.map((row) => mapContextRow(row, locale)),
      ancestors,
    };
  } else {
    // Children are matrices
    const matrices = await db
      .selectFrom("matrices")
      .select([
        "id",
        "ins_code",
        "metadata",
        "dimensions",
        "sync_status",
        "last_sync_at",
      ])
      .where("context_id", "=", id)
      .orderBy(sql`metadata->'names'->>'ro'`, "asc")
      .execute();

    const localizedContextName =
      locale === "en" && context.names.en ? context.names.en : context.names.ro;

    const matrixDtos: MatrixSummaryDto[] = matrices.map((m) => {
      const metadata = m.metadata;
      const dimensions = m.dimensions ?? [];

      return {
        id: m.id,
        insCode: m.ins_code,
        name:
          locale === "en" && metadata.names.en
            ? metadata.names.en
            : metadata.names.ro,
        contextPath: context.path,
        contextName: localizedContextName,
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
      context: contextDto,
      children: matrixDtos,
      ancestors,
    };
  }
}

/**
 * Get context by INS code
 */
export async function getContextByCode(
  insCode: string,
  locale: "ro" | "en" = "ro"
): Promise<ContextDto> {
  const context = await db
    .selectFrom("contexts")
    .select([
      "id",
      "ins_code",
      "names",
      "level",
      "parent_id",
      "path",
      "children_type",
    ])
    .where("ins_code", "=", insCode)
    .executeTakeFirst();

  if (!context) {
    throw new NotFoundError(`Context with code ${insCode} not found`);
  }

  return mapContextRow(context, locale);
}

// ============================================================================
// Helpers
// ============================================================================

interface ContextRow {
  id: number;
  ins_code: string;
  names: BilingualText;
  level: number;
  parent_id: number | null;
  path: string;
  children_type: "context" | "matrix";
}

function mapContextRow(
  row: ContextRow,
  locale: "ro" | "en" = "ro"
): ContextDto {
  return {
    id: row.id,
    insCode: row.ins_code,
    name: locale === "en" && row.names.en ? row.names.en : row.names.ro,
    level: row.level,
    parentId: row.parent_id,
    path: row.path,
    childrenType: row.children_type,
  };
}
