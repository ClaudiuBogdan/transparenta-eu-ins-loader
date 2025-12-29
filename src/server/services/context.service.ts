/**
 * Context Service - Business logic for context operations
 */

import { db } from "../../db/connection.js";
import {
  createPaginationMeta,
  parseLimit,
  validateCursor,
  type PaginatedResult,
} from "../../utils/pagination.js";
import { NotFoundError } from "../plugins/error-handler.js";

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

  let query = db
    .selectFrom("contexts")
    .select([
      "id",
      "ins_code",
      "name",
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
    query = query.where("path", "like", `${options.pathPrefix}%`);
  }

  // Apply cursor-based pagination
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

  // Fetch one extra to check for more
  const rows = await query
    .orderBy("name", "asc")
    .orderBy("id", "asc")
    .limit(limit + 1)
    .execute();

  const hasMore = rows.length > limit;
  const items = rows.slice(0, limit).map(mapContextRow);

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
export async function getContextById(id: number): Promise<ContextDetailDto> {
  // Get the context
  const context = await db
    .selectFrom("contexts")
    .select([
      "id",
      "ins_code",
      "name",
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

  const contextDto = mapContextRow(context);

  // Get ancestors by traversing up the hierarchy
  const ancestors: ContextDto[] = [];
  let currentParentId = context.parent_id;

  while (currentParentId !== null) {
    const parent = await db
      .selectFrom("contexts")
      .select([
        "id",
        "ins_code",
        "name",
        "level",
        "parent_id",
        "path",
        "children_type",
      ])
      .where("id", "=", currentParentId)
      .executeTakeFirst();

    if (parent) {
      ancestors.unshift(mapContextRow(parent));
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
        "name",
        "level",
        "parent_id",
        "path",
        "children_type",
      ])
      .where("parent_id", "=", id)
      .orderBy("name", "asc")
      .execute();

    return {
      context: contextDto,
      children: childContexts.map(mapContextRow),
      ancestors,
    };
  } else {
    // Children are matrices
    const matrices = await db
      .selectFrom("matrices")
      .select([
        "id",
        "ins_code",
        "name",
        "periodicity",
        "has_uat_data",
        "has_county_data",
        "dimension_count",
        "start_year",
        "end_year",
        "last_update",
        "status",
      ])
      .where("context_id", "=", id)
      .orderBy("name", "asc")
      .execute();

    const matrixDtos: MatrixSummaryDto[] = matrices.map((m) => ({
      id: m.id,
      insCode: m.ins_code,
      name: m.name,
      contextPath: context.path,
      contextName: context.name,
      periodicity: m.periodicity ?? [],
      hasUatData: m.has_uat_data,
      hasCountyData: m.has_county_data,
      dimensionCount: m.dimension_count,
      startYear: m.start_year,
      endYear: m.end_year,
      lastUpdate: m.last_update?.toISOString() ?? null,
      status: m.status,
    }));

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
export async function getContextByCode(insCode: string): Promise<ContextDto> {
  const context = await db
    .selectFrom("contexts")
    .select([
      "id",
      "ins_code",
      "name",
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

  return mapContextRow(context);
}

// ============================================================================
// Helpers
// ============================================================================

interface ContextRow {
  id: number;
  ins_code: string;
  name: string;
  level: number;
  parent_id: number | null;
  path: string;
  children_type: string;
}

function mapContextRow(row: ContextRow): ContextDto {
  return {
    id: row.id,
    insCode: row.ins_code,
    name: row.name,
    level: row.level,
    parentId: row.parent_id,
    path: row.path,
    childrenType: row.children_type as "context" | "matrix",
  };
}
