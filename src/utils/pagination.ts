/**
 * Cursor-based pagination utilities
 */

export interface CursorPayload {
  sortValue: string | number;
  id: number;
  direction: "forward" | "backward";
}

export interface PaginationMeta {
  cursor: string | null;
  hasMore: boolean;
  limit: number;
  total?: number;
}

export interface PaginationOptions {
  cursor?: string;
  limit: number;
}

export interface PaginatedResult<T> {
  items: T[];
  pagination: PaginationMeta;
}

/**
 * Encode cursor payload to base64url string
 */
export function encodeCursor(payload: CursorPayload): string {
  return Buffer.from(JSON.stringify(payload)).toString("base64url");
}

/**
 * Decode base64url cursor string to payload
 */
export function decodeCursor(cursor: string): CursorPayload | null {
  try {
    const decoded = Buffer.from(cursor, "base64url").toString("utf-8");
    return JSON.parse(decoded) as CursorPayload;
  } catch {
    return null;
  }
}

/**
 * Create pagination meta from results
 */
export function createPaginationMeta<T extends { id: number }>(
  items: T[],
  limit: number,
  sortField: keyof T,
  hasMore: boolean,
  total?: number
): PaginationMeta {
  let cursor: string | null = null;

  if (hasMore && items.length > 0) {
    const lastItem = items[items.length - 1]!;
    cursor = encodeCursor({
      sortValue: lastItem[sortField] as string | number,
      id: lastItem.id,
      direction: "forward",
    });
  }

  return {
    cursor,
    hasMore,
    limit,
    total,
  };
}

/**
 * Parse limit from query parameter with bounds
 */
export function parseLimit(
  value: string | number | undefined,
  defaultLimit = 50,
  maxLimit = 100
): number {
  if (value === undefined) {
    return defaultLimit;
  }

  const parsed = typeof value === "string" ? parseInt(value, 10) : value;

  if (isNaN(parsed) || parsed < 1) {
    return defaultLimit;
  }

  return Math.min(parsed, maxLimit);
}

/**
 * Validate cursor and extract payload
 */
export function validateCursor(
  cursor: string | undefined
): CursorPayload | null {
  if (!cursor) {
    return null;
  }

  const payload = decodeCursor(cursor);
  if (!payload) {
    return null;
  }

  // Validate payload structure
  if (
    typeof payload.sortValue !== "string" &&
    typeof payload.sortValue !== "number"
  ) {
    return null;
  }

  if (typeof payload.id !== "number") {
    return null;
  }

  return payload;
}
