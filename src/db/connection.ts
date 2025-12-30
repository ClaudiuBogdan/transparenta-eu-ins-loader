import { Kysely, PostgresDialect, sql, type RawBuilder } from "kysely";
import pg from "pg";

import { logger } from "../logger.js";

import type { Database } from "./types-v2.js";

const { Pool, types } = pg;

// Configure pg to parse JSON/JSONB as objects instead of strings
types.setTypeParser(
  types.builtins.JSON,
  (val: string) => JSON.parse(val) as unknown
);
types.setTypeParser(
  types.builtins.JSONB,
  (val: string) => JSON.parse(val) as unknown
);

/**
 * Helper to convert JavaScript objects to JSONB SQL expressions for Kysely inserts/updates.
 * This is needed because Kysely doesn't automatically serialize objects to JSON for PostgreSQL.
 */
export function jsonb<T>(value: T): RawBuilder<T> {
  return sql<T>`${JSON.stringify(value)}::jsonb`;
}

/**
 * Helper to convert JavaScript string arrays to PostgreSQL TEXT[] expressions.
 * This is needed because Kysely doesn't automatically handle array types.
 */
export function textArray(values: string[]): RawBuilder<string[]> {
  if (values.length === 0) {
    return sql<string[]>`'{}'::TEXT[]`;
  }
  // Escape single quotes by doubling them for PostgreSQL string literals
  const escaped = values.map((v) => `'${v.replaceAll("'", "''")}'`).join(",");
  return sql<string[]>`ARRAY[${sql.raw(escaped)}]::TEXT[]`;
}

// ============================================================================
// Configuration
// ============================================================================

const DATABASE_URL =
  process.env.DATABASE_URL ?? "postgresql://localhost:5432/ins_tempo";

const poolConfig: pg.PoolConfig = {
  connectionString: DATABASE_URL,
  max: 20, // Maximum pool connections
  idleTimeoutMillis: 30_000, // Close idle connections after 30s
  connectionTimeoutMillis: 5000, // Connection timeout
};

// ============================================================================
// Pool and Kysely Instance
// ============================================================================

export const pool = new Pool(poolConfig);

export const db = new Kysely<Database>({
  dialect: new PostgresDialect({ pool }),
});

// ============================================================================
// Connection Management
// ============================================================================

/**
 * Check if the database connection is healthy
 */
export async function checkConnection(): Promise<boolean> {
  const client = await pool.connect();
  try {
    await client.query("SELECT 1");
    return true;
  } catch {
    return false;
  } finally {
    client.release();
  }
}

/**
 * Gracefully close the database connection
 */
export async function closeConnection(): Promise<void> {
  try {
    // db.destroy() already closes the pool, so we only need to call it once
    await db.destroy();
    logger.info("Database connection closed");
  } catch (error) {
    logger.error({ error }, "Error closing database connection");
    throw error;
  }
}

/**
 * Get the current database URL (for display, with password masked)
 */
export function getDatabaseUrl(): string {
  const url = new URL(DATABASE_URL);
  if (url.password !== "") {
    url.password = "****";
  }
  return url.toString();
}

/**
 * Get pool statistics
 */
export function getPoolStats(): {
  totalCount: number;
  idleCount: number;
  waitingCount: number;
} {
  return {
    totalCount: pool.totalCount,
    idleCount: pool.idleCount,
    waitingCount: pool.waitingCount,
  };
}
