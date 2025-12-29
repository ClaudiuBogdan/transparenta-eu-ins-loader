import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { logger } from "../logger.js";
import { closeConnection, pool } from "./connection.js";

const currentFilePath = fileURLToPath(import.meta.url);
const currentDirPath = dirname(currentFilePath);

// ============================================================================
// Migration Functions
// ============================================================================

/**
 * Run the PostgreSQL schema migration from postgres-schema.sql
 */
export async function runMigration(options?: {
  fresh?: boolean;
}): Promise<void> {
  const client = await pool.connect();

  try {
    if (options?.fresh === true) {
      logger.info("Dropping existing schema (--fresh mode)...");
      await client.query("DROP SCHEMA public CASCADE");
      await client.query("CREATE SCHEMA public");
      logger.info("Schema dropped and recreated");
    }

    // Read the schema file
    const schemaPath = join(currentDirPath, "postgres-schema.sql");
    const schema = readFileSync(schemaPath, "utf8");

    logger.info("Running PostgreSQL schema migration...");

    await client.query("BEGIN");
    await client.query(schema);
    await client.query("COMMIT");

    logger.info("Schema migration completed successfully");
  } catch (error) {
    await client.query("ROLLBACK");
    logger.error({ error }, "Schema migration failed");
    throw error;
  } finally {
    client.release();
  }
}

/**
 * Check if the schema exists (has tables)
 */
export async function hasSchema(): Promise<boolean> {
  const client = await pool.connect();
  try {
    const result = await client.query<{ count: number }>(`
      SELECT COUNT(*)::int as count
      FROM information_schema.tables
      WHERE table_schema = 'public'
      AND table_type = 'BASE TABLE'
    `);
    const row = result.rows[0];
    return row !== undefined && row.count > 0;
  } finally {
    client.release();
  }
}

interface TableStat {
  table_name: string;
  row_count: number;
}

/**
 * Get table statistics
 */
export async function getTableStats(): Promise<TableStat[]> {
  const client = await pool.connect();
  try {
    const result = await client.query<TableStat>(`
      SELECT
        relname as table_name,
        n_live_tup::bigint as row_count
      FROM pg_stat_user_tables
      WHERE schemaname = 'public'
      ORDER BY relname
    `);
    return result.rows;
  } finally {
    client.release();
  }
}

// ============================================================================
// CLI Entry Point (only runs when executed directly, not when imported)
// ============================================================================

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const fresh = args.includes("--fresh");

  if (fresh) {
    console.log("Running migration with --fresh flag (will drop all tables)");
  }

  try {
    await runMigration({ fresh });
    console.log("Migration completed successfully!");

    // Show table stats
    const stats = await getTableStats();
    if (stats.length > 0) {
      console.log("\nTable statistics:");
      for (const row of stats) {
        console.log(`  ${row.table_name}: ${String(row.row_count)} rows`);
      }
    }
  } catch (error) {
    console.error("Migration failed:", error);
    process.exitCode = 1;
  } finally {
    await closeConnection();
  }
}

// Only run main() if this file is executed directly (not imported)
const isMainModule = process.argv[1]?.includes("migrate");
if (isMainModule) {
  void main();
}
