import ora from "ora";

import {
  checkConnection,
  closeConnection,
  getDatabaseUrl,
  getPoolStats,
} from "../../db/connection.js";
import { runMigration, hasSchema, getTableStats } from "../../db/migrate.js";

import type { Command } from "commander";

// ============================================================================
// Database Commands
// ============================================================================

export function registerDbCommand(program: Command): void {
  const db = program.command("db").description("Database management commands");

  // db migrate
  db.command("migrate")
    .description("Run database migration from postgres-schema.sql")
    .option("--fresh", "Drop all tables first (destructive!)")
    .action(async (options: { fresh?: boolean }) => {
      const spinner = ora("Running migration...").start();

      try {
        if (options.fresh === true) {
          spinner.text = "Dropping existing schema...";
        }

        await runMigration({ fresh: options.fresh });
        spinner.succeed("Migration completed successfully");

        // Show table stats
        const stats = await getTableStats();
        if (stats.length > 0) {
          console.log("\nTables created:");
          for (const row of stats) {
            console.log(`  ${row.table_name}: ${String(row.row_count)} rows`);
          }
        }
      } catch (error) {
        spinner.fail(`Migration failed: ${(error as Error).message}`);
        process.exitCode = 1;
      } finally {
        await closeConnection();
      }
    });

  // db status
  db.command("status")
    .description("Check database connection and show statistics")
    .action(async () => {
      const spinner = ora("Checking database connection...").start();

      try {
        const connected = await checkConnection();

        if (!connected) {
          spinner.fail("Database connection failed");
          console.log(`\nDatabase URL: ${getDatabaseUrl()}`);
          process.exitCode = 1;
          return;
        }

        spinner.succeed("Database connected");
        console.log(`\nDatabase URL: ${getDatabaseUrl()}`);

        // Pool stats
        const poolStats = getPoolStats();
        console.log("\nPool statistics:");
        console.log(`  Total connections: ${String(poolStats.totalCount)}`);
        console.log(`  Idle connections: ${String(poolStats.idleCount)}`);
        console.log(`  Waiting requests: ${String(poolStats.waitingCount)}`);

        // Check if schema exists
        const schemaExists = await hasSchema();
        if (!schemaExists) {
          console.log("\nSchema: Not initialized (run 'db migrate')");
        } else {
          // Show table stats
          const stats = await getTableStats();
          console.log("\nTable statistics:");
          for (const row of stats) {
            console.log(`  ${row.table_name}: ${String(row.row_count)} rows`);
          }
        }
      } catch (error) {
        spinner.fail(`Error: ${(error as Error).message}`);
        process.exitCode = 1;
      } finally {
        await closeConnection();
      }
    });

  // db reset
  db.command("reset")
    .description("Reset database (drop and recreate schema)")
    .action(async () => {
      const spinner = ora("Resetting database...").start();

      try {
        await runMigration({ fresh: true });
        spinner.succeed("Database reset completed");
      } catch (error) {
        spinner.fail(`Reset failed: ${(error as Error).message}`);
        process.exitCode = 1;
      } finally {
        await closeConnection();
      }
    });
}
