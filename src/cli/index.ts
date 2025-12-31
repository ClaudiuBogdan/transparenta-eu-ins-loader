#!/usr/bin/env node

/**
 * Transparenta EU INS Loader CLI
 *
 * CLI for loading Romanian statistical datasets from INS Tempo for Transparenta.eu.
 */

import { Command } from "commander";

import { registerContextsCommand } from "./commands/contexts.js";
import { registerDbCommand } from "./commands/db.js";
import { registerDimensionsCommand } from "./commands/dimensions.js";
import { registerExploreCommand } from "./commands/explore.js";
import { registerMatricesCommand } from "./commands/matrices.js";
import { registerMatrixCommand } from "./commands/matrix.js";
import { registerQueryCommand } from "./commands/query.js";
import { registerSyncCommand } from "./commands/sync.js";

const program = new Command();

program
  .name("transparenta-ins")
  .description("Transparenta EU - INS Tempo statistical data loader CLI")
  .version("0.3.0");

// Register all commands
registerDbCommand(program);
registerSyncCommand(program);
registerExploreCommand(program);
registerContextsCommand(program);
registerMatricesCommand(program);
registerMatrixCommand(program);
registerDimensionsCommand(program);
registerQueryCommand(program);

// Top-level alias for 'sync status'
program
  .command("status")
  .description("Show sync status (alias for 'sync status')")
  .option("--failed", "Show only failed syncs")
  .action(async (options: { failed?: boolean }) => {
    // Dynamically import to get the same handler
    const { db, closeConnection } = await import("../db/connection.js");

    try {
      let query = db
        .selectFrom("matrices")
        .select([
          "ins_code",
          "metadata",
          "sync_status",
          "last_sync_at",
          "sync_error",
        ])
        .orderBy("ins_code");

      if (options.failed === true) {
        query = query.where("sync_status", "=", "FAILED");
      }

      const rows = await query.limit(50).execute();

      if (rows.length === 0) {
        console.log("No matrices found matching criteria");
      } else {
        console.log("\nSync Status:");
        console.log("─".repeat(100));
        console.log(
          "Code".padEnd(12) +
            "Status".padEnd(12) +
            "Last Sync".padEnd(15) +
            "Name"
        );
        console.log("─".repeat(100));

        for (const row of rows) {
          const lastSync =
            row.last_sync_at !== null
              ? (new Date(row.last_sync_at).toISOString().split("T")[0] ??
                "Never")
              : "Never";
          const syncStatus = row.sync_status ?? "PENDING";
          const name = row.metadata?.names?.ro ?? "Unknown";
          const displayName =
            name.length > 55 ? name.slice(0, 52) + "..." : name;

          console.log(
            row.ins_code.padEnd(12) +
              syncStatus.padEnd(12) +
              lastSync.padEnd(15) +
              displayName
          );
        }

        if (rows.length === 50) {
          console.log("\n... (showing first 50 results)");
        }
      }
    } catch (error) {
      console.error(`Error: ${(error as Error).message}`);
      process.exitCode = 1;
    } finally {
      await closeConnection();
    }
  });

// Default to explore if no command specified
program.action(() => {
  // Show help by default
  program.outputHelp();
});

program.parse();
