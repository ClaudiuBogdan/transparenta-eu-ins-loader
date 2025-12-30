import ora from "ora";

import { db, closeConnection } from "../../db/connection.js";
import {
  ContextSyncService,
  TerritoryService,
  MatrixSyncService,
  DataSyncService,
} from "../../services/sync/index.js";

import type { Command } from "commander";

// ============================================================================
// Helper
// ============================================================================

const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

// ============================================================================
// Sync Commands
// ============================================================================

export function registerSyncCommand(program: Command): void {
  const sync = program
    .command("sync")
    .description("Synchronize data from INS Tempo API");

  // sync contexts
  sync
    .command("contexts")
    .description("Sync context hierarchy (domains and categories)")
    .action(async () => {
      const spinner = ora("Syncing contexts...").start();

      try {
        const service = new ContextSyncService(db);
        const result = await service.sync();

        spinner.succeed(
          `Synced contexts: ${String(result.inserted)} inserted, ${String(result.updated)} updated (${String(result.duration ?? 0)}ms)`
        );
      } catch (error) {
        spinner.fail(`Failed: ${(error as Error).message}`);
        process.exitCode = 1;
      } finally {
        await closeConnection();
      }
    });

  // sync territories
  sync
    .command("territories")
    .description("Bootstrap territory hierarchy (NUTS levels)")
    .action(async () => {
      const spinner = ora("Seeding territory hierarchy...").start();

      try {
        const service = new TerritoryService(db);
        const result = await service.seedNutsHierarchy();

        spinner.succeed(
          `Seeded territories: ${String(result.inserted)} inserted, ${String(result.updated)} updated`
        );
      } catch (error) {
        spinner.fail(`Failed: ${(error as Error).message}`);
        process.exitCode = 1;
      } finally {
        await closeConnection();
      }
    });

  // sync matrices
  sync
    .command("matrices")
    .description("Sync matrix catalog and optionally metadata")
    .option("--full", "Sync full metadata for each matrix (slow)")
    .option("--code <code>", "Sync specific matrix only")
    .action(async (options: { full?: boolean; code?: string }) => {
      const spinner = ora("Syncing matrices...").start();

      // Track failures for summary
      const failures: { code: string; error: string }[] = [];

      try {
        const service = new MatrixSyncService(db);

        if (options.code !== undefined && options.code !== "") {
          spinner.text = `Syncing matrix ${options.code}...`;
          await service.syncMatrixDetails(options.code);
          spinner.succeed(`Matrix ${options.code} synced`);
        } else {
          // Catalog sync
          const result = await service.syncCatalog();
          spinner.succeed(
            `Catalog synced: ${String(result.inserted)} new, ${String(result.updated)} updated`
          );

          if (options.full === true) {
            // Full metadata sync for all matrices
            spinner.start("Fetching matrix list...");
            const matrices = await db
              .selectFrom("matrices")
              .select("ins_code")
              .execute();

            let completed = 0;
            const total = matrices.length;

            for (const m of matrices) {
              spinner.text = `Syncing metadata: ${String(completed + 1)}/${String(total)} (${m.ins_code})`;
              try {
                await service.syncMatrixDetails(m.ins_code);
                completed++;
              } catch (error) {
                const errorMsg = (error as Error).message;
                failures.push({ code: m.ins_code, error: errorMsg });
                // Don't log inline, we'll show summary at the end
              }
              await sleep(750); // Rate limit
            }

            spinner.succeed(
              `Metadata synced for ${String(completed)}/${String(total)} matrices`
            );

            // Print summary at the end
            if (failures.length > 0) {
              console.log("\n" + "═".repeat(80));
              console.log("SYNC SUMMARY");
              console.log("═".repeat(80));
              console.log(`\n✓ Succeeded: ${String(completed)}`);
              console.log(`✗ Failed: ${String(failures.length)}`);

              console.log("\n" + "─".repeat(80));
              console.log("FAILURES:");
              console.log("─".repeat(80));
              for (const f of failures) {
                console.log(`  ${f.code}: ${f.error}`);
              }
              console.log("─".repeat(80));
            }
          }
        }
      } catch (error) {
        spinner.fail(`Failed: ${(error as Error).message}`);
        process.exitCode = 1;
      } finally {
        await closeConnection();
      }
    });

  // sync data <matrix>
  sync
    .command("data <matrix>")
    .description("Sync statistical data for a matrix")
    .option("--years <range>", "Year range, e.g., 2020-2024")
    .option("--resume", "Resume from last incomplete job")
    .option("--limit <number>", "Limit number of rows to sync (for testing)")
    .option("--force-refresh", "Force re-fetch even if data exists")
    .option(
      "--incremental",
      "Only sync chunks that have changed (uses checkpoints)"
    )
    .action(
      async (
        matrixCode: string,
        options: {
          years?: string;
          resume?: boolean;
          limit?: string;
          forceRefresh?: boolean;
          incremental?: boolean;
        }
      ) => {
        const spinner = ora(`Syncing data for ${matrixCode}...`).start();

        try {
          const service = new DataSyncService(db);
          const limit = options.limit
            ? Number.parseInt(options.limit, 10)
            : undefined;
          const result = await service.syncMatrixData(matrixCode, {
            yearRange: options.years,
            resume: options.resume,
            limit,
            forceRefresh: options.forceRefresh,
            incremental: options.incremental,
          });

          // Build result message showing inserted vs updated
          const inserted = result.rowsInserted;
          const updated = result.rowsUpdated;
          const totalProcessed = inserted + updated;

          let message: string;
          if (result.totalChunks !== undefined && result.totalChunks > 0) {
            message =
              `Data synced: ${String(inserted)} inserted, ${String(updated)} updated ` +
              `(${String(result.chunksCompleted ?? 0)}/${String(result.totalChunks)} chunks)`;
          } else {
            message = `Data synced: ${String(inserted)} inserted, ${String(updated)} updated (${String(totalProcessed)} total)`;
          }

          spinner.succeed(message);
        } catch (error) {
          spinner.fail(`Failed: ${(error as Error).message}`);
          process.exitCode = 1;
        } finally {
          await closeConnection();
        }
      }
    );

  // sync status
  sync
    .command("status")
    .description("Show sync status for all matrices")
    .option("--failed", "Show only failed syncs")
    .action(async (options: { failed?: boolean }) => {
      try {
        let query = db
          .selectFrom("matrices")
          .leftJoin(
            "matrix_sync_status",
            "matrices.id",
            "matrix_sync_status.matrix_id"
          )
          .select([
            "matrices.ins_code",
            "matrices.name",
            "matrices.status",
            "matrix_sync_status.sync_status",
            "matrix_sync_status.last_full_sync",
            "matrix_sync_status.row_count",
            "matrix_sync_status.consecutive_failures",
          ])
          .orderBy("matrices.ins_code");

        if (options.failed === true) {
          query = query.where("matrix_sync_status.sync_status", "in", [
            "FAILED",
            "PARTIAL",
          ]);
        }

        const rows = await query.limit(50).execute();

        if (rows.length === 0) {
          console.log("No matrices found matching criteria");
        } else {
          console.log("\nSync Status:");
          console.log("─".repeat(100));
          console.log(
            "Code".padEnd(12) +
              "Status".padEnd(15) +
              "Last Sync".padEnd(15) +
              "Rows".padEnd(10) +
              "Failures".padEnd(10) +
              "Name"
          );
          console.log("─".repeat(100));

          for (const row of rows) {
            const lastSync =
              row.last_full_sync !== null
                ? (new Date(row.last_full_sync).toISOString().split("T")[0] ??
                  "Never")
                : "Never";
            const syncStatus = row.sync_status ?? "NEVER_SYNCED";
            const rowCount = String(row.row_count ?? 0);
            const failures = String(row.consecutive_failures ?? 0);
            const name =
              row.name.length > 45 ? row.name.slice(0, 42) + "..." : row.name;

            console.log(
              row.ins_code.padEnd(12) +
                syncStatus.padEnd(15) +
                lastSync.padEnd(15) +
                rowCount.padEnd(10) +
                failures.padEnd(10) +
                name
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

  // sync all
  sync
    .command("all")
    .description("Run full sync: contexts, territories, matrices, all data")
    .option("--skip-data", "Skip data sync (only sync metadata)")
    .action(async (options: { skipData?: boolean }) => {
      const spinner = ora("Starting full sync...").start();

      try {
        // 1. Contexts
        spinner.text = "Syncing contexts...";
        const contextService = new ContextSyncService(db);
        const contextResult = await contextService.sync();
        console.log(
          `\nContexts: ${String(contextResult.inserted)} inserted, ${String(contextResult.updated)} updated`
        );

        // 2. Territories
        spinner.text = "Seeding territories...";
        const territoryService = new TerritoryService(db);
        const territoryResult = await territoryService.seedNutsHierarchy();
        console.log(
          `Territories: ${String(territoryResult.inserted)} inserted, ${String(territoryResult.updated)} updated`
        );

        // 3. Matrices (catalog only for speed)
        spinner.text = "Syncing matrix catalog...";
        const matrixService = new MatrixSyncService(db);
        const matrixResult = await matrixService.syncCatalog();
        console.log(
          `Matrices: ${String(matrixResult.inserted)} new, ${String(matrixResult.updated)} updated`
        );

        if (options.skipData === true) {
          spinner.succeed("Metadata sync completed (data sync skipped)");
        } else {
          spinner.info("Data sync not implemented in 'all' command");
          spinner.succeed("Metadata sync completed");
        }
      } catch (error) {
        spinner.fail(`Sync failed: ${(error as Error).message}`);
        process.exitCode = 1;
      } finally {
        await closeConnection();
      }
    });
}
