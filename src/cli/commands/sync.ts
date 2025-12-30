import { sql } from "kysely";
import ora from "ora";

import { db, closeConnection } from "../../db/connection.js";
import { SyncOrchestrator } from "../../services/sync-v2/index.js";

import type { Command } from "commander";

// ============================================================================
// Sync Commands
// ============================================================================

export function registerSyncCommand(program: Command): void {
  const sync = program
    .command("sync")
    .description("Synchronize data from INS Tempo API (V2 schema)")
    .addHelpText(
      "after",
      `
SYNC WORKFLOW:
══════════════════════════════════════════════════════════════════════════════
The recommended sync order for a fresh database:

  1. pnpm cli sync all           # Full sync: contexts, matrices, metadata
     └─ OR step by step:
        pnpm cli sync contexts   # Domain hierarchy
        pnpm cli sync matrices   # Matrix catalog only
        pnpm cli sync matrices --full --skip-existing  # Detailed metadata

  2. pnpm cli sync partitions    # Create statistics partitions
     └─ Preview first:
        pnpm cli sync partitions --dry-run

  3. pnpm cli sync data <CODE>   # Sync statistical data for a matrix
     └─ Example:
        pnpm cli sync data POP105A

PARTITION EXPLANATION:
──────────────────────────────────────────────────────────────────────────────
The statistics table is partitioned by matrix_id for performance. Each matrix
needs its own partition before data can be inserted. The partitions are:
  - statistics_matrix_<id>
  - statistic_classifications_matrix_<id>

Use 'sync partitions' to automatically create missing partitions.
══════════════════════════════════════════════════════════════════════════════
`
    );

  // sync contexts
  sync
    .command("contexts")
    .description("Sync context hierarchy (domains and categories)")
    .action(async () => {
      const spinner = ora("Syncing contexts...").start();

      try {
        const orchestrator = new SyncOrchestrator(db);
        orchestrator.setProgressCallback((progress) => {
          spinner.text = `Syncing contexts: ${String(progress.current)}/${String(progress.total)} (${progress.currentItem ?? ""})`;
        });

        const result = await orchestrator.syncAll({
          contexts: true,
          matrices: false,
          metadata: false,
        });

        spinner.succeed(`Synced ${String(result.contexts)} contexts`);
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
        const orchestrator = new SyncOrchestrator(db);
        await orchestrator.syncAll({
          contexts: false,
          matrices: false,
          metadata: false,
        });

        spinner.succeed("Territory hierarchy seeded");
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
    .option("--skip-existing", "Skip matrices that already have metadata")
    .action(
      async (options: {
        full?: boolean;
        code?: string;
        skipExisting?: boolean;
      }) => {
        const spinner = ora("Syncing matrices...").start();

        try {
          const orchestrator = new SyncOrchestrator(db);
          orchestrator.setProgressCallback((progress) => {
            spinner.text = `${progress.phase}: ${String(progress.current)}/${String(progress.total)} (${progress.currentItem ?? ""})`;
          });

          if (options.code !== undefined && options.code !== "") {
            // Sync specific matrix
            const result = await orchestrator.syncAll({
              contexts: false,
              matrices: false,
              metadata: true,
              matrixCodes: [options.code],
            });

            if (result.errors.length > 0) {
              spinner.fail(`Failed: ${result.errors[0] ?? "Unknown error"}`);
            } else {
              spinner.succeed(
                `Matrix ${options.code} synced: ${String(result.nomItems)} dimension options`
              );
            }
          } else {
            // Catalog sync
            const catalogResult = await orchestrator.syncAll({
              contexts: false,
              matrices: true,
              metadata: false,
            });
            spinner.succeed(
              `Catalog synced: ${String(catalogResult.matrices)} matrices`
            );

            if (options.full === true) {
              // Full metadata sync
              spinner.start("Syncing matrix metadata...");
              const metaResult = await orchestrator.syncAll({
                contexts: false,
                matrices: false,
                metadata: true,
                skipExisting: options.skipExisting,
              });

              spinner.succeed(
                `Metadata synced: ${String(metaResult.dimensions)} dimensions, ${String(metaResult.nomItems)} options`
              );

              // Print failures if any
              if (metaResult.errors.length > 0) {
                console.log("\n" + "═".repeat(80));
                console.log("SYNC SUMMARY");
                console.log("═".repeat(80));
                console.log(`\n✗ Failed: ${String(metaResult.errors.length)}`);

                console.log("\n" + "─".repeat(80));
                console.log("FAILURES:");
                console.log("─".repeat(80));
                for (const error of metaResult.errors.slice(0, 20)) {
                  console.log(`  ${error}`);
                }
                if (metaResult.errors.length > 20) {
                  console.log(
                    `  ... and ${String(metaResult.errors.length - 20)} more`
                  );
                }
                console.log("─".repeat(80));
              }

              // Show next steps for partition creation
              console.log("\n" + "═".repeat(80));
              console.log("NEXT STEPS");
              console.log("═".repeat(80));
              console.log(
                "  Before syncing statistical data, create partitions:"
              );
              console.log(
                "    pnpm cli sync partitions           # For all SYNCED matrices"
              );
              console.log(
                "    pnpm cli sync partitions --dry-run # Preview partitions"
              );
              console.log(
                "    pnpm cli sync partitions --code <CODE> # Specific matrix"
              );
              console.log("");
              console.log("  Then sync data:");
              console.log("    pnpm cli sync data <MATRIX_CODE>");
              console.log("═".repeat(80));
            }
          }
        } catch (error) {
          spinner.fail(`Failed: ${(error as Error).message}`);
          process.exitCode = 1;
        } finally {
          await closeConnection();
        }
      }
    );

  // sync partitions - create statistics partitions for synced matrices
  sync
    .command("partitions")
    .description("Create statistics partitions for matrices")
    .option("--code <code>", "Create partition for specific matrix only")
    .option("--all", "Create partitions for ALL matrices (can take a while)")
    .option("--dry-run", "Show what partitions would be created")
    .action(
      async (options: { code?: string; all?: boolean; dryRun?: boolean }) => {
        const spinner = ora("Checking partitions...").start();

        try {
          // Get matrices that need partitions
          let query = db
            .selectFrom("matrices")
            .select(["id", "ins_code", "sync_status"]);

          if (options.code !== undefined && options.code !== "") {
            query = query.where("ins_code", "=", options.code);
          } else if (options.all !== true) {
            // By default, only create partitions for SYNCED matrices
            query = query.where("sync_status", "=", "SYNCED");
          }

          const matrices = await query.execute();

          if (matrices.length === 0) {
            spinner.warn("No matrices found matching criteria");
            console.log(
              "\nTip: Run 'pnpm cli sync matrices --full' first to sync matrix metadata."
            );
            return;
          }

          // Check which partitions already exist
          const existingPartitions = await sql<{ tablename: string }>`
          SELECT tablename FROM pg_tables
          WHERE schemaname = 'public' AND tablename LIKE 'statistics_matrix_%'
        `.execute(db);

          const existingSet = new Set(
            existingPartitions.rows.map((p) => p.tablename)
          );

          const toCreate = matrices.filter(
            (m) => !existingSet.has(`statistics_matrix_${String(m.id)}`)
          );

          if (options.dryRun === true) {
            spinner.info(
              `Dry run: Would create ${String(toCreate.length)} partitions`
            );
            console.log("\nPartitions to create:");
            for (const m of toCreate.slice(0, 20)) {
              console.log(
                `  statistics_matrix_${String(m.id)} (${m.ins_code})`
              );
            }
            if (toCreate.length > 20) {
              console.log(`  ... and ${String(toCreate.length - 20)} more`);
            }
            return;
          }

          if (toCreate.length === 0) {
            spinner.succeed("All partitions already exist");
            return;
          }

          spinner.text = `Creating ${String(toCreate.length)} partitions...`;

          let created = 0;
          for (const m of toCreate) {
            await sql`SELECT create_statistics_partition(${m.id})`.execute(db);
            created++;

            if (created % 50 === 0) {
              spinner.text = `Creating partitions: ${String(created)}/${String(toCreate.length)}`;
            }
          }

          spinner.succeed(`Created ${String(created)} statistics partitions`);

          console.log("\n" + "─".repeat(60));
          console.log("NEXT STEPS:");
          console.log("─".repeat(60));
          console.log(
            "  1. Use 'pnpm cli sync data <MATRIX_CODE>' to sync statistical data"
          );
          console.log("  2. Or query the database directly for statistics");
          console.log("─".repeat(60));
        } catch (error) {
          spinner.fail(`Failed: ${(error as Error).message}`);
          process.exitCode = 1;
        } finally {
          await closeConnection();
        }
      }
    );

  // sync data <matrix> - sync statistical data
  sync
    .command("data <matrix>")
    .description("Sync statistical data for a matrix (requires partition)")
    .option("--years <range>", "Year range, e.g., 2020-2024")
    .action(async (matrixCode: string, _options: { years?: string }) => {
      try {
        // Check if matrix exists and is synced
        const matrix = await db
          .selectFrom("matrices")
          .select(["id", "sync_status"])
          .where("ins_code", "=", matrixCode)
          .executeTakeFirst();

        if (!matrix) {
          console.error(
            `Matrix ${matrixCode} not found. Run 'pnpm cli sync matrices' first.`
          );
          process.exitCode = 1;
          return;
        }

        if (matrix.sync_status !== "SYNCED") {
          console.error(
            `Matrix ${matrixCode} metadata not synced (status: ${matrix.sync_status ?? "PENDING"}).`
          );
          console.log(
            "Run 'pnpm cli sync matrices --code " + matrixCode + "' first."
          );
          process.exitCode = 1;
          return;
        }

        // Check if partition exists
        const partitionName = `statistics_matrix_${String(matrix.id)}`;
        const partitionCheck = await sql<{ exists: boolean }>`
          SELECT EXISTS (
            SELECT 1 FROM pg_tables
            WHERE schemaname = 'public' AND tablename = ${partitionName}
          ) as exists
        `.execute(db);
        const partitionExists = partitionCheck.rows[0]?.exists ?? false;

        if (!partitionExists) {
          console.error(
            `No partition exists for matrix ${matrixCode} (id: ${String(matrix.id)}).`
          );
          console.log("\nCreate a partition first:");
          console.log(`  pnpm cli sync partitions --code ${matrixCode}`);
          process.exitCode = 1;
          return;
        }

        // Data sync implementation would go here
        console.log(
          `Data sync for ${matrixCode} is not yet fully implemented.`
        );
        console.log("Partition exists: " + partitionName);
        console.log(
          "\nThe infrastructure is ready. Statistical data sync coming soon!"
        );
      } catch (error) {
        console.error(`Error: ${(error as Error).message}`);
        process.exitCode = 1;
      } finally {
        await closeConnection();
      }
    });

  // sync status
  sync
    .command("status")
    .description("Show sync status for all matrices")
    .option("--failed", "Show only failed syncs")
    .action(async (options: { failed?: boolean }) => {
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

  // sync guide - detailed workflow documentation
  sync
    .command("guide")
    .description("Show detailed sync workflow and partition information")
    .action(() => {
      console.log(`
╔══════════════════════════════════════════════════════════════════════════════╗
║                           INS TEMPO SYNC GUIDE                               ║
╠══════════════════════════════════════════════════════════════════════════════╣

OVERVIEW
────────────────────────────────────────────────────────────────────────────────
This CLI syncs data from the Romanian National Institute of Statistics (INS)
Tempo API into a PostgreSQL database using a three-layer architecture:

  1. RAW LAYER      → Stores API responses for debugging
  2. CANONICAL LAYER → Normalized entities (territories, time periods, etc.)
  3. FACT LAYER     → Statistical data in partitioned tables


COMPLETE SYNC WORKFLOW
────────────────────────────────────────────────────────────────────────────────

STEP 1: Initial Metadata Sync
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  pnpm cli sync all                                                      │
  │  └─ Syncs contexts (domains), matrices (datasets), and metadata         │
  │                                                                         │
  │  OR step-by-step:                                                       │
  │    pnpm cli sync contexts             # Domain hierarchy (~340)         │
  │    pnpm cli sync territories          # NUTS hierarchy (55)             │
  │    pnpm cli sync matrices             # Matrix catalog (~1900)          │
  │    pnpm cli sync matrices --full      # Full metadata (SLOW: ~4 hours)  │
  └─────────────────────────────────────────────────────────────────────────┘

STEP 2: Create Statistics Partitions
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  pnpm cli sync partitions                                               │
  │  └─ Creates partition tables for each SYNCED matrix                     │
  │                                                                         │
  │  Options:                                                               │
  │    --dry-run             Preview what partitions will be created        │
  │    --code <MATRIX_CODE>  Create partition for specific matrix only      │
  │    --all                 Create for ALL matrices (not just SYNCED)      │
  └─────────────────────────────────────────────────────────────────────────┘

STEP 3: Sync Statistical Data
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  pnpm cli sync data <MATRIX_CODE>                                       │
  │  └─ Syncs actual statistical values for a specific matrix               │
  │                                                                         │
  │  Example: pnpm cli sync data POP105A                                    │
  └─────────────────────────────────────────────────────────────────────────┘


WHY PARTITIONS?
────────────────────────────────────────────────────────────────────────────────
The statistics tables are partitioned by matrix_id for:
  • Query performance (each matrix's data is in its own table)
  • Efficient data management (easy to drop/refresh a single matrix)
  • Parallel loading (multiple matrices can sync concurrently)

Partition structure:
  statistics                          ← Parent table (empty)
   ├── statistics_default             ← Fallback (should be empty)
   ├── statistics_matrix_1            ← POP105A data
   ├── statistics_matrix_2            ← POP106A data
   └── ...

  statistic_classifications           ← Junction table for classifications
   ├── statistic_classifications_default
   ├── statistic_classifications_matrix_1
   └── ...


MONITORING SYNC STATUS
────────────────────────────────────────────────────────────────────────────────
  pnpm cli sync status            # Show status of all matrices
  pnpm cli sync status --failed   # Show only failed syncs


TROUBLESHOOTING
────────────────────────────────────────────────────────────────────────────────
  • If "sync data" fails with "No partition exists":
    → Run: pnpm cli sync partitions --code <MATRIX_CODE>

  • If metadata sync fails for a matrix:
    → Retry: pnpm cli sync matrices --code <MATRIX_CODE>

  • To resync a matrix from scratch:
    → The partition will be reused, data will be upserted

╚══════════════════════════════════════════════════════════════════════════════╝
`);
    });

  // sync all
  sync
    .command("all")
    .description(
      "Run full sync: contexts, territories, matrices catalog and metadata"
    )
    .option("--skip-metadata", "Skip matrix metadata sync (only sync catalog)")
    .action(async (options: { skipMetadata?: boolean }) => {
      const spinner = ora("Starting full sync...").start();

      try {
        const orchestrator = new SyncOrchestrator(db);
        orchestrator.setProgressCallback((progress) => {
          spinner.text = `${progress.phase}: ${String(progress.current)}/${String(progress.total)} (${progress.currentItem ?? ""})`;
        });

        const result = await orchestrator.syncAll({
          contexts: true,
          matrices: true,
          metadata: options.skipMetadata !== true,
        });

        console.log("\n" + "═".repeat(70));
        console.log("SYNC COMPLETED");
        console.log("═".repeat(70));
        console.log(`  Contexts:   ${String(result.contexts)}`);
        console.log(`  Matrices:   ${String(result.matrices)}`);
        console.log(`  Dimensions: ${String(result.dimensions)}`);
        console.log(`  Nom Items:  ${String(result.nomItems)}`);
        console.log(`  Errors:     ${String(result.errors.length)}`);
        console.log("═".repeat(70));

        if (result.errors.length > 0) {
          console.log("\nErrors:");
          for (const error of result.errors.slice(0, 10)) {
            console.log(`  - ${error}`);
          }
          if (result.errors.length > 10) {
            console.log(`  ... and ${String(result.errors.length - 10)} more`);
          }
        }

        // Show next steps
        console.log("\n" + "─".repeat(70));
        console.log("NEXT STEPS");
        console.log("─".repeat(70));
        console.log(
          "  1. Create statistics partitions (required before data sync):"
        );
        console.log("       pnpm cli sync partitions");
        console.log("");
        console.log("  2. Sync statistical data for specific matrices:");
        console.log("       pnpm cli sync data POP105A");
        console.log("─".repeat(70));

        spinner.succeed("Full sync completed");
      } catch (error) {
        spinner.fail(`Sync failed: ${(error as Error).message}`);
        process.exitCode = 1;
      } finally {
        await closeConnection();
      }
    });
}
