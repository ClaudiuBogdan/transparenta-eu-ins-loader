import { sql } from "kysely";
import ora from "ora";

import { db, closeConnection } from "../../db/connection.js";
import { DataSyncService } from "../../services/sync/data.js";
import { SyncOrchestrator } from "../../services/sync/index.js";

import type { Command } from "commander";

// ============================================================================
// Sync Commands
// ============================================================================

export function registerSyncCommand(program: Command): void {
  const sync = program
    .command("sync")
    .description("Synchronize data from INS Tempo API")
    .addHelpText(
      "after",
      `
SYNC WORKFLOW:
══════════════════════════════════════════════════════════════════════════════
The recommended sync order for a fresh database:

  1. pnpm db:migrate             # Creates schema + 2000 pre-created partitions

  2. pnpm cli sync all           # Full sync: contexts, matrices, metadata
     └─ OR step by step:
        pnpm cli sync contexts   # Domain hierarchy
        pnpm cli sync matrices   # Matrix catalog only
        pnpm cli sync matrices --full --skip-existing  # Detailed metadata

  3. pnpm cli sync data <CODE>   # Sync statistical data for a matrix
     └─ Example:
        pnpm cli sync data POP105A

PARTITION STRATEGY:
──────────────────────────────────────────────────────────────────────────────
Statistics partitions (1-2000) are PRE-CREATED at schema initialization.
No manual partition creation is needed for most use cases.

  pnpm cli sync partitions       # Verify partition status
  pnpm cli sync partitions --code <CODE>  # Create partition for ID > 2000
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

  // sync partitions - verify/create statistics partitions
  // NOTE: Partitions for IDs 1-2000 are pre-created at schema initialization.
  // This command is mainly for verification or creating partitions for IDs > 2000.
  sync
    .command("partitions")
    .description(
      "Verify or create statistics partitions (usually not needed - partitions 1-2000 are pre-created)"
    )
    .option("--code <code>", "Create partition for specific matrix only")
    .option(
      "--verify",
      "Verify all matrices have partitions (default behavior)"
    )
    .option("--dry-run", "Show what partitions would be created")
    .action(
      async (options: {
        code?: string;
        verify?: boolean;
        dryRun?: boolean;
      }) => {
        const spinner = ora("Checking partitions...").start();

        try {
          // Check how many partitions exist
          const existingPartitions = await sql<{ tablename: string }>`
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public' AND tablename LIKE 'statistics_matrix_%'
          `.execute(db);

          const existingCount = existingPartitions.rows.length;

          // If just verifying, show status
          if (options.code === undefined && options.dryRun !== true) {
            // Get matrices that might need partitions (ID > 2000)
            const highIdMatrices = await db
              .selectFrom("matrices")
              .select(["id", "ins_code"])
              .where("id", ">", 2000)
              .execute();

            const existingSet = new Set(
              existingPartitions.rows.map((p) => p.tablename)
            );

            const missingHighId = highIdMatrices.filter(
              (m) => !existingSet.has(`statistics_matrix_${String(m.id)}`)
            );

            spinner.succeed(
              `${String(existingCount)} partitions exist (pre-created: 1-2000)`
            );

            if (missingHighId.length > 0) {
              console.log(
                `\n⚠️  ${String(missingHighId.length)} matrices with ID > 2000 need partitions:`
              );
              for (const m of missingHighId.slice(0, 10)) {
                console.log(`    ${m.ins_code} (id: ${String(m.id)})`);
              }
              if (missingHighId.length > 10) {
                console.log(
                  `    ... and ${String(missingHighId.length - 10)} more`
                );
              }
              console.log(
                "\nRun 'pnpm cli sync partitions --code <CODE>' to create missing partitions."
              );
            } else {
              console.log("\n✓ All matrices have partitions.");
            }
            return;
          }

          // Create partition for specific matrix
          if (options.code !== undefined && options.code !== "") {
            const matrix = await db
              .selectFrom("matrices")
              .select(["id", "ins_code"])
              .where("ins_code", "=", options.code)
              .executeTakeFirst();

            if (!matrix) {
              spinner.fail(`Matrix ${options.code} not found`);
              process.exitCode = 1;
              return;
            }

            const partitionName = `statistics_matrix_${String(matrix.id)}`;
            const existingSet = new Set(
              existingPartitions.rows.map((p) => p.tablename)
            );

            if (existingSet.has(partitionName)) {
              spinner.succeed(
                `Partition already exists: ${partitionName} (${options.code})`
              );
              return;
            }

            if (options.dryRun === true) {
              spinner.info(`Dry run: Would create ${partitionName}`);
              return;
            }

            await sql`SELECT create_statistics_partition(${matrix.id})`.execute(
              db
            );
            spinner.succeed(`Created partition: ${partitionName}`);
            return;
          }

          // Dry run for all missing partitions
          if (options.dryRun === true) {
            const allMatrices = await db
              .selectFrom("matrices")
              .select(["id", "ins_code"])
              .execute();

            const existingSet = new Set(
              existingPartitions.rows.map((p) => p.tablename)
            );

            const missing = allMatrices.filter(
              (m) => !existingSet.has(`statistics_matrix_${String(m.id)}`)
            );

            spinner.info(
              `Dry run: ${String(missing.length)} partitions would be created`
            );
            for (const m of missing.slice(0, 20)) {
              console.log(
                `  statistics_matrix_${String(m.id)} (${m.ins_code})`
              );
            }
            if (missing.length > 20) {
              console.log(`  ... and ${String(missing.length - 20)} more`);
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

  // sync data <matrix> - sync statistical data
  sync
    .command("data <matrix>")
    .description("Sync statistical data for a matrix (requires partition)")
    .option("--years <range>", "Year range, e.g., 2020-2024")
    .action(async (matrixCode: string, options: { years?: string }) => {
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

        // Parse year range
        let yearFrom: number | undefined;
        let yearTo: number | undefined;
        if (options.years) {
          const [from, to] = options.years
            .split("-")
            .map((s) => parseInt(s, 10));
          yearFrom = from;
          yearTo = to ?? from;
        }

        const spinner = ora(`Syncing data for ${matrixCode}...`).start();

        const dataService = new DataSyncService(db);
        const result = await dataService.syncData({
          matrixCode,
          yearFrom,
          yearTo,
          onProgress: (progress) => {
            spinner.text = `${progress.phase}: ${String(progress.current)}/${String(progress.total)}`;
          },
        });

        spinner.succeed(
          `Data sync complete: ${String(result.rowsInserted)} inserted, ${String(result.rowsUpdated)} updated`
        );

        if (result.errors.length > 0) {
          console.log(`\nWarnings (${String(result.errors.length)}):`);
          for (const err of result.errors.slice(0, 10)) {
            console.log(`  - ${err}`);
          }
          if (result.errors.length > 10) {
            console.log(`  ... and ${String(result.errors.length - 10)} more`);
          }
        }
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

STEP 1: Database Setup
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  pnpm db:migrate                                                        │
  │  └─ Creates schema + pre-creates 2000 statistics partitions             │
  │     (No manual partition creation needed!)                              │
  └─────────────────────────────────────────────────────────────────────────┘

STEP 2: Metadata Sync
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

STEP 3: Sync Statistical Data
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  pnpm cli sync data <MATRIX_CODE>                                       │
  │  └─ Syncs actual statistical values for a specific matrix               │
  │                                                                         │
  │  Example: pnpm cli sync data POP105A                                    │
  └─────────────────────────────────────────────────────────────────────────┘


PARTITION STRATEGY
────────────────────────────────────────────────────────────────────────────────
Statistics partitions (IDs 1-2000) are PRE-CREATED at schema initialization.

  • No manual 'sync partitions' command is needed for most use cases
  • Currently ~1898 matrices exist; 2000 provides headroom for growth
  • For matrices with ID > 2000, run: pnpm cli sync partitions --code <CODE>

Partition structure:
  statistics                          ← Parent table (routes to partitions)
   ├── statistics_default             ← Fallback (should stay empty)
   ├── statistics_matrix_1            ← Matrix ID 1 data
   ├── statistics_matrix_2            ← Matrix ID 2 data
   └── ... (up to 2000)

  statistic_classifications           ← Junction table for classifications
   ├── statistic_classifications_default
   ├── statistic_classifications_matrix_1
   └── ... (up to 2000)


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
          "  Partitions 1-2000 are pre-created. You can now sync data:"
        );
        console.log("");
        console.log("    pnpm cli sync data POP105A");
        console.log("");
        console.log("  To verify partition status:");
        console.log("    pnpm cli sync partitions");
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
