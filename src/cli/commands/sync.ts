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

  // sync data-all - bulk sync statistical data for all matrices with metadata
  sync
    .command("data-all")
    .description(
      "Sync statistical data for ALL matrices with metadata (initial bulk sync)"
    )
    .option("--years <range>", "Year range, e.g., 2020-2024")
    .option("--limit <n>", "Limit number of matrices to sync", parseInt)
    .option(
      "--continue-on-error",
      "Continue syncing even if individual matrices fail"
    )
    .action(
      async (options: {
        years?: string;
        limit?: number;
        continueOnError?: boolean;
      }) => {
        const spinner = ora("Preparing bulk data sync...").start();

        try {
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

          // Get all matrices with sync_status = 'SYNCED' (metadata ready)
          let query = db
            .selectFrom("matrices")
            .select(["id", "ins_code", "metadata"])
            .where("sync_status", "=", "SYNCED")
            .orderBy("ins_code");

          if (options.limit !== undefined) {
            query = query.limit(options.limit);
          }

          const matrices = await query.execute();

          if (matrices.length === 0) {
            spinner.fail(
              "No matrices with synced metadata found. Run 'pnpm cli sync all' first."
            );
            process.exitCode = 1;
            return;
          }

          // Check which matrices have partitions
          const existingPartitions = await sql<{ tablename: string }>`
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public' AND tablename LIKE 'statistics_matrix_%'
          `.execute(db);

          const partitionSet = new Set(
            existingPartitions.rows.map((p) => p.tablename)
          );

          const matricesWithPartitions = matrices.filter((m) =>
            partitionSet.has(`statistics_matrix_${String(m.id)}`)
          );

          const skippedNoPartition =
            matrices.length - matricesWithPartitions.length;

          spinner.succeed(
            `Found ${String(matricesWithPartitions.length)} matrices ready for data sync`
          );

          if (skippedNoPartition > 0) {
            console.log(
              `  ⚠️  Skipping ${String(skippedNoPartition)} matrices without partitions`
            );
          }

          const yearsDisplay = options.years ?? "all years";
          console.log("\n" + "═".repeat(80));
          console.log("BULK DATA SYNC");
          console.log("═".repeat(80));
          console.log(`  Matrices:  ${String(matricesWithPartitions.length)}`);
          console.log(`  Years:     ${yearsDisplay}`);
          console.log("═".repeat(80) + "\n");

          const dataService = new DataSyncService(db);
          let successCount = 0;
          let failedCount = 0;
          const failedMatrices: string[] = [];
          let totalInserted = 0;
          let totalUpdated = 0;

          const startTime = Date.now();

          for (let i = 0; i < matricesWithPartitions.length; i++) {
            const matrix = matricesWithPartitions[i];
            if (!matrix) continue;

            const progress = `[${String(i + 1)}/${String(matricesWithPartitions.length)}]`;
            const name =
              (matrix.metadata as { names?: { ro?: string } })?.names?.ro ??
              matrix.ins_code;
            const displayName =
              name.length > 40 ? name.slice(0, 37) + "..." : name;

            spinner.start(
              `${progress} Syncing ${matrix.ins_code} - ${displayName}`
            );

            try {
              const result = await dataService.syncData({
                matrixCode: matrix.ins_code,
                yearFrom,
                yearTo,
                onProgress: (p) => {
                  spinner.text = `${progress} ${matrix.ins_code}: ${p.phase} ${String(p.current)}/${String(p.total)}`;
                },
              });

              totalInserted += result.rowsInserted;
              totalUpdated += result.rowsUpdated;
              successCount++;

              spinner.succeed(
                `${progress} ${matrix.ins_code}: +${String(result.rowsInserted)} inserted, ${String(result.rowsUpdated)} updated`
              );
            } catch (error) {
              failedCount++;
              failedMatrices.push(matrix.ins_code);

              const errorMsg =
                error instanceof Error ? error.message : String(error);
              spinner.fail(`${progress} ${matrix.ins_code}: ${errorMsg}`);

              if (options.continueOnError !== true) {
                console.log(
                  "\nStopping due to error. Use --continue-on-error to skip failures."
                );
                break;
              }
            }
          }

          const duration = Math.round((Date.now() - startTime) / 1000);
          const hours = Math.floor(duration / 3600);
          const minutes = Math.floor((duration % 3600) / 60);
          const seconds = duration % 60;

          console.log("\n" + "═".repeat(80));
          console.log("BULK SYNC COMPLETE");
          console.log("═".repeat(80));
          console.log(`  Success:   ${String(successCount)} matrices`);
          console.log(`  Failed:    ${String(failedCount)} matrices`);
          console.log(`  Inserted:  ${String(totalInserted)} rows`);
          console.log(`  Updated:   ${String(totalUpdated)} rows`);
          console.log(
            `  Duration:  ${String(hours)}h ${String(minutes)}m ${String(seconds)}s`
          );
          console.log("═".repeat(80));

          if (failedMatrices.length > 0) {
            console.log("\nFailed matrices:");
            for (const code of failedMatrices.slice(0, 20)) {
              console.log(`  - ${code}`);
            }
            if (failedMatrices.length > 20) {
              console.log(
                `  ... and ${String(failedMatrices.length - 20)} more`
              );
            }
            console.log("\nRetry failed matrices with:");
            console.log(
              `  for code in ${failedMatrices.slice(0, 10).join(" ")}; do pnpm cli sync data "$code" ${options.years ? `--years ${options.years}` : ""}; done`
            );
          }
        } catch (error) {
          spinner.fail(`Error: ${(error as Error).message}`);
          process.exitCode = 1;
        } finally {
          await closeConnection();
        }
      }
    );

  // sync data-refresh - resync matrices that already have data
  sync
    .command("data-refresh")
    .description("Re-sync data for matrices that already have statistical data")
    .option("--years <range>", "Year range, e.g., 2020-2024")
    .option("--stale-only", "Only refresh matrices marked as STALE")
    .option(
      "--older-than <days>",
      "Refresh matrices synced more than N days ago",
      parseInt
    )
    .option("--limit <n>", "Limit number of matrices to refresh", parseInt)
    .option(
      "--continue-on-error",
      "Continue syncing even if individual matrices fail"
    )
    .action(
      async (options: {
        years?: string;
        staleOnly?: boolean;
        olderThan?: number;
        limit?: number;
        continueOnError?: boolean;
      }) => {
        const spinner = ora("Finding matrices to refresh...").start();

        try {
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

          // Find matrices that have statistics data
          // Join with statistics to find matrices with actual data
          let query = db
            .selectFrom("matrices")
            .innerJoin("statistics", "matrices.id", "statistics.matrix_id")
            .select([
              "matrices.id",
              "matrices.ins_code",
              "matrices.metadata",
              "matrices.sync_status",
              "matrices.last_sync_at",
            ])
            .distinct()
            .orderBy("matrices.ins_code");

          // Apply filters
          if (options.staleOnly === true) {
            query = query.where("matrices.sync_status", "=", "STALE");
          } else {
            query = query.where("matrices.sync_status", "in", [
              "SYNCED",
              "STALE",
            ]);
          }

          if (options.olderThan !== undefined) {
            const cutoffDate = new Date();
            cutoffDate.setDate(cutoffDate.getDate() - options.olderThan);
            query = query.where("matrices.last_sync_at", "<", cutoffDate);
          }

          if (options.limit !== undefined) {
            query = query.limit(options.limit);
          }

          const matrices = await query.execute();

          if (matrices.length === 0) {
            spinner.info("No matrices found matching refresh criteria.");
            if (options.staleOnly === true) {
              console.log("  No matrices are marked as STALE.");
            }
            if (options.olderThan !== undefined) {
              console.log(
                `  No matrices were synced more than ${String(options.olderThan)} days ago.`
              );
            }
            console.log("\nTo sync data for the first time, use:");
            console.log("  pnpm cli sync data-all");
            return;
          }

          spinner.succeed(
            `Found ${String(matrices.length)} matrices with data to refresh`
          );

          const yearsDisplay = options.years ?? "all years";
          console.log("\n" + "═".repeat(80));
          console.log("DATA REFRESH");
          console.log("═".repeat(80));
          console.log(`  Matrices:  ${String(matrices.length)}`);
          console.log(`  Years:     ${yearsDisplay}`);
          if (options.staleOnly === true) {
            console.log("  Filter:    STALE only");
          }
          if (options.olderThan !== undefined) {
            console.log(
              `  Filter:    Older than ${String(options.olderThan)} days`
            );
          }
          console.log("═".repeat(80) + "\n");

          const dataService = new DataSyncService(db);
          let successCount = 0;
          let failedCount = 0;
          const failedMatrices: string[] = [];
          let totalInserted = 0;
          let totalUpdated = 0;

          const startTime = Date.now();

          for (let i = 0; i < matrices.length; i++) {
            const matrix = matrices[i];
            if (!matrix) continue;

            const progress = `[${String(i + 1)}/${String(matrices.length)}]`;
            const name =
              (matrix.metadata as { names?: { ro?: string } })?.names?.ro ??
              matrix.ins_code;
            const displayName =
              name.length > 40 ? name.slice(0, 37) + "..." : name;

            spinner.start(
              `${progress} Refreshing ${matrix.ins_code} - ${displayName}`
            );

            try {
              const result = await dataService.syncData({
                matrixCode: matrix.ins_code,
                yearFrom,
                yearTo,
                onProgress: (p) => {
                  spinner.text = `${progress} ${matrix.ins_code}: ${p.phase} ${String(p.current)}/${String(p.total)}`;
                },
              });

              totalInserted += result.rowsInserted;
              totalUpdated += result.rowsUpdated;
              successCount++;

              spinner.succeed(
                `${progress} ${matrix.ins_code}: +${String(result.rowsInserted)} inserted, ${String(result.rowsUpdated)} updated`
              );
            } catch (error) {
              failedCount++;
              failedMatrices.push(matrix.ins_code);

              const errorMsg =
                error instanceof Error ? error.message : String(error);
              spinner.fail(`${progress} ${matrix.ins_code}: ${errorMsg}`);

              if (options.continueOnError !== true) {
                console.log(
                  "\nStopping due to error. Use --continue-on-error to skip failures."
                );
                break;
              }
            }
          }

          const duration = Math.round((Date.now() - startTime) / 1000);
          const hours = Math.floor(duration / 3600);
          const minutes = Math.floor((duration % 3600) / 60);
          const seconds = duration % 60;

          console.log("\n" + "═".repeat(80));
          console.log("REFRESH COMPLETE");
          console.log("═".repeat(80));
          console.log(`  Success:   ${String(successCount)} matrices`);
          console.log(`  Failed:    ${String(failedCount)} matrices`);
          console.log(`  Inserted:  ${String(totalInserted)} rows`);
          console.log(`  Updated:   ${String(totalUpdated)} rows`);
          console.log(
            `  Duration:  ${String(hours)}h ${String(minutes)}m ${String(seconds)}s`
          );
          console.log("═".repeat(80));

          if (failedMatrices.length > 0) {
            console.log("\nFailed matrices:");
            for (const code of failedMatrices.slice(0, 20)) {
              console.log(`  - ${code}`);
            }
            if (failedMatrices.length > 20) {
              console.log(
                `  ... and ${String(failedMatrices.length - 20)} more`
              );
            }
          }
        } catch (error) {
          spinner.fail(`Error: ${(error as Error).message}`);
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

  // sync worker - process queued sync jobs
  sync
    .command("worker")
    .description(
      "Process queued sync jobs (runs continuously until queue is empty)"
    )
    .option("--once", "Process one job and exit")
    .option("--limit <n>", "Maximum number of jobs to process", Number.parseInt)
    .option(
      "--poll-interval <ms>",
      "Interval to poll for new jobs (default: 5000ms)",
      Number.parseInt
    )
    .action(
      async (options: {
        once?: boolean;
        limit?: number;
        pollInterval?: number;
      }) => {
        const pollInterval = options.pollInterval ?? 5000;
        let processedCount = 0;
        let running = true;

        // Handle graceful shutdown
        const shutdown = () => {
          console.log("\n\nShutting down worker...");
          running = false;
        };
        process.on("SIGINT", shutdown);
        process.on("SIGTERM", shutdown);

        console.log("\n" + "═".repeat(80));
        console.log("SYNC JOB WORKER");
        console.log("═".repeat(80));
        console.log(
          `  Mode:          ${options.once === true ? "Single job" : "Continuous"}`
        );
        if (options.limit !== undefined) {
          console.log(`  Limit:         ${String(options.limit)} jobs`);
        }
        console.log(`  Poll interval: ${String(pollInterval)}ms`);
        console.log("═".repeat(80) + "\n");

        const dataService = new DataSyncService(db);

        while (running) {
          // Check limit
          if (options.limit !== undefined && processedCount >= options.limit) {
            console.log(
              `\nReached job limit (${String(options.limit)}). Stopping.`
            );
            break;
          }

          // Get next pending job (highest priority, oldest first)
          const job = await db
            .selectFrom("sync_jobs")
            .innerJoin("matrices", "sync_jobs.matrix_id", "matrices.id")
            .select([
              "sync_jobs.id",
              "sync_jobs.matrix_id",
              "sync_jobs.year_from",
              "sync_jobs.year_to",
              "sync_jobs.flags",
              "matrices.ins_code",
              "matrices.metadata",
            ])
            .where("sync_jobs.status", "=", "PENDING")
            .orderBy("sync_jobs.priority", "desc")
            .orderBy("sync_jobs.created_at", "asc")
            .executeTakeFirst();

          if (!job) {
            if (options.once === true) {
              console.log("No pending jobs in queue.");
              break;
            }
            // Wait and poll again
            await new Promise((resolve) => setTimeout(resolve, pollInterval));
            continue;
          }

          const matrixName =
            (job.metadata as { names?: { ro?: string } })?.names?.ro ??
            job.ins_code;
          const displayName =
            matrixName.length > 50
              ? matrixName.slice(0, 47) + "..."
              : matrixName;

          const yearRange = `${String(job.year_from ?? "all")}-${String(job.year_to ?? "all")}`;
          const flagsSummary =
            Object.entries(job.flags ?? {})
              .filter(([, v]) => v === true)
              .map(([k]) => k)
              .join(", ") || "defaults";

          console.log(
            `\n─────────────────────────────────────────────────────────────────────`
          );
          console.log(
            `Job #${String(job.id)}: ${job.ins_code} - ${displayName}`
          );
          console.log(`  Years: ${yearRange} | Flags: ${flagsSummary}`);
          console.log(
            `─────────────────────────────────────────────────────────────────────`
          );

          // Mark job as running
          await db
            .updateTable("sync_jobs")
            .set({
              status: "RUNNING",
              started_at: new Date(),
            })
            .where("id", "=", job.id)
            .execute();

          const spinner = ora(`Syncing ${job.ins_code}...`).start();

          try {
            const result = await dataService.syncData({
              matrixCode: job.ins_code,
              yearFrom: job.year_from ?? undefined,
              yearTo: job.year_to ?? undefined,
              onProgress: (p) => {
                spinner.text = `${job.ins_code}: ${p.phase} ${String(p.current)}/${String(p.total)}`;
              },
            });

            // Mark job as completed
            await db
              .updateTable("sync_jobs")
              .set({
                status: "COMPLETED",
                completed_at: new Date(),
                rows_inserted: result.rowsInserted,
                rows_updated: result.rowsUpdated,
                error_message:
                  result.errors.length > 0
                    ? result.errors.slice(0, 5).join("; ")
                    : null,
              })
              .where("id", "=", job.id)
              .execute();

            spinner.succeed(
              `Job #${String(job.id)} completed: +${String(result.rowsInserted)} inserted, ${String(result.rowsUpdated)} updated`
            );
            processedCount++;
          } catch (error) {
            const errorMsg =
              error instanceof Error ? error.message : String(error);

            // Mark job as failed
            await db
              .updateTable("sync_jobs")
              .set({
                status: "FAILED",
                completed_at: new Date(),
                error_message: errorMsg.slice(0, 1000),
              })
              .where("id", "=", job.id)
              .execute();

            spinner.fail(`Job #${String(job.id)} failed: ${errorMsg}`);
            processedCount++;
          }

          if (options.once === true) {
            break;
          }
        }

        // Summary
        console.log("\n" + "═".repeat(80));
        console.log("WORKER STOPPED");
        console.log("═".repeat(80));
        console.log(`  Jobs processed: ${String(processedCount)}`);
        console.log("═".repeat(80));

        await closeConnection();
      }
    );

  // sync jobs - list queued jobs
  sync
    .command("jobs")
    .description("List sync jobs in the queue")
    .option(
      "--status <status>",
      "Filter by status (PENDING, RUNNING, COMPLETED, FAILED, CANCELLED)"
    )
    .option(
      "--limit <n>",
      "Number of jobs to show (default: 20)",
      Number.parseInt
    )
    .action(async (options: { status?: string; limit?: number }) => {
      try {
        const limit = options.limit ?? 20;

        let query = db
          .selectFrom("sync_jobs")
          .innerJoin("matrices", "sync_jobs.matrix_id", "matrices.id")
          .select([
            "sync_jobs.id",
            "sync_jobs.status",
            "sync_jobs.priority",
            "sync_jobs.created_at",
            "sync_jobs.started_at",
            "sync_jobs.completed_at",
            "sync_jobs.rows_inserted",
            "sync_jobs.rows_updated",
            "sync_jobs.error_message",
            "matrices.ins_code",
            "matrices.metadata",
          ])
          .orderBy("sync_jobs.created_at", "desc")
          .limit(limit);

        if (options.status !== undefined) {
          const validStatuses = [
            "PENDING",
            "RUNNING",
            "COMPLETED",
            "FAILED",
            "CANCELLED",
          ];
          if (!validStatuses.includes(options.status.toUpperCase())) {
            console.error(
              `Invalid status. Must be one of: ${validStatuses.join(", ")}`
            );
            process.exitCode = 1;
            return;
          }
          query = query.where(
            "sync_jobs.status",
            "=",
            options.status.toUpperCase() as
              | "PENDING"
              | "RUNNING"
              | "COMPLETED"
              | "FAILED"
              | "CANCELLED"
          );
        }

        const jobs = await query.execute();

        // Get queue summary
        const queueSummary = await db
          .selectFrom("sync_jobs")
          .select(["status", sql<number>`COUNT(*)::int`.as("count")])
          .groupBy("status")
          .execute();

        console.log("\n" + "═".repeat(100));
        console.log("SYNC JOB QUEUE");
        console.log("═".repeat(100));

        // Print summary
        const counts: Record<string, number> = {};
        for (const row of queueSummary) {
          counts[row.status] = row.count;
        }
        console.log(
          `  PENDING: ${String(counts.PENDING ?? 0).padEnd(5)} | ` +
            `RUNNING: ${String(counts.RUNNING ?? 0).padEnd(5)} | ` +
            `COMPLETED: ${String(counts.COMPLETED ?? 0).padEnd(5)} | ` +
            `FAILED: ${String(counts.FAILED ?? 0).padEnd(5)} | ` +
            `CANCELLED: ${String(counts.CANCELLED ?? 0)}`
        );
        console.log("═".repeat(100));

        if (jobs.length === 0) {
          console.log("\nNo jobs found matching criteria.");
        } else {
          console.log(
            "\n" +
              "ID".padEnd(8) +
              "Status".padEnd(12) +
              "Matrix".padEnd(12) +
              "Priority".padEnd(10) +
              "Created".padEnd(12) +
              "Rows".padEnd(12) +
              "Name"
          );
          console.log("─".repeat(100));

          for (const job of jobs) {
            const name =
              (job.metadata as { names?: { ro?: string } })?.names?.ro ??
              job.ins_code;
            const displayName =
              name.length > 30 ? name.slice(0, 27) + "..." : name;
            const created = job.created_at.toISOString().split("T")[0] ?? "";
            const rows =
              job.status === "COMPLETED"
                ? `+${String(job.rows_inserted)}/${String(job.rows_updated)}`
                : "-";

            console.log(
              String(job.id).padEnd(8) +
                job.status.padEnd(12) +
                job.ins_code.padEnd(12) +
                String(job.priority).padEnd(10) +
                created.padEnd(12) +
                rows.padEnd(12) +
                displayName
            );
          }

          if (jobs.length === limit) {
            console.log(`\n... (showing first ${String(limit)} results)`);
          }
        }

        // Show worker hint if there are pending jobs
        if ((counts.PENDING ?? 0) > 0 || (counts.RUNNING ?? 0) > 0) {
          console.log("\n" + "─".repeat(100));
          console.log("To process queued jobs, run:");
          console.log("  pnpm cli sync worker");
          console.log("─".repeat(100));
        }
      } catch (error) {
        console.error(`Error: ${(error as Error).message}`);
        process.exitCode = 1;
      } finally {
        await closeConnection();
      }
    });
}
