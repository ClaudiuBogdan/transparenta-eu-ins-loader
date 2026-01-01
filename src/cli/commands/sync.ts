import { sql } from "kysely";
import ora from "ora";

import { db, closeConnection } from "../../db/connection.js";
import { SYNC_DEFAULTS } from "../../db/types.js";
import { DataSyncService } from "../../services/sync/data.js";
import { SyncOrchestrator } from "../../services/sync/index.js";

import type { Command } from "commander";

// ============================================================================
// Sync Commands
// ============================================================================

const collectOption = (value: string, previous: string[]): string[] => {
  previous.push(value);
  return previous;
};

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

  3. pnpm cli sync data          # Sync statistical data for ALL matrices
     └─ Or sync a single matrix:
        pnpm cli sync data --matrix POP105A

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
              console.log("    pnpm cli sync data --matrix <MATRIX_CODE>");
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

  // sync data - sync statistical data
  sync
    .command("data")
    .description("Sync statistical data (all matrices or specific ones)")
    .option(
      "--matrix <code>",
      "Matrix code to sync (repeatable, defaults to all matrices)",
      collectOption,
      []
    )
    .option("--years <range>", "Year range, e.g., 2020-2024")
    .option(
      "--classifications <mode>",
      "Classification sync mode: totals | all (default: totals)"
    )
    .option("--county <code>", "County code (e.g., AB for Alba)")
    .option("--limit <n>", "Limit number of matrices to sync", parseInt)
    .option(
      "--refresh",
      "Only sync matrices that already have statistical data"
    )
    .option("--stale-only", "Only refresh matrices marked as STALE")
    .option(
      "--older-than <days>",
      "Refresh matrices synced more than N days ago",
      parseInt
    )
    .option(
      "--continue-on-error",
      "Continue syncing even if individual matrices fail"
    )
    .option("--force", "Force re-sync even if checkpoints exist")
    .option("--no-resume", "Do not resume from last checkpoint")
    .option("--verbose", "Enable detailed debug logging")
    .action(
      async (options: {
        matrix?: string[];
        years?: string;
        classifications?: string;
        county?: string;
        limit?: number;
        refresh?: boolean;
        staleOnly?: boolean;
        olderThan?: number;
        continueOnError?: boolean;
        force?: boolean;
        resume?: boolean;
        verbose?: boolean;
      }) => {
        const spinner = ora("Preparing data sync...").start();

        try {
          const matrixCodes = (options.matrix ?? []).map((code) =>
            code.toUpperCase()
          );
          const refreshMode =
            options.refresh === true ||
            options.staleOnly === true ||
            options.olderThan !== undefined;

          if (matrixCodes.length > 0 && refreshMode) {
            spinner.info(
              "Ignoring --refresh/--stale-only/--older-than because --matrix is set."
            );
          }

          // Parse year range
          const currentYear = new Date().getFullYear();
          let yearFrom = 2020;
          let yearTo = currentYear;
          if (options.years) {
            const [fromRaw, toRaw] = options.years.split("-");
            const from = Number.parseInt(fromRaw ?? "", 10);
            const to = Number.parseInt(toRaw ?? fromRaw ?? "", 10);
            if (Number.isNaN(from)) {
              spinner.fail(
                `Invalid year range "${options.years}". Use format YYYY or YYYY-YYYY.`
              );
              process.exitCode = 1;
              return;
            }
            yearFrom = from;
            yearTo = Number.isNaN(to) ? from : to;
          }

          const classificationsRaw = options.classifications?.toLowerCase();
          let classificationMode: "totals-only" | "all" = "totals-only";
          if (classificationsRaw) {
            if (classificationsRaw === "all") {
              classificationMode = "all";
            } else if (
              classificationsRaw === "totals" ||
              classificationsRaw === "totals-only"
            ) {
              classificationMode = "totals-only";
            } else {
              spinner.fail(
                `Invalid classifications mode "${classificationsRaw}". Use "totals" or "all".`
              );
              process.exitCode = 1;
              return;
            }
          }

          let matrices: {
            id: number;
            ins_code: string;
            metadata: unknown;
            sync_status: string | null;
            last_sync_at?: Date | null;
          }[] = [];

          if (matrixCodes.length > 0) {
            const rows = await db
              .selectFrom("matrices")
              .select(["id", "ins_code", "metadata", "sync_status"])
              .where("ins_code", "in", matrixCodes)
              .execute();

            const rowMap = new Map(rows.map((row) => [row.ins_code, row]));
            const missing = matrixCodes.filter((code) => !rowMap.has(code));

            if (missing.length > 0) {
              spinner.fail(
                `Matrix not found: ${missing.join(", ")}. Run 'pnpm cli sync matrices' first.`
              );
              process.exitCode = 1;
              return;
            }

            const notSynced = rows.filter(
              (row) => row.sync_status !== "SYNCED"
            );
            if (notSynced.length > 0) {
              spinner.fail(
                `Metadata not synced for: ${notSynced
                  .map((row) => row.ins_code)
                  .join(", ")}.`
              );
              console.log(
                "Run 'pnpm cli sync matrices --full --skip-existing' first."
              );
              process.exitCode = 1;
              return;
            }

            matrices = matrixCodes.map((code) => rowMap.get(code)!);
          } else if (refreshMode) {
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

            matrices = await query.execute();
          } else {
            let query = db
              .selectFrom("matrices")
              .select(["id", "ins_code", "metadata", "sync_status"])
              .where("sync_status", "=", "SYNCED")
              .orderBy("ins_code");

            if (options.limit !== undefined) {
              query = query.limit(options.limit);
            }

            matrices = await query.execute();
          }

          if (matrices.length === 0) {
            spinner.info("No matrices found matching sync criteria.");
            if (refreshMode) {
              console.log("\nTo sync data for the first time, use:");
              console.log("  pnpm cli sync data");
            }
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

          if (matrixCodes.length > 0 && skippedNoPartition > 0) {
            const missingPartitions = matrices
              .filter(
                (m) => !partitionSet.has(`statistics_matrix_${String(m.id)}`)
              )
              .map((m) => m.ins_code);
            spinner.fail(
              `No partition exists for: ${missingPartitions.join(", ")}.`
            );
            console.log("\nCreate partitions first:");
            for (const code of missingPartitions) {
              console.log(`  pnpm cli sync partitions --code ${code}`);
            }
            process.exitCode = 1;
            return;
          }

          if (matricesWithPartitions.length === 0) {
            spinner.fail("No matrices with partitions are ready for sync.");
            process.exitCode = 1;
            return;
          }

          if (skippedNoPartition > 0) {
            spinner.info(
              `Skipping ${String(skippedNoPartition)} matrices without partitions`
            );
          }

          const yearsDisplay = `${String(yearFrom)}-${String(yearTo)}`;
          const modeLabel = refreshMode ? "DATA REFRESH" : "DATA SYNC";

          spinner.succeed(
            `Found ${String(matricesWithPartitions.length)} matrices ready for data sync`
          );

          const dataService = new DataSyncService(db);
          const resume = options.resume !== false;
          const force = options.force === true;
          const verbose = options.verbose === true;

          const isBulk =
            matrixCodes.length === 0 || matricesWithPartitions.length > 1;

          if (!isBulk) {
            const matrix = matricesWithPartitions[0]!;
            const spinnerSingle = ora(
              `Syncing data for ${matrix.ins_code}...`
            ).start();

            const result = await dataService.syncMatrixFull({
              matrixCode: matrix.ins_code,
              yearFrom,
              yearTo,
              classificationMode,
              countyCode: options.county,
              resume,
              force,
              verbose,
              onProgress: (progress) => {
                if (progress.phase === "planning") {
                  spinnerSingle.text = "Generating sync chunks...";
                } else if (progress.phase === "syncing") {
                  spinnerSingle.text = `${matrix.ins_code}: ${progress.currentChunk ?? ""} [${String(progress.chunksCompleted)}/${String(progress.chunksTotal)}] - ${String(progress.rowsInserted)} rows`;
                } else {
                  spinnerSingle.text = "Updating coverage metrics...";
                }
              },
            });

            const durationSec = Math.round(result.duration / 1000);
            spinnerSingle.succeed(
              `Sync complete: ${String(result.chunksCompleted)} chunks, ` +
                `${String(result.rowsInserted)} inserted, ${String(result.rowsUpdated)} updated ` +
                `(${String(durationSec)}s)`
            );

            if (result.chunksSkipped > 0) {
              console.log(
                `  Skipped: ${String(result.chunksSkipped)} chunks (already synced)`
              );
            }

            if (result.chunksFailed > 0) {
              console.log(`  Failed: ${String(result.chunksFailed)} chunks`);
            }

            if (result.errors.length > 0) {
              console.log(`\nWarnings (${String(result.errors.length)}):`);
              for (const err of result.errors.slice(0, 10)) {
                console.log(`  - ${err}`);
              }
              if (result.errors.length > 10) {
                console.log(
                  `  ... and ${String(result.errors.length - 10)} more`
                );
              }
            }

            return;
          }

          console.log("\n" + "═".repeat(80));
          console.log(modeLabel);
          console.log("═".repeat(80));
          console.log(`  Matrices:  ${String(matricesWithPartitions.length)}`);
          console.log(`  Years:     ${yearsDisplay}`);
          console.log(
            `  Classifications: ${classificationMode === "all" ? "all" : "totals"}`
          );
          if (options.county) {
            console.log(`  County:    ${options.county}`);
          }
          if (refreshMode && options.staleOnly === true) {
            console.log("  Filter:    STALE only");
          }
          if (refreshMode && options.olderThan !== undefined) {
            console.log(
              `  Filter:    Older than ${String(options.olderThan)} days`
            );
          }
          console.log("═".repeat(80) + "\n");

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
              const result = await dataService.syncMatrixFull({
                matrixCode: matrix.ins_code,
                yearFrom,
                yearTo,
                classificationMode,
                countyCode: options.county,
                resume,
                force,
                verbose,
                onProgress: (p) => {
                  if (p.phase === "planning") {
                    spinner.text = `${progress} ${matrix.ins_code}: planning chunks`;
                  } else if (p.phase === "syncing") {
                    spinner.text = `${progress} ${matrix.ins_code}: ${p.currentChunk ?? ""} [${String(p.chunksCompleted)}/${String(p.chunksTotal)}]`;
                  } else {
                    spinner.text = `${progress} ${matrix.ins_code}: updating coverage`;
                  }
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
          console.log(`${modeLabel} COMPLETE`);
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
              `  for code in ${failedMatrices.slice(0, 10).join(" ")}; do pnpm cli sync data --matrix "$code" ${options.years ? `--years ${options.years}` : ""}; done`
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
  │  pnpm cli sync data                                                    │
  │  └─ Syncs statistical data for ALL matrices (default year range)       │
  │                                                                         │
  │  Or sync a specific matrix:                                            │
  │    pnpm cli sync data --matrix POP105A                                 │
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
        console.log("    pnpm cli sync data --matrix POP105A");
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
            const flags = job.flags ?? {};
            const yearFrom = job.year_from ?? SYNC_DEFAULTS.yearFrom;
            const yearTo = job.year_to ?? SYNC_DEFAULTS.yearTo;
            const classificationMode =
              flags.includeAllClassifications === true ||
              flags.fullSync === true
                ? "all"
                : "totals-only";
            const resume = flags.resume !== false;
            const force = flags.force === true;

            const result = await dataService.syncMatrixFull({
              matrixCode: job.ins_code,
              yearFrom,
              yearTo,
              classificationMode,
              cellLimit: flags.chunkSize,
              resume,
              force,
              onProgress: (p) => {
                if (p.phase === "planning") {
                  spinner.text = `${job.ins_code}: planning chunks`;
                } else if (p.phase === "syncing") {
                  spinner.text = `${job.ins_code}: ${p.currentChunk ?? ""} [${String(p.chunksCompleted)}/${String(p.chunksTotal)}]`;
                } else {
                  spinner.text = `${job.ins_code}: updating coverage`;
                }
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

  // sync coverage [matrix] - show sync coverage statistics
  sync
    .command("coverage [matrix]")
    .description("Show sync coverage statistics for a matrix or all matrices")
    .option("--incomplete", "Show only matrices with <100% coverage")
    .action(
      async (
        matrixCode: string | undefined,
        options: { incomplete?: boolean }
      ) => {
        try {
          if (matrixCode) {
            // Show coverage for specific matrix
            const coverage = await db
              .selectFrom("sync_coverage")
              .innerJoin("matrices", "sync_coverage.matrix_id", "matrices.id")
              .select([
                "matrices.ins_code",
                "matrices.metadata",
                "sync_coverage.total_territories",
                "sync_coverage.synced_territories",
                "sync_coverage.total_years",
                "sync_coverage.synced_years",
                "sync_coverage.total_classifications",
                "sync_coverage.synced_classifications",
                "sync_coverage.actual_data_points",
                "sync_coverage.expected_data_points",
                "sync_coverage.null_value_count",
                "sync_coverage.missing_value_count",
                "sync_coverage.first_sync_at",
                "sync_coverage.last_sync_at",
              ])
              .where("matrices.ins_code", "=", matrixCode)
              .executeTakeFirst();

            if (!coverage) {
              console.log(
                `No coverage data for ${matrixCode}. Run sync first.`
              );
              return;
            }

            const matrixName =
              (coverage.metadata as { names?: { ro?: string } })?.names?.ro ??
              matrixCode;
            const territoryPct =
              coverage.total_territories > 0
                ? (
                    (coverage.synced_territories / coverage.total_territories) *
                    100
                  ).toFixed(1)
                : "100.0";
            const yearPct =
              coverage.total_years > 0
                ? (
                    (coverage.synced_years / coverage.total_years) *
                    100
                  ).toFixed(1)
                : "100.0";
            const overallPct =
              coverage.expected_data_points && coverage.expected_data_points > 0
                ? (
                    ((coverage.actual_data_points ?? 0) /
                      coverage.expected_data_points) *
                    100
                  ).toFixed(1)
                : "N/A";

            console.log("\n" + "═".repeat(80));
            console.log(`SYNC COVERAGE: ${matrixCode}`);
            console.log("═".repeat(80));
            console.log(`  Matrix:     ${matrixName}`);
            console.log(
              `  Last Sync:  ${coverage.last_sync_at?.toISOString() ?? "Never"}`
            );
            console.log("─".repeat(80));
            console.log("  COVERAGE SUMMARY");
            console.log("─".repeat(80));
            console.log(
              `  Territories:    ${String(coverage.synced_territories).padStart(6)} / ${String(coverage.total_territories).padStart(6)} (${territoryPct}%)`
            );
            console.log(
              `  Years:          ${String(coverage.synced_years).padStart(6)} / ${String(coverage.total_years).padStart(6)} (${yearPct}%)`
            );
            console.log("─".repeat(80));
            console.log(
              `  Data Points:    ${String(coverage.actual_data_points ?? 0).padStart(10)} (${overallPct}% of expected)`
            );
            console.log(
              `    - Null values:    ${String(coverage.null_value_count ?? 0)}`
            );
            console.log(
              `    - Missing (:):    ${String(coverage.missing_value_count ?? 0)}`
            );
            console.log("═".repeat(80));
          } else {
            // Show coverage summary for all matrices
            let query = db
              .selectFrom("sync_coverage")
              .innerJoin("matrices", "sync_coverage.matrix_id", "matrices.id")
              .select([
                "matrices.ins_code",
                "sync_coverage.total_territories",
                "sync_coverage.synced_territories",
                "sync_coverage.total_years",
                "sync_coverage.synced_years",
                "sync_coverage.actual_data_points",
                "sync_coverage.last_sync_at",
              ])
              .orderBy("sync_coverage.last_sync_at", "desc");

            if (options.incomplete === true) {
              query = query.where(({ eb }) =>
                eb.or([
                  eb(
                    "sync_coverage.synced_territories",
                    "<",
                    eb.ref("sync_coverage.total_territories")
                  ),
                  eb(
                    "sync_coverage.synced_years",
                    "<",
                    eb.ref("sync_coverage.total_years")
                  ),
                ])
              );
            }

            const coverages = await query.limit(50).execute();

            if (coverages.length === 0) {
              console.log("No coverage data found. Run sync first.");
              return;
            }

            console.log("\n" + "═".repeat(100));
            console.log("SYNC COVERAGE SUMMARY");
            console.log("═".repeat(100));
            console.log(
              "  " +
                "Matrix".padEnd(15) +
                "Territories".padEnd(20) +
                "Years".padEnd(15) +
                "Data Points".padEnd(15) +
                "Last Sync"
            );
            console.log("─".repeat(100));

            for (const cov of coverages) {
              const territoryStr = `${String(cov.synced_territories)}/${String(cov.total_territories)}`;
              const yearStr = `${String(cov.synced_years)}/${String(cov.total_years)}`;
              const lastSync = cov.last_sync_at
                ? cov.last_sync_at.toISOString().substring(0, 10)
                : "Never";

              console.log(
                "  " +
                  cov.ins_code.padEnd(15) +
                  territoryStr.padEnd(20) +
                  yearStr.padEnd(15) +
                  String(cov.actual_data_points ?? 0).padEnd(15) +
                  lastSync
              );
            }

            console.log("═".repeat(100));
            console.log(`  Total: ${String(coverages.length)} matrices`);
          }
        } catch (error) {
          console.error(`Error: ${(error as Error).message}`);
          process.exitCode = 1;
        } finally {
          await closeConnection();
        }
      }
    );

  // sync plan - show sync execution plan without executing
  sync
    .command("plan")
    .description("Show sync execution plan for a matrix without executing")
    .requiredOption("--matrix <code>", "Matrix code to plan")
    .option("--years <range>", "Year range, e.g., 2020-2024")
    .option(
      "--classifications <mode>",
      "Classification sync mode: totals | all (default: totals)"
    )
    .option("--county <code>", "County code (e.g., AB for Alba)")
    .action(
      async (options: {
        matrix: string;
        years?: string;
        classifications?: string;
        county?: string;
      }) => {
        // Import dynamically to avoid circular deps
        const { ChunkGenerator, getChunkDisplayName } =
          await import("../../services/sync/chunking.js");

        try {
          const chunkGenerator = new ChunkGenerator(db);
          const matrixCode = options.matrix.toUpperCase();
          const matrixInfo = await chunkGenerator.loadMatrixInfo(matrixCode);

          if (!matrixInfo) {
            console.error(`Matrix ${matrixCode} not found.`);
            process.exitCode = 1;
            return;
          }

          // Parse year range
          const currentYear = new Date().getFullYear();
          let yearFrom = 2020;
          let yearTo = currentYear;
          if (options.years) {
            const [fromRaw, toRaw] = options.years.split("-");
            const from = Number.parseInt(fromRaw ?? "", 10);
            const to = Number.parseInt(toRaw ?? fromRaw ?? "", 10);
            if (Number.isNaN(from)) {
              console.error(
                `Invalid year range "${options.years}". Use format YYYY or YYYY-YYYY.`
              );
              process.exitCode = 1;
              return;
            }
            yearFrom = from;
            yearTo = Number.isNaN(to) ? from : to;
          }

          const classificationsRaw = options.classifications?.toLowerCase();
          let classificationMode: "totals-only" | "all" = "totals-only";
          if (classificationsRaw) {
            if (classificationsRaw === "all") {
              classificationMode = "all";
            } else if (
              classificationsRaw === "totals" ||
              classificationsRaw === "totals-only"
            ) {
              classificationMode = "totals-only";
            } else {
              console.error(
                `Invalid classifications mode "${classificationsRaw}". Use "totals" or "all".`
              );
              process.exitCode = 1;
              return;
            }
          }

          const chunkResult = await chunkGenerator.generateChunks(matrixInfo, {
            yearFrom,
            yearTo,
            classificationMode,
            countyCode: options.county,
          });

          const matrixName = matrixInfo.metadata?.names?.ro ?? matrixCode;
          const totalCells = chunkResult.chunks.reduce(
            (sum, chunk) => sum + chunk.cellCount,
            0
          );
          const totalBytes = chunkResult.chunks.reduce(
            (sum, chunk) => sum + chunk.estimatedCsvBytes,
            0
          );
          const formatBytes = (bytes: number): string => {
            if (bytes >= 1_000_000_000) {
              return `${(bytes / 1_000_000_000).toFixed(2)} GB`;
            }
            if (bytes >= 1_000_000) {
              return `${(bytes / 1_000_000).toFixed(2)} MB`;
            }
            if (bytes >= 1_000) {
              return `${(bytes / 1_000).toFixed(2)} KB`;
            }
            return `${String(bytes)} B`;
          };

          console.log("\n" + "═".repeat(80));
          console.log(
            `SYNC PLAN: ${matrixCode} (${String(yearFrom)}-${String(yearTo)})`
          );
          console.log("═".repeat(80));
          console.log(`  Matrix: ${matrixName}`);
          console.log(`  Classifications: ${classificationMode}`);
          if (options.county) {
            console.log(`  County: ${options.county}`);
          }
          console.log("");

          if (chunkResult.hasUatData) {
            console.log(
              "  Matrix has UAT-level data. Will chunk by county and year."
            );
          } else if (chunkResult.hasCountyData) {
            console.log("  Matrix has county-level data. Will chunk by year.");
          } else {
            console.log("  Matrix has national-level data only.");
          }

          console.log("");
          console.log("  EXECUTION PLAN:");
          console.log(
            `    Total chunks:     ${String(chunkResult.chunks.length)}`
          );
          console.log(
            `    Est. API calls:   ${String(chunkResult.estimatedApiCalls)}`
          );
          console.log(`    Est. cells:       ${String(totalCells)}`);
          console.log(`    Est. CSV size:    ${formatBytes(totalBytes)}`);
          console.log(`    Est. duration:    ${chunkResult.estimatedDuration}`);

          // Show sample chunks
          console.log("");
          console.log("  SAMPLE CHUNKS:");
          const sampleChunks = chunkResult.chunks.slice(0, 5);
          for (const chunk of sampleChunks) {
            console.log(
              `    - ${getChunkDisplayName(chunk)} (${String(chunk.cellCount)} cells)`
            );
          }
          if (chunkResult.chunks.length > 5) {
            console.log(
              `    ... and ${String(chunkResult.chunks.length - 5)} more`
            );
          }

          // Check existing checkpoints
          const chunkHashes = chunkResult.chunks.map(
            (chunk) => chunk.chunkHash
          );
          let checkpointCount = 0;
          if (chunkHashes.length > 0) {
            const checkpoints = await db
              .selectFrom("sync_checkpoints")
              .select(["chunk_hash"])
              .where("matrix_id", "=", matrixInfo.id)
              .where("error_message", "is", null)
              .where("cells_returned", "is not", null)
              .where("chunk_hash", "in", chunkHashes)
              .execute();

            checkpointCount = checkpoints.length;
          }

          console.log("");
          console.log(
            `  Already synced:     ${String(checkpointCount)} chunks`
          );
          console.log(
            `  Remaining:          ${String(chunkResult.chunks.length - checkpointCount)} chunks`
          );

          console.log("");
          console.log("═".repeat(80));
          console.log("  Run to start sync:");
          const commandParts = [
            "pnpm cli sync data",
            `--matrix ${matrixCode}`,
            `--years ${String(yearFrom)}-${String(yearTo)}`,
          ];
          if (classificationMode === "all") {
            commandParts.push("--classifications all");
          }
          if (options.county) {
            commandParts.push(`--county ${options.county}`);
          }
          console.log(`    ${commandParts.join(" ")}`);
          console.log("═".repeat(80));
        } catch (error) {
          console.error(`Error: ${(error as Error).message}`);
          process.exitCode = 1;
        } finally {
          await closeConnection();
        }
      }
    );
}
