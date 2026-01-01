import fs from "fs";
import path from "path";

import { parse } from "csv-parse/sync";
import { sql, type Kysely } from "kysely";
import ora from "ora";

import { db, closeConnection } from "../../db/connection.js";

import type {
  TerritoryLevel,
  SirutaMetadata,
  Database,
} from "../../db/types.js";
import type { Command } from "commander";

// ============================================================================
// Types
// ============================================================================

interface SeedRow {
  id: string;
  code: string;
  siruta_code: string;
  level: string;
  parent_code: string;
  name_ro: string;
  nuts: string;
  type: string;
  urban: string;
  source: string;
}

interface TerritoryRecord {
  id: number;
  code: string;
  siruta_code: string | null;
  level: TerritoryLevel;
  parent_code: string | null;
  path: string;
  parent_id: number | null;
  name: string;
  siruta_metadata: SirutaMetadata | null;
}

// ============================================================================
// Seed Command
// ============================================================================

export function registerSeedCommand(program: Command): void {
  const seed = program
    .command("seed")
    .description("Seed database tables from CSV files");

  // seed territories
  seed
    .command("territories")
    .description("Seed territories table from CSV file")
    .option(
      "-f, --file <path>",
      "Path to CSV seed file",
      "seed/territories.csv"
    )
    .option("--dry-run", "Parse and validate without making changes")
    .option(
      "--force",
      "Force clean seed (TRUNCATE instead of upsert) - WARNING: deletes linked data"
    )
    .action(
      async (options: { file: string; dryRun?: boolean; force?: boolean }) => {
        const spinner = ora("Loading seed file...").start();

        try {
          // Resolve file path
          const filePath = path.isAbsolute(options.file)
            ? options.file
            : path.join(process.cwd(), options.file);

          if (!fs.existsSync(filePath)) {
            throw new Error(`Seed file not found: ${filePath}`);
          }

          // Parse CSV
          const content = fs.readFileSync(filePath, "utf-8");
          const rows: SeedRow[] = parse(content, {
            columns: true,
            skip_empty_lines: true,
            trim: true,
          });

          spinner.text = `Parsed ${String(rows.length)} rows from seed file`;

          // Build records with parent lookup
          const records: TerritoryRecord[] = [];
          const codeToRecord = new Map<string, TerritoryRecord>();

          // First pass: create all records
          for (const row of rows) {
            const record: TerritoryRecord = {
              id: parseInt(row.id, 10),
              code: row.code,
              siruta_code: row.siruta_code || null,
              level: row.level as TerritoryLevel,
              parent_code: row.parent_code || null,
              path: "", // Will be computed
              parent_id: null, // Will be resolved
              name: row.name_ro,
              siruta_metadata: null,
            };

            // Build siruta_metadata if available
            if (row.type || row.urban || row.nuts) {
              record.siruta_metadata = {};
              if (row.type) record.siruta_metadata.tip = parseInt(row.type, 10);
              if (row.urban)
                record.siruta_metadata.med = parseInt(row.urban, 10);
            }

            records.push(record);
            codeToRecord.set(row.code, record);
          }

          // Second pass: resolve parent_id and build paths
          spinner.text = "Resolving parent relationships and building paths...";

          for (const record of records) {
            if (record.parent_code) {
              const parent = codeToRecord.get(record.parent_code);
              if (parent) {
                record.parent_id = parent.id;
              } else {
                throw new Error(
                  `Parent not found for ${record.code}: ${record.parent_code}`
                );
              }
            }

            // Build ltree path
            record.path = buildPath(record, codeToRecord);
          }

          // Validation
          spinner.text = "Validating data...";
          const validation = validateRecords(records, codeToRecord);
          if (!validation.valid) {
            throw new Error(
              `Validation failed:\n${validation.errors.join("\n")}`
            );
          }

          // Stats
          const levelCounts = new Map<string, number>();
          for (const r of records) {
            levelCounts.set(r.level, (levelCounts.get(r.level) ?? 0) + 1);
          }

          console.log("\n📊 Seed file statistics:");
          console.log(`  Total records: ${String(records.length)}`);
          for (const [level, count] of levelCounts) {
            console.log(`  ${level}: ${String(count)}`);
          }

          if (options.dryRun) {
            spinner.succeed("Dry run completed - no changes made");
            console.log("\n✅ Validation passed, ready to seed");
            return;
          }

          // Execute seeding in transaction
          spinner.text = "Seeding territories...";

          const seedMode: "force" | "upsert" = options.force
            ? "force"
            : "upsert";

          await db.transaction().execute(async (trx) => {
            if (seedMode === "force") {
              // Force mode: TRUNCATE and re-insert (clean slate)
              spinner.text = "Force mode: Truncating territories...";
              await sql`TRUNCATE territories CASCADE`.execute(trx);

              spinner.text = "Inserting territories...";
              await insertTerritories(trx, records, spinner);

              // Reset sequence
              spinner.text = "Resetting ID sequence...";
              const maxId = Math.max(...records.map((r) => r.id));
              await sql`SELECT setval('territories_id_seq', ${maxId}, true)`.execute(
                trx
              );

              console.log(
                `\n📊 Force seed: ${String(records.length)} territories inserted (clean slate)`
              );
            } else {
              // Upsert mode: preserve existing data
              spinner.text = "Fetching existing territories...";
              const existingTerritories = await trx
                .selectFrom("territories")
                .select(["code", "id"])
                .execute();
              const existingCodes = new Set(
                existingTerritories.map((t) => t.code)
              );
              const seedCodes = new Set(records.map((r) => r.code));

              // Find territories that were removed from seed
              const removedCodes = [...existingCodes].filter(
                (code) => !seedCodes.has(code)
              );

              // Upsert territories
              spinner.text = "Upserting territories...";
              const { inserted, updated } = await upsertTerritories(
                trx,
                records,
                existingCodes,
                spinner
              );

              // Report removed territories (don't delete to preserve linked data)
              if (removedCodes.length > 0) {
                console.log(
                  `\n⚠️  ${String(removedCodes.length)} territories exist in DB but not in seed:`
                );
                for (const code of removedCodes.slice(0, 10)) {
                  console.log(`    - ${code}`);
                }
                if (removedCodes.length > 10) {
                  console.log(
                    `    ... and ${String(removedCodes.length - 10)} more`
                  );
                }
                console.log(
                  "    These were NOT deleted to preserve linked statistics."
                );
              }

              // Update sequence to max id
              spinner.text = "Updating ID sequence...";
              const maxId = Math.max(...records.map((r) => r.id));
              await sql`SELECT setval('territories_id_seq', GREATEST(${maxId}, (SELECT COALESCE(MAX(id), 0) FROM territories)), true)`.execute(
                trx
              );

              console.log(
                `\n📊 Upsert summary: ${String(inserted)} inserted, ${String(updated)} updated`
              );
            }
          });

          // Verify
          spinner.text = "Verifying...";
          const result = await db
            .selectFrom("territories")
            .select(db.fn.count<number>("id").as("count"))
            .executeTakeFirstOrThrow();

          const count = result.count;
          // In upsert mode, count may be >= records.length (if removed territories were kept)
          // In force mode, count should equal records.length
          if (seedMode === "force" && count !== records.length) {
            throw new Error(
              `Verification failed: expected ${String(records.length)} rows, got ${String(count)}`
            );
          } else if (seedMode === "upsert" && count < records.length) {
            throw new Error(
              `Verification failed: expected at least ${String(records.length)} rows, got ${String(count)}`
            );
          }

          spinner.succeed(`Successfully seeded ${String(count)} territories`);

          // Show sample of major cities
          console.log("\n🏙️  Sample major cities:");
          const majorCities = [
            "MUNICIPIUL BUCURESTI",
            "MUNICIPIUL CLUJ-NAPOCA",
            "MUNICIPIUL TIMISOARA",
            "MUNICIPIUL IASI",
            "MUNICIPIUL CONSTANTA",
          ];
          const samples = await db
            .selectFrom("territories as t")
            .innerJoin("territories as p", "t.parent_id", "p.id")
            .select(["t.code", "t.name", "t.level", "p.code as county_code"])
            .where("t.level", "=", "LAU")
            .where((eb) =>
              eb.or(
                majorCities.map((city) =>
                  eb(sql`UPPER(${eb.ref("t.name")})`, "like", `%${city}%`)
                )
              )
            )
            .execute();

          for (const city of samples) {
            console.log(`  ${city.name} → County: ${city.county_code}`);
          }
        } catch (error) {
          spinner.fail(`Seeding failed: ${(error as Error).message}`);
          process.exitCode = 1;
        } finally {
          await closeConnection();
        }
      }
    );
}

// ============================================================================
// Helper Functions
// ============================================================================

function buildPath(
  record: TerritoryRecord,
  codeToRecord: Map<string, TerritoryRecord>
): string {
  const parts: string[] = [];
  let current: TerritoryRecord | undefined = record;

  while (current) {
    // Use siruta_code for LAU, code otherwise
    const pathPart =
      current.level === "LAU" && current.siruta_code
        ? current.siruta_code
        : current.code;
    parts.unshift(pathPart);

    if (current.parent_code) {
      current = codeToRecord.get(current.parent_code);
    } else {
      current = undefined;
    }
  }

  return parts.join(".");
}

interface ValidationResult {
  valid: boolean;
  errors: string[];
}

function validateRecords(
  records: TerritoryRecord[],
  codeToRecord: Map<string, TerritoryRecord>
): ValidationResult {
  const errors: string[] = [];

  // Check for duplicate codes
  const codes = new Set<string>();
  for (const r of records) {
    if (codes.has(r.code)) {
      errors.push(`Duplicate code: ${r.code}`);
    }
    codes.add(r.code);
  }

  // Check for duplicate siruta_codes (among non-null)
  const sirutaCodes = new Set<string>();
  for (const r of records) {
    if (r.siruta_code) {
      if (sirutaCodes.has(r.siruta_code)) {
        errors.push(`Duplicate siruta_code: ${r.siruta_code}`);
      }
      sirutaCodes.add(r.siruta_code);
    }
  }

  // Check parent references
  for (const r of records) {
    if (r.parent_code && !codeToRecord.has(r.parent_code)) {
      errors.push(`Missing parent for ${r.code}: ${r.parent_code}`);
    }
  }

  // Check paths are valid
  for (const r of records) {
    if (!r.path || r.path === "") {
      errors.push(`Empty path for ${r.code}`);
    }
  }

  // Check NATIONAL has no parent
  const national = records.find((r) => r.level === "NATIONAL");
  if (national && national.parent_id !== null) {
    errors.push("NATIONAL level should have no parent");
  }

  // Check all 42 counties exist
  const counties = records.filter((r) => r.level === "NUTS3");
  if (counties.length !== 42) {
    errors.push(`Expected 42 counties, found ${String(counties.length)}`);
  }

  // Check each county has LAU children
  const lauByParent = new Map<string, number>();
  for (const r of records) {
    if (r.level === "LAU" && r.parent_code) {
      lauByParent.set(r.parent_code, (lauByParent.get(r.parent_code) ?? 0) + 1);
    }
  }

  for (const county of counties) {
    const lauCount = lauByParent.get(county.code) ?? 0;
    if (lauCount === 0) {
      errors.push(`County ${county.code} has no LAU children`);
    }
  }

  return {
    valid: errors.length === 0,
    errors,
  };
}

// Insert territories in batches (for force mode)
async function insertTerritories(
  trx: Kysely<Database>,
  records: TerritoryRecord[],
  spinner: ReturnType<typeof ora>
): Promise<void> {
  const batchSize = 500;

  for (let i = 0; i < records.length; i += batchSize) {
    const batch = records.slice(i, i + batchSize);
    spinner.text = `Inserting territories... ${String(i + batch.length)}/${String(records.length)}`;

    for (const r of batch) {
      await sql`
        INSERT INTO territories (id, code, siruta_code, level, path, parent_id, name, siruta_metadata)
        VALUES (
          ${r.id},
          ${r.code},
          ${r.siruta_code},
          ${r.level},
          ${r.path}::ltree,
          ${r.parent_id},
          ${r.name},
          ${r.siruta_metadata ? JSON.stringify(r.siruta_metadata) : null}::jsonb
        )
      `.execute(trx);
    }
  }
}

// Upsert territories (INSERT ... ON CONFLICT UPDATE)
async function upsertTerritories(
  trx: Kysely<Database>,
  records: TerritoryRecord[],
  existingCodes: Set<string>,
  spinner: ReturnType<typeof ora>
): Promise<{ inserted: number; updated: number }> {
  const batchSize = 500;
  let inserted = 0;
  let updated = 0;

  for (let i = 0; i < records.length; i += batchSize) {
    const batch = records.slice(i, i + batchSize);
    spinner.text = `Upserting territories... ${String(i + batch.length)}/${String(records.length)}`;

    for (const r of batch) {
      const isNew = !existingCodes.has(r.code);

      await sql`
        INSERT INTO territories (id, code, siruta_code, level, path, parent_id, name, siruta_metadata)
        VALUES (
          ${r.id},
          ${r.code},
          ${r.siruta_code},
          ${r.level},
          ${r.path}::ltree,
          ${r.parent_id},
          ${r.name},
          ${r.siruta_metadata ? JSON.stringify(r.siruta_metadata) : null}::jsonb
        )
        ON CONFLICT (code) DO UPDATE SET
          siruta_code = EXCLUDED.siruta_code,
          level = EXCLUDED.level,
          path = EXCLUDED.path,
          parent_id = EXCLUDED.parent_id,
          name = EXCLUDED.name,
          siruta_metadata = EXCLUDED.siruta_metadata,
          updated_at = NOW()
      `.execute(trx);

      if (isNew) {
        inserted++;
      } else {
        updated++;
      }
    }
  }

  return { inserted, updated };
}
