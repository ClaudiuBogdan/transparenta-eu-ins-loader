/**
 * Upsert Module - Idempotent insert/update for statistics
 *
 * Provides natural key hash computation and upsert operations
 * to ensure sync operations are idempotent.
 */

import { createHash } from "node:crypto";

import { sql, type Kysely } from "kysely";

import type { Database, NewStatistic } from "../../db/types.js";

// ============================================================================
// Types
// ============================================================================

export interface UpsertResult {
  id: number;
  inserted: boolean;
  updated: boolean;
}

export interface StatisticWithHash extends NewStatistic {
  natural_key_hash: string;
}

// ============================================================================
// Hash Computation
// ============================================================================

/**
 * Compute the natural key hash for a statistic.
 *
 * The natural key uniquely identifies a data point:
 * - matrix_id: Which dataset
 * - territory_id: Which location (null for non-territorial data)
 * - time_period_id: Which time period
 * - unit_of_measure_id: Which unit (null if not applicable)
 * - classification_ids: Sorted array of classification value IDs
 *
 * This matches the PostgreSQL function compute_statistic_natural_key().
 */
export function computeNaturalKeyHash(
  matrixId: number,
  territoryId: number | null,
  timePeriodId: number,
  unitOfMeasureId: number | null,
  classificationIds: number[]
): string {
  // Sort classification IDs for deterministic hashing
  const sortedClassIds = [...classificationIds].sort((a, b) => a - b);

  // Build the key string in the same format as the PostgreSQL function
  const key = [
    String(matrixId),
    territoryId !== null ? String(territoryId) : "N",
    String(timePeriodId),
    unitOfMeasureId !== null ? String(unitOfMeasureId) : "N",
    sortedClassIds.join(","),
  ].join(":");

  // Compute SHA-256 hash
  return createHash("sha256").update(key).digest("hex");
}

// ============================================================================
// Upsert Operations
// ============================================================================

/**
 * Upsert a statistic with its natural key hash.
 *
 * Uses PostgreSQL's ON CONFLICT DO UPDATE to:
 * - Insert new rows
 * - Update existing rows (value, version, updated_at)
 *
 * Returns the ID and whether it was inserted or updated.
 *
 * Note: We insert directly into the partition table because PostgreSQL
 * doesn't support ON CONFLICT with partial indexes on partitioned tables.
 */
export async function upsertStatistic(
  db: Kysely<Database>,
  statistic: StatisticWithHash
): Promise<UpsertResult> {
  // Build the partition table name
  const partitionName = `statistics_matrix_${String(statistic.matrix_id)}`;

  // Use raw SQL for proper ON CONFLICT handling
  // Insert directly into the partition to work around PostgreSQL limitation
  const result = await sql<{ id: number; xmax: string }>`
    INSERT INTO ${sql.raw(partitionName)} (
      matrix_id,
      territory_id,
      time_period_id,
      unit_of_measure_id,
      value,
      value_status,
      source_enc_query,
      scraped_at,
      natural_key_hash,
      version
    ) VALUES (
      ${statistic.matrix_id},
      ${statistic.territory_id ?? null},
      ${statistic.time_period_id},
      ${statistic.unit_of_measure_id ?? null},
      ${statistic.value ?? null},
      ${statistic.value_status ?? null},
      ${statistic.source_enc_query ?? null},
      NOW(),
      ${statistic.natural_key_hash},
      1
    )
    ON CONFLICT (natural_key_hash) WHERE natural_key_hash IS NOT NULL
    DO UPDATE SET
      value = EXCLUDED.value,
      value_status = EXCLUDED.value_status,
      source_enc_query = EXCLUDED.source_enc_query,
      scraped_at = NOW(),
      updated_at = NOW(),
      version = ${sql.raw(partitionName)}.version + 1
    RETURNING id, xmax::text
  `.execute(db);

  if (!result.rows[0]) {
    throw new Error("Upsert failed: no row returned");
  }

  const row = result.rows[0];

  // xmax = 0 means INSERT, xmax > 0 means UPDATE
  // This is a PostgreSQL-specific way to detect if row was inserted or updated
  const wasInserted = row.xmax === "0";

  return {
    id: row.id,
    inserted: wasInserted,
    updated: !wasInserted,
  };
}

/**
 * Batch upsert multiple statistics.
 *
 * More efficient than individual upserts for bulk operations.
 * Processes in batches to avoid memory issues with large datasets.
 */
export async function batchUpsertStatistics(
  db: Kysely<Database>,
  statistics: StatisticWithHash[],
  batchSize = 100
): Promise<{ inserted: number; updated: number }> {
  let totalInserted = 0;
  let totalUpdated = 0;

  for (let i = 0; i < statistics.length; i += batchSize) {
    const batch = statistics.slice(i, i + batchSize);

    // Build VALUES clause for batch insert
    const values = batch.map(
      (s) => sql`(
        ${s.matrix_id},
        ${s.territory_id ?? null},
        ${s.time_period_id},
        ${s.unit_of_measure_id ?? null},
        ${s.value ?? null},
        ${s.value_status ?? null},
        ${s.source_enc_query ?? null},
        NOW(),
        ${s.natural_key_hash},
        1
      )`
    );

    const result = await sql<{ id: number; xmax: string }>`
      INSERT INTO statistics (
        matrix_id,
        territory_id,
        time_period_id,
        unit_of_measure_id,
        value,
        value_status,
        source_enc_query,
        scraped_at,
        natural_key_hash,
        version
      ) VALUES ${sql.join(values, sql`, `)}
      ON CONFLICT (natural_key_hash) WHERE natural_key_hash IS NOT NULL
      DO UPDATE SET
        value = EXCLUDED.value,
        value_status = EXCLUDED.value_status,
        source_enc_query = EXCLUDED.source_enc_query,
        scraped_at = NOW(),
        updated_at = NOW(),
        version = statistics.version + 1
      RETURNING id, xmax::text
    `.execute(db);

    for (const row of result.rows) {
      if (row.xmax === "0") {
        totalInserted++;
      } else {
        totalUpdated++;
      }
    }
  }

  return {
    inserted: totalInserted,
    updated: totalUpdated,
  };
}

/**
 * Prepare a statistic record with its natural key hash.
 *
 * Convenience function to add the hash to a statistic object.
 */
export function prepareStatisticWithHash(
  statistic: NewStatistic,
  classificationIds: number[]
): StatisticWithHash {
  const hash = computeNaturalKeyHash(
    statistic.matrix_id,
    statistic.territory_id ?? null,
    statistic.time_period_id,
    statistic.unit_of_measure_id ?? null,
    classificationIds
  );

  return {
    ...statistic,
    natural_key_hash: hash,
  };
}
