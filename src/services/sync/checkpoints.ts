/**
 * Checkpoint Service - Track sync progress per chunk
 *
 * Enables incremental sync by tracking the last sync time for each
 * chunk (enc_query) of a matrix. This allows skipping already-synced
 * chunks when running sync again.
 */

import { createHash } from "node:crypto";

import type {
  Database,
  DataSyncCheckpoint,
  NewDataSyncCheckpoint,
} from "../../db/types.js";
import type { Kysely } from "kysely";

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Compute SHA-256 hash of the enc_query string.
 *
 * Used for the unique constraint since the full enc_query can exceed
 * PostgreSQL's B-tree index limit of ~8KB for matrices with many localities.
 */
function computeEncQueryHash(encQuery: string): string {
  return createHash("sha256").update(encQuery).digest("hex");
}

// ============================================================================
// Types
// ============================================================================

export interface CheckpointInfo {
  lastScrapedAt: Date;
  rowCount: number;
}

// ============================================================================
// Checkpoint Service
// ============================================================================

export class SyncCheckpointService {
  constructor(private db: Kysely<Database>) {}

  /**
   * Get the last checkpoint for a matrix chunk.
   *
   * Returns null if the chunk has never been synced.
   */
  async getLastCheckpoint(
    matrixId: number,
    chunkEncQuery: string
  ): Promise<CheckpointInfo | null> {
    const hash = computeEncQueryHash(chunkEncQuery);

    const checkpoint = await this.db
      .selectFrom("data_sync_checkpoints")
      .select(["last_scraped_at", "row_count"])
      .where("matrix_id", "=", matrixId)
      .where("chunk_enc_query_hash", "=", hash)
      .executeTakeFirst();

    if (!checkpoint) {
      return null;
    }

    return {
      lastScrapedAt: checkpoint.last_scraped_at,
      rowCount: checkpoint.row_count,
    };
  }

  /**
   * Save a checkpoint after successfully syncing a chunk.
   *
   * Uses upsert to update existing checkpoints.
   */
  async saveCheckpoint(
    matrixId: number,
    chunkEncQuery: string,
    rowCount: number
  ): Promise<void> {
    const now = new Date();
    const hash = computeEncQueryHash(chunkEncQuery);

    await this.db
      .insertInto("data_sync_checkpoints")
      .values({
        matrix_id: matrixId,
        chunk_enc_query_hash: hash,
        chunk_enc_query: chunkEncQuery,
        last_scraped_at: now,
        row_count: rowCount,
      } as NewDataSyncCheckpoint)
      .onConflict((oc) =>
        oc.columns(["matrix_id", "chunk_enc_query_hash"]).doUpdateSet({
          last_scraped_at: now,
          row_count: rowCount,
          updated_at: now,
        })
      )
      .execute();
  }

  /**
   * Check if a chunk should be re-synced.
   *
   * Returns true if:
   * - forceRefresh is true
   * - The chunk has never been synced
   * - The chunk was synced more than maxAge ago
   */
  async shouldResync(
    matrixId: number,
    chunkEncQuery: string,
    options: {
      forceRefresh?: boolean;
      maxAgeMs?: number;
    } = {}
  ): Promise<boolean> {
    const { forceRefresh = false, maxAgeMs } = options;

    if (forceRefresh) {
      return true;
    }

    const checkpoint = await this.getLastCheckpoint(matrixId, chunkEncQuery);

    if (!checkpoint) {
      return true;
    }

    if (maxAgeMs !== undefined) {
      const age = Date.now() - checkpoint.lastScrapedAt.getTime();
      return age > maxAgeMs;
    }

    // Default: don't resync if already synced
    return false;
  }

  /**
   * Get all checkpoints for a matrix.
   *
   * Useful for displaying sync status.
   */
  async getMatrixCheckpoints(matrixId: number): Promise<DataSyncCheckpoint[]> {
    return await this.db
      .selectFrom("data_sync_checkpoints")
      .selectAll()
      .where("matrix_id", "=", matrixId)
      .orderBy("last_scraped_at", "desc")
      .execute();
  }

  /**
   * Get the total row count for a matrix from all checkpoints.
   */
  async getMatrixTotalRows(matrixId: number): Promise<number> {
    const result = await this.db
      .selectFrom("data_sync_checkpoints")
      .select((eb) => eb.fn.sum<number>("row_count").as("total"))
      .where("matrix_id", "=", matrixId)
      .executeTakeFirst();

    return result?.total ?? 0;
  }

  /**
   * Delete all checkpoints for a matrix.
   *
   * Use this when you want to force a full resync of a matrix.
   */
  async clearMatrixCheckpoints(matrixId: number): Promise<number> {
    const result = await this.db
      .deleteFrom("data_sync_checkpoints")
      .where("matrix_id", "=", matrixId)
      .executeTakeFirst();

    return Number(result.numDeletedRows ?? 0);
  }

  /**
   * Get checkpoint statistics for a matrix.
   */
  async getMatrixCheckpointStats(matrixId: number): Promise<{
    chunkCount: number;
    totalRows: number;
    oldestSync: Date | null;
    newestSync: Date | null;
  }> {
    const result = await this.db
      .selectFrom("data_sync_checkpoints")
      .select((eb) => [
        eb.fn.count<number>("id").as("chunk_count"),
        eb.fn.sum<number>("row_count").as("total_rows"),
        eb.fn.min("last_scraped_at").as("oldest_sync"),
        eb.fn.max("last_scraped_at").as("newest_sync"),
      ])
      .where("matrix_id", "=", matrixId)
      .executeTakeFirst();

    return {
      chunkCount: result?.chunk_count ?? 0,
      totalRows: result?.total_rows ?? 0,
      oldestSync: result?.oldest_sync ?? null,
      newestSync: result?.newest_sync ?? null,
    };
  }
}
