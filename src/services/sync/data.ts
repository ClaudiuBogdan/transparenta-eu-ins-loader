/**
 * Data Sync Service - Syncs statistical data from INS Tempo API
 *
 * Flow:
 * 1. Load matrix dimensions and nom_items from database
 * 2. Generate chunks (by county + year for UAT data)
 * 3. For each chunk:
 *    a. Check checkpoint - skip if already synced
 *    b. Build encQuery with chunk selections
 *    c. Query INS API pivot endpoint
 *    d. Parse CSV and map to canonical entities
 *    e. Insert into statistics table
 *    f. Record checkpoint
 * 4. Update coverage metrics
 */

import { createHash } from "node:crypto";

import { sql, type Kysely } from "kysely";

import {
  ChunkGenerator,
  getChunkDisplayName,
  type SyncChunk,
  type ClassificationMode,
} from "./chunking.js";
import { apiLogger } from "../../logger.js";
import {
  queryMatrix,
  buildEncQuery,
  estimateCellCount,
} from "../../scraper/client.js";

import type { Database, DimensionType } from "../../db/types.js";

// ============================================================================
// Lease Constants
// ============================================================================

/** Lease duration for chunk processing (2 minutes) */
const CHUNK_LEASE_DURATION_MS = 2 * 60 * 1000;

/** Generate a unique worker ID for this process */
function generateWorkerId(): string {
  const hostname = process.env.HOSTNAME ?? "local";
  const pid = process.pid;
  const random = Math.random().toString(36).substring(2, 8);
  return `${hostname}-${String(pid)}-${random}`;
}

// ============================================================================
// Types
// ============================================================================

interface NomItemMapping {
  nomItemId: number;
  dimensionType: DimensionType;
  territoryId: number | null;
  timePeriodId: number | null;
  classificationValueId: number | null;
  unitId: number | null;
  labelRo: string;
}

interface DimensionInfo {
  dimIndex: number;
  dimensionType: DimensionType;
  nomItems: NomItemMapping[];
}

interface DataSyncOptions {
  matrixCode: string;
  yearFrom?: number;
  yearTo?: number;
  /** Expand territorial dimension to all localities (UATs) instead of just national total */
  expandTerritorial?: boolean;
  /** County code (e.g., "AB" for Alba) to sync only that county's localities */
  countyCode?: string;
  onProgress?: (progress: DataSyncProgress) => void;
}

/** Options for full matrix sync with chunking */
export interface FullSyncOptions {
  matrixCode: string;
  yearFrom: number;
  yearTo: number;
  /** Sync all classifications or just totals */
  classificationMode: ClassificationMode;
  /** Specific county to sync (for targeted sync) */
  countyCode?: string;
  /** Override cell limit for chunk planning */
  cellLimit?: number;
  /** Resume from last checkpoint */
  resume?: boolean;
  /** Force re-sync even if checkpoints exist */
  force?: boolean;
  /** Enable verbose debug logging */
  verbose?: boolean;
  /** Make a single API call instead of chunking (may fail if > 30,000 cells) */
  noChunking?: boolean;
  /** Progress callback */
  onProgress?: (progress: FullSyncProgress) => void;
  /** Task ID from sync queue (optional - null for direct CLI syncs) */
  taskId?: number;
}

/** Progress for full sync */
export interface FullSyncProgress {
  phase: "planning" | "syncing" | "updating_coverage";
  chunksCompleted: number;
  chunksTotal: number;
  currentChunk?: string;
  rowsInserted: number;
  rowsUpdated: number;
  estimatedTimeRemaining?: string;
}

/** Result of full sync */
export interface FullSyncResult {
  chunksTotal: number;
  chunksCompleted: number;
  chunksSkipped: number;
  chunksFailed: number;
  rowsInserted: number;
  rowsUpdated: number;
  errors: string[];
  duration: number;
}

/** Maps county info for county-specific sync */
interface CountyInfo {
  countyDimIndex: number;
  localityDimIndex: number;
  countyNomItemId: number;
  countyTerritoryPath: string;
  localityNomItemIds: number[];
}

interface DataSyncProgress {
  phase: string;
  current: number;
  total: number;
  message?: string;
}

interface DataSyncResult {
  rowsInserted: number;
  rowsUpdated: number;
  errors: string[];
}

/** Batch size for bulk insert operations */
const BATCH_SIZE = 1000;

/**
 * Pre-parsed row for batch insert operations
 * Contains all computed values needed for database insertion
 */
interface ParsedRow {
  naturalKeyHash: string;
  territoryId: number | null;
  timePeriodId: number | null;
  unitId: number | null;
  value: number | null;
  valueStatus: string | null;
  classificationValueIds: number[];
  sourceEncQuery: string;
}

// ============================================================================
// Data Sync Service
// ============================================================================

export class DataSyncService {
  private readonly workerId: string;

  constructor(private readonly db: Kysely<Database>) {
    this.workerId = generateWorkerId();
    apiLogger.debug({ workerId: this.workerId }, "DataSyncService initialized");
  }

  /**
   * Sync statistical data for a matrix
   */
  async syncData(options: DataSyncOptions): Promise<DataSyncResult> {
    const {
      matrixCode,
      yearFrom,
      yearTo,
      expandTerritorial,
      countyCode,
      onProgress,
    } = options;

    apiLogger.info(
      { matrixCode, yearFrom, yearTo, countyCode },
      "Starting data sync"
    );

    // 1. Get matrix info
    const matrix = await this.db
      .selectFrom("matrices")
      .select(["id", "ins_code", "metadata"])
      .where("ins_code", "=", matrixCode)
      .executeTakeFirst();

    if (!matrix) {
      throw new Error(`Matrix ${matrixCode} not found`);
    }

    // 2. Load dimensions and nom_items
    onProgress?.({ phase: "Loading dimensions", current: 0, total: 1 });

    const dimensions = await this.loadDimensions(matrix.id);
    if (dimensions.length === 0) {
      throw new Error(`No dimensions found for matrix ${matrixCode}`);
    }

    // 2b. Load county info if county-specific sync requested
    let countyInfo: CountyInfo | null = null;
    if (countyCode) {
      countyInfo = await this.loadCountyInfo(matrix.id, countyCode);
      if (!countyInfo) {
        throw new Error(
          `County ${countyCode} not found in matrix ${matrixCode} or matrix doesn't have locality dimension`
        );
      }
    }

    // 3. Build query selections
    const selections = this.buildSelections(
      dimensions,
      yearFrom,
      yearTo,
      expandTerritorial,
      countyInfo
    );
    const encQuery = buildEncQuery(selections);

    apiLogger.debug(
      { matrixCode, encQuery, dimensionCount: dimensions.length },
      "Built query"
    );

    // 4. Get matrix details for query
    const details = matrix.metadata?.details as
      | Record<string, number>
      | undefined;
    const matMaxDim = details?.matMaxDim ?? dimensions.length;
    const matRegJ = details?.matRegJ ?? 0;
    const matUMSpec = details?.matUMSpec ?? 0;

    // 5. Query INS API
    onProgress?.({ phase: "Fetching data", current: 0, total: 1 });

    const csvData = await queryMatrix({
      encQuery,
      language: "ro",
      matCode: matrixCode,
      matMaxDim,
      matRegJ,
      matUMSpec,
    });

    // 6. Parse CSV response
    const rows = this.parseCsv(csvData);
    apiLogger.info({ matrixCode, rowCount: rows.length }, "Parsed CSV data");

    if (rows.length === 0) {
      return { rowsInserted: 0, rowsUpdated: 0, errors: [] };
    }

    // 7. Map and insert data
    onProgress?.({
      phase: "Inserting data",
      current: 0,
      total: rows.length,
    });

    const result = await this.insertData(
      matrix.id,
      dimensions,
      rows,
      encQuery,
      onProgress
    );

    apiLogger.info({ matrixCode, ...result }, "Data sync completed");

    return result;
  }

  /**
   * Sync ALL data for a matrix using chunking strategy
   * This is the new full sync method that handles 30,000 cell limit
   */
  async syncMatrixFull(options: FullSyncOptions): Promise<FullSyncResult> {
    const {
      matrixCode,
      yearFrom,
      yearTo,
      classificationMode,
      countyCode,
      cellLimit,
      resume = true,
      force = false,
      verbose = false,
      noChunking = false,
      onProgress,
      taskId,
    } = options;

    // If no-chunking mode, use single API call approach
    if (noChunking) {
      return this.syncMatrixSingleCall(options);
    }

    const startTime = Date.now();
    const result: FullSyncResult = {
      chunksTotal: 0,
      chunksCompleted: 0,
      chunksSkipped: 0,
      chunksFailed: 0,
      rowsInserted: 0,
      rowsUpdated: 0,
      errors: [],
      duration: 0,
    };

    apiLogger.info(
      {
        matrixCode,
        yearFrom,
        yearTo,
        classificationMode,
        countyCode,
        resume,
        force,
        cellLimit,
        noChunking,
      },
      "Starting full matrix sync"
    );

    // 1. Load matrix info
    const chunkGenerator = new ChunkGenerator(this.db);
    const matrixInfo = await chunkGenerator.loadMatrixInfo(matrixCode);

    if (!matrixInfo) {
      throw new Error(`Matrix ${matrixCode} not found`);
    }

    // 2. Generate chunks
    onProgress?.({
      phase: "planning",
      chunksCompleted: 0,
      chunksTotal: 0,
      rowsInserted: 0,
      rowsUpdated: 0,
    });

    const chunkResult = await chunkGenerator.generateChunks(matrixInfo, {
      yearFrom,
      yearTo,
      classificationMode,
      countyCode,
      cellLimit,
    });

    apiLogger.info(
      {
        matrixCode,
        chunkCount: chunkResult.chunks.length,
        estimatedDuration: chunkResult.estimatedDuration,
        hasUatData: chunkResult.hasUatData,
      },
      "Generated sync chunks"
    );

    // 3. Get matrix details for API calls
    const details = matrixInfo.metadata?.details as
      | Record<string, number>
      | undefined;
    const matMaxDim = details?.matMaxDim ?? matrixInfo.dimensions.length;
    const matRegJ = details?.matRegJ ?? 0;
    const matUMSpec = details?.matUMSpec ?? 0;

    // 4. Process each chunk
    const totalChunks = chunkResult.chunks.length;
    result.chunksTotal = totalChunks;

    for (let i = 0; i < totalChunks; i++) {
      const chunk = chunkResult.chunks[i]!;
      const chunkName = getChunkDisplayName(chunk);

      onProgress?.({
        phase: "syncing",
        chunksCompleted: result.chunksCompleted,
        chunksTotal: totalChunks,
        currentChunk: chunkName,
        rowsInserted: result.rowsInserted,
        rowsUpdated: result.rowsUpdated,
      });

      // Check if chunk already successfully synced (via checkpoint)
      if (resume && !force) {
        const checkpoint = await this.getCheckpoint(
          matrixInfo.id,
          chunk.chunkHash
        );
        if (checkpoint) {
          if (verbose) {
            apiLogger.debug(
              { chunkName, chunkHash: chunk.chunkHash, checkpoint },
              "Skipping synced chunk"
            );
          }
          result.chunksSkipped++;
          continue;
        }
      }

      // Try to claim chunk lease (prevents concurrent processing)
      const claimed = await this.claimChunk(matrixInfo.id, chunk, taskId);
      if (!claimed) {
        if (verbose) {
          apiLogger.debug(
            { chunkName },
            "Skipping chunk - locked by another worker"
          );
        }
        result.chunksSkipped++;
        continue;
      }

      try {
        // Build selections for this chunk
        const selections = chunk.selections;
        const encQuery = buildEncQuery(selections);
        const cellCount = chunk.cellCount;

        if (verbose) {
          apiLogger.debug(
            {
              chunkName,
              chunkHash: chunk.chunkHash,
              chunkQuery: chunk.chunkQuery,
              encQuery: encQuery.substring(0, 100),
              cellCount,
              estimatedCsvBytes: chunk.estimatedCsvBytes,
            },
            "Querying chunk"
          );
        }

        // Query INS API
        const csvData = await queryMatrix({
          encQuery,
          language: "ro",
          matCode: matrixCode,
          matMaxDim,
          matRegJ,
          matUMSpec,
        });

        // Parse CSV
        const rows = this.parseCsv(csvData);

        if (verbose) {
          apiLogger.debug(
            { chunkName, rowCount: rows.length },
            "Parsed CSV data"
          );
        }

        // Insert data
        if (rows.length > 0) {
          const insertResult = await this.insertData(
            matrixInfo.id,
            matrixInfo.dimensions.map((d) => ({
              dimIndex: d.dimIndex,
              dimensionType: d.dimensionType,
              nomItems: d.nomItems.map((n) => ({
                nomItemId: n.nomItemId,
                dimensionType: d.dimensionType,
                territoryId: n.territoryId,
                timePeriodId: n.timePeriodId,
                classificationValueId: n.classificationValueId,
                unitId: n.unitId,
                labelRo: n.labelRo,
              })),
            })),
            rows,
            encQuery
          );

          result.rowsInserted += insertResult.rowsInserted;
          result.rowsUpdated += insertResult.rowsUpdated;
          result.errors.push(...insertResult.errors);
        }

        // Record checkpoint
        await this.recordCheckpoint(matrixInfo.id, chunk, rows.length, taskId);

        result.chunksCompleted++;

        apiLogger.info(
          {
            chunkName,
            progress: `${String(result.chunksCompleted)}/${String(totalChunks)}`,
            rowsInChunk: rows.length,
          },
          "Chunk sync complete"
        );
      } catch (error) {
        const errorMsg = error instanceof Error ? error.message : String(error);
        apiLogger.error({ chunkName, error: errorMsg }, "Chunk sync failed");
        result.chunksFailed++;
        result.errors.push(`Chunk ${chunkName}: ${errorMsg}`);

        // Record failed checkpoint
        await this.recordFailedCheckpoint(
          matrixInfo.id,
          chunk,
          errorMsg,
          taskId
        );
      }
    }

    // 5. Update coverage
    onProgress?.({
      phase: "updating_coverage",
      chunksCompleted: result.chunksCompleted,
      chunksTotal: totalChunks,
      rowsInserted: result.rowsInserted,
      rowsUpdated: result.rowsUpdated,
    });

    await this.updateCoverage(matrixInfo.id);

    result.duration = Date.now() - startTime;

    apiLogger.info(
      {
        matrixCode,
        ...result,
        durationSec: Math.round(result.duration / 1000),
      },
      "Full matrix sync completed"
    );

    return result;
  }

  /**
   * Sync matrix with a single API call (no chunking)
   * Used when --no-chunking flag is specified
   */
  private async syncMatrixSingleCall(
    options: FullSyncOptions
  ): Promise<FullSyncResult> {
    const {
      matrixCode,
      yearFrom,
      yearTo,
      classificationMode,
      countyCode,
      force = false,
      verbose = false,
      onProgress,
      taskId,
    } = options;

    const startTime = Date.now();
    const result: FullSyncResult = {
      chunksTotal: 1, // Single call = 1 chunk
      chunksCompleted: 0,
      chunksSkipped: 0,
      chunksFailed: 0,
      rowsInserted: 0,
      rowsUpdated: 0,
      errors: [],
      duration: 0,
    };

    apiLogger.info(
      { matrixCode, yearFrom, yearTo, classificationMode, noChunking: true },
      "Starting single-call matrix sync (no chunking)"
    );

    // 1. Load matrix info
    const chunkGenerator = new ChunkGenerator(this.db);
    const matrixInfo = await chunkGenerator.loadMatrixInfo(matrixCode);

    if (!matrixInfo) {
      throw new Error(`Matrix ${matrixCode} not found`);
    }

    // 2. Build selections for full matrix
    onProgress?.({
      phase: "planning",
      chunksCompleted: 0,
      chunksTotal: 1,
      rowsInserted: 0,
      rowsUpdated: 0,
    });

    const selections = this.buildFullMatrixSelections(
      matrixInfo.dimensions.map((d) => ({
        dimIndex: d.dimIndex,
        dimensionType: d.dimensionType,
        nomItems: d.nomItems.map((n) => ({
          nomItemId: n.nomItemId,
          dimensionType: d.dimensionType,
          territoryId: n.territoryId,
          timePeriodId: n.timePeriodId,
          classificationValueId: n.classificationValueId,
          unitId: n.unitId,
          labelRo: n.labelRo,
        })),
      })),
      yearFrom,
      yearTo,
      classificationMode
    );

    const cellCount = estimateCellCount(selections);
    const CELL_LIMIT = 30_000;

    // 3. Warn if estimated cells exceed limit
    if (cellCount > CELL_LIMIT) {
      apiLogger.warn(
        { matrixCode, estimatedCells: cellCount, limit: CELL_LIMIT },
        "Estimated cells exceed INS API limit - request may fail"
      );
      result.errors.push(
        `Warning: Estimated ${String(cellCount)} cells exceeds ${String(CELL_LIMIT)} limit`
      );
    }

    // 4. Check checkpoint (single checkpoint for whole matrix)
    const chunkHash = this.computeFullSyncHash(
      matrixCode,
      yearFrom,
      yearTo,
      classificationMode,
      countyCode
    );

    if (!force) {
      const checkpoint = await this.getCheckpoint(matrixInfo.id, chunkHash);
      if (checkpoint) {
        if (verbose) {
          apiLogger.debug(
            { chunkHash, checkpoint },
            "Skipping - already synced"
          );
        }
        result.chunksSkipped = 1;
        result.duration = Date.now() - startTime;
        return result;
      }
    }

    // 5. Build encQuery and make API call
    const encQuery = buildEncQuery(selections);
    const details = matrixInfo.metadata?.details as
      | Record<string, number>
      | undefined;
    const matMaxDim = details?.matMaxDim ?? matrixInfo.dimensions.length;
    const matRegJ = details?.matRegJ ?? 0;
    const matUMSpec = details?.matUMSpec ?? 0;

    onProgress?.({
      phase: "syncing",
      chunksCompleted: 0,
      chunksTotal: 1,
      currentChunk: "Full matrix (no chunking)",
      rowsInserted: 0,
      rowsUpdated: 0,
    });

    try {
      if (verbose) {
        apiLogger.debug(
          { encQuery: encQuery.substring(0, 100), cellCount },
          "Querying full matrix"
        );
      }

      const csvData = await queryMatrix({
        encQuery,
        language: "ro",
        matCode: matrixCode,
        matMaxDim,
        matRegJ,
        matUMSpec,
      });

      // 6. Parse CSV
      const rows = this.parseCsv(csvData);

      if (verbose) {
        apiLogger.debug({ rowCount: rows.length }, "Parsed CSV data");
      }

      // 7. Insert data
      if (rows.length > 0) {
        const insertResult = await this.insertData(
          matrixInfo.id,
          matrixInfo.dimensions.map((d) => ({
            dimIndex: d.dimIndex,
            dimensionType: d.dimensionType,
            nomItems: d.nomItems.map((n) => ({
              nomItemId: n.nomItemId,
              dimensionType: d.dimensionType,
              territoryId: n.territoryId,
              timePeriodId: n.timePeriodId,
              classificationValueId: n.classificationValueId,
              unitId: n.unitId,
              labelRo: n.labelRo,
            })),
          })),
          rows,
          encQuery
        );

        result.rowsInserted = insertResult.rowsInserted;
        result.rowsUpdated = insertResult.rowsUpdated;
        result.errors.push(...insertResult.errors);
      }

      // 8. Record checkpoint for whole sync
      await this.recordFullSyncCheckpoint(
        matrixInfo.id,
        chunkHash,
        yearFrom,
        yearTo,
        classificationMode,
        countyCode,
        rows.length,
        cellCount,
        undefined, // no error
        taskId
      );

      result.chunksCompleted = 1;

      apiLogger.info(
        { matrixCode, rowCount: rows.length },
        "Single-call sync complete"
      );
    } catch (error) {
      const errorMsg = error instanceof Error ? error.message : String(error);
      apiLogger.error(
        { matrixCode, error: errorMsg },
        "Single-call sync failed"
      );
      result.chunksFailed = 1;
      result.errors.push(`Full sync failed: ${errorMsg}`);

      // Record failed checkpoint
      await this.recordFullSyncCheckpoint(
        matrixInfo.id,
        chunkHash,
        yearFrom,
        yearTo,
        classificationMode,
        countyCode,
        0,
        cellCount,
        errorMsg,
        taskId
      );
    }

    // 9. Update coverage
    onProgress?.({
      phase: "updating_coverage",
      chunksCompleted: result.chunksCompleted,
      chunksTotal: 1,
      rowsInserted: result.rowsInserted,
      rowsUpdated: result.rowsUpdated,
    });

    await this.updateCoverage(matrixInfo.id);

    result.duration = Date.now() - startTime;

    apiLogger.info(
      {
        matrixCode,
        ...result,
        durationSec: Math.round(result.duration / 1000),
      },
      "Single-call matrix sync completed"
    );

    return result;
  }

  /**
   * Build selections for full matrix sync (no chunking)
   */
  private buildFullMatrixSelections(
    dimensions: DimensionInfo[],
    yearFrom: number,
    yearTo: number,
    classificationMode: ClassificationMode
  ): number[][] {
    return dimensions.map((dim) => {
      // TEMPORAL: filter by year range
      if (dim.dimensionType === "TEMPORAL") {
        const filtered = dim.nomItems.filter((item) => {
          const yearMatch = /\d{4}/.exec(item.labelRo);
          if (!yearMatch) return false;
          const year = Number.parseInt(yearMatch[0], 10);
          return year >= yearFrom && year <= yearTo;
        });
        if (filtered.length === 0) {
          return dim.nomItems[0] ? [dim.nomItems[0].nomItemId] : [];
        }
        return filtered.map((item) => item.nomItemId);
      }

      // TERRITORIAL: take all items (no chunking by county)
      if (dim.dimensionType === "TERRITORIAL") {
        return dim.nomItems.map((item) => item.nomItemId);
      }

      // CLASSIFICATION: respect classificationMode
      if (dim.dimensionType === "CLASSIFICATION") {
        if (classificationMode === "totals-only") {
          // Find TOTAL item
          const totalItem = dim.nomItems.find(
            (item) =>
              item.labelRo.toLowerCase() === "total" ||
              item.labelRo.toLowerCase().startsWith("total")
          );
          return totalItem
            ? [totalItem.nomItemId]
            : [dim.nomItems[0]?.nomItemId ?? 1];
        }
        // "all" mode: take all classifications
        return dim.nomItems.map((item) => item.nomItemId);
      }

      // UNIT_OF_MEASURE and others: take all
      return dim.nomItems.map((item) => item.nomItemId);
    });
  }

  /**
   * Compute hash for full matrix sync checkpoint
   */
  private computeFullSyncHash(
    matrixCode: string,
    yearFrom: number,
    yearTo: number,
    classificationMode: ClassificationMode,
    countyCode?: string
  ): string {
    const key = `${matrixCode}:FULL:${String(yearFrom)}-${String(yearTo)}:${classificationMode}:${countyCode ?? "ALL"}`;
    return createHash("md5").update(key).digest("hex").substring(0, 16);
  }

  /**
   * Record full sync checkpoint (for full matrix sync)
   */
  private async recordFullSyncCheckpoint(
    matrixId: number,
    chunkHash: string,
    yearFrom: number,
    yearTo: number,
    _classificationMode: ClassificationMode,
    countyCode: string | undefined,
    rowCount: number,
    cellsEstimated: number,
    errorMessage?: string,
    taskId?: number
  ): Promise<void> {
    const now = new Date();
    const status = errorMessage ? "FAILED" : "COMPLETED";

    await this.db
      .insertInto("sync_checkpoints")
      .values({
        task_id: taskId ?? null, // Link to task if provided
        matrix_id: matrixId,
        chunk_hash: chunkHash,
        chunk_index: 0,
        chunk_name: `FULL:${String(yearFrom)}-${String(yearTo)}`,
        county_code: countyCode ?? null,
        year_from: yearFrom,
        year_to: yearTo,
        cells_estimated: cellsEstimated,
        cells_returned: rowCount,
        rows_synced: rowCount,
        status,
        started_at: now,
        completed_at: now,
        error_message: errorMessage?.substring(0, 1000) ?? null,
        retry_count: errorMessage ? 1 : 0,
      })
      .onConflict((oc) =>
        oc.columns(["matrix_id", "chunk_hash"]).doUpdateSet({
          task_id: taskId ?? null, // Update task_id on conflict too
          rows_synced: rowCount,
          cells_estimated: cellsEstimated,
          cells_returned: rowCount,
          status,
          completed_at: now,
          error_message: errorMessage?.substring(0, 1000) ?? null,
        })
      )
      .execute();
  }

  /**
   * Get checkpoint for a chunk
   */
  private async getCheckpoint(
    matrixId: number,
    chunkHash: string
  ): Promise<{ rowCount: number; lastSyncedAt: Date } | null> {
    const checkpoint = await this.db
      .selectFrom("sync_checkpoints")
      .select(["rows_synced", "completed_at"])
      .where("matrix_id", "=", matrixId)
      .where("chunk_hash", "=", chunkHash)
      .where("status", "=", "COMPLETED")
      .where("cells_returned", "is not", null)
      .executeTakeFirst();

    if (!checkpoint?.completed_at) return null;

    return {
      rowCount: checkpoint.rows_synced ?? 0,
      lastSyncedAt: checkpoint.completed_at,
    };
  }

  /**
   * Record successful checkpoint
   */
  private async recordCheckpoint(
    matrixId: number,
    chunk: SyncChunk,
    rowCount: number,
    taskId?: number
  ): Promise<void> {
    const now = new Date();
    await this.db
      .insertInto("sync_checkpoints")
      .values({
        task_id: taskId ?? null, // Link to task if provided
        matrix_id: matrixId,
        chunk_hash: chunk.chunkHash,
        chunk_index: 0,
        chunk_name: getChunkDisplayName(chunk),
        county_code: chunk.countyCode,
        year_from: chunk.yearFrom,
        year_to: chunk.yearTo,
        cells_estimated: chunk.cellCount,
        cells_returned: rowCount,
        rows_synced: rowCount,
        status: "COMPLETED",
        started_at: now,
        completed_at: now,
        error_message: null,
        retry_count: 0,
        // Clear lease on successful completion
        locked_until: null,
        locked_by: null,
      })
      .onConflict((oc) =>
        oc.columns(["matrix_id", "chunk_hash"]).doUpdateSet({
          task_id: taskId ?? null, // Update task_id on conflict too
          rows_synced: rowCount,
          cells_estimated: chunk.cellCount,
          cells_returned: rowCount,
          status: "COMPLETED",
          completed_at: now,
          error_message: null,
          // Clear lease on successful completion
          locked_until: null,
          locked_by: null,
        })
      )
      .execute();
  }

  /**
   * Record failed checkpoint
   */
  private async recordFailedCheckpoint(
    matrixId: number,
    chunk: SyncChunk,
    errorMessage: string,
    taskId?: number
  ): Promise<void> {
    const now = new Date();
    await this.db
      .insertInto("sync_checkpoints")
      .values({
        task_id: taskId ?? null, // Link to task if provided
        matrix_id: matrixId,
        chunk_hash: chunk.chunkHash,
        chunk_index: 0,
        chunk_name: getChunkDisplayName(chunk),
        county_code: chunk.countyCode,
        year_from: chunk.yearFrom,
        year_to: chunk.yearTo,
        cells_estimated: chunk.cellCount,
        cells_returned: null,
        rows_synced: 0,
        status: "FAILED",
        started_at: now,
        completed_at: now,
        error_message: errorMessage.substring(0, 1000),
        retry_count: 1,
        // Clear lease on failure (allows retry)
        locked_until: null,
        locked_by: null,
      })
      .onConflict((oc) =>
        oc.columns(["matrix_id", "chunk_hash"]).doUpdateSet({
          task_id: taskId ?? null, // Update task_id on conflict too
          status: "FAILED",
          error_message: errorMessage.substring(0, 1000),
          completed_at: now,
          retry_count: sql`sync_checkpoints.retry_count + 1`,
          // Clear lease on failure (allows retry)
          locked_until: null,
          locked_by: null,
        })
      )
      .execute();
  }

  /**
   * Try to claim a chunk for processing using lease-based locking.
   * Returns true if the chunk was claimed, false if it's locked by another worker.
   */
  private async claimChunk(
    matrixId: number,
    chunk: SyncChunk,
    taskId?: number
  ): Promise<boolean> {
    const leaseExpiry = new Date(Date.now() + CHUNK_LEASE_DURATION_MS);
    const now = new Date();

    // First, check if checkpoint exists
    const existing = await this.db
      .selectFrom("sync_checkpoints")
      .select(["id", "locked_by", "locked_until"])
      .where("matrix_id", "=", matrixId)
      .where("chunk_hash", "=", chunk.chunkHash)
      .executeTakeFirst();

    if (!existing) {
      // No checkpoint exists - create one with lease
      await this.db
        .insertInto("sync_checkpoints")
        .values({
          task_id: taskId ?? null, // Link to task if provided
          matrix_id: matrixId,
          chunk_hash: chunk.chunkHash,
          chunk_index: 0,
          chunk_name: getChunkDisplayName(chunk),
          county_code: chunk.countyCode,
          year_from: chunk.yearFrom,
          year_to: chunk.yearTo,
          cells_estimated: chunk.cellCount,
          cells_returned: null,
          rows_synced: 0,
          status: "PENDING",
          started_at: now,
          error_message: null,
          retry_count: 0,
          locked_until: leaseExpiry,
          locked_by: this.workerId,
        })
        .execute();

      apiLogger.debug(
        { chunkHash: chunk.chunkHash, workerId: this.workerId },
        "Created checkpoint with lease"
      );
      return true;
    }

    // Checkpoint exists - check if we can claim it
    // We can claim if: lease is null, expired, or we already own it
    if (existing.locked_by === this.workerId) {
      // We own it - extend the lease
      await this.db
        .updateTable("sync_checkpoints")
        .set({ locked_until: leaseExpiry })
        .where("id", "=", existing.id)
        .execute();
      return true;
    }

    if (existing.locked_until && existing.locked_until > now) {
      // Lease is active and owned by someone else
      apiLogger.debug(
        {
          chunkHash: chunk.chunkHash,
          lockedBy: existing.locked_by,
          lockedUntil: existing.locked_until,
        },
        "Chunk locked by another worker"
      );
      return false;
    }

    // Lease is null or expired - try to claim it
    const updateResult = await this.db
      .updateTable("sync_checkpoints")
      .set({
        locked_until: leaseExpiry,
        locked_by: this.workerId,
      })
      .where("id", "=", existing.id)
      .where((eb) =>
        eb.or([
          eb("locked_until", "is", null),
          eb("locked_until", "<=", now),
          eb("locked_by", "=", this.workerId),
        ])
      )
      .executeTakeFirst();

    const claimed = (updateResult.numUpdatedRows ?? 0n) > 0n;

    if (claimed) {
      apiLogger.debug(
        { chunkHash: chunk.chunkHash, workerId: this.workerId },
        "Claimed expired lease"
      );
    } else {
      apiLogger.debug(
        { chunkHash: chunk.chunkHash },
        "Failed to claim - another worker got it first"
      );
    }

    return claimed;
  }

  /**
   * Update matrix sync status and timestamp
   */
  private async updateCoverage(matrixId: number): Promise<void> {
    // Count synced statistics for logging
    const stats = await this.db
      .selectFrom("statistics")
      .select([this.db.fn.count<number>("id").as("total_rows")])
      .where("matrix_id", "=", matrixId)
      .executeTakeFirst();

    // Update matrix last_sync_at timestamp
    await this.db
      .updateTable("matrices")
      .set({
        last_sync_at: new Date(),
      })
      .where("id", "=", matrixId)
      .execute();

    apiLogger.info(
      {
        matrixId,
        totalRows: stats?.total_rows ?? 0,
      },
      "Updated matrix sync timestamp"
    );
  }

  /**
   * Load county info for county-specific sync
   * Returns null if matrix doesn't have county+locality dimensions
   */
  private async loadCountyInfo(
    matrixId: number,
    countyCode: string
  ): Promise<CountyInfo | null> {
    // Find county dimension (NUTS3 level) and locality dimension (LAU level)
    const countyNomItem = await this.db
      .selectFrom("matrix_nom_items")
      .innerJoin(
        "territories",
        "matrix_nom_items.territory_id",
        "territories.id"
      )
      .select([
        "matrix_nom_items.dim_index",
        "matrix_nom_items.nom_item_id",
        "territories.code",
        "territories.path",
      ])
      .where("matrix_nom_items.matrix_id", "=", matrixId)
      .where("matrix_nom_items.dimension_type", "=", "TERRITORIAL")
      .where("territories.level", "=", "NUTS3")
      .where("territories.code", "=", countyCode)
      .executeTakeFirst();

    if (!countyNomItem) {
      apiLogger.warn(
        { matrixId, countyCode },
        "County not found in matrix dimensions"
      );
      return null;
    }

    // Find locality dimension (the one with LAU level items)
    const localityDim = await this.db
      .selectFrom("matrix_nom_items")
      .innerJoin(
        "territories",
        "matrix_nom_items.territory_id",
        "territories.id"
      )
      .select(["matrix_nom_items.dim_index"])
      .where("matrix_nom_items.matrix_id", "=", matrixId)
      .where("matrix_nom_items.dimension_type", "=", "TERRITORIAL")
      .where("territories.level", "=", "LAU")
      .executeTakeFirst();

    if (!localityDim) {
      apiLogger.warn({ matrixId }, "No locality dimension found");
      return null;
    }

    // Get all localities that belong to this county (path starts with county path)
    const countyPath = countyNomItem.path as unknown as string;
    const localities = await this.db
      .selectFrom("matrix_nom_items")
      .innerJoin(
        "territories",
        "matrix_nom_items.territory_id",
        "territories.id"
      )
      .select(["matrix_nom_items.nom_item_id"])
      .where("matrix_nom_items.matrix_id", "=", matrixId)
      .where("matrix_nom_items.dim_index", "=", localityDim.dim_index)
      .where("territories.level", "=", "LAU")
      .$call((qb) => qb.where("territories.path", "~", `${countyPath}.*`))
      .execute();

    apiLogger.info(
      {
        countyCode,
        countyDimIndex: countyNomItem.dim_index,
        localityDimIndex: localityDim.dim_index,
        localityCount: localities.length,
      },
      "Loaded county info for sync"
    );

    return {
      countyDimIndex: countyNomItem.dim_index,
      localityDimIndex: localityDim.dim_index,
      countyNomItemId: countyNomItem.nom_item_id,
      countyTerritoryPath: countyPath,
      localityNomItemIds: localities.map((l) => l.nom_item_id),
    };
  }

  /**
   * Load dimension info with nom_item mappings
   */
  private async loadDimensions(matrixId: number): Promise<DimensionInfo[]> {
    const nomItems = await this.db
      .selectFrom("matrix_nom_items")
      .innerJoin("matrix_dimensions", (join) =>
        join
          .onRef(
            "matrix_nom_items.matrix_id",
            "=",
            "matrix_dimensions.matrix_id"
          )
          .onRef(
            "matrix_nom_items.dim_index",
            "=",
            "matrix_dimensions.dim_index"
          )
      )
      .select([
        "matrix_nom_items.dim_index",
        "matrix_nom_items.nom_item_id",
        "matrix_nom_items.dimension_type",
        "matrix_nom_items.territory_id",
        "matrix_nom_items.time_period_id",
        "matrix_nom_items.classification_value_id",
        "matrix_nom_items.unit_id",
        "matrix_nom_items.labels",
      ])
      .where("matrix_nom_items.matrix_id", "=", matrixId)
      .orderBy("matrix_nom_items.dim_index")
      .orderBy("matrix_nom_items.offset_order")
      .execute();

    // Group by dimension
    const dimensionMap = new Map<number, DimensionInfo>();

    for (const item of nomItems) {
      if (!dimensionMap.has(item.dim_index)) {
        dimensionMap.set(item.dim_index, {
          dimIndex: item.dim_index,
          dimensionType: item.dimension_type,
          nomItems: [],
        });
      }

      dimensionMap.get(item.dim_index)!.nomItems.push({
        nomItemId: item.nom_item_id,
        dimensionType: item.dimension_type,
        territoryId: item.territory_id,
        timePeriodId: item.time_period_id,
        classificationValueId: item.classification_value_id,
        unitId: item.unit_id,
        labelRo: item.labels?.ro ?? "",
      });
    }

    return Array.from(dimensionMap.values()).sort(
      (a, b) => a.dimIndex - b.dimIndex
    );
  }

  /**
   * Build selections for encQuery
   */
  private buildSelections(
    dimensions: DimensionInfo[],
    yearFrom?: number,
    yearTo?: number,
    expandTerritorial?: boolean,
    countyInfo?: CountyInfo | null
  ): number[][] {
    return dimensions.map((dim, idx) => {
      // For temporal dimensions, filter by year range
      if (dim.dimensionType === "TEMPORAL" && (yearFrom || yearTo)) {
        const filtered = dim.nomItems.filter((item) => {
          // Extract year from label (e.g., "Anul 2020" -> 2020)
          const yearMatch = /\d{4}/.exec(item.labelRo);
          if (!yearMatch) return true;

          const year = parseInt(yearMatch[0], 10);
          if (yearFrom && year < yearFrom) return false;
          if (yearTo && year > yearTo) return false;
          return true;
        });

        // If filtering resulted in no items, take first item (Total usually)
        if (filtered.length === 0) {
          return [dim.nomItems[0]?.nomItemId ?? 1];
        }

        return filtered.map((item) => item.nomItemId);
      }

      // For other dimensions, take all items (or just first for TOTAL)
      // We'll take the first item which is usually "Total"
      if (dim.nomItems.length > 0) {
        const firstItem = dim.nomItems[0];
        // If it's a classification with "Total", just use that to avoid combinatorial explosion
        if (
          dim.dimensionType === "CLASSIFICATION" &&
          firstItem?.labelRo.toLowerCase().includes("total")
        ) {
          return [firstItem.nomItemId];
        }
        // For territorial dimensions with county-specific sync
        if (dim.dimensionType === "TERRITORIAL" && countyInfo) {
          // County dimension: select only the specified county
          if (dim.dimIndex === countyInfo.countyDimIndex) {
            apiLogger.info(
              {
                dimIndex: idx,
                countyNomItemId: countyInfo.countyNomItemId,
              },
              "Selecting specific county"
            );
            return [countyInfo.countyNomItemId];
          }
          // Locality dimension: select only localities in this county
          if (dim.dimIndex === countyInfo.localityDimIndex) {
            apiLogger.info(
              {
                dimIndex: idx,
                localityCount: countyInfo.localityNomItemIds.length,
              },
              "Selecting county localities"
            );
            return countyInfo.localityNomItemIds;
          }
        }
        // For territorial, expand all items when expandTerritorial is set
        // Note: For matrices with county+locality dimensions, API requires matching pairs
        // This expands all items - for county+locality matrices, sync must iterate by county
        if (dim.dimensionType === "TERRITORIAL") {
          if (expandTerritorial) {
            // Return all territorial items EXCLUDING the TOTAL (first item)
            const items = dim.nomItems.slice(1).map((item) => item.nomItemId);
            apiLogger.info(
              {
                dimIndex: idx,
                territorialCount: items.length,
              },
              "Expanding territorial dimension"
            );
            return items;
          }
          return [dim.nomItems[0]!.nomItemId];
        }
        // For units, take first
        if (dim.dimensionType === "UNIT_OF_MEASURE") {
          return [dim.nomItems[0]!.nomItemId];
        }
      }

      // Default: take all items
      return dim.nomItems.map((item) => item.nomItemId);
    });
  }

  /**
   * Parse CSV response from pivot endpoint
   * Skips header row (first line contains column names)
   */
  private parseCsv(csvText: string): string[][] {
    const lines = csvText.split("\n").filter((row) => row.trim() !== "");
    // Skip header row (first line)
    return lines
      .slice(1)
      .map((row) => row.split(", ").map((cell) => cell.trim()));
  }

  /**
   * Preprocess CSV rows into ParsedRow objects for batch insertion.
   * Performs all parsing and hash computation upfront to enable efficient batching.
   */
  private preprocessRows(
    matrixId: number,
    dimensions: DimensionInfo[],
    rows: string[][],
    sourceEncQuery: string
  ): { parsedRows: ParsedRow[]; errors: string[] } {
    const parsedRows: ParsedRow[] = [];
    const errors: string[] = [];

    for (let i = 0; i < rows.length; i++) {
      const row = rows[i]!;

      try {
        // Last column is value, others are dimension labels
        const valueStr = row[row.length - 1] ?? "";
        const value =
          valueStr === ":" || valueStr === "" ? null : parseFloat(valueStr);
        const valueStatus = valueStr === ":" ? "missing" : null;

        // Find matching entities for each dimension
        let territoryId: number | null = null;
        let timePeriodId: number | null = null;
        let unitId: number | null = null;
        const classificationValueIds: number[] = [];

        for (
          let dimIdx = 0;
          dimIdx < dimensions.length && dimIdx < row.length - 1;
          dimIdx++
        ) {
          const dim = dimensions[dimIdx]!;
          const cellLabel = row[dimIdx]!;

          // Find matching nom_item by label
          const nomItem = dim.nomItems.find(
            (item) =>
              item.labelRo === cellLabel ||
              item.labelRo.includes(cellLabel) ||
              cellLabel.includes(item.labelRo)
          );

          if (nomItem) {
            switch (dim.dimensionType) {
              case "TERRITORIAL":
                territoryId = nomItem.territoryId;
                break;
              case "TEMPORAL":
                timePeriodId = nomItem.timePeriodId;
                break;
              case "UNIT_OF_MEASURE":
                unitId = nomItem.unitId;
                break;
              case "CLASSIFICATION":
                if (nomItem.classificationValueId) {
                  classificationValueIds.push(nomItem.classificationValueId);
                }
                break;
            }
          }
        }

        // Skip if no time period (required)
        if (!timePeriodId) {
          errors.push(`Row ${String(i)}: Could not resolve time period`);
          continue;
        }

        // Generate natural key hash
        const keyParts = [
          matrixId,
          territoryId ?? "null",
          timePeriodId,
          unitId ?? "null",
          ...classificationValueIds.sort(),
        ];
        const naturalKeyHash = createHash("md5")
          .update(keyParts.join(":"))
          .digest("hex");

        parsedRows.push({
          naturalKeyHash,
          territoryId,
          timePeriodId,
          unitId,
          value,
          valueStatus,
          classificationValueIds,
          sourceEncQuery,
        });
      } catch (error) {
        errors.push(`Row ${String(i)}: ${(error as Error).message}`);
      }
    }

    return { parsedRows, errors };
  }

  /**
   * Batch upsert statistics using ON CONFLICT with xmax tracking.
   * Uses PostgreSQL's xmax system column to distinguish inserts from updates.
   */
  private async batchUpsertStatistics(
    matrixId: number,
    parsedRows: ParsedRow[]
  ): Promise<{
    inserted: number;
    updated: number;
    hashToId: Map<string, number>;
  }> {
    let totalInserted = 0;
    let totalUpdated = 0;
    const hashToId = new Map<string, number>();

    // Deduplicate by natural_key_hash (keep last occurrence for each hash)
    const deduplicatedMap = new Map<string, ParsedRow>();
    for (const row of parsedRows) {
      deduplicatedMap.set(row.naturalKeyHash, row);
    }
    const deduplicatedRows = Array.from(deduplicatedMap.values());

    apiLogger.debug(
      { original: parsedRows.length, deduplicated: deduplicatedRows.length },
      "Deduplicated rows for batch insert"
    );

    for (let i = 0; i < deduplicatedRows.length; i += BATCH_SIZE) {
      const batch = deduplicatedRows.slice(i, i + BATCH_SIZE);

      // Build values for batch insert
      const values = batch.map((row) => ({
        matrix_id: matrixId,
        territory_id: row.territoryId,
        time_period_id: row.timePeriodId,
        unit_id: row.unitId,
        value: row.value,
        value_status: row.valueStatus,
        natural_key_hash: row.naturalKeyHash,
        source_enc_query: row.sourceEncQuery,
      }));

      // Use raw SQL for batch upsert with xmax tracking
      // xmax = 0 means the row was inserted, otherwise it was updated
      // Insert directly to partition table for ON CONFLICT with partial unique index
      const partitionTable = `statistics_matrix_${String(matrixId)}`;
      const result = await sql<{
        id: number;
        natural_key_hash: string;
        was_inserted: boolean;
      }>`
        INSERT INTO ${sql.raw(partitionTable)} (matrix_id, territory_id, time_period_id, unit_id, value, value_status, natural_key_hash, source_enc_query)
        SELECT
          (v->>'matrix_id')::int,
          (v->>'territory_id')::int,
          (v->>'time_period_id')::int,
          (v->>'unit_id')::int,
          (v->>'value')::numeric,
          v->>'value_status',
          v->>'natural_key_hash',
          v->>'source_enc_query'
        FROM jsonb_array_elements(${JSON.stringify(values)}::jsonb) AS v
        ON CONFLICT (natural_key_hash) WHERE natural_key_hash IS NOT NULL DO UPDATE SET
          value = EXCLUDED.value,
          value_status = EXCLUDED.value_status,
          updated_at = NOW()
        RETURNING id, natural_key_hash, (xmax = 0) AS was_inserted
      `.execute(this.db);

      // Count inserts vs updates using xmax
      for (const row of result.rows) {
        hashToId.set(row.natural_key_hash, row.id);
        if (row.was_inserted) {
          totalInserted++;
        } else {
          totalUpdated++;
        }
      }

      apiLogger.debug(
        {
          batchIndex: Math.floor(i / BATCH_SIZE),
          batchSize: batch.length,
          inserted: result.rows.filter((r) => r.was_inserted).length,
          updated: result.rows.filter((r) => !r.was_inserted).length,
        },
        "Batch upsert complete"
      );
    }

    return { inserted: totalInserted, updated: totalUpdated, hashToId };
  }

  /**
   * Batch insert classifications for newly inserted statistics.
   * Uses ON CONFLICT DO NOTHING for idempotency.
   */
  private async batchInsertClassifications(
    matrixId: number,
    parsedRows: ParsedRow[],
    hashToId: Map<string, number>
  ): Promise<void> {
    // Collect all classification associations
    const classificationValues: {
      matrix_id: number;
      statistic_id: number;
      classification_value_id: number;
    }[] = [];

    for (const row of parsedRows) {
      const statisticId = hashToId.get(row.naturalKeyHash);
      if (!statisticId || row.classificationValueIds.length === 0) continue;

      for (const cvId of row.classificationValueIds) {
        classificationValues.push({
          matrix_id: matrixId,
          statistic_id: statisticId,
          classification_value_id: cvId,
        });
      }
    }

    if (classificationValues.length === 0) return;

    // Batch insert classifications in chunks
    for (let i = 0; i < classificationValues.length; i += BATCH_SIZE) {
      const batch = classificationValues.slice(i, i + BATCH_SIZE);

      await sql`
        INSERT INTO statistic_classifications (matrix_id, statistic_id, classification_value_id)
        SELECT
          (v->>'matrix_id')::int,
          (v->>'statistic_id')::int,
          (v->>'classification_value_id')::int
        FROM jsonb_array_elements(${JSON.stringify(batch)}::jsonb) AS v
        ON CONFLICT (matrix_id, statistic_id, classification_value_id) DO NOTHING
      `.execute(this.db);

      apiLogger.debug(
        {
          batchIndex: Math.floor(i / BATCH_SIZE),
          batchSize: batch.length,
        },
        "Classification batch insert complete"
      );
    }
  }

  /**
   * Insert parsed data into statistics table using batch operations.
   * Falls back to row-by-row if batch fails.
   */
  private async insertData(
    matrixId: number,
    dimensions: DimensionInfo[],
    rows: string[][],
    sourceEncQuery: string,
    onProgress?: (progress: DataSyncProgress) => void
  ): Promise<DataSyncResult> {
    if (rows.length === 0) {
      return { rowsInserted: 0, rowsUpdated: 0, errors: [] };
    }

    onProgress?.({
      phase: "Preprocessing rows",
      current: 0,
      total: rows.length,
    });

    // Preprocess all rows
    const { parsedRows, errors: parseErrors } = this.preprocessRows(
      matrixId,
      dimensions,
      rows,
      sourceEncQuery
    );

    if (parsedRows.length === 0) {
      return { rowsInserted: 0, rowsUpdated: 0, errors: parseErrors };
    }

    apiLogger.info(
      {
        totalRows: rows.length,
        parsedRows: parsedRows.length,
        parseErrors: parseErrors.length,
      },
      "Preprocessing complete, starting batch insert"
    );

    try {
      onProgress?.({
        phase: "Batch inserting statistics",
        current: 0,
        total: parsedRows.length,
      });

      // Batch upsert statistics
      const { inserted, updated, hashToId } = await this.batchUpsertStatistics(
        matrixId,
        parsedRows
      );

      onProgress?.({
        phase: "Inserting classifications",
        current: inserted + updated,
        total: parsedRows.length,
      });

      // Batch insert classifications
      await this.batchInsertClassifications(matrixId, parsedRows, hashToId);

      apiLogger.info(
        { inserted, updated, errors: parseErrors.length },
        "Batch insert completed successfully"
      );

      return {
        rowsInserted: inserted,
        rowsUpdated: updated,
        errors: parseErrors,
      };
    } catch (error) {
      // Fallback to row-by-row for debugging
      const errorMsg = error instanceof Error ? error.message : String(error);
      apiLogger.warn(
        { error: errorMsg },
        "Batch insert failed, falling back to row-by-row"
      );

      return this.insertDataRowByRow(
        matrixId,
        dimensions,
        rows,
        sourceEncQuery,
        onProgress
      );
    }
  }

  /**
   * Insert parsed data into statistics table (row-by-row fallback)
   * Used when batch insert fails for debugging purposes
   */
  private async insertDataRowByRow(
    matrixId: number,
    dimensions: DimensionInfo[],
    rows: string[][],
    sourceEncQuery: string,
    onProgress?: (progress: DataSyncProgress) => void
  ): Promise<DataSyncResult> {
    let rowsInserted = 0;
    let rowsUpdated = 0;
    const errors: string[] = [];

    // Process each row
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i]!;

      if (i % 100 === 0) {
        onProgress?.({
          phase: "Inserting data",
          current: i,
          total: rows.length,
        });
      }

      try {
        // Last column is value, others are dimension labels
        const valueStr = row[row.length - 1] ?? "";
        const value =
          valueStr === ":" || valueStr === "" ? null : parseFloat(valueStr);
        const valueStatus = valueStr === ":" ? "missing" : null;

        // Find matching entities for each dimension
        let territoryId: number | null = null;
        let timePeriodId: number | null = null;
        let unitId: number | null = null;
        const classificationValueIds: number[] = [];

        for (
          let dimIdx = 0;
          dimIdx < dimensions.length && dimIdx < row.length - 1;
          dimIdx++
        ) {
          const dim = dimensions[dimIdx]!;
          const cellLabel = row[dimIdx]!;

          // Find matching nom_item by label
          const nomItem = dim.nomItems.find(
            (item) =>
              item.labelRo === cellLabel ||
              item.labelRo.includes(cellLabel) ||
              cellLabel.includes(item.labelRo)
          );

          if (nomItem) {
            switch (dim.dimensionType) {
              case "TERRITORIAL":
                territoryId = nomItem.territoryId;
                break;
              case "TEMPORAL":
                timePeriodId = nomItem.timePeriodId;
                break;
              case "UNIT_OF_MEASURE":
                unitId = nomItem.unitId;
                break;
              case "CLASSIFICATION":
                if (nomItem.classificationValueId) {
                  classificationValueIds.push(nomItem.classificationValueId);
                }
                break;
            }
          }
        }

        // Skip if no time period (required)
        if (!timePeriodId) {
          errors.push(`Row ${String(i)}: Could not resolve time period`);
          continue;
        }

        // Generate natural key hash
        const keyParts = [
          matrixId,
          territoryId ?? "null",
          timePeriodId,
          unitId ?? "null",
          ...classificationValueIds.sort(),
        ];
        const naturalKeyHash = createHash("md5")
          .update(keyParts.join(":"))
          .digest("hex");

        // Upsert into statistics
        const existing = await this.db
          .selectFrom("statistics")
          .select("id")
          .where("matrix_id", "=", matrixId)
          .where("natural_key_hash", "=", naturalKeyHash)
          .executeTakeFirst();

        if (existing) {
          await this.db
            .updateTable("statistics")
            .set({
              value: value,
              value_status: valueStatus,
              updated_at: new Date(),
            })
            .where("id", "=", existing.id)
            .execute();
          rowsUpdated++;
        } else {
          const insertResult = await this.db
            .insertInto("statistics")
            .values({
              matrix_id: matrixId,
              territory_id: territoryId,
              time_period_id: timePeriodId,
              unit_id: unitId,
              value: value,
              value_status: valueStatus,
              natural_key_hash: naturalKeyHash,
              source_enc_query: sourceEncQuery,
            })
            .returning("id")
            .executeTakeFirst();

          if (insertResult && classificationValueIds.length > 0) {
            // Insert classification associations
            await this.db
              .insertInto("statistic_classifications")
              .values(
                classificationValueIds.map((cvId) => ({
                  matrix_id: matrixId,
                  statistic_id: insertResult.id,
                  classification_value_id: cvId,
                }))
              )
              .execute();
          }

          rowsInserted++;
        }
      } catch (error) {
        errors.push(`Row ${String(i)}: ${(error as Error).message}`);
      }
    }

    return { rowsInserted, rowsUpdated, errors };
  }
}
