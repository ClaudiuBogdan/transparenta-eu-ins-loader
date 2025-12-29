import { sql, type Kysely } from "kysely";

import { SyncCheckpointService } from "./checkpoints.js";
import { MatrixSyncService } from "./matrices.js";
import {
  computeNaturalKeyHash,
  upsertStatistic,
  type StatisticWithHash,
} from "./upsert.js";
import { logger } from "../../logger.js";
import {
  fetchMatrix,
  queryMatrix,
  buildEncQuery,
  estimateCellCount,
  parsePivotResponse,
} from "../../scraper/client.js";

import type {
  Database,
  ChunkStrategy,
  DataSyncResult,
  NewScrapeJob,
  NewScrapeChunk,
  ScrapeStatus,
} from "../../db/types.js";
import type { InsMatrix, InsDimension } from "../../types/index.js";

// ============================================================================
// Constants
// ============================================================================

const CELL_LIMIT = 30_000;
const RATE_LIMIT_MS = 750;

const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

// ============================================================================
// Types
// ============================================================================

interface ParsedDataRow {
  dimensionValues: Map<number, string>;
  value: number | null;
  valueStatus: string | null;
}

// ============================================================================
// Data Sync Service
// ============================================================================

export class DataSyncService {
  private matrixService: MatrixSyncService;
  private checkpointService: SyncCheckpointService;

  constructor(private db: Kysely<Database>) {
    this.matrixService = new MatrixSyncService(db);
    this.checkpointService = new SyncCheckpointService(db);
  }

  /**
   * Sync statistical data for a matrix
   */
  async syncMatrixData(
    matrixCode: string,
    options?: {
      yearRange?: string;
      resume?: boolean;
      limit?: number;
      forceRefresh?: boolean;
      incremental?: boolean;
    }
  ): Promise<DataSyncResult> {
    logger.info({ matrixCode, options }, "Starting data sync");

    // Get matrix ID
    let matrixId = await this.matrixService.getMatrixId(matrixCode);

    if (!matrixId) {
      // Sync matrix metadata first
      await this.matrixService.syncMatrixDetails(matrixCode);
      matrixId = await this.matrixService.getMatrixId(matrixCode);
    }

    if (!matrixId) {
      throw new Error(`Matrix ${matrixCode} not found`);
    }

    // Ensure partitions exist
    await this.ensurePartitions(matrixId);

    // Check for resume
    if (options?.resume) {
      const pendingJob = await this.findPendingJob(matrixId);
      if (pendingJob) {
        logger.info({ jobId: pendingJob.id }, "Resuming pending job");
        return this.resumeJob(pendingJob.id, matrixCode);
      }
    }

    // Fetch matrix metadata
    const matrix = await fetchMatrix(matrixCode);

    // Estimate cells
    const selections = this.getAllSelections(matrix);
    const estimatedCells = estimateCellCount(selections);

    logger.info({ matrixCode, estimatedCells }, "Estimated cell count");

    // Create job
    const job = await this.createJob(matrixId, estimatedCells);

    try {
      await this.updateJobStatus(job.id, "RUNNING");

      const limit = options?.limit;

      if (estimatedCells > CELL_LIMIT) {
        // Chunk by year
        return await this.syncChunked(
          job.id,
          matrixCode,
          matrix,
          matrixId,
          limit
        );
      } else {
        // Single query
        return await this.syncSingle(
          job.id,
          matrixCode,
          matrix,
          matrixId,
          limit
        );
      }
    } catch (error) {
      await this.handleJobError(job.id, error as Error);
      throw error;
    }
  }

  /**
   * Sync with a single query (for small matrices)
   */
  private async syncSingle(
    jobId: number,
    matrixCode: string,
    matrix: InsMatrix,
    matrixId: number,
    limit?: number
  ): Promise<DataSyncResult> {
    const selections = this.getAllSelections(matrix);
    const encQuery = buildEncQuery(selections);

    const csvData = await queryMatrix({
      encQuery,
      language: "ro",
      matCode: matrixCode,
      matMaxDim: matrix.details.matMaxDim,
      matRegJ: matrix.details.matRegJ,
      matUMSpec: matrix.details.matUMSpec,
    });

    const rows = parsePivotResponse(csvData);
    const parsed = this.parseRows(rows, matrix);

    // Apply limit if specified
    const rowsToProcess = limit ? parsed.slice(0, limit) : parsed;
    if (limit) {
      logger.info({ limit, totalRows: parsed.length }, "Applying row limit");
    }

    let rowsInserted = 0;
    let rowsUpdated = 0;
    for (const row of rowsToProcess) {
      const result = await this.upsertStatisticRow(
        matrixId,
        matrix,
        row,
        encQuery
      );
      if (result.inserted) rowsInserted++;
      if (result.updated) rowsUpdated++;
    }

    await this.completeJob(jobId, rowsInserted + rowsUpdated);

    return { rowsInserted, rowsUpdated };
  }

  /**
   * Sync with chunked queries (for large matrices)
   */
  private async syncChunked(
    jobId: number,
    matrixCode: string,
    matrix: InsMatrix,
    matrixId: number,
    limit?: number
  ): Promise<DataSyncResult> {
    // Find time dimension
    const timeDim = matrix.dimensionsMap.find(
      (d) => this.matrixService.detectDimensionType(d) === "TEMPORAL"
    );

    if (!timeDim) {
      throw new Error("No temporal dimension found for chunking");
    }

    // Create chunks (one per time period)
    const chunks: { chunkNumber: number; nomItemId: number; label: string }[] =
      [];
    for (const opt of timeDim.options) {
      chunks.push({
        chunkNumber: chunks.length + 1,
        nomItemId: opt.nomItemId,
        label: opt.label,
      });
    }

    // Update job with chunk info
    await this.db
      .updateTable("scrape_jobs")
      .set({
        strategy: "BY_YEAR" as ChunkStrategy,
        total_chunks: chunks.length,
      })
      .where("id", "=", jobId)
      .execute();

    // Insert chunk records
    for (const chunk of chunks) {
      const encQuery = this.buildChunkQuery(matrix, timeDim, chunk.nomItemId);
      const newChunk: NewScrapeChunk = {
        job_id: jobId,
        chunk_number: chunk.chunkNumber,
        enc_query: encQuery,
        status: "PENDING",
      };
      await this.db.insertInto("scrape_chunks").values(newChunk).execute();
    }

    // Process chunks (apply limit if specified - limits number of chunks)
    const chunksToProcess = limit ? chunks.slice(0, limit) : chunks;
    if (limit) {
      logger.info(
        { limit, totalChunks: chunks.length },
        "Applying chunk limit"
      );
    }

    let totalInserted = 0;
    let totalUpdated = 0;
    let completedChunks = 0;

    for (const chunk of chunksToProcess) {
      const encQuery = this.buildChunkQuery(matrix, timeDim, chunk.nomItemId);

      try {
        await this.updateChunkStatus(jobId, chunk.chunkNumber, "RUNNING");

        const csvData = await queryMatrix({
          encQuery,
          language: "ro",
          matCode: matrixCode,
          matMaxDim: matrix.details.matMaxDim,
          matRegJ: matrix.details.matRegJ,
          matUMSpec: matrix.details.matUMSpec,
        });

        const rows = parsePivotResponse(csvData);
        const parsed = this.parseRows(rows, matrix);

        let chunkInserted = 0;
        let chunkUpdated = 0;
        for (const row of parsed) {
          const result = await this.upsertStatisticRow(
            matrixId,
            matrix,
            row,
            encQuery
          );
          if (result.inserted) chunkInserted++;
          if (result.updated) chunkUpdated++;
        }

        await this.completeChunk(
          jobId,
          chunk.chunkNumber,
          chunkInserted + chunkUpdated
        );
        totalInserted += chunkInserted;
        totalUpdated += chunkUpdated;
        completedChunks++;

        // Save checkpoint for incremental sync
        await this.checkpointService.saveCheckpoint(
          matrixId,
          encQuery,
          chunkInserted + chunkUpdated
        );

        // Update job progress
        await this.db
          .updateTable("scrape_jobs")
          .set({ completed_chunks: completedChunks })
          .where("id", "=", jobId)
          .execute();

        logger.info(
          {
            matrixCode,
            chunk: chunk.chunkNumber,
            total: chunks.length,
            inserted: chunkInserted,
            updated: chunkUpdated,
          },
          "Completed chunk"
        );

        await sleep(RATE_LIMIT_MS);
      } catch (error) {
        await this.handleChunkError(jobId, chunk.chunkNumber, error as Error);
        logger.error(
          { matrixCode, chunk: chunk.chunkNumber, error },
          "Chunk failed"
        );
        // Continue with next chunk
      }
    }

    await this.completeJob(jobId, totalInserted + totalUpdated);

    return {
      rowsInserted: totalInserted,
      rowsUpdated: totalUpdated,
      chunksCompleted: completedChunks,
      totalChunks: chunks.length,
    };
  }

  /**
   * Build encQuery for a specific time period chunk
   */
  private buildChunkQuery(
    matrix: InsMatrix,
    timeDim: InsDimension,
    timeNomItemId: number
  ): string {
    const selections: number[][] = [];

    for (const dim of matrix.dimensionsMap) {
      if (dim.dimCode === timeDim.dimCode) {
        // Only select the specific time period
        selections.push([timeNomItemId]);
      } else {
        // Select all options
        selections.push(dim.options.map((o) => o.nomItemId));
      }
    }

    return buildEncQuery(selections);
  }

  /**
   * Get all selections (all options for all dimensions)
   */
  private getAllSelections(matrix: InsMatrix): number[][] {
    return matrix.dimensionsMap.map((dim) =>
      dim.options.map((opt) => opt.nomItemId)
    );
  }

  /**
   * Parse CSV rows into structured data
   */
  private parseRows(rows: string[][], matrix: InsMatrix): ParsedDataRow[] {
    if (rows.length < 2) return [];

    const headers = rows[0];
    if (!headers) return [];

    const dataRows = rows.slice(1);
    const result: ParsedDataRow[] = [];

    for (const row of dataRows) {
      if (row.length < headers.length) continue;

      const dimensionValues = new Map<number, string>();

      // Map each column to its dimension
      for (let i = 0; i < matrix.dimensionsMap.length; i++) {
        const dim = matrix.dimensionsMap[i];
        const cell = row[i];
        if (dim && cell !== undefined) {
          dimensionValues.set(dim.dimCode, cell.trim());
        }
      }

      // Last column is the value
      const lastValue = row.at(-1);
      const valueStr = lastValue?.trim() ?? "";
      let value: number | null = null;
      let valueStatus: string | null = null;

      // Parse value
      if (valueStr === ":" || valueStr === "-" || valueStr === "*") {
        valueStatus = valueStr;
      } else if (valueStr.startsWith("<")) {
        valueStatus = valueStr;
      } else {
        const parsed = Number.parseFloat(valueStr.replaceAll(",", "."));
        if (!Number.isNaN(parsed)) {
          value = parsed;
        }
      }

      result.push({ dimensionValues, value, valueStatus });
    }

    return result;
  }

  /**
   * Upsert a statistic row with idempotency support
   *
   * Returns { inserted: true } for new rows, { updated: true } for updates
   */
  private async upsertStatisticRow(
    matrixId: number,
    matrix: InsMatrix,
    row: ParsedDataRow,
    encQuery: string
  ): Promise<{ inserted: boolean; updated: boolean }> {
    // Lookup reference IDs
    const territoryId = await this.lookupTerritory(matrixId, matrix, row);
    const timePeriodId = await this.lookupTimePeriod(matrixId, matrix, row);
    const unitId = await this.lookupUnit(matrixId, matrix, row);

    if (timePeriodId === null) {
      logger.warn(
        { row: Object.fromEntries(row.dimensionValues) },
        "No time period found"
      );
      return { inserted: false, updated: false };
    }

    // Collect classification IDs BEFORE inserting (needed for natural key hash)
    const classificationIds = await this.collectClassificationIds(
      matrixId,
      matrix,
      row
    );

    // Compute natural key hash
    const naturalKeyHash = computeNaturalKeyHash(
      matrixId,
      territoryId,
      timePeriodId,
      unitId,
      classificationIds
    );

    // Prepare statistic with hash
    const statWithHash: StatisticWithHash = {
      matrix_id: matrixId,
      territory_id: territoryId,
      time_period_id: timePeriodId,
      unit_of_measure_id: unitId,
      value: row.value,
      value_status: row.valueStatus,
      source_enc_query: encQuery,
      natural_key_hash: naturalKeyHash,
    };

    try {
      const result = await upsertStatistic(this.db, statWithHash);

      // Only insert classifications for new rows
      if (result.inserted) {
        await this.insertClassifications(
          matrixId,
          result.id,
          matrix,
          row,
          classificationIds
        );
      }

      return { inserted: result.inserted, updated: result.updated };
    } catch (error) {
      logger.error(
        { matrixId, error, hash: naturalKeyHash },
        "Statistic upsert failed"
      );
      return { inserted: false, updated: false };
    }
  }

  /**
   * Collect classification value IDs for a row
   */
  private async collectClassificationIds(
    matrixId: number,
    matrix: InsMatrix,
    row: ParsedDataRow
  ): Promise<number[]> {
    const classificationDims = matrix.dimensionsMap.filter(
      (d) => this.matrixService.detectDimensionType(d) === "CLASSIFICATION"
    );

    const ids: number[] = [];

    for (const dim of classificationDims) {
      const label = row.dimensionValues.get(dim.dimCode);
      if (!label) {
        logger.debug(
          { dimCode: dim.dimCode, dimLabel: dim.label },
          "No label found for dimension"
        );
        continue;
      }

      // Query by dimension code to get correct classification
      // Labels are normalized (trimmed) at storage time in matrix sync
      const result = await this.db
        .selectFrom("matrix_dimension_options")
        .innerJoin(
          "matrix_dimensions",
          "matrix_dimension_options.matrix_dimension_id",
          "matrix_dimensions.id"
        )
        .select("matrix_dimension_options.classification_value_id")
        .where("matrix_dimensions.matrix_id", "=", matrixId)
        .where("matrix_dimensions.dim_code", "=", dim.dimCode)
        .where("matrix_dimension_options.label", "=", label)
        .executeTakeFirst();

      if (result?.classification_value_id) {
        ids.push(result.classification_value_id);
      } else {
        logger.debug(
          { dimCode: dim.dimCode, dimLabel: dim.label, optionLabel: label },
          "No classification value found for option"
        );
      }
    }

    return ids;
  }

  /**
   * Lookup territory ID from row data
   */
  private async lookupTerritory(
    matrixId: number,
    matrix: InsMatrix,
    row: ParsedDataRow
  ): Promise<number | null> {
    // Find territorial dimension
    const territorialDim = matrix.dimensionsMap.find(
      (d) => this.matrixService.detectDimensionType(d) === "TERRITORIAL"
    );

    if (!territorialDim) return null;

    const label = row.dimensionValues.get(territorialDim.dimCode);
    if (!label) return null;

    // Find in matrix_dimension_options (labels normalized at storage)
    const result = await this.db
      .selectFrom("matrix_dimension_options")
      .innerJoin(
        "matrix_dimensions",
        "matrix_dimension_options.matrix_dimension_id",
        "matrix_dimensions.id"
      )
      .select("matrix_dimension_options.territory_id")
      .where("matrix_dimensions.matrix_id", "=", matrixId)
      .where("matrix_dimension_options.label", "=", label)
      .executeTakeFirst();

    return result?.territory_id ?? null;
  }

  /**
   * Lookup time period ID from row data
   */
  private async lookupTimePeriod(
    matrixId: number,
    matrix: InsMatrix,
    row: ParsedDataRow
  ): Promise<number | null> {
    // Find temporal dimension
    const temporalDim = matrix.dimensionsMap.find(
      (d) => this.matrixService.detectDimensionType(d) === "TEMPORAL"
    );

    if (!temporalDim) return null;

    const label = row.dimensionValues.get(temporalDim.dimCode);
    if (!label) return null;

    // Find in matrix_dimension_options (labels normalized at storage)
    const result = await this.db
      .selectFrom("matrix_dimension_options")
      .innerJoin(
        "matrix_dimensions",
        "matrix_dimension_options.matrix_dimension_id",
        "matrix_dimensions.id"
      )
      .select("matrix_dimension_options.time_period_id")
      .where("matrix_dimensions.matrix_id", "=", matrixId)
      .where("matrix_dimension_options.label", "=", label)
      .executeTakeFirst();

    return result?.time_period_id ?? null;
  }

  /**
   * Lookup unit of measure ID from row data
   */
  private async lookupUnit(
    matrixId: number,
    matrix: InsMatrix,
    row: ParsedDataRow
  ): Promise<number | null> {
    // Find unit dimension
    const unitDim = matrix.dimensionsMap.find(
      (d) => this.matrixService.detectDimensionType(d) === "UNIT_OF_MEASURE"
    );

    if (!unitDim) return null;

    const label = row.dimensionValues.get(unitDim.dimCode);
    if (!label) return null;

    // Find in matrix_dimension_options (labels normalized at storage)
    const result = await this.db
      .selectFrom("matrix_dimension_options")
      .innerJoin(
        "matrix_dimensions",
        "matrix_dimension_options.matrix_dimension_id",
        "matrix_dimensions.id"
      )
      .select("matrix_dimension_options.unit_of_measure_id")
      .where("matrix_dimensions.matrix_id", "=", matrixId)
      .where("matrix_dimension_options.label", "=", label)
      .executeTakeFirst();

    return result?.unit_of_measure_id ?? null;
  }

  /**
   * Insert classification junction records
   *
   * Uses pre-collected classification IDs for efficiency
   */
  private async insertClassifications(
    matrixId: number,
    statisticId: number,
    _matrix: InsMatrix,
    _row: ParsedDataRow,
    classificationIds: number[]
  ): Promise<void> {
    for (const classificationValueId of classificationIds) {
      try {
        await this.db
          .insertInto("statistic_classifications")
          .values({
            matrix_id: matrixId,
            statistic_id: statisticId,
            classification_value_id: classificationValueId,
          })
          .execute();
      } catch {
        // Duplicate, ignore
      }
    }
  }

  // ============================================================================
  // Job Management
  // ============================================================================

  private async ensurePartitions(matrixId: number): Promise<void> {
    await sql`SELECT create_statistics_partition(${matrixId})`.execute(this.db);
    await sql`SELECT create_stat_classifications_partition(${matrixId})`.execute(
      this.db
    );
  }

  private async createJob(
    matrixId: number,
    estimatedCells: number
  ): Promise<{ id: number }> {
    const newJob: NewScrapeJob = {
      matrix_id: matrixId,
      status: "PENDING",
      estimated_cells: estimatedCells,
    };

    const result = await this.db
      .insertInto("scrape_jobs")
      .values(newJob)
      .returning("id")
      .executeTakeFirst();

    return { id: result!.id };
  }

  private async updateJobStatus(
    jobId: number,
    status: ScrapeStatus
  ): Promise<void> {
    const updates: Record<string, unknown> = { status };
    if (status === "RUNNING") {
      updates.started_at = new Date();
    } else if (status === "COMPLETED" || status === "FAILED") {
      updates.completed_at = new Date();
    }

    await this.db
      .updateTable("scrape_jobs")
      .set(updates)
      .where("id", "=", jobId)
      .execute();
  }

  private async completeJob(jobId: number, rowsFetched: number): Promise<void> {
    await this.db
      .updateTable("scrape_jobs")
      .set({
        status: "COMPLETED",
        completed_at: new Date(),
        rows_fetched: rowsFetched,
      })
      .where("id", "=", jobId)
      .execute();
  }

  private async handleJobError(jobId: number, error: Error): Promise<void> {
    await this.db
      .updateTable("scrape_jobs")
      .set({
        status: "FAILED",
        completed_at: new Date(),
        error_message: error.message,
      })
      .where("id", "=", jobId)
      .execute();
  }

  private async updateChunkStatus(
    jobId: number,
    chunkNumber: number,
    status: ScrapeStatus
  ): Promise<void> {
    const updates: Record<string, unknown> = { status };
    if (status === "RUNNING") {
      updates.started_at = new Date();
    }

    await this.db
      .updateTable("scrape_chunks")
      .set(updates)
      .where("job_id", "=", jobId)
      .where("chunk_number", "=", chunkNumber)
      .execute();
  }

  private async completeChunk(
    jobId: number,
    chunkNumber: number,
    rowsFetched: number
  ): Promise<void> {
    await this.db
      .updateTable("scrape_chunks")
      .set({
        status: "COMPLETED",
        completed_at: new Date(),
        rows_fetched: rowsFetched,
      })
      .where("job_id", "=", jobId)
      .where("chunk_number", "=", chunkNumber)
      .execute();
  }

  private async handleChunkError(
    jobId: number,
    chunkNumber: number,
    error: Error
  ): Promise<void> {
    await this.db
      .updateTable("scrape_chunks")
      .set({
        status: "FAILED",
        completed_at: new Date(),
        error_message: error.message,
      })
      .where("job_id", "=", jobId)
      .where("chunk_number", "=", chunkNumber)
      .execute();
  }

  private async findPendingJob(
    matrixId: number
  ): Promise<{ id: number } | null> {
    const result = await this.db
      .selectFrom("scrape_jobs")
      .select("id")
      .where("matrix_id", "=", matrixId)
      .where("status", "in", ["PENDING", "RUNNING"])
      .orderBy("created_at", "desc")
      .executeTakeFirst();

    return result ? { id: result.id } : null;
  }

  private async resumeJob(
    jobId: number,
    matrixCode: string
  ): Promise<DataSyncResult> {
    // Get job info
    const job = await this.db
      .selectFrom("scrape_jobs")
      .selectAll()
      .where("id", "=", jobId)
      .executeTakeFirst();

    if (!job) {
      throw new Error(`Job ${String(jobId)} not found`);
    }

    // Get pending chunks
    const pendingChunks = await this.db
      .selectFrom("scrape_chunks")
      .selectAll()
      .where("job_id", "=", jobId)
      .where("status", "in", ["PENDING", "FAILED"])
      .orderBy("chunk_number")
      .execute();

    if (pendingChunks.length === 0) {
      logger.info({ jobId }, "No pending chunks to resume");
      return { rowsInserted: 0, rowsUpdated: 0 };
    }

    const matrix = await fetchMatrix(matrixCode);
    let totalInserted = 0;
    let totalUpdated = 0;

    for (const chunk of pendingChunks) {
      try {
        await this.updateChunkStatus(jobId, chunk.chunk_number, "RUNNING");

        const csvData = await queryMatrix({
          encQuery: chunk.enc_query,
          language: "ro",
          matCode: matrixCode,
          matMaxDim: matrix.details.matMaxDim,
          matRegJ: matrix.details.matRegJ,
          matUMSpec: matrix.details.matUMSpec,
        });

        const rows = parsePivotResponse(csvData);
        const parsed = this.parseRows(rows, matrix);

        let chunkInserted = 0;
        let chunkUpdated = 0;
        for (const row of parsed) {
          const result = await this.upsertStatisticRow(
            job.matrix_id,
            matrix,
            row,
            chunk.enc_query
          );
          if (result.inserted) chunkInserted++;
          if (result.updated) chunkUpdated++;
        }

        await this.completeChunk(
          jobId,
          chunk.chunk_number,
          chunkInserted + chunkUpdated
        );
        totalInserted += chunkInserted;
        totalUpdated += chunkUpdated;

        // Save checkpoint
        await this.checkpointService.saveCheckpoint(
          job.matrix_id,
          chunk.enc_query,
          chunkInserted + chunkUpdated
        );

        await sleep(RATE_LIMIT_MS);
      } catch (error) {
        await this.handleChunkError(jobId, chunk.chunk_number, error as Error);
      }
    }

    // Check if all chunks completed
    const remainingChunks = await this.db
      .selectFrom("scrape_chunks")
      .select(sql`count(*)`.as("count"))
      .where("job_id", "=", jobId)
      .where("status", "!=", "COMPLETED")
      .executeTakeFirst();

    if (Number(remainingChunks?.count ?? 0) === 0) {
      await this.completeJob(jobId, totalInserted + totalUpdated);
    }

    return { rowsInserted: totalInserted, rowsUpdated: totalUpdated };
  }
}
