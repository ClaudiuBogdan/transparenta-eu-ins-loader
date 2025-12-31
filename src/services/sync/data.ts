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
import { queryMatrix, buildEncQuery } from "../../scraper/client.js";

import type { Database, DimensionType } from "../../db/types.js";

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
  /** Resume from last checkpoint */
  resume?: boolean;
  /** Force re-sync even if checkpoints exist */
  force?: boolean;
  /** Enable verbose debug logging */
  verbose?: boolean;
  /** Progress callback */
  onProgress?: (progress: FullSyncProgress) => void;
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

// ============================================================================
// Data Sync Service
// ============================================================================

export class DataSyncService {
  constructor(private readonly db: Kysely<Database>) {}

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
      resume = true,
      force = false,
      verbose = false,
      onProgress,
    } = options;

    const startTime = Date.now();
    const result: FullSyncResult = {
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

    const chunkResult = chunkGenerator.generateChunks(matrixInfo, {
      yearFrom,
      yearTo,
      classificationMode,
      countyCode,
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

      // Check if chunk already synced (via checkpoint)
      if (resume && !force) {
        const checkpoint = await this.getCheckpoint(
          matrixInfo.id,
          chunk.chunkHash
        );
        if (checkpoint) {
          if (verbose) {
            apiLogger.debug({ chunkName, checkpoint }, "Skipping synced chunk");
          }
          result.chunksSkipped++;
          continue;
        }
      }

      try {
        // Build selections for this chunk
        const selections = await chunkGenerator.buildChunkSelections(
          matrixInfo,
          chunk
        );
        const encQuery = buildEncQuery(selections);
        const cellCount = chunkGenerator.estimateCellCount(selections);

        if (verbose) {
          apiLogger.debug(
            { chunkName, encQuery: encQuery.substring(0, 100), cellCount },
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
        await this.recordCheckpoint(
          matrixInfo.id,
          chunk,
          rows.length,
          cellCount
        );

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
        await this.recordFailedCheckpoint(matrixInfo.id, chunk, errorMsg);
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
   * Get checkpoint for a chunk
   */
  private async getCheckpoint(
    matrixId: number,
    chunkHash: string
  ): Promise<{ rowCount: number; lastSyncedAt: Date } | null> {
    const checkpoint = await this.db
      .selectFrom("sync_checkpoints")
      .select(["row_count", "last_synced_at"])
      .where("matrix_id", "=", matrixId)
      .where("chunk_hash", "=", chunkHash)
      .where("error_message", "is", null)
      .executeTakeFirst();

    if (!checkpoint) return null;

    return {
      rowCount: checkpoint.row_count,
      lastSyncedAt: checkpoint.last_synced_at,
    };
  }

  /**
   * Record successful checkpoint
   */
  private async recordCheckpoint(
    matrixId: number,
    chunk: SyncChunk,
    rowCount: number,
    cellsQueried: number
  ): Promise<void> {
    await this.db
      .insertInto("sync_checkpoints")
      .values({
        matrix_id: matrixId,
        chunk_hash: chunk.chunkHash,
        chunk_query: `${chunk.countyCode ?? "NAT"}:${String(chunk.year)}:${chunk.classificationMode}`,
        county_code: chunk.countyCode,
        year: chunk.year,
        classification_mode: chunk.classificationMode,
        cells_queried: cellsQueried,
        cells_returned: rowCount,
        row_count: rowCount,
        last_synced_at: new Date(),
        error_message: null,
        retry_count: 0,
      })
      .onConflict((oc) =>
        oc.columns(["matrix_id", "chunk_hash"]).doUpdateSet({
          row_count: rowCount,
          cells_queried: cellsQueried,
          cells_returned: rowCount,
          last_synced_at: new Date(),
          error_message: null,
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
    errorMessage: string
  ): Promise<void> {
    await this.db
      .insertInto("sync_checkpoints")
      .values({
        matrix_id: matrixId,
        chunk_hash: chunk.chunkHash,
        chunk_query: `${chunk.countyCode ?? "NAT"}:${String(chunk.year)}:${chunk.classificationMode}`,
        county_code: chunk.countyCode,
        year: chunk.year,
        classification_mode: chunk.classificationMode,
        cells_queried: null,
        cells_returned: null,
        row_count: 0,
        last_synced_at: new Date(),
        error_message: errorMessage.substring(0, 1000),
        retry_count: 1,
      })
      .onConflict((oc) =>
        oc.columns(["matrix_id", "chunk_hash"]).doUpdateSet({
          error_message: errorMessage.substring(0, 1000),
          last_synced_at: new Date(),
          retry_count: sql`retry_count + 1`,
        })
      )
      .execute();
  }

  /**
   * Update coverage metrics for a matrix
   */
  private async updateCoverage(matrixId: number): Promise<void> {
    // Count synced statistics
    const stats = await this.db
      .selectFrom("statistics")
      .select([
        this.db.fn.count<number>("id").as("total_rows"),
        this.db.fn
          .countAll<number>()
          .filterWhere("value", "is", null)
          .as("null_count"),
        this.db.fn
          .countAll<number>()
          .filterWhere("value_status", "=", "missing")
          .as("missing_count"),
      ])
      .where("matrix_id", "=", matrixId)
      .executeTakeFirst();

    // Count unique territories
    const territories = await this.db
      .selectFrom("statistics")
      .select(this.db.fn.count<number>("territory_id").distinct().as("count"))
      .where("matrix_id", "=", matrixId)
      .where("territory_id", "is not", null)
      .executeTakeFirst();

    // Count unique years
    const years = await this.db
      .selectFrom("statistics")
      .innerJoin("time_periods", "statistics.time_period_id", "time_periods.id")
      .select(
        this.db.fn.count<number>("time_periods.year").distinct().as("count")
      )
      .where("statistics.matrix_id", "=", matrixId)
      .executeTakeFirst();

    // Get total territories from nom_items
    const totalTerritories = await this.db
      .selectFrom("matrix_nom_items")
      .select(this.db.fn.count<number>("id").as("count"))
      .where("matrix_id", "=", matrixId)
      .where("dimension_type", "=", "TERRITORIAL")
      .executeTakeFirst();

    // Get total years from nom_items
    const totalYears = await this.db
      .selectFrom("matrix_nom_items")
      .select(this.db.fn.count<number>("id").as("count"))
      .where("matrix_id", "=", matrixId)
      .where("dimension_type", "=", "TEMPORAL")
      .executeTakeFirst();

    // Upsert coverage
    await this.db
      .insertInto("sync_coverage")
      .values({
        matrix_id: matrixId,
        total_territories: totalTerritories?.count ?? 0,
        synced_territories: territories?.count ?? 0,
        total_years: totalYears?.count ?? 0,
        synced_years: years?.count ?? 0,
        total_classifications: 0, // TODO: calculate
        synced_classifications: 0,
        actual_data_points: stats?.total_rows ?? 0,
        expected_data_points: null,
        null_value_count: stats?.null_count ?? 0,
        missing_value_count: stats?.missing_count ?? 0,
        first_sync_at: new Date(),
        last_sync_at: new Date(),
      })
      .onConflict((oc) =>
        oc.column("matrix_id").doUpdateSet({
          synced_territories: territories?.count ?? 0,
          synced_years: years?.count ?? 0,
          actual_data_points: stats?.total_rows ?? 0,
          null_value_count: stats?.null_count ?? 0,
          missing_value_count: stats?.missing_count ?? 0,
          last_sync_at: new Date(),
          last_coverage_update: new Date(),
        })
      )
      .execute();

    apiLogger.info(
      {
        matrixId,
        syncedTerritories: territories?.count ?? 0,
        syncedYears: years?.count ?? 0,
        totalRows: stats?.total_rows ?? 0,
      },
      "Updated coverage metrics"
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
   * Insert parsed data into statistics table
   */
  private async insertData(
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
