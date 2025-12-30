import { sql, type Kysely } from "kysely";

import { SyncCheckpointService } from "./checkpoints.js";
import { MatrixSyncService } from "./matrices.js";
import { computeNaturalKeyHash, type StatisticWithHash } from "./upsert.js";
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
const BATCH_SIZE = 500;

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

/**
 * Cached lookup maps for dimension values.
 * Built once per matrix, used for all rows.
 */
interface LookupCache {
  // dimCode -> label -> territory_id
  territoryMap: Map<number, Map<string, number>>;
  // dimCode -> label -> time_period_id
  timePeriodMap: Map<number, Map<string, number>>;
  // dimCode -> label -> unit_of_measure_id
  unitMap: Map<number, Map<string, number>>;
  // dimCode -> label -> classification_value_id
  classificationMap: Map<number, Map<string, number>>;
}

/**
 * Prepared statistic ready for batch insert
 */
interface PreparedStatistic {
  stat: StatisticWithHash;
  classificationIds: number[];
}

/**
 * Chunk info for county+year chunking strategy
 */
interface CountyYearChunk {
  chunkNumber: number;
  countyNomItemId: number;
  countyLabel: string;
  localityNomItemIds: number[];
  timeNomItemId: number;
  timeLabel: string;
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

  // ============================================================================
  // Label Normalization
  // ============================================================================

  /**
   * Normalize a label for consistent lookup.
   * Handles: trailing whitespace, case normalization, diacritic handling.
   *
   * This ensures labels from CSV match stored labels even when there are
   * minor differences in whitespace, casing, or character encoding.
   */
  private normalizeLabel(label: string): string {
    return label
      .trim()
      .toLowerCase()
      .normalize("NFD")
      .replaceAll(/[\u0300-\u036f]/g, ""); // Remove diacritics
  }

  // ============================================================================
  // Lookup Cache
  // ============================================================================

  /**
   * Build lookup cache for all dimension options of a matrix.
   * This is called once per matrix and used for all rows.
   * Labels are normalized for consistent matching.
   */
  private async buildLookupCache(
    matrixId: number,
    _matrix: InsMatrix
  ): Promise<LookupCache> {
    const cache: LookupCache = {
      territoryMap: new Map(),
      timePeriodMap: new Map(),
      unitMap: new Map(),
      classificationMap: new Map(),
    };

    // Load all dimension options for this matrix in a single query
    const options = await this.db
      .selectFrom("matrix_dimension_options")
      .innerJoin(
        "matrix_dimensions",
        "matrix_dimension_options.matrix_dimension_id",
        "matrix_dimensions.id"
      )
      .select([
        "matrix_dimensions.dim_code",
        "matrix_dimensions.dimension_type",
        "matrix_dimension_options.label",
        "matrix_dimension_options.territory_id",
        "matrix_dimension_options.time_period_id",
        "matrix_dimension_options.unit_of_measure_id",
        "matrix_dimension_options.classification_value_id",
      ])
      .where("matrix_dimensions.matrix_id", "=", matrixId)
      .execute();

    // Build lookup maps using normalized labels for consistent matching
    for (const opt of options) {
      const dimCode = opt.dim_code;
      const normalizedLabel = this.normalizeLabel(opt.label);

      switch (opt.dimension_type) {
        case "TERRITORIAL":
          if (opt.territory_id !== null) {
            if (!cache.territoryMap.has(dimCode)) {
              cache.territoryMap.set(dimCode, new Map());
            }
            cache.territoryMap
              .get(dimCode)!
              .set(normalizedLabel, opt.territory_id);
          }
          break;
        case "TEMPORAL":
          if (opt.time_period_id !== null) {
            if (!cache.timePeriodMap.has(dimCode)) {
              cache.timePeriodMap.set(dimCode, new Map());
            }
            cache.timePeriodMap
              .get(dimCode)!
              .set(normalizedLabel, opt.time_period_id);
          }
          break;
        case "UNIT_OF_MEASURE":
          if (opt.unit_of_measure_id !== null) {
            if (!cache.unitMap.has(dimCode)) {
              cache.unitMap.set(dimCode, new Map());
            }
            cache.unitMap
              .get(dimCode)!
              .set(normalizedLabel, opt.unit_of_measure_id);
          }
          break;
        case "CLASSIFICATION":
          if (opt.classification_value_id !== null) {
            if (!cache.classificationMap.has(dimCode)) {
              cache.classificationMap.set(dimCode, new Map());
            }
            cache.classificationMap
              .get(dimCode)!
              .set(normalizedLabel, opt.classification_value_id);
          }
          break;
      }
    }

    logger.info(
      {
        matrixId,
        territories: cache.territoryMap.size,
        timePeriods: cache.timePeriodMap.size,
        units: cache.unitMap.size,
        classifications: cache.classificationMap.size,
      },
      "Built lookup cache"
    );

    // Validate cache completeness
    this.validateLookupCache(cache, _matrix);

    return cache;
  }

  /**
   * Validate lookup cache completeness.
   * Logs warnings when dimension options have low mapping rates.
   *
   * This helps identify issues where:
   * - Territory labels don't match any known territories
   * - Time period labels use unexpected formats
   * - Classification values weren't created during metadata sync
   */
  private validateLookupCache(cache: LookupCache, matrix: InsMatrix): void {
    for (const dim of matrix.dimensionsMap) {
      const dimType = this.matrixService.detectDimensionType(
        dim,
        matrix.details
      );
      const optCount = dim.options.length;

      switch (dimType) {
        case "TERRITORIAL": {
          const mapSize = cache.territoryMap.get(dim.dimCode)?.size ?? 0;
          // Many territorial dimensions have aggregate entries (TOTAL, regions)
          // that intentionally don't map to specific territories
          if (mapSize < optCount * 0.5 && optCount > 5) {
            logger.warn(
              {
                dimCode: dim.dimCode,
                label: dim.label,
                mapped: mapSize,
                total: optCount,
                percentage: Math.round((mapSize / optCount) * 100),
              },
              "Less than 50% of territorial options have territory_id mappings"
            );
          }
          break;
        }
        case "TEMPORAL": {
          const mapSize = cache.timePeriodMap.get(dim.dimCode)?.size ?? 0;
          // Time periods should have high mapping rates
          if (mapSize < optCount * 0.9 && optCount > 1) {
            logger.warn(
              {
                dimCode: dim.dimCode,
                label: dim.label,
                mapped: mapSize,
                total: optCount,
                percentage: Math.round((mapSize / optCount) * 100),
              },
              "Some temporal options missing time_period_id mappings"
            );
          }
          break;
        }
        case "UNIT_OF_MEASURE": {
          const mapSize = cache.unitMap.get(dim.dimCode)?.size ?? 0;
          // All unit options should be mapped
          if (mapSize < optCount && optCount > 0) {
            logger.warn(
              {
                dimCode: dim.dimCode,
                label: dim.label,
                mapped: mapSize,
                total: optCount,
              },
              "Some unit of measure options missing unit_of_measure_id mappings"
            );
          }
          break;
        }
        case "CLASSIFICATION": {
          const mapSize = cache.classificationMap.get(dim.dimCode)?.size ?? 0;
          // Classification values should have high mapping rates
          if (mapSize < optCount * 0.9 && optCount > 1) {
            logger.warn(
              {
                dimCode: dim.dimCode,
                label: dim.label,
                mapped: mapSize,
                total: optCount,
                percentage: Math.round((mapSize / optCount) * 100),
              },
              "Some classification options missing classification_value_id mappings"
            );
          }
          break;
        }
      }
    }
  }

  /**
   * Lookup territory ID from cache.
   * Prioritizes locality (LAU) dimension over county (NUTS3) dimension
   * when both exist, since locality is the more specific territory level.
   * Labels are normalized before lookup for consistent matching.
   */
  private lookupTerritoryFromCache(
    cache: LookupCache,
    matrix: InsMatrix,
    row: ParsedDataRow
  ): number | null {
    // Get specific territorial dimensions (county vs locality)
    const { countyDim, localityDim } =
      this.matrixService.getTerritorialDimensionInfo(matrix);

    // Prefer locality dimension when available (more specific territory)
    if (localityDim) {
      const localityLabel = row.dimensionValues.get(localityDim.dimCode);
      if (localityLabel) {
        const normalizedLabel = this.normalizeLabel(localityLabel);
        const territoryId = cache.territoryMap
          .get(localityDim.dimCode)
          ?.get(normalizedLabel);
        if (territoryId !== undefined) {
          return territoryId;
        }
      }
    }

    // Fall back to county dimension
    if (countyDim) {
      const countyLabel = row.dimensionValues.get(countyDim.dimCode);
      if (countyLabel) {
        const normalizedLabel = this.normalizeLabel(countyLabel);
        const territoryId = cache.territoryMap
          .get(countyDim.dimCode)
          ?.get(normalizedLabel);
        if (territoryId !== undefined) {
          return territoryId;
        }
      }
    }

    // If no specific county/locality dims, try any territorial dimension
    // (for matrices with other territorial structures)
    const territorialDim = matrix.dimensionsMap.find(
      (d) =>
        this.matrixService.detectDimensionType(d) === "TERRITORIAL" &&
        d.dimCode !== countyDim?.dimCode &&
        d.dimCode !== localityDim?.dimCode
    );
    if (!territorialDim) return null;

    const label = row.dimensionValues.get(territorialDim.dimCode);
    if (!label) return null;

    const normalizedLabel = this.normalizeLabel(label);
    return (
      cache.territoryMap.get(territorialDim.dimCode)?.get(normalizedLabel) ??
      null
    );
  }

  /**
   * Lookup time period ID from cache.
   * Labels are normalized before lookup for consistent matching.
   */
  private lookupTimePeriodFromCache(
    cache: LookupCache,
    matrix: InsMatrix,
    row: ParsedDataRow
  ): number | null {
    const temporalDim = matrix.dimensionsMap.find(
      (d) => this.matrixService.detectDimensionType(d) === "TEMPORAL"
    );
    if (!temporalDim) return null;

    const label = row.dimensionValues.get(temporalDim.dimCode);
    if (!label) return null;

    const normalizedLabel = this.normalizeLabel(label);
    return (
      cache.timePeriodMap.get(temporalDim.dimCode)?.get(normalizedLabel) ?? null
    );
  }

  /**
   * Lookup unit ID from cache.
   * Labels are normalized before lookup for consistent matching.
   */
  private lookupUnitFromCache(
    cache: LookupCache,
    matrix: InsMatrix,
    row: ParsedDataRow
  ): number | null {
    const unitDim = matrix.dimensionsMap.find(
      (d) => this.matrixService.detectDimensionType(d) === "UNIT_OF_MEASURE"
    );
    if (!unitDim) return null;

    const label = row.dimensionValues.get(unitDim.dimCode);
    if (!label) return null;

    const normalizedLabel = this.normalizeLabel(label);
    return cache.unitMap.get(unitDim.dimCode)?.get(normalizedLabel) ?? null;
  }

  /**
   * Collect classification IDs from cache.
   * Labels are normalized before lookup for consistent matching.
   */
  private collectClassificationIdsFromCache(
    cache: LookupCache,
    matrix: InsMatrix,
    row: ParsedDataRow
  ): number[] {
    const classificationDims = matrix.dimensionsMap.filter(
      (d) => this.matrixService.detectDimensionType(d) === "CLASSIFICATION"
    );

    const ids: number[] = [];
    for (const dim of classificationDims) {
      const label = row.dimensionValues.get(dim.dimCode);
      if (!label) continue;

      const normalizedLabel = this.normalizeLabel(label);
      const id = cache.classificationMap.get(dim.dimCode)?.get(normalizedLabel);
      if (id !== undefined) {
        ids.push(id);
      }
    }

    return ids;
  }

  // ============================================================================
  // Response Validation
  // ============================================================================

  /**
   * Validate that the API response appears complete and not truncated.
   * Checks for cell limit error messages and row count mismatches.
   *
   * @throws Error if response contains cell limit error message
   * @logs Warning if response has fewer rows than expected
   */
  private validateResponse(
    csvData: string,
    expectedSelections: number[][],
    matrixCode: string
  ): void {
    // Check for INS API cell limit error message
    // The API returns a Romanian error message when limit is exceeded
    if (csvData.includes("celule") && csvData.includes("30000")) {
      logger.error(
        { matrixCode, responseLength: csvData.length },
        "Response contains cell limit error message - data truncated"
      );
      throw new Error(
        `Query exceeded 30,000 cell limit for ${matrixCode}. ` +
          "Use chunked sync strategy to split the query."
      );
    }

    // Check if response seems complete
    const rows = csvData.split("\n").filter((r) => r.trim());
    const dataRows = rows.length - 1; // Exclude header

    // Calculate expected rows from selections
    const expectedRows = expectedSelections.reduce(
      (acc, sel) => acc * sel.length,
      1
    );

    // Allow 10% variance for missing data cells
    if (dataRows < expectedRows * 0.9 && expectedRows > 10) {
      logger.warn(
        { matrixCode, expected: expectedRows, actual: dataRows },
        "Response may be incomplete - fewer rows than expected"
      );
    }
  }

  // ============================================================================
  // Chunking Strategy Selection
  // ============================================================================

  /**
   * Select the best chunking strategy based on matrix structure and cell count.
   */
  private selectChunkingStrategy(
    matrix: InsMatrix,
    estimatedCells: number
  ): "NONE" | "BY_YEAR" | "BY_YEAR_TERRITORY" {
    if (estimatedCells <= CELL_LIMIT) {
      return "NONE";
    }

    // Check if matrix has locality data
    const { countyDim, localityDim } =
      this.matrixService.getTerritorialDimensionInfo(matrix);

    if (localityDim && countyDim) {
      // Has both county and locality dimensions - need county+year chunking
      const timeDim = matrix.dimensionsMap.find(
        (d) => this.matrixService.detectDimensionType(d) === "TEMPORAL"
      );

      if (timeDim) {
        // Estimate cells per county per year
        const cellsPerCountyPerYear = this.estimateCellsPerCountyYear(
          matrix,
          countyDim,
          localityDim,
          timeDim
        );

        logger.debug(
          { cellsPerCountyPerYear, limit: CELL_LIMIT },
          "Estimated cells per county/year chunk"
        );

        if (cellsPerCountyPerYear <= CELL_LIMIT) {
          return "BY_YEAR_TERRITORY";
        }
        // If still too large, log warning and fall back to BY_YEAR
        logger.warn(
          { cellsPerCountyPerYear, limit: CELL_LIMIT },
          "County+year chunks still exceed limit - falling back to BY_YEAR"
        );
      }
    }

    // Default to year-only chunking
    return "BY_YEAR";
  }

  /**
   * Estimate cells per county per year for chunking decisions.
   */
  private estimateCellsPerCountyYear(
    matrix: InsMatrix,
    countyDim: InsDimension,
    localityDim: InsDimension,
    _timeDim: InsDimension
  ): number {
    // Count non-territorial, non-temporal dimensions
    let cellCount = 1;
    const avgLocalitiesPerCounty = Math.ceil(
      localityDim.options.length / countyDim.options.length
    );

    for (const dim of matrix.dimensionsMap) {
      const dimType = this.matrixService.detectDimensionType(dim);

      if (dimType === "TEMPORAL") {
        // One time period per chunk
        continue;
      } else if (dim.dimCode === countyDim.dimCode) {
        // One county per chunk
        continue;
      } else if (dim.dimCode === localityDim.dimCode) {
        // Use average localities per county
        cellCount *= avgLocalitiesPerCounty;
      } else {
        // All options for other dimensions
        cellCount *= dim.options.length;
      }
    }

    return cellCount;
  }

  /**
   * Build a map from county nomItemId to its child locality nomItemIds.
   * Uses the parentId field which directly references the parent county.
   */
  private buildCountyLocalityMap(
    countyDim: InsDimension,
    localityDim: InsDimension
  ): Map<number, number[]> {
    const map = new Map<number, number[]>();

    // Build a set of valid county nomItemIds
    const validCountyIds = new Set<number>();
    for (const countyOpt of countyDim.options) {
      if (!this.isAggregateTerritory(countyOpt.label)) {
        validCountyIds.add(countyOpt.nomItemId);
      }
    }

    // Map each locality to its parent county using parentId
    for (const localityOpt of localityDim.options) {
      const parentId = localityOpt.parentId;

      // Skip localities without a parent or with aggregate parent (TOTAL)
      if (parentId === null || !validCountyIds.has(parentId)) {
        continue;
      }

      if (!map.has(parentId)) {
        map.set(parentId, []);
      }
      map.get(parentId)!.push(localityOpt.nomItemId);
    }

    logger.debug(
      {
        counties: map.size,
        localities: localityDim.options.length,
        mappedLocalities: [...map.values()].reduce((a, b) => a + b.length, 0),
      },
      "Built county-locality map using parentId"
    );

    return map;
  }

  /**
   * Check if a territory label is an aggregate (TOTAL, macroregion, region).
   * These should be skipped when chunking by county.
   */
  private isAggregateTerritory(label: string): boolean {
    const lower = label.toLowerCase();
    return (
      lower === "total" ||
      /macroregiunea/i.test(lower) ||
      /regiunea/i.test(lower)
    );
  }

  /**
   * Build encQuery for a specific county+year chunk.
   */
  private buildCountyYearChunkQuery(
    matrix: InsMatrix,
    countyDim: InsDimension,
    localityDim: InsDimension,
    timeDim: InsDimension,
    chunk: CountyYearChunk
  ): string {
    const selections: number[][] = [];

    for (const dim of matrix.dimensionsMap) {
      if (dim.dimCode === countyDim.dimCode) {
        // Single county
        selections.push([chunk.countyNomItemId]);
      } else if (dim.dimCode === localityDim.dimCode) {
        // All localities in this county
        selections.push(chunk.localityNomItemIds);
      } else if (dim.dimCode === timeDim.dimCode) {
        // Single year
        selections.push([chunk.timeNomItemId]);
      } else {
        // All options for other dimensions
        selections.push(dim.options.map((o) => o.nomItemId));
      }
    }

    return buildEncQuery(selections);
  }

  // ============================================================================
  // Batch Operations
  // ============================================================================

  /**
   * Prepare a batch of statistics from parsed rows using cached lookups.
   */
  private prepareBatch(
    matrixId: number,
    matrix: InsMatrix,
    rows: ParsedDataRow[],
    cache: LookupCache,
    encQuery: string
  ): PreparedStatistic[] {
    const prepared: PreparedStatistic[] = [];

    for (const row of rows) {
      const territoryId = this.lookupTerritoryFromCache(cache, matrix, row);
      const timePeriodId = this.lookupTimePeriodFromCache(cache, matrix, row);
      const unitId = this.lookupUnitFromCache(cache, matrix, row);
      const classificationIds = this.collectClassificationIdsFromCache(
        cache,
        matrix,
        row
      );

      if (timePeriodId === null) {
        continue; // Skip rows without time period
      }

      const naturalKeyHash = computeNaturalKeyHash(
        matrixId,
        territoryId,
        timePeriodId,
        unitId,
        classificationIds
      );

      prepared.push({
        stat: {
          matrix_id: matrixId,
          territory_id: territoryId,
          time_period_id: timePeriodId,
          unit_of_measure_id: unitId,
          value: row.value,
          value_status: row.valueStatus,
          source_enc_query: encQuery,
          natural_key_hash: naturalKeyHash,
        },
        classificationIds,
      });
    }

    return prepared;
  }

  /**
   * Batch upsert statistics and their classifications.
   * Much faster than individual upserts.
   */
  private async batchUpsert(
    matrixId: number,
    prepared: PreparedStatistic[]
  ): Promise<{ inserted: number; updated: number }> {
    if (prepared.length === 0) {
      return { inserted: 0, updated: 0 };
    }

    // Deduplicate by natural_key_hash (keep last occurrence)
    const deduped = new Map<string, PreparedStatistic>();
    for (const p of prepared) {
      deduped.set(p.stat.natural_key_hash, p);
    }
    const uniquePrepared = [...deduped.values()];

    const partitionName = `statistics_matrix_${String(matrixId)}`;
    let totalInserted = 0;
    let totalUpdated = 0;

    // Process in batches
    for (let i = 0; i < uniquePrepared.length; i += BATCH_SIZE) {
      const batch = uniquePrepared.slice(i, i + BATCH_SIZE);

      // Build VALUES clause
      const values = batch.map(
        (p) => sql`(
          ${p.stat.matrix_id},
          ${p.stat.territory_id ?? null},
          ${p.stat.time_period_id},
          ${p.stat.unit_of_measure_id ?? null},
          ${p.stat.value ?? null},
          ${p.stat.value_status ?? null},
          ${p.stat.source_enc_query ?? null},
          NOW(),
          ${p.stat.natural_key_hash},
          1
        )`
      );

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
        ) VALUES ${sql.join(values, sql`, `)}
        ON CONFLICT (natural_key_hash) WHERE natural_key_hash IS NOT NULL
        DO UPDATE SET
          value = EXCLUDED.value,
          value_status = EXCLUDED.value_status,
          source_enc_query = EXCLUDED.source_enc_query,
          scraped_at = NOW(),
          updated_at = NOW(),
          version = ${sql.raw(partitionName)}.version + 1
        RETURNING id, xmax::text
      `.execute(this.db);

      // Track inserts vs updates and collect IDs for new rows
      const newRowIds: { id: number; idx: number }[] = [];
      for (let j = 0; j < result.rows.length; j++) {
        const row = result.rows[j];
        if (!row) continue;
        if (row.xmax === "0") {
          totalInserted++;
          newRowIds.push({ id: row.id, idx: j });
        } else {
          totalUpdated++;
        }
      }

      // Batch insert classifications for new rows only
      if (newRowIds.length > 0) {
        const classificationValues: {
          matrix_id: number;
          statistic_id: number;
          classification_value_id: number;
        }[] = [];

        for (const { id, idx } of newRowIds) {
          const p = batch[idx];
          if (!p) continue;
          for (const classId of p.classificationIds) {
            classificationValues.push({
              matrix_id: matrixId,
              statistic_id: id,
              classification_value_id: classId,
            });
          }
        }

        if (classificationValues.length > 0) {
          await this.db
            .insertInto("statistic_classifications")
            .values(classificationValues)
            .onConflict((oc) => oc.doNothing())
            .execute();
        }
      }
    }

    return { inserted: totalInserted, updated: totalUpdated };
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

      // Select chunking strategy based on matrix structure and cell count
      const strategy = this.selectChunkingStrategy(matrix, estimatedCells);
      logger.info(
        { matrixCode, estimatedCells, strategy },
        "Selected chunking strategy"
      );

      if (strategy === "NONE") {
        // Single query for small matrices
        return await this.syncSingle(
          job.id,
          matrixCode,
          matrix,
          matrixId,
          limit
        );
      } else if (strategy === "BY_YEAR_TERRITORY") {
        // County + year chunking for locality-level matrices
        return await this.syncChunkedByCountyAndYear(
          job.id,
          matrixCode,
          matrix,
          matrixId,
          limit
        );
      } else {
        // BY_YEAR: Year-only chunking for county-level matrices
        return await this.syncChunked(
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
    // Build lookup cache once
    const cache = await this.buildLookupCache(matrixId, matrix);

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

    // Validate response integrity
    this.validateResponse(csvData, selections, matrixCode);

    const rows = parsePivotResponse(csvData);
    const parsed = this.parseRows(rows, matrix);

    // Apply limit if specified
    const rowsToProcess = limit ? parsed.slice(0, limit) : parsed;
    if (limit) {
      logger.info({ limit, totalRows: parsed.length }, "Applying row limit");
    }

    // Prepare and batch upsert
    const prepared = this.prepareBatch(
      matrixId,
      matrix,
      rowsToProcess,
      cache,
      encQuery
    );
    const { inserted: rowsInserted, updated: rowsUpdated } =
      await this.batchUpsert(matrixId, prepared);

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
    // Build lookup cache once for all chunks
    const cache = await this.buildLookupCache(matrixId, matrix);

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

        // Check for cell limit error (shouldn't happen with proper chunking)
        if (csvData.includes("celule") && csvData.includes("30000")) {
          logger.error(
            { matrixCode, chunk: chunk.chunkNumber },
            "Chunk exceeded cell limit - chunking strategy needs refinement"
          );
          throw new Error(
            `Chunk ${String(chunk.chunkNumber)} for ${matrixCode} exceeded 30,000 cell limit`
          );
        }

        const rows = parsePivotResponse(csvData);
        const parsed = this.parseRows(rows, matrix);

        // Prepare and batch upsert
        const prepared = this.prepareBatch(
          matrixId,
          matrix,
          parsed,
          cache,
          encQuery
        );
        const { inserted: chunkInserted, updated: chunkUpdated } =
          await this.batchUpsert(matrixId, prepared);

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
   * Sync with chunked queries by county and year (for matrices with locality data)
   */
  private async syncChunkedByCountyAndYear(
    jobId: number,
    matrixCode: string,
    matrix: InsMatrix,
    matrixId: number,
    limit?: number
  ): Promise<DataSyncResult> {
    // Build lookup cache once for all chunks
    const cache = await this.buildLookupCache(matrixId, matrix);

    // Get dimensions
    const { countyDim, localityDim } =
      this.matrixService.getTerritorialDimensionInfo(matrix);
    const timeDim = matrix.dimensionsMap.find(
      (d) => this.matrixService.detectDimensionType(d) === "TEMPORAL"
    );

    if (!timeDim || !countyDim || !localityDim) {
      throw new Error(
        "Missing required dimensions for BY_COUNTY_AND_YEAR chunking"
      );
    }

    // Build county -> localities mapping
    const countyLocalityMap = this.buildCountyLocalityMap(
      countyDim,
      localityDim
    );

    // Create chunks: one per (county, year) combination
    const chunks: CountyYearChunk[] = [];
    let chunkNumber = 0;

    for (const countyOpt of countyDim.options) {
      // Skip aggregate entries (TOTAL, macroregions, regions)
      if (this.isAggregateTerritory(countyOpt.label)) continue;

      const localityIds = countyLocalityMap.get(countyOpt.nomItemId) ?? [];
      if (localityIds.length === 0) {
        logger.warn(
          { county: countyOpt.label, nomItemId: countyOpt.nomItemId },
          "No localities found for county - skipping"
        );
        continue;
      }

      for (const timeOpt of timeDim.options) {
        chunkNumber++;
        chunks.push({
          chunkNumber,
          countyNomItemId: countyOpt.nomItemId,
          countyLabel: countyOpt.label,
          localityNomItemIds: localityIds,
          timeNomItemId: timeOpt.nomItemId,
          timeLabel: timeOpt.label,
        });
      }
    }

    logger.info(
      {
        matrixCode,
        counties: countyLocalityMap.size,
        timePeriods: timeDim.options.length,
        totalChunks: chunks.length,
      },
      "Created county+year chunks"
    );

    // Update job with chunk info
    await this.db
      .updateTable("scrape_jobs")
      .set({
        strategy: "BY_YEAR_TERRITORY" as ChunkStrategy,
        total_chunks: chunks.length,
      })
      .where("id", "=", jobId)
      .execute();

    // Insert chunk records
    for (const chunk of chunks) {
      const encQuery = this.buildCountyYearChunkQuery(
        matrix,
        countyDim,
        localityDim,
        timeDim,
        chunk
      );
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
      const encQuery = this.buildCountyYearChunkQuery(
        matrix,
        countyDim,
        localityDim,
        timeDim,
        chunk
      );

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

        // Check for cell limit error (shouldn't happen with county+year chunking)
        if (csvData.includes("celule") && csvData.includes("30000")) {
          logger.error(
            {
              matrixCode,
              chunk: chunk.chunkNumber,
              county: chunk.countyLabel,
              year: chunk.timeLabel,
            },
            "County+year chunk exceeded cell limit"
          );
          throw new Error(
            `Chunk ${String(chunk.chunkNumber)} (${chunk.countyLabel}, ${chunk.timeLabel}) ` +
              `for ${matrixCode} exceeded 30,000 cell limit`
          );
        }

        const rows = parsePivotResponse(csvData);
        const parsed = this.parseRows(rows, matrix);

        // Prepare and batch upsert
        const prepared = this.prepareBatch(
          matrixId,
          matrix,
          parsed,
          cache,
          encQuery
        );
        const { inserted: chunkInserted, updated: chunkUpdated } =
          await this.batchUpsert(matrixId, prepared);

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
            county: chunk.countyLabel,
            time: chunk.timeLabel,
            inserted: chunkInserted,
            updated: chunkUpdated,
          },
          "Completed county+year chunk"
        );

        await sleep(RATE_LIMIT_MS);
      } catch (error) {
        await this.handleChunkError(jobId, chunk.chunkNumber, error as Error);
        logger.error(
          {
            matrixCode,
            chunk: chunk.chunkNumber,
            county: chunk.countyLabel,
            time: chunk.timeLabel,
            error,
          },
          "County+year chunk failed"
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

    // Build lookup cache once for all chunks
    const cache = await this.buildLookupCache(job.matrix_id, matrix);

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

        // Check for cell limit error
        if (csvData.includes("celule") && csvData.includes("30000")) {
          logger.error(
            { matrixCode, chunk: chunk.chunk_number },
            "Chunk exceeded cell limit during resume"
          );
          throw new Error(
            `Chunk ${String(chunk.chunk_number)} for ${matrixCode} exceeded 30,000 cell limit`
          );
        }

        const rows = parsePivotResponse(csvData);
        const parsed = this.parseRows(rows, matrix);

        // Prepare and batch upsert
        const prepared = this.prepareBatch(
          job.matrix_id,
          matrix,
          parsed,
          cache,
          chunk.enc_query
        );
        const { inserted: chunkInserted, updated: chunkUpdated } =
          await this.batchUpsert(job.matrix_id, prepared);

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
