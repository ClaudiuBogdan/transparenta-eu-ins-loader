/**
 * Chunk Generator - Generates chunks for syncing matrix data
 *
 * Handles the 30,000 cell limit by:
 * 1. For UAT-level matrices: chunks by county + year
 * 2. For county-level matrices: chunks by year
 * 3. For national matrices: single chunk
 *
 * If a chunk still exceeds the limit, it can be subdivided by classification.
 */

import { createHash } from "node:crypto";

import { apiLogger } from "../../logger.js";
import { COUNTIES } from "./canonical/territories.js";
import { estimateCellCount } from "../../scraper/client.js";

import type {
  Database,
  DimensionType,
  MatrixMetadata,
} from "../../db/types.js";
import type { Kysely } from "kysely";

// ============================================================================
// Types
// ============================================================================

/** Classification mode for syncing */
export type ClassificationMode = "all" | "totals-only";

/** Territory level for chunking */
export type TerritoryLevel = "national" | "county" | "uat";

/** A sync chunk represents a unit of work for syncing */
export interface SyncChunk {
  /** County code (e.g., "AB" for Alba) - null for national/county-aggregate chunks */
  countyCode: string | null;
  /** Year to sync (single year per chunk for UAT data) */
  year: number;
  /** Classification mode - 'all' syncs all breakdowns, 'totals-only' syncs only aggregates */
  classificationMode: ClassificationMode;
  /** Territory level - determines what territorial data to include */
  territoryLevel: TerritoryLevel;
  /** Unique hash for checkpoint tracking */
  chunkHash: string;
}

/** Matrix info needed for chunk generation */
export interface MatrixInfo {
  id: number;
  insCode: string;
  metadata: MatrixMetadata;
  dimensions: DimensionInfo[];
}

/** Dimension info with nom_item mappings */
export interface DimensionInfo {
  dimIndex: number;
  dimensionType: DimensionType;
  nomItems: NomItemInfo[];
}

/** Nom item info for building queries */
export interface NomItemInfo {
  nomItemId: number;
  labelRo: string;
  territoryId: number | null;
  timePeriodId: number | null;
  classificationValueId: number | null;
  unitId: number | null;
  parentNomItemId: number | null;
}

/** Chunk generation options */
export interface ChunkGeneratorOptions {
  /** Year range to sync */
  yearFrom: number;
  yearTo: number;
  /** Sync all classifications or just totals */
  classificationMode: ClassificationMode;
  /** Specific county to sync (for targeted sync) */
  countyCode?: string;
}

/** Result of chunk generation */
export interface ChunkGenerationResult {
  chunks: SyncChunk[];
  estimatedApiCalls: number;
  estimatedDuration: string;
  hasUatData: boolean;
  hasCountyData: boolean;
}

// ============================================================================
// Constants
// ============================================================================

const CELL_LIMIT = 30_000;
const RATE_LIMIT_MS = 750;

// County codes extracted from COUNTIES array
export const COUNTY_CODES = COUNTIES.map((c) => c.code);

// ============================================================================
// Chunk Generator
// ============================================================================

export class ChunkGenerator {
  // Store detected dimension indexes for use in buildChunkSelections
  private detectedLocalityDimIndex: number | null = null;
  private detectedCountyDimIndex: number | null = null;

  constructor(private readonly db: Kysely<Database>) {}

  /**
   * Generate chunks for a matrix based on its structure
   */
  generateChunks(
    matrix: MatrixInfo,
    options: ChunkGeneratorOptions
  ): ChunkGenerationResult {
    const { yearFrom, yearTo, classificationMode, countyCode } = options;
    const years = this.generateYearRange(yearFrom, yearTo);
    const chunks: SyncChunk[] = [];

    const hasUatData = matrix.metadata.flags.hasUatData;
    const hasCountyData = matrix.metadata.flags.hasCountyData;

    // Get localityDimIndex from metadata, or detect from dimensions
    let localityDimIndex =
      matrix.metadata.dimensionIndicators?.localityDimIndex;
    let countyDimIndex = matrix.metadata.dimensionIndicators?.countyDimIndex;

    // Auto-detect locality and county dimensions if not set
    if (hasUatData && localityDimIndex == null) {
      // Find territorial dimensions with large option counts (localities)
      const territorialDims = matrix.dimensions
        .filter((d) => d.dimensionType === "TERRITORIAL")
        .sort((a, b) => b.nomItems.length - a.nomItems.length);

      if (territorialDims.length >= 2) {
        // Largest is localities, second is counties
        localityDimIndex = territorialDims[0]?.dimIndex;
        countyDimIndex = territorialDims[1]?.dimIndex;
      } else if (
        territorialDims.length === 1 &&
        territorialDims[0]!.nomItems.length > 100
      ) {
        // Single large territorial dim is likely localities
        localityDimIndex = territorialDims[0]?.dimIndex;
      }
    }

    // Store detected indexes for use in buildChunkSelections
    this.detectedLocalityDimIndex = localityDimIndex ?? null;
    this.detectedCountyDimIndex = countyDimIndex ?? null;

    apiLogger.debug(
      {
        matrixCode: matrix.insCode,
        hasUatData,
        hasCountyData,
        localityDimIndex,
        countyDimIndex,
        years: years.length,
      },
      "Generating chunks"
    );

    if (hasUatData && localityDimIndex != null) {
      // UAT-level matrix: TWO types of chunks

      // 1. County-aggregate chunk: All counties WITHOUT localities
      //    Cell count: 43 counties × classifications × 1 year ≈ 13,416 cells
      if (!countyCode) {
        for (const year of years) {
          chunks.push({
            countyCode: null,
            year,
            classificationMode,
            territoryLevel: "county",
            chunkHash: this.computeChunkHash(
              matrix.insCode,
              "COUNTIES",
              year,
              classificationMode
            ),
          });
        }
      }

      // 2. UAT-detail chunks: Per county with localities
      //    Cell count: ~76 localities × classifications × 1 year ≈ 23,712 cells
      const countiesToSync = countyCode ? [countyCode] : COUNTY_CODES;
      for (const county of countiesToSync) {
        for (const year of years) {
          chunks.push({
            countyCode: county,
            year,
            classificationMode,
            territoryLevel: "uat",
            chunkHash: this.computeChunkHash(
              matrix.insCode,
              county,
              year,
              classificationMode
            ),
          });
        }
      }
    } else if (hasCountyData) {
      // County-level matrix (no UATs): single chunk per year with all territories
      for (const year of years) {
        chunks.push({
          countyCode: null,
          year,
          classificationMode,
          territoryLevel: "county",
          chunkHash: this.computeChunkHash(
            matrix.insCode,
            null,
            year,
            classificationMode
          ),
        });
      }
    } else {
      // National-level matrix: single chunk per year
      for (const year of years) {
        chunks.push({
          countyCode: null,
          year,
          classificationMode,
          territoryLevel: "national",
          chunkHash: this.computeChunkHash(
            matrix.insCode,
            null,
            year,
            classificationMode
          ),
        });
      }
    }

    const estimatedApiCalls = chunks.length;
    const estimatedMs = estimatedApiCalls * RATE_LIMIT_MS;
    const estimatedDuration = this.formatDuration(estimatedMs);

    apiLogger.info(
      {
        matrixCode: matrix.insCode,
        chunkCount: chunks.length,
        estimatedDuration,
      },
      "Generated chunks"
    );

    return {
      chunks,
      estimatedApiCalls,
      estimatedDuration,
      hasUatData,
      hasCountyData,
    };
  }

  /**
   * Build encQuery selections for a specific chunk
   */
  async buildChunkSelections(
    matrix: MatrixInfo,
    chunk: SyncChunk
  ): Promise<number[][]> {
    const selections: number[][] = [];

    for (const dim of matrix.dimensions) {
      const dimSelections = await this.buildDimensionSelections(
        matrix,
        dim,
        chunk
      );
      selections.push(dimSelections);
    }

    return selections;
  }

  /**
   * Build selections for a single dimension
   */
  private async buildDimensionSelections(
    matrix: MatrixInfo,
    dim: DimensionInfo,
    chunk: SyncChunk
  ): Promise<number[]> {
    switch (dim.dimensionType) {
      case "TEMPORAL":
        return this.buildTemporalSelections(dim, chunk.year);

      case "TERRITORIAL":
        return await this.buildTerritorialSelections(matrix, dim, chunk);

      case "CLASSIFICATION":
        return this.buildClassificationSelections(
          dim,
          chunk.classificationMode
        );

      case "UNIT_OF_MEASURE":
        // Always take all units (usually just one)
        return dim.nomItems.map((item) => item.nomItemId);

      default:
        // Unknown dimension type - take all
        return dim.nomItems.map((item) => item.nomItemId);
    }
  }

  /**
   * Build temporal selections for a specific year
   */
  private buildTemporalSelections(dim: DimensionInfo, year: number): number[] {
    const filtered = dim.nomItems.filter((item) => {
      const yearMatch = /\d{4}/.exec(item.labelRo);
      if (!yearMatch) return false;
      return Number.parseInt(yearMatch[0], 10) === year;
    });

    if (filtered.length === 0) {
      // Fallback: take first item if no year match
      apiLogger.warn(
        { year, dimIndex: dim.dimIndex, nomItemCount: dim.nomItems.length },
        "No temporal items match year, taking first"
      );
      return dim.nomItems[0] ? [dim.nomItems[0].nomItemId] : [];
    }

    return filtered.map((item) => item.nomItemId);
  }

  /**
   * Build territorial selections based on chunk configuration
   */
  private async buildTerritorialSelections(
    matrix: MatrixInfo,
    dim: DimensionInfo,
    chunk: SyncChunk
  ): Promise<number[]> {
    // Use detected indexes (from generateChunks) or fallback to metadata
    const localityDimIndex =
      this.detectedLocalityDimIndex ??
      matrix.metadata.dimensionIndicators?.localityDimIndex;
    const countyDimIndex =
      this.detectedCountyDimIndex ??
      matrix.metadata.dimensionIndicators?.countyDimIndex;

    // National-level chunk: take TOTAL only for all territorial dimensions
    if (chunk.territoryLevel === "national") {
      return this.findTotalNomItem(dim);
    }

    // County-aggregate chunk: select all counties, skip localities
    if (chunk.territoryLevel === "county" && chunk.countyCode === null) {
      if (dim.dimIndex === countyDimIndex) {
        // County dimension: select ALL counties (excluding TOTAL)
        const nonTotalItems = dim.nomItems.filter(
          (item) => !item.labelRo.toLowerCase().includes("total")
        );
        return nonTotalItems.length > 0
          ? nonTotalItems.map((item) => item.nomItemId)
          : dim.nomItems.map((item) => item.nomItemId);
      }
      if (dim.dimIndex === localityDimIndex) {
        // Locality dimension: select ONLY Total (skip individual UATs)
        return this.findTotalNomItem(dim);
      }
      // Other territorial dimension: take all
      return dim.nomItems.map((item) => item.nomItemId);
    }

    // County-level chunk (for county-only matrices without UATs)
    if (chunk.territoryLevel === "county" && chunk.countyCode !== null) {
      // Single county selected - find it
      const countyItem = await this.findCountyNomItem(
        matrix.id,
        dim.dimIndex,
        chunk.countyCode
      );
      if (countyItem) {
        return [countyItem];
      }
      return dim.nomItems.map((item) => item.nomItemId);
    }

    // UAT-detail chunk: select single county + its localities
    if (chunk.territoryLevel === "uat" && chunk.countyCode !== null) {
      if (dim.dimIndex === countyDimIndex) {
        // County dimension: select single county
        const countyItem = await this.findCountyNomItem(
          matrix.id,
          dim.dimIndex,
          chunk.countyCode
        );
        if (countyItem) {
          return [countyItem];
        }
        apiLogger.warn(
          {
            matrixCode: matrix.insCode,
            countyCode: chunk.countyCode,
            dimIndex: dim.dimIndex,
          },
          "County not found in dimension, taking first item"
        );
        return [dim.nomItems[0]?.nomItemId ?? 1];
      }

      if (dim.dimIndex === localityDimIndex) {
        // Locality dimension: select all localities for the county
        const localityNomItemIds = await this.findLocalitiesInCounty(
          matrix.id,
          dim.dimIndex,
          chunk.countyCode
        );
        if (localityNomItemIds.length > 0) {
          return localityNomItemIds;
        }
        apiLogger.warn(
          {
            matrixCode: matrix.insCode,
            countyCode: chunk.countyCode,
            dimIndex: dim.dimIndex,
          },
          "No localities found for county"
        );
        return [dim.nomItems[0]?.nomItemId ?? 1];
      }
    }

    // Default: take all items
    return dim.nomItems.map((item) => item.nomItemId);
  }

  /**
   * Find TOTAL nom_item in a dimension
   */
  private findTotalNomItem(dim: DimensionInfo): number[] {
    const totalItem = dim.nomItems.find(
      (item) =>
        item.labelRo.toLowerCase() === "total" ||
        item.labelRo.toLowerCase().includes("total")
    );
    return totalItem
      ? [totalItem.nomItemId]
      : [dim.nomItems[0]?.nomItemId ?? 1];
  }

  /**
   * Build classification selections based on mode
   */
  private buildClassificationSelections(
    dim: DimensionInfo,
    mode: ClassificationMode
  ): number[] {
    if (mode === "totals-only") {
      // Only sync totals/aggregates
      const totals = dim.nomItems.filter(
        (item) =>
          item.labelRo.toLowerCase() === "total" ||
          item.labelRo.toLowerCase().includes("total") ||
          item.parentNomItemId === null
      );
      if (totals.length > 0) {
        return totals.map((item) => item.nomItemId);
      }
      // Fallback to first item
      return dim.nomItems[0] ? [dim.nomItems[0].nomItemId] : [];
    }

    // 'all' mode - sync ALL classification values
    return dim.nomItems.map((item) => item.nomItemId);
  }

  /**
   * Find the county nom_item_id for a given county code
   */
  private async findCountyNomItem(
    matrixId: number,
    dimIndex: number,
    countyCode: string
  ): Promise<number | null> {
    const result = await this.db
      .selectFrom("matrix_nom_items")
      .innerJoin(
        "territories",
        "matrix_nom_items.territory_id",
        "territories.id"
      )
      .select("matrix_nom_items.nom_item_id")
      .where("matrix_nom_items.matrix_id", "=", matrixId)
      .where("matrix_nom_items.dim_index", "=", dimIndex)
      .where("territories.level", "=", "NUTS3")
      .where("territories.code", "=", countyCode)
      .executeTakeFirst();

    return result?.nom_item_id ?? null;
  }

  /**
   * Find all locality nom_item_ids for a given county
   */
  private async findLocalitiesInCounty(
    matrixId: number,
    dimIndex: number,
    countyCode: string
  ): Promise<number[]> {
    // First get the county's path
    const county = await this.db
      .selectFrom("territories")
      .select("path")
      .where("code", "=", countyCode)
      .where("level", "=", "NUTS3")
      .executeTakeFirst();

    if (!county) {
      return [];
    }

    const countyPath = county.path as unknown as string;

    // Find all LAU localities under this county
    const localities = await this.db
      .selectFrom("matrix_nom_items")
      .innerJoin(
        "territories",
        "matrix_nom_items.territory_id",
        "territories.id"
      )
      .select("matrix_nom_items.nom_item_id")
      .where("matrix_nom_items.matrix_id", "=", matrixId)
      .where("matrix_nom_items.dim_index", "=", dimIndex)
      .where("territories.level", "=", "LAU")
      .$call((qb) => qb.where("territories.path", "~", `${countyPath}.*`))
      .execute();

    return localities.map((l) => l.nom_item_id);
  }

  /**
   * Estimate cell count for a chunk
   */
  estimateCellCount(selections: number[][]): number {
    return estimateCellCount(selections);
  }

  /**
   * Check if selections would exceed the cell limit
   */
  wouldExceedLimit(selections: number[][]): boolean {
    return this.estimateCellCount(selections) > CELL_LIMIT;
  }

  /**
   * Compute a unique hash for a chunk (for checkpoint tracking)
   */
  private computeChunkHash(
    matrixCode: string,
    countyCode: string | null,
    year: number,
    classificationMode: ClassificationMode
  ): string {
    const key = `${matrixCode}:${countyCode ?? "NAT"}:${String(year)}:${classificationMode}`;
    return createHash("md5").update(key).digest("hex").substring(0, 16);
  }

  /**
   * Generate an array of years from a range
   */
  private generateYearRange(from: number, to: number): number[] {
    const years: number[] = [];
    for (let y = from; y <= to; y++) {
      years.push(y);
    }
    return years;
  }

  /**
   * Format milliseconds as human-readable duration
   */
  private formatDuration(ms: number): string {
    const seconds = Math.floor(ms / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);

    if (hours > 0) {
      return `~${String(hours)}h ${String(minutes % 60)}m`;
    }
    if (minutes > 0) {
      return `~${String(minutes)}m ${String(seconds % 60)}s`;
    }
    return `~${String(seconds)}s`;
  }

  /**
   * Load matrix info from database
   */
  async loadMatrixInfo(matrixCode: string): Promise<MatrixInfo | null> {
    const matrix = await this.db
      .selectFrom("matrices")
      .select(["id", "ins_code", "metadata"])
      .where("ins_code", "=", matrixCode)
      .executeTakeFirst();

    if (!matrix) {
      return null;
    }

    const dimensions = await this.loadDimensions(matrix.id);

    return {
      id: matrix.id,
      insCode: matrix.ins_code,
      metadata: matrix.metadata,
      dimensions,
    };
  }

  /**
   * Load dimensions with nom_items for a matrix
   */
  private async loadDimensions(matrixId: number): Promise<DimensionInfo[]> {
    const nomItems = await this.db
      .selectFrom("matrix_nom_items")
      .select([
        "dim_index",
        "nom_item_id",
        "dimension_type",
        "territory_id",
        "time_period_id",
        "classification_value_id",
        "unit_id",
        "labels",
        "parent_nom_item_id",
      ])
      .where("matrix_id", "=", matrixId)
      .orderBy("dim_index")
      .orderBy("offset_order")
      .execute();

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
        labelRo: item.labels?.ro ?? "",
        territoryId: item.territory_id,
        timePeriodId: item.time_period_id,
        classificationValueId: item.classification_value_id,
        unitId: item.unit_id,
        parentNomItemId: item.parent_nom_item_id,
      });
    }

    return Array.from(dimensionMap.values()).sort(
      (a, b) => a.dimIndex - b.dimIndex
    );
  }
}

// ============================================================================
// Utility Functions
// ============================================================================

/**
 * Get display name for a chunk (for logging)
 */
export function getChunkDisplayName(chunk: SyncChunk): string {
  let prefix: string;
  if (chunk.territoryLevel === "national") {
    prefix = "NAT";
  } else if (chunk.territoryLevel === "county" && chunk.countyCode === null) {
    prefix = "COUNTIES";
  } else {
    prefix = chunk.countyCode ?? "NAT";
  }
  return `${prefix}-${String(chunk.year)}-${chunk.territoryLevel}`;
}

/**
 * Estimate total data points for a matrix sync
 */
export function estimateTotalDataPoints(result: ChunkGenerationResult): number {
  // Rough estimate: ~76 localities per county average, ~2 classifications
  if (result.hasUatData) {
    return result.chunks.length * 76 * 2;
  }
  if (result.hasCountyData) {
    return result.chunks.length * 42 * 2;
  }
  return result.chunks.length * 2;
}
