/**
 * Chunk Generator - Generates chunks for syncing matrix data
 *
 * Handles the 30,000 cell limit by adaptively splitting selections:
 * 1. Build dimension axes from metadata (time, territory, classification, etc.)
 * 2. Start with maximal selections and split along a priority order
 * 3. Enforce deterministic chunk signatures for resumability
 */

import { createHash } from "node:crypto";

import { apiLogger } from "../../logger.js";
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
  /** Unique hash for checkpoint tracking */
  chunkHash: string;
  /** Human-readable chunk summary */
  chunkQuery: string;
  /** EncQuery selections per dimension */
  selections: number[][];
  /** Estimated cell count */
  cellCount: number;
  /** Estimated CSV size in bytes (approximate) */
  estimatedCsvBytes: number;
  /** Classification mode - 'all' syncs all breakdowns, 'totals-only' syncs only aggregates */
  classificationMode: ClassificationMode;
  /** Territory level - determines what territorial data to include */
  territoryLevel: TerritoryLevel;
  /** County code (e.g., "AB" for Alba) - null for national/county-aggregate chunks */
  countyCode: string | null;
  /** Year range covered by the chunk */
  yearFrom: number | null;
  yearTo: number | null;
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
  /** Override cell limit (default: 30,000) */
  cellLimit?: number;
}

/** Result of chunk generation */
export interface ChunkGenerationResult {
  chunks: SyncChunk[];
  estimatedApiCalls: number;
  estimatedDuration: string;
  hasUatData: boolean;
  hasCountyData: boolean;
}

interface AxisGroup {
  selections: Map<number, number[]>;
  optionIds?: number[];
  optionCount: number;
  meta?: {
    countyCode?: string;
  };
}

interface Axis {
  axisId: string;
  kind:
    | "temporal"
    | "classification"
    | "territorial"
    | "unit"
    | "paired-territorial";
  dimIndices: number[];
  dimIndex?: number;
  groups: AxisGroup[];
  splitPriority: number;
  canSplit: boolean;
}

interface ChunkPlan {
  groups: AxisGroup[];
}

// ============================================================================
// Constants
// ============================================================================

const CELL_LIMIT = 30_000;
const RATE_LIMIT_MS = 750;

// ============================================================================
// Chunk Generator
// ============================================================================

export class ChunkGenerator {
  constructor(private readonly db: Kysely<Database>) {}

  /**
   * Generate chunks for a matrix based on its structure
   */
  async generateChunks(
    matrix: MatrixInfo,
    options: ChunkGeneratorOptions
  ): Promise<ChunkGenerationResult> {
    const { classificationMode, countyCode, cellLimit } = options;
    const chunks: SyncChunk[] = [];
    const effectiveLimit = Math.min(cellLimit ?? CELL_LIMIT, CELL_LIMIT);

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

    apiLogger.debug(
      {
        matrixCode: matrix.insCode,
        hasUatData,
        hasCountyData,
        localityDimIndex,
        countyDimIndex,
        yearFrom: options.yearFrom,
        yearTo: options.yearTo,
        classificationMode,
        cellLimit: effectiveLimit,
      },
      "Generating adaptive chunks"
    );

    if (hasUatData && localityDimIndex != null) {
      if (!countyCode) {
        const aggregateChunks = await this.planChunks(matrix, options, {
          territoryLevel: "county",
          countyCode: null,
          useUatPairing: false,
          localityDimIndex: localityDimIndex ?? null,
          countyDimIndex: countyDimIndex ?? null,
          cellLimit: effectiveLimit,
        });
        chunks.push(...aggregateChunks);
      }

      const detailChunks = await this.planChunks(matrix, options, {
        territoryLevel: "uat",
        countyCode: countyCode ?? null,
        useUatPairing: true,
        localityDimIndex: localityDimIndex ?? null,
        countyDimIndex: countyDimIndex ?? null,
        cellLimit: effectiveLimit,
      });
      chunks.push(...detailChunks);
    } else if (hasCountyData) {
      const countyChunks = await this.planChunks(matrix, options, {
        territoryLevel: "county",
        countyCode: countyCode ?? null,
        useUatPairing: false,
        localityDimIndex: localityDimIndex ?? null,
        countyDimIndex: countyDimIndex ?? null,
        cellLimit: effectiveLimit,
      });
      chunks.push(...countyChunks);
    } else {
      const nationalChunks = await this.planChunks(matrix, options, {
        territoryLevel: "national",
        countyCode: null,
        useUatPairing: false,
        localityDimIndex: localityDimIndex ?? null,
        countyDimIndex: countyDimIndex ?? null,
        cellLimit: effectiveLimit,
      });
      chunks.push(...nationalChunks);
    }

    const estimatedApiCalls = chunks.length;
    const estimatedMs = estimatedApiCalls * RATE_LIMIT_MS;
    const estimatedDuration = this.formatDuration(estimatedMs);

    if (chunks.length > 0) {
      let totalCells = 0;
      let minCells = Number.POSITIVE_INFINITY;
      let maxCells = 0;
      let totalBytes = 0;
      let maxBytes = 0;
      let maxChunk = chunks[0]!;

      for (const chunk of chunks) {
        totalCells += chunk.cellCount;
        totalBytes += chunk.estimatedCsvBytes;
        if (chunk.cellCount < minCells) {
          minCells = chunk.cellCount;
        }
        if (chunk.cellCount > maxCells) {
          maxCells = chunk.cellCount;
          maxChunk = chunk;
        }
        if (chunk.estimatedCsvBytes > maxBytes) {
          maxBytes = chunk.estimatedCsvBytes;
        }
      }

      const avgCells = Math.round(totalCells / chunks.length);
      const avgBytes = Math.round(totalBytes / chunks.length);

      apiLogger.info(
        {
          matrixCode: matrix.insCode,
          chunkCount: chunks.length,
          cellLimit: effectiveLimit,
          minCells,
          maxCells,
          avgCells,
          totalCells,
          maxEstimatedCsvBytes: maxBytes,
          avgEstimatedCsvBytes: avgBytes,
          totalEstimatedCsvBytes: totalBytes,
          maxChunkQuery: maxChunk.chunkQuery,
          maxChunkHash: maxChunk.chunkHash,
        },
        "Chunk plan summary"
      );
    } else {
      apiLogger.warn(
        { matrixCode: matrix.insCode },
        "Chunk plan returned no chunks"
      );
    }

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

  private async planChunks(
    matrix: MatrixInfo,
    options: ChunkGeneratorOptions,
    plan: {
      territoryLevel: TerritoryLevel;
      countyCode: string | null;
      useUatPairing: boolean;
      localityDimIndex: number | null;
      countyDimIndex: number | null;
      cellLimit: number;
    }
  ): Promise<SyncChunk[]> {
    const axes: Axis[] = [];
    const yearMap = new Map<number, number>();
    const dimIndexToPosition = new Map<number, number>();
    const avgRowBytes = this.estimateAverageRowBytes(matrix.dimensions);

    for (let i = 0; i < matrix.dimensions.length; i++) {
      const dim = matrix.dimensions[i]!;
      dimIndexToPosition.set(dim.dimIndex, i);
    }

    if (plan.useUatPairing) {
      if (plan.countyDimIndex == null || plan.localityDimIndex == null) {
        throw new Error(
          `Cannot build UAT chunking plan for ${matrix.insCode}: missing county/locality dimensions`
        );
      }

      const pairedAxis = await this.buildUatAxis(
        matrix,
        plan.countyDimIndex,
        plan.localityDimIndex,
        plan.countyCode
      );
      axes.push(pairedAxis);
    }

    for (const dim of matrix.dimensions) {
      if (
        plan.useUatPairing &&
        (dim.dimIndex === plan.countyDimIndex ||
          dim.dimIndex === plan.localityDimIndex)
      ) {
        continue;
      }

      if (dim.dimensionType === "TEMPORAL") {
        const temporalOptions = this.buildTemporalOptions(
          dim,
          options.yearFrom,
          options.yearTo,
          yearMap
        );
        axes.push(
          this.buildSingleAxis(
            dim,
            "temporal",
            temporalOptions,
            this.getSplitPriority("temporal")
          )
        );
        continue;
      }

      if (dim.dimensionType === "TERRITORIAL") {
        const selections = await this.buildTerritorialSelections(
          matrix,
          dim,
          plan.territoryLevel,
          plan.countyCode,
          plan.countyDimIndex,
          plan.localityDimIndex
        );
        axes.push(
          this.buildSingleAxis(
            dim,
            "territorial",
            selections,
            this.getSplitPriority("territorial")
          )
        );
        continue;
      }

      if (dim.dimensionType === "CLASSIFICATION") {
        const selections = this.buildClassificationSelections(
          dim,
          options.classificationMode
        );
        axes.push(
          this.buildSingleAxis(
            dim,
            "classification",
            selections,
            this.getSplitPriority("classification")
          )
        );
        continue;
      }

      if (dim.dimensionType === "UNIT_OF_MEASURE") {
        const selections = dim.nomItems.map((item) => item.nomItemId);
        axes.push(
          this.buildSingleAxis(
            dim,
            "unit",
            selections,
            this.getSplitPriority("unit")
          )
        );
        continue;
      }

      // Unknown dimension types: select all
      const selections = dim.nomItems.map((item) => item.nomItemId);
      axes.push(
        this.buildSingleAxis(
          dim,
          "classification",
          selections,
          this.getSplitPriority("classification")
        )
      );
    }

    axes.sort((a, b) => {
      if (a.splitPriority !== b.splitPriority) {
        return a.splitPriority - b.splitPriority;
      }
      return a.axisId.localeCompare(b.axisId);
    });

    const initialPlans = this.buildInitialPlans(axes);
    const finalChunks: SyncChunk[] = [];

    const pending = [...initialPlans];
    while (pending.length > 0) {
      const planItem = pending.pop();
      if (!planItem) continue;

      const selections = this.buildSelectionsFromPlan(
        matrix,
        axes,
        planItem.groups
      );
      const cappedCellCount = this.estimateCellCountCapped(
        selections,
        plan.cellLimit
      );

      if (cappedCellCount <= plan.cellLimit) {
        finalChunks.push(
          this.materializeChunk(
            matrix,
            axes,
            planItem.groups,
            selections,
            avgRowBytes,
            yearMap,
            dimIndexToPosition,
            options.classificationMode,
            plan.territoryLevel,
            plan.countyCode
          )
        );
        continue;
      }

      const splitIndex = this.pickSplitAxisIndex(
        axes,
        planItem.groups,
        selections,
        plan.cellLimit,
        dimIndexToPosition
      );

      if (splitIndex == null) {
        throw new Error(
          `Unable to split chunk for ${matrix.insCode} within cell limit`
        );
      }

      const axis = axes[splitIndex]!;
      const group = planItem.groups[splitIndex]!;
      const splitGroups = this.splitAxisGroup(
        axis,
        group,
        selections,
        plan.cellLimit,
        dimIndexToPosition
      );

      for (const nextGroup of splitGroups) {
        const nextGroups = [...planItem.groups];
        nextGroups[splitIndex] = nextGroup;
        pending.push({ groups: nextGroups });
      }
    }

    return finalChunks;
  }

  private buildTemporalOptions(
    dim: DimensionInfo,
    yearFrom: number,
    yearTo: number,
    yearMap: Map<number, number>
  ): number[] {
    const selections: number[] = [];

    for (const item of dim.nomItems) {
      const match = /\d{4}/.exec(item.labelRo);
      if (!match) continue;
      const year = Number.parseInt(match[0], 10);
      if (Number.isNaN(year)) continue;

      yearMap.set(item.nomItemId, year);
      if (year >= yearFrom && year <= yearTo) {
        selections.push(item.nomItemId);
      }
    }

    if (selections.length === 0) {
      const fallback = dim.nomItems[0];
      if (fallback) {
        const match = /\d{4}/.exec(fallback.labelRo);
        if (match) {
          yearMap.set(fallback.nomItemId, Number.parseInt(match[0], 10));
        }
        apiLogger.warn(
          { yearFrom, yearTo, dimIndex: dim.dimIndex },
          "No temporal items match year range, taking first"
        );
        return [fallback.nomItemId];
      }
    }

    return selections;
  }

  private buildSingleAxis(
    dim: DimensionInfo,
    kind: Axis["kind"],
    selectionIds: number[],
    splitPriority: number
  ): Axis {
    const options =
      selectionIds.length > 0
        ? selectionIds
        : dim.nomItems[0]
          ? [dim.nomItems[0].nomItemId]
          : [];
    const group = this.buildAxisGroup(dim.dimIndex, options);

    return {
      axisId: `${kind}-${String(dim.dimIndex)}`,
      kind,
      dimIndices: [dim.dimIndex],
      dimIndex: dim.dimIndex,
      groups: [group],
      splitPriority,
      canSplit: options.length > 1,
    };
  }

  private buildAxisGroup(dimIndex: number, optionIds: number[]): AxisGroup {
    return {
      selections: new Map([[dimIndex, optionIds]]),
      optionIds,
      optionCount: optionIds.length,
    };
  }

  private getSplitPriority(kind: Axis["kind"]): number {
    switch (kind) {
      case "temporal":
        return 1;
      case "paired-territorial":
        return 2;
      case "territorial":
        return 3;
      case "classification":
        return 4;
      case "unit":
        return 5;
      default:
        return 6;
    }
  }

  private buildInitialPlans(axes: Axis[]): ChunkPlan[] {
    let plans: ChunkPlan[] = [{ groups: [] }];

    for (const axis of axes) {
      const nextPlans: ChunkPlan[] = [];
      for (const plan of plans) {
        for (const group of axis.groups) {
          nextPlans.push({ groups: [...plan.groups, group] });
        }
      }
      plans = nextPlans;
    }

    return plans;
  }

  private buildSelectionsFromPlan(
    matrix: MatrixInfo,
    axes: Axis[],
    groups: AxisGroup[]
  ): number[][] {
    const selectionsByDimIndex = new Map<number, number[]>();

    for (let i = 0; i < axes.length; i++) {
      const group = groups[i];
      if (!group) continue;
      for (const [dimIndex, ids] of group.selections) {
        selectionsByDimIndex.set(dimIndex, ids);
      }
    }

    return matrix.dimensions.map((dim) => {
      return (
        selectionsByDimIndex.get(dim.dimIndex) ??
        dim.nomItems.map((item) => item.nomItemId)
      );
    });
  }

  private estimateCellCountCapped(
    selections: number[][],
    limit: number
  ): number {
    let count = 1;
    for (const dimSelections of selections) {
      count *= dimSelections.length;
      if (count > limit) {
        return limit + 1;
      }
    }
    return count;
  }

  private pickSplitAxisIndex(
    axes: Axis[],
    groups: AxisGroup[],
    selections: number[][],
    cellLimit: number,
    dimIndexToPosition: Map<number, number>
  ): number | null {
    const axisOrder = axes
      .map((axis, index) => ({ axis, index }))
      .sort((a, b) => {
        if (a.axis.splitPriority !== b.axis.splitPriority) {
          return a.axis.splitPriority - b.axis.splitPriority;
        }
        return a.axis.axisId.localeCompare(b.axis.axisId);
      });

    for (const { axis, index } of axisOrder) {
      if (!axis.canSplit || axis.dimIndex == null) continue;
      const group = groups[index];
      const optionIds = group?.optionIds ?? [];
      if (optionIds.length <= 1) continue;

      const dimPos = dimIndexToPosition.get(axis.dimIndex);
      if (dimPos == null) continue;

      const otherProduct = this.productExcept(selections, dimPos, cellLimit);
      const maxOptions = Math.floor(cellLimit / otherProduct);

      if (maxOptions >= 1 && maxOptions < optionIds.length) {
        return index;
      }
    }

    return null;
  }

  private splitAxisGroup(
    axis: Axis,
    group: AxisGroup,
    selections: number[][],
    cellLimit: number,
    dimIndexToPosition: Map<number, number>
  ): AxisGroup[] {
    if (!axis.canSplit || axis.dimIndex == null) {
      return [group];
    }

    const optionIds = group.optionIds ?? [];
    if (optionIds.length <= 1) {
      return [group];
    }

    const dimPos = dimIndexToPosition.get(axis.dimIndex);
    if (dimPos == null) {
      return [group];
    }

    const otherProduct = this.productExcept(selections, dimPos, cellLimit);
    const maxOptions = Math.floor(cellLimit / otherProduct);

    if (maxOptions < 1) {
      return [group];
    }

    const chunkCount = Math.ceil(optionIds.length / maxOptions);
    const groupSize = Math.ceil(optionIds.length / chunkCount);
    const groups: AxisGroup[] = [];

    for (let i = 0; i < optionIds.length; i += groupSize) {
      const nextIds = optionIds.slice(i, i + groupSize);
      groups.push(this.buildAxisGroup(axis.dimIndex, nextIds));
    }

    return groups;
  }

  private productExcept(
    selections: number[][],
    excludeIndex: number,
    cap: number
  ): number {
    let product = 1;
    for (let i = 0; i < selections.length; i++) {
      if (i === excludeIndex) continue;
      product *= selections[i]?.length ?? 0;
      if (product > cap) {
        return cap + 1;
      }
    }
    return product;
  }

  private async buildUatAxis(
    matrix: MatrixInfo,
    countyDimIndex: number,
    localityDimIndex: number,
    countyCode: string | null
  ): Promise<Axis> {
    const rows = await this.db
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
        "territories.level",
        "territories.path",
      ])
      .where("matrix_nom_items.matrix_id", "=", matrix.id)
      .where("matrix_nom_items.dimension_type", "=", "TERRITORIAL")
      .where("matrix_nom_items.dim_index", "in", [
        countyDimIndex,
        localityDimIndex,
      ])
      .execute();

    const counties = rows.filter(
      (row) => row.dim_index === countyDimIndex && row.level === "NUTS3"
    );
    const localities = rows.filter(
      (row) => row.dim_index === localityDimIndex && row.level === "LAU"
    );

    const countyEntries = counties
      .map((row) => ({
        code: row.code,
        path: row.path,
        nomItemId: row.nom_item_id,
      }))
      .filter((row) => (countyCode ? row.code === countyCode : true));

    if (countyCode && countyEntries.length === 0) {
      throw new Error(
        `County ${countyCode} not found in matrix ${matrix.insCode}`
      );
    }

    countyEntries.sort((a, b) => a.code.localeCompare(b.code));

    const localitiesByCounty = new Map<string, number[]>();

    for (const locality of localities) {
      const localityPath = locality.path;
      let matched: { code: string; path: string; nomItemId: number } | null =
        null;

      for (const county of countyEntries) {
        if (
          localityPath.startsWith(county.path) &&
          (!matched || county.path.length > matched.path.length)
        ) {
          matched = county;
        }
      }

      if (!matched) continue;

      const list = localitiesByCounty.get(matched.code) ?? [];
      list.push(locality.nom_item_id);
      localitiesByCounty.set(matched.code, list);
    }

    const groups: AxisGroup[] = [];
    for (const county of countyEntries) {
      const localityIds = localitiesByCounty.get(county.code) ?? [];
      localityIds.sort((a, b) => a - b);
      if (localityIds.length === 0) {
        apiLogger.warn(
          { matrixCode: matrix.insCode, countyCode: county.code },
          "No localities found for county"
        );
        continue;
      }

      groups.push({
        selections: new Map([
          [countyDimIndex, [county.nomItemId]],
          [localityDimIndex, localityIds],
        ]),
        optionCount: 1,
        meta: { countyCode: county.code },
      });
    }

    if (groups.length === 0) {
      throw new Error(`No UAT locality selections found for ${matrix.insCode}`);
    }

    return {
      axisId: "territorial-paired",
      kind: "paired-territorial",
      dimIndices: [countyDimIndex, localityDimIndex],
      groups,
      splitPriority: this.getSplitPriority("paired-territorial"),
      canSplit: false,
    };
  }

  private async buildTerritorialSelections(
    matrix: MatrixInfo,
    dim: DimensionInfo,
    territoryLevel: TerritoryLevel,
    countyCode: string | null,
    countyDimIndex: number | null,
    localityDimIndex: number | null
  ): Promise<number[]> {
    if (territoryLevel === "national") {
      return this.findTotalNomItem(dim);
    }

    if (territoryLevel === "county") {
      if (dim.dimIndex === localityDimIndex) {
        return this.findTotalNomItem(dim);
      }

      if (
        dim.dimIndex === countyDimIndex ||
        (countyDimIndex == null && countyCode)
      ) {
        if (countyCode) {
          const countyItem = await this.findCountyNomItem(
            matrix.id,
            dim.dimIndex,
            countyCode
          );
          if (!countyItem) {
            throw new Error(
              `County ${countyCode} not found in matrix ${matrix.insCode}`
            );
          }
          return [countyItem];
        }

        const nonTotalItems = dim.nomItems.filter(
          (item) => !item.labelRo.toLowerCase().includes("total")
        );
        return nonTotalItems.length > 0
          ? nonTotalItems.map((item) => item.nomItemId)
          : dim.nomItems.map((item) => item.nomItemId);
      }
    }

    if (territoryLevel === "uat") {
      return dim.nomItems.map((item) => item.nomItemId);
    }

    return dim.nomItems.map((item) => item.nomItemId);
  }

  private materializeChunk(
    matrix: MatrixInfo,
    axes: Axis[],
    groups: AxisGroup[],
    selections: number[][],
    avgRowBytes: number,
    yearMap: Map<number, number>,
    dimIndexToPosition: Map<number, number>,
    classificationMode: ClassificationMode,
    territoryLevel: TerritoryLevel,
    defaultCountyCode: string | null
  ): SyncChunk {
    const cellCount = estimateCellCount(selections);
    const estimatedCsvBytes = Math.round(cellCount * avgRowBytes + avgRowBytes);

    const temporalAxis = axes.find((axis) => axis.kind === "temporal");
    let yearFrom: number | null = null;
    let yearTo: number | null = null;

    if (temporalAxis?.dimIndex != null) {
      const pos = dimIndexToPosition.get(temporalAxis.dimIndex);
      if (pos != null) {
        const years = (selections[pos] ?? [])
          .map((id) => yearMap.get(id))
          .filter((year): year is number => year !== undefined);
        if (years.length > 0) {
          yearFrom = Math.min(...years);
          yearTo = Math.max(...years);
        }
      }
    }

    const countyCode =
      groups.find((group) => group.meta?.countyCode)?.meta?.countyCode ??
      defaultCountyCode ??
      null;

    const dimSummary = selections
      .map((ids, index) => `${String(index)}:${String(ids.length)}`)
      .join(",");
    const yearFromStr = yearFrom != null ? String(yearFrom) : "ALL";
    const yearToStr = yearTo != null ? String(yearTo) : "ALL";
    const chunkQuery = `T:${yearFromStr}-${yearToStr}|C:${countyCode ?? "ALL"}|L:${territoryLevel}|CL:${classificationMode}|dims:${dimSummary}`;
    const encQuery = selections.map((ids) => ids.join(",")).join(":");
    const signature = `v2|${matrix.insCode}|${classificationMode}|${territoryLevel}|${countyCode ?? "ALL"}|${encQuery}`;
    const chunkHash = createHash("md5")
      .update(signature)
      .digest("hex")
      .substring(0, 16);

    return {
      chunkHash,
      chunkQuery,
      selections,
      cellCount,
      estimatedCsvBytes,
      classificationMode,
      territoryLevel,
      countyCode,
      yearFrom,
      yearTo,
    };
  }

  private estimateAverageRowBytes(dimensions: DimensionInfo[]): number {
    let labelBytes = 0;
    for (const dim of dimensions) {
      const sampleSize = Math.min(dim.nomItems.length, 50);
      if (sampleSize === 0) continue;
      const sample = dim.nomItems.slice(0, sampleSize);
      const total = sample.reduce((sum, item) => sum + item.labelRo.length, 0);
      labelBytes += total / sampleSize;
    }

    const separators = dimensions.length * 2;
    const valueBytes = 12;
    return Math.max(1, Math.round(labelBytes + separators + valueBytes + 1));
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
  const yearLabel =
    chunk.yearFrom !== null
      ? chunk.yearTo !== null && chunk.yearTo !== chunk.yearFrom
        ? `${String(chunk.yearFrom)}-${String(chunk.yearTo)}`
        : String(chunk.yearFrom)
      : "ALL";
  return `${prefix}-${yearLabel}-${chunk.territoryLevel}`;
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
