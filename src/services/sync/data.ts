/**
 * Data Sync Service - Syncs statistical data from INS Tempo API
 *
 * Flow:
 * 1. Load matrix dimensions and nom_items from database
 * 2. Build encQuery selecting specific year ranges
 * 3. Query INS API pivot endpoint
 * 4. Parse CSV and map to canonical entities
 * 5. Insert into statistics table
 */

import { createHash } from "node:crypto";

import { apiLogger } from "../../logger.js";
import { queryMatrix, buildEncQuery } from "../../scraper/client.js";

import type { Database, DimensionType } from "../../db/types.js";
import type { Kysely } from "kysely";

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
  onProgress?: (progress: DataSyncProgress) => void;
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
  private db: Kysely<Database>;

  constructor(db: Kysely<Database>) {
    this.db = db;
  }

  /**
   * Sync statistical data for a matrix
   */
  async syncData(options: DataSyncOptions): Promise<DataSyncResult> {
    const { matrixCode, yearFrom, yearTo, onProgress } = options;

    apiLogger.info({ matrixCode, yearFrom, yearTo }, "Starting data sync");

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

    // 3. Build query selections
    const selections = this.buildSelections(dimensions, yearFrom, yearTo);
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
    yearTo?: number
  ): number[][] {
    return dimensions.map((dim) => {
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
        // For territorial, take first (usually national level)
        if (dim.dimensionType === "TERRITORIAL") {
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
