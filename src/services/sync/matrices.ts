import { ClassificationService } from "./classifications.js";
import { TerritoryService } from "./territories.js";
import { TimePeriodService } from "./time-periods.js";
import { UnitOfMeasureService } from "./units.js";
import { logger } from "../../logger.js";
import { fetchMatrix, fetchMatricesList } from "../../scraper/client.js";

import type {
  Database,
  DimensionType,
  NewMatrix,
  NewMatrixDimension,
  NewMatrixDimensionOption,
  PeriodicityType,
  SyncResult,
} from "../../db/types.js";
import type { InsMatrix, InsDimension } from "../../types/index.js";
import type { Kysely } from "kysely";

// ============================================================================
// Helper Functions
// ============================================================================

const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Parse INS date format (DD-MM-YYYY) to Date
 */
function parseInsDate(dateStr: string | undefined): Date | null {
  if (!dateStr) return null;

  const match = /(\d{2})-(\d{2})-(\d{4})/.exec(dateStr);
  if (!match?.[1] || !match[2] || !match[3]) return null;

  const [, day, month, year] = match;
  return new Date(
    Number.parseInt(year, 10),
    Number.parseInt(month, 10) - 1,
    Number.parseInt(day, 10)
  );
}

/**
 * Map INS periodicity strings to our enum
 */
function mapPeriodicity(
  periodicitati: string[] | undefined
): PeriodicityType[] {
  if (!periodicitati) return [];

  const mapping: Record<string, PeriodicityType> = {
    anuala: "ANNUAL",
    trimestriala: "QUARTERLY",
    lunara: "MONTHLY",
  };

  return periodicitati
    .map((p) => mapping[p.toLowerCase()])
    .filter((p): p is PeriodicityType => p !== undefined);
}

// ============================================================================
// Matrix Sync Service
// ============================================================================

export class MatrixSyncService {
  private timePeriodService: TimePeriodService;
  private territoryService: TerritoryService;
  private classificationService: ClassificationService;
  private unitService: UnitOfMeasureService;

  constructor(private db: Kysely<Database>) {
    this.timePeriodService = new TimePeriodService(db);
    this.territoryService = new TerritoryService(db);
    this.classificationService = new ClassificationService(db);
    this.unitService = new UnitOfMeasureService(db);
  }

  /**
   * Sync the matrix catalog (list only, no details)
   */
  async syncCatalog(): Promise<SyncResult> {
    const startTime = Date.now();
    logger.info("Starting matrix catalog sync");

    const matrices = await fetchMatricesList();
    logger.info({ count: matrices.length }, "Fetched matrix list from API");

    let inserted = 0;
    let updated = 0;

    for (const m of matrices) {
      const existing = await this.db
        .selectFrom("matrices")
        .select("id")
        .where("ins_code", "=", m.code)
        .executeTakeFirst();

      if (existing) {
        await this.db
          .updateTable("matrices")
          .set({ name: m.name, updated_at: new Date() })
          .where("id", "=", existing.id)
          .execute();
        updated++;
      } else {
        const newMatrix: NewMatrix = {
          ins_code: m.code,
          name: m.name,
          status: "ACTIVE",
          dimension_count: 0,
          has_county_data: false,
          has_uat_data: false,
          has_siruta: false,
          has_caen_rev1: false,
          has_caen_rev2: false,
          um_special: false,
          periodicity: [],
        };

        await this.db.insertInto("matrices").values(newMatrix).execute();
        inserted++;
      }
    }

    const duration = Date.now() - startTime;
    logger.info(
      { inserted, updated, duration },
      "Matrix catalog sync completed"
    );

    return { inserted, updated, duration };
  }

  /**
   * Sync full metadata for a specific matrix
   */
  async syncMatrixDetails(matrixCode: string): Promise<void> {
    logger.info({ matrixCode }, "Syncing matrix details");

    const metadata = await fetchMatrix(matrixCode);
    const matrixId = await this.getMatrixId(matrixCode);

    if (!matrixId) {
      // Create matrix first
      const newMatrix: NewMatrix = {
        ins_code: matrixCode,
        name: metadata.matrixName,
        status: "ACTIVE",
        dimension_count: 0,
        has_county_data: false,
        has_uat_data: false,
        has_siruta: false,
        has_caen_rev1: false,
        has_caen_rev2: false,
        um_special: false,
        periodicity: [],
      };
      await this.db.insertInto("matrices").values(newMatrix).execute();
    }

    const id = (await this.getMatrixId(matrixCode))!;

    // Update matrix with full details
    await this.db
      .updateTable("matrices")
      .set({
        name: metadata.matrixName,
        periodicity: mapPeriodicity(metadata.periodicitati),
        definition: metadata.definitie ?? null,
        methodology: metadata.metodologie ?? null,
        observations: metadata.observatii ?? null,
        series_break: metadata.intrerupere ?? null,
        series_continuation: metadata.continuareSerie ?? null,
        responsible_persons: metadata.persoaneResponsabile ?? null,
        last_update: parseInsDate(metadata.ultimaActualizare),
        dimension_count: metadata.details.matMaxDim,
        has_county_data: metadata.details.nomJud > 0,
        has_uat_data: metadata.details.nomLoc > 0,
        has_siruta: metadata.details.matSiruta > 0,
        has_caen_rev1: metadata.details.matCaen1 > 0,
        has_caen_rev2: metadata.details.matCaen2 > 0,
        territorial_dim_index:
          metadata.details.matRegJ > 0 ? metadata.details.matRegJ : null,
        time_dim_index:
          metadata.details.matTime > 0 ? metadata.details.matTime : null,
        county_dim_index:
          metadata.details.nomJud > 0 ? metadata.details.nomJud : null,
        locality_dim_index:
          metadata.details.nomLoc > 0 ? metadata.details.nomLoc : null,
        um_special: metadata.details.matUMSpec > 0,
        status: metadata.details.matActive > 0 ? "ACTIVE" : "DISCONTINUED",
        view_count: metadata.details.matViews ?? 0,
        download_count: metadata.details.matDownloads ?? 0,
        query_complexity: metadata.details.matCharge ?? 0,
        updated_at: new Date(),
      })
      .where("id", "=", id)
      .execute();

    // Link to context
    if (metadata.ancestors && metadata.ancestors.length > 0) {
      const leafContext = metadata.ancestors.at(-1);
      if (leafContext) {
        const contextId = await this.getContextIdByCode(leafContext.code);
        if (contextId) {
          await this.db
            .updateTable("matrices")
            .set({ context_id: contextId })
            .where("id", "=", id)
            .execute();
        }
      }
    }

    // Sync dimensions
    await this.syncDimensions(id, metadata);

    // Update sync status
    await this.updateSyncStatus(id, "metadata");

    logger.info({ matrixCode, matrixId: id }, "Matrix details synced");
  }

  /**
   * Sync dimensions and their options for a matrix
   */
  private async syncDimensions(
    matrixId: number,
    metadata: InsMatrix
  ): Promise<void> {
    // Clear existing dimensions
    await this.db
      .deleteFrom("matrix_dimensions")
      .where("matrix_id", "=", matrixId)
      .execute();

    for (const dim of metadata.dimensionsMap) {
      // Detect dimension type
      const dimType = this.detectDimensionType(dim);
      let classificationTypeId: number | null = null;

      if (dimType === "CLASSIFICATION") {
        classificationTypeId =
          await this.classificationService.findOrCreateType(dim.label);
      }

      // Insert dimension
      const newDim: NewMatrixDimension = {
        matrix_id: matrixId,
        dim_code: dim.dimCode,
        label: dim.label,
        dimension_type: dimType,
        classification_type_id: classificationTypeId,
        is_hierarchical: dim.options.some((o) => o.parentId !== null),
        option_count: dim.options.length,
      };

      const dimResult = await this.db
        .insertInto("matrix_dimensions")
        .values(newDim)
        .returning("id")
        .executeTakeFirst();

      const dimId = dimResult!.id;

      // Sync options
      await this.syncDimensionOptions(
        dimId,
        dim,
        dimType,
        classificationTypeId
      );
    }
  }

  /**
   * Sync dimension options with reference table linkage
   */
  private async syncDimensionOptions(
    dimId: number,
    dim: InsDimension,
    dimType: DimensionType,
    classificationTypeId: number | null
  ): Promise<void> {
    // Build parent nomItemId to classification value ID mapping
    const parentMap = new Map<number, number>();

    for (const opt of dim.options) {
      let territoryId: number | null = null;
      let timePeriodId: number | null = null;
      let classificationValueId: number | null = null;
      let unitOfMeasureId: number | null = null;

      // Link to appropriate reference table
      switch (dimType) {
        case "TERRITORIAL":
          territoryId = await this.territoryService.findOrCreateFromLabel(
            opt.label
          );
          break;

        case "TEMPORAL":
          timePeriodId = await this.timePeriodService.findOrCreate(opt.label);
          break;

        case "CLASSIFICATION":
          if (classificationTypeId) {
            const parentClassId = opt.parentId
              ? (parentMap.get(opt.parentId) ?? null)
              : null;
            classificationValueId =
              await this.classificationService.findOrCreateValue(
                classificationTypeId,
                opt.label,
                parentClassId,
                opt.offset
              );
            parentMap.set(opt.nomItemId, classificationValueId);
          }
          break;

        case "UNIT_OF_MEASURE":
          unitOfMeasureId = await this.unitService.findOrCreate(opt.label);
          break;
      }

      const newOption: NewMatrixDimensionOption = {
        matrix_dimension_id: dimId,
        nom_item_id: opt.nomItemId,
        label: opt.label,
        offset_order: opt.offset,
        parent_nom_item_id: opt.parentId ?? null,
        territory_id: territoryId,
        time_period_id: timePeriodId,
        classification_value_id: classificationValueId,
        unit_of_measure_id: unitOfMeasureId,
      };

      await this.db
        .insertInto("matrix_dimension_options")
        .values(newOption)
        .execute();
    }
  }

  /**
   * Detect dimension type from dimension metadata
   */
  detectDimensionType(dim: InsDimension): DimensionType {
    const label = dim.label.toLowerCase();

    // Temporal patterns
    if (/^ani$/i.test(label)) return "TEMPORAL";
    if (/perioade/i.test(label)) return "TEMPORAL";
    if (/trimestre/i.test(label)) return "TEMPORAL";
    if (/^luni$/i.test(label)) return "TEMPORAL";

    // Territorial patterns
    if (/judete/i.test(label)) return "TERRITORIAL";
    if (/regiuni/i.test(label)) return "TERRITORIAL";
    if (/localitati/i.test(label)) return "TERRITORIAL";
    if (/macroregiuni/i.test(label)) return "TERRITORIAL";

    // Unit of measure patterns
    if (/^um:/i.test(label)) return "UNIT_OF_MEASURE";

    // Default to classification
    return "CLASSIFICATION";
  }

  /**
   * Get matrix ID by INS code
   */
  async getMatrixId(code: string): Promise<number | null> {
    const result = await this.db
      .selectFrom("matrices")
      .select("id")
      .where("ins_code", "=", code)
      .executeTakeFirst();
    return result?.id ?? null;
  }

  /**
   * Get context ID by INS code
   */
  private async getContextIdByCode(code: string): Promise<number | null> {
    const result = await this.db
      .selectFrom("contexts")
      .select("id")
      .where("ins_code", "=", code)
      .executeTakeFirst();
    return result?.id ?? null;
  }

  /**
   * Update matrix sync status
   */
  private async updateSyncStatus(
    matrixId: number,
    syncType: "metadata" | "full" | "incremental"
  ): Promise<void> {
    const existing = await this.db
      .selectFrom("matrix_sync_status")
      .select("id")
      .where("matrix_id", "=", matrixId)
      .executeTakeFirst();

    const now = new Date();
    const updates: Record<string, unknown> = {
      sync_status: "SYNCED",
      updated_at: now,
    };

    if (syncType === "metadata") {
      updates.last_metadata_sync = now;
    } else if (syncType === "full") {
      updates.last_full_sync = now;
    } else {
      updates.last_incremental_sync = now;
    }

    if (existing) {
      await this.db
        .updateTable("matrix_sync_status")
        .set(updates)
        .where("id", "=", existing.id)
        .execute();
    } else {
      await this.db
        .insertInto("matrix_sync_status")
        .values({
          matrix_id: matrixId,
          sync_status: "SYNCED",
          last_metadata_sync: syncType === "metadata" ? now : null,
          last_full_sync: syncType === "full" ? now : null,
          last_incremental_sync: syncType === "incremental" ? now : null,
        })
        .execute();
    }
  }

  /**
   * Sync all matrices with full details (slow!)
   */
  async syncAllMatricesWithDetails(rateLimit = 750): Promise<SyncResult> {
    // First sync catalog
    await this.syncCatalog();

    // Get all matrix codes
    const matrices = await this.db
      .selectFrom("matrices")
      .select("ins_code")
      .execute();

    let synced = 0;
    let errors = 0;

    for (const m of matrices) {
      try {
        await this.syncMatrixDetails(m.ins_code);
        synced++;
        logger.info(
          {
            code: m.ins_code,
            progress: `${String(synced)}/${String(matrices.length)}`,
          },
          "Synced matrix"
        );
      } catch (error) {
        errors++;
        logger.error({ code: m.ins_code, error }, "Failed to sync matrix");
      }

      await sleep(rateLimit);
    }

    return { inserted: synced, updated: 0, errors };
  }
}
