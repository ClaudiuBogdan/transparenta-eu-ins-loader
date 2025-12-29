import { ClassificationService } from "./classifications.js";
import { TerritoryService } from "./territories.js";
import { TimePeriodService } from "./time-periods.js";
import { UnitOfMeasureService } from "./units.js";
import { logger } from "../../logger.js";
import {
  fetchMatrixBilingual,
  fetchMatricesListBilingual,
} from "../../scraper/client.js";

import type {
  Database,
  DimensionType,
  NewMatrix,
  NewMatrixDimension,
  NewMatrixDimensionOption,
  PeriodicityType,
  SyncResult,
} from "../../db/types.js";
import type {
  InsMatrix,
  InsDimension,
  InsDataSource,
} from "../../types/index.js";
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
   * Fetches both Romanian and English names in parallel
   */
  async syncCatalog(): Promise<SyncResult> {
    const startTime = Date.now();
    logger.info("Starting matrix catalog sync (bilingual)");

    // Fetch both languages in parallel
    const { ro: matricesRo, en: matricesEn } =
      await fetchMatricesListBilingual();
    logger.info(
      { roCount: matricesRo.length, enCount: matricesEn.length },
      "Fetched matrix lists from API (RO and EN)"
    );

    // Build EN name lookup map by code
    const enNameMap = new Map<string, string>();
    for (const m of matricesEn) {
      enNameMap.set(m.code, m.name);
    }

    let inserted = 0;
    let updated = 0;

    for (const m of matricesRo) {
      const nameEn = enNameMap.get(m.code) ?? null;

      const existing = await this.db
        .selectFrom("matrices")
        .select("id")
        .where("ins_code", "=", m.code)
        .executeTakeFirst();

      if (existing) {
        await this.db
          .updateTable("matrices")
          .set({ name: m.name, name_en: nameEn, updated_at: new Date() })
          .where("id", "=", existing.id)
          .execute();
        updated++;
      } else {
        const newMatrix: NewMatrix = {
          ins_code: m.code,
          name: m.name,
          name_en: nameEn,
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
      "Matrix catalog sync completed (bilingual)"
    );

    return { inserted, updated, duration };
  }

  /**
   * Sync full metadata for a specific matrix (bilingual)
   */
  async syncMatrixDetails(matrixCode: string): Promise<void> {
    logger.info({ matrixCode }, "Syncing matrix details (bilingual)");

    // Fetch both languages in parallel
    const { ro: metadataRo, en: metadataEn } =
      await fetchMatrixBilingual(matrixCode);

    const matrixId = await this.getMatrixId(matrixCode);

    if (!matrixId) {
      // Create matrix first
      const newMatrix: NewMatrix = {
        ins_code: matrixCode,
        name: metadataRo.matrixName,
        name_en: metadataEn.matrixName ?? null,
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

    // Build EN lookup maps for dimensions and options
    const enDimLabelMap = new Map<number, string>();
    for (const dim of metadataEn.dimensionsMap) {
      enDimLabelMap.set(dim.dimCode, dim.label);
    }

    const enOptLabelMap = new Map<number, string>();
    for (const dim of metadataEn.dimensionsMap) {
      for (const opt of dim.options) {
        enOptLabelMap.set(opt.nomItemId, opt.label);
      }
    }

    // Update matrix with full details (RO + EN bilingual data)
    await this.db
      .updateTable("matrices")
      .set({
        name: metadataRo.matrixName,
        name_en: metadataEn.matrixName ?? null,
        periodicity: mapPeriodicity(metadataRo.periodicitati),
        definition: metadataRo.definitie ?? null,
        definition_en: metadataEn.definitie ?? null,
        methodology: metadataRo.metodologie ?? null,
        methodology_en: metadataEn.metodologie ?? null,
        observations: metadataRo.observatii ?? null,
        observations_en: metadataEn.observatii ?? null,
        series_break: metadataRo.intrerupere
          ? JSON.stringify(metadataRo.intrerupere)
          : null,
        series_break_en: metadataEn.intrerupere
          ? JSON.stringify(metadataEn.intrerupere)
          : null,
        series_continuation: metadataRo.continuareSerie
          ? JSON.stringify(metadataRo.continuareSerie)
          : null,
        series_continuation_en: metadataEn.continuareSerie
          ? JSON.stringify(metadataEn.continuareSerie)
          : null,
        responsible_persons: metadataRo.persoaneResponsabile ?? null,
        last_update: parseInsDate(metadataRo.ultimaActualizare),
        dimension_count: metadataRo.details.matMaxDim,
        has_county_data: metadataRo.details.nomJud > 0,
        has_uat_data: metadataRo.details.nomLoc > 0,
        has_siruta: metadataRo.details.matSiruta > 0,
        has_caen_rev1: metadataRo.details.matCaen1 > 0,
        has_caen_rev2: metadataRo.details.matCaen2 > 0,
        territorial_dim_index:
          metadataRo.details.matRegJ > 0 ? metadataRo.details.matRegJ : null,
        time_dim_index:
          metadataRo.details.matTime > 0 ? metadataRo.details.matTime : null,
        county_dim_index:
          metadataRo.details.nomJud > 0 ? metadataRo.details.nomJud : null,
        locality_dim_index:
          metadataRo.details.nomLoc > 0 ? metadataRo.details.nomLoc : null,
        um_special: metadataRo.details.matUMSpec > 0,
        status: metadataRo.details.matActive > 0 ? "ACTIVE" : "DISCONTINUED",
        view_count: metadataRo.details.matViews ?? 0,
        download_count: metadataRo.details.matDownloads ?? 0,
        query_complexity: metadataRo.details.matCharge ?? 0,
        updated_at: new Date(),
      })
      .where("id", "=", id)
      .execute();

    // Link to context
    if (metadataRo.ancestors && metadataRo.ancestors.length > 0) {
      const leafContext = metadataRo.ancestors.at(-1);
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

    // Build EN lookup for data sources (by linkNumber)
    const enDataSourceMap = new Map<number, string>();
    for (const ds of metadataEn.surseDeDate ?? []) {
      enDataSourceMap.set(ds.linkNumber, ds.nume);
    }

    // Sync data sources with EN translations
    await this.syncDataSources(
      id,
      metadataRo.surseDeDate ?? [],
      enDataSourceMap
    );

    // Sync dimensions with EN label maps
    await this.syncDimensions(id, metadataRo, enDimLabelMap, enOptLabelMap);

    // Update sync status
    await this.updateSyncStatus(id, "metadata");

    logger.info(
      { matrixCode, matrixId: id },
      "Matrix details synced (bilingual)"
    );
  }

  /**
   * Sync data sources for a matrix (bilingual)
   */
  private async syncDataSources(
    matrixId: number,
    dataSources: InsDataSource[],
    enNameMap: Map<number, string>
  ): Promise<void> {
    // Clear existing data sources
    await this.db
      .deleteFrom("matrix_data_sources")
      .where("matrix_id", "=", matrixId)
      .execute();

    for (const ds of dataSources) {
      const nameEn = enNameMap.get(ds.linkNumber) ?? null;

      await this.db
        .insertInto("matrix_data_sources")
        .values({
          matrix_id: matrixId,
          name: ds.nume,
          name_en: nameEn,
          source_type: ds.tip || null,
          link_number: ds.linkNumber,
          source_code: ds.codTip,
        })
        .execute();
    }
  }

  /**
   * Sync dimensions and their options for a matrix
   */
  private async syncDimensions(
    matrixId: number,
    metadata: InsMatrix,
    enDimLabelMap: Map<number, string>,
    enOptLabelMap: Map<number, string>
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

      // Get EN label for this dimension
      const labelEn = enDimLabelMap.get(dim.dimCode) ?? null;

      // Insert dimension
      const newDim: NewMatrixDimension = {
        matrix_id: matrixId,
        dim_code: dim.dimCode,
        label: dim.label,
        label_en: labelEn,
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

      // Sync options with EN label map
      await this.syncDimensionOptions(
        dimId,
        dim,
        dimType,
        classificationTypeId,
        enOptLabelMap
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
    classificationTypeId: number | null,
    enOptLabelMap: Map<number, string>
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

      // Get EN label for this option
      const labelEn = enOptLabelMap.get(opt.nomItemId) ?? null;

      const newOption: NewMatrixDimensionOption = {
        matrix_dimension_id: dimId,
        nom_item_id: opt.nomItemId,
        label: opt.label.trim(), // Normalize whitespace at storage
        label_en: labelEn?.trim() ?? null,
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
