import { logger } from "../../logger.js";
import { LabelResolver } from "./canonical/label-resolver.js";
import { jsonb } from "../../db/connection.js";
import * as insClient from "../../scraper/client.js";

import type {
  Database,
  MatrixMetadata,
  DimensionSummary,
  BilingualText,
  DimensionType,
} from "../../db/types-v2.js";
import type {
  InsContext,
  InsMatrix,
  InsMatrixListItem,
  InsMatrixDetails,
  InsDimension,
  InsDimensionOption,
} from "../../types/index.js";
import type { Kysely } from "kysely";

// ============================================================================
// Types
// ============================================================================

export interface SyncOptions {
  contexts?: boolean;
  matrices?: boolean;
  metadata?: boolean;
  matrixCodes?: string[];
  skipExisting?: boolean;
}

export interface SyncProgress {
  phase: string;
  current: number;
  total: number;
  currentItem?: string;
}

type ProgressCallback = (progress: SyncProgress) => void;

// ============================================================================
// Sync Orchestrator
// ============================================================================

export class SyncOrchestrator {
  private labelResolver: LabelResolver;
  private onProgress?: ProgressCallback;

  constructor(private db: Kysely<Database>) {
    this.labelResolver = new LabelResolver(db);
  }

  setProgressCallback(callback: ProgressCallback): void {
    this.onProgress = callback;
  }

  /**
   * Run the full sync process
   */
  async syncAll(options: SyncOptions = {}): Promise<{
    contexts: number;
    matrices: number;
    dimensions: number;
    nomItems: number;
    errors: string[];
  }> {
    const result = {
      contexts: 0,
      matrices: 0,
      dimensions: 0,
      nomItems: 0,
      errors: [] as string[],
    };

    try {
      // 1. Seed static data
      logger.info("Seeding NUTS hierarchy...");
      await this.labelResolver.getTerritoryService().seedNutsHierarchy();

      // 2. Sync contexts
      if (options.contexts !== false) {
        logger.info("Syncing contexts...");
        result.contexts = await this.syncContexts();
      }

      // 3. Sync matrices catalog
      if (options.matrices !== false) {
        logger.info("Syncing matrices catalog...");
        result.matrices = await this.syncMatricesCatalog();
      }

      // 4. Sync matrix metadata (dimensions and options)
      if (options.metadata !== false) {
        logger.info("Syncing matrix metadata...");
        const metaResult = await this.syncMatricesMetadata(
          options.matrixCodes,
          options.skipExisting
        );
        result.dimensions = metaResult.dimensions;
        result.nomItems = metaResult.nomItems;
        result.errors.push(...metaResult.errors);
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      logger.error({ error: message }, "Sync failed");
      result.errors.push(message);
    }

    return result;
  }

  /**
   * Sync contexts from INS API (bilingual)
   */
  async syncContexts(): Promise<number> {
    const { ro, en } = await insClient.fetchContextsBilingual();

    // Build lookup map for English names by code
    const enMap = new Map<string, InsContext>();
    for (const ctx of en) {
      enMap.set(ctx.context.code, ctx);
    }

    let synced = 0;
    const contextIdMap = new Map<string, number>();

    // Process all contexts with proper parent handling
    const allContexts = this.flattenContexts(ro);
    const total = allContexts.length;

    for (let i = 0; i < allContexts.length; i++) {
      const ctx = allContexts[i];
      if (!ctx) continue;

      const code = ctx.context.code;
      const ctxEn = enMap.get(code);

      this.onProgress?.({
        phase: "contexts",
        current: i + 1,
        total,
        currentItem: code,
      });

      const parentId = ctx.parentCode
        ? (contextIdMap.get(ctx.parentCode) ?? null)
        : null;

      const names: BilingualText = {
        ro: ctx.context.name,
        en: ctxEn?.context.name,
        normalized: this.normalize(ctx.context.name),
      };

      const path = this.computeContextPath(ctx, allContexts);

      const existing = await this.db
        .selectFrom("contexts")
        .select("id")
        .where("ins_code", "=", code)
        .executeTakeFirst();

      if (existing) {
        await this.db
          .updateTable("contexts")
          .set({
            names: jsonb(names),
            parent_id: parentId,
            path,
            updated_at: new Date(),
          })
          .where("id", "=", existing.id)
          .execute();
        contextIdMap.set(code, existing.id);
      } else {
        const newContext = {
          ins_code: code,
          names: jsonb(names),
          level: ctx.level ?? 0,
          parent_id: parentId,
          path,
          children_type:
            ctx.context.childrenUrl === "matrix"
              ? ("matrix" as const)
              : ("context" as const),
        };

        const result = await this.db
          .insertInto("contexts")
          .values(newContext)
          .returning("id")
          .executeTakeFirst();

        if (result) {
          contextIdMap.set(code, result.id);
        }
      }
      synced++;
    }

    logger.info({ synced }, "Contexts synced");
    return synced;
  }

  /**
   * Sync matrices catalog from INS API (bilingual)
   * Note: InsMatrixListItem only has name/code. Full metadata comes from fetchMatrixBilingual later.
   */
  async syncMatricesCatalog(): Promise<number> {
    const { ro, en } = await insClient.fetchMatricesListBilingual();

    // Build lookup map for English data
    const enMap = new Map<string, InsMatrixListItem>();
    for (const m of en) {
      enMap.set(m.code, m);
    }

    let synced = 0;
    const total = ro.length;

    for (let i = 0; i < ro.length; i++) {
      const mRo = ro[i];
      if (!mRo) continue;

      const mEn = enMap.get(mRo.code);

      this.onProgress?.({
        phase: "matrices-catalog",
        current: i + 1,
        total,
        currentItem: mRo.code,
      });

      // Build basic metadata (full metadata comes from fetchMatrixBilingual later)
      const metadata: MatrixMetadata = {
        names: {
          ro: mRo.name,
          en: mEn?.name,
        },
        periodicity: [],
        flags: {
          hasUatData: false,
          hasCountyData: false,
          hasSiruta: false,
          hasCaenRev1: false,
          hasCaenRev2: false,
        },
      };

      const existing = await this.db
        .selectFrom("matrices")
        .select(["id", "metadata"])
        .where("ins_code", "=", mRo.code)
        .executeTakeFirst();

      if (existing) {
        // CRITICAL FIX: Only update names, preserve all other metadata
        // Previously this was overwriting full metadata with a stub
        const existingMetadata = existing.metadata as MatrixMetadata | null;
        const updatedMetadata: MatrixMetadata = existingMetadata
          ? {
              ...existingMetadata,
              names: {
                ro: mRo.name,
                en: mEn?.name ?? existingMetadata.names.en,
              },
            }
          : metadata; // Use stub only if no existing metadata

        await this.db
          .updateTable("matrices")
          .set({
            metadata: jsonb(updatedMetadata),
            updated_at: new Date(),
          })
          .where("id", "=", existing.id)
          .execute();
      } else {
        const newMatrix = {
          ins_code: mRo.code,
          context_id: null,
          metadata: jsonb(metadata),
          dimensions: jsonb([] as DimensionSummary[]),
          sync_status: "PENDING" as const,
          last_sync_at: null,
          sync_error: null,
        };

        await this.db.insertInto("matrices").values(newMatrix).execute();
      }
      synced++;
    }

    logger.info({ synced }, "Matrices catalog synced");
    return synced;
  }

  /**
   * Sync matrix metadata (dimensions and options) for all or selected matrices
   */
  async syncMatricesMetadata(
    matrixCodes?: string[],
    skipExisting = false
  ): Promise<{ dimensions: number; nomItems: number; errors: string[] }> {
    const result = { dimensions: 0, nomItems: 0, errors: [] as string[] };

    // Get matrices to sync
    let query = this.db.selectFrom("matrices").select(["id", "ins_code"]);
    if (matrixCodes && matrixCodes.length > 0) {
      query = query.where("ins_code", "in", matrixCodes);
    }
    if (skipExisting) {
      query = query.where("sync_status", "=", "PENDING");
    }

    const matrices = await query.execute();
    const total = matrices.length;

    logger.info({ total }, "Syncing matrix metadata");

    for (let i = 0; i < matrices.length; i++) {
      const matrix = matrices[i];
      if (!matrix) continue;

      this.onProgress?.({
        phase: "matrix-metadata",
        current: i + 1,
        total,
        currentItem: matrix.ins_code,
      });

      try {
        const metaResult = await this.syncSingleMatrixMetadata(
          matrix.id,
          matrix.ins_code
        );
        result.dimensions += metaResult.dimensions;
        result.nomItems += metaResult.nomItems;

        // Update sync status
        await this.db
          .updateTable("matrices")
          .set({
            sync_status: "SYNCED",
            last_sync_at: new Date(),
            sync_error: null,
          })
          .where("id", "=", matrix.id)
          .execute();
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        result.errors.push(`${matrix.ins_code}: ${message}`);

        // Update sync status with error
        await this.db
          .updateTable("matrices")
          .set({ sync_status: "FAILED", sync_error: message })
          .where("id", "=", matrix.id)
          .execute();

        logger.warn(
          { matrixCode: matrix.ins_code, error: message },
          "Matrix metadata sync failed"
        );
      }
    }

    logger.info(result, "Matrix metadata sync completed");
    return result;
  }

  /**
   * Sync metadata for a single matrix
   * HIGH FIX: Wrapped in transaction to prevent partial state on failure
   */
  private async syncSingleMatrixMetadata(
    matrixId: number,
    matrixCode: string
  ): Promise<{ dimensions: number; nomItems: number }> {
    // Fetch bilingual metadata OUTSIDE transaction (network call)
    const { ro, en } = await insClient.fetchMatrixBilingual(matrixCode);

    // Process within a transaction to ensure atomicity
    return await this.db.transaction().execute(async (trx) => {
      // Build EN lookup maps by ID instead of relying on array index
      const { dimMap: enDimMap, optMap: enOptMap } = this.buildEnLookupMaps(
        en.dimensionsMap
      );

      // Update matrix with full metadata
      const metadata = this.buildFullMatrixMetadata(ro, en);
      const dimensionsSummary = this.buildDimensionsSummary(
        ro.dimensionsMap,
        enDimMap,
        ro.details
      );

      // MEDIUM FIX: Look up context_id from ancestors
      // The last ancestor is the immediate parent context of this matrix
      let contextId: number | null = null;
      if (ro.ancestors && ro.ancestors.length > 0) {
        const immediateParent = ro.ancestors[ro.ancestors.length - 1];
        if (immediateParent) {
          const context = await trx
            .selectFrom("contexts")
            .select("id")
            .where("ins_code", "=", immediateParent.code)
            .executeTakeFirst();
          contextId = context?.id ?? null;
        }
      }

      await trx
        .updateTable("matrices")
        .set({
          metadata: jsonb(metadata),
          dimensions: jsonb(dimensionsSummary),
          context_id: contextId,
        })
        .where("id", "=", matrixId)
        .execute();

      // Clear existing dimensions and nom items for this matrix
      await trx
        .deleteFrom("matrix_nom_items")
        .where("matrix_id", "=", matrixId)
        .execute();
      await trx
        .deleteFrom("matrix_dimensions")
        .where("matrix_id", "=", matrixId)
        .execute();

      let totalDimensions = 0;
      let totalNomItems = 0;

      // Process each dimension
      for (let dimIndex = 0; dimIndex < ro.dimensionsMap.length; dimIndex++) {
        const dimRo = ro.dimensionsMap[dimIndex];
        if (!dimRo) continue;

        // FIX: Match EN dimension by dimCode instead of array index
        const dimEn = enDimMap.get(dimRo.dimCode);

        // Determine dimension type using metadata hints and patterns
        const dimType = this.inferDimensionType(dimRo, dimIndex, ro.details);
        let classificationTypeId: number | null = null;

        if (dimType === "CLASSIFICATION") {
          const typeInfo = this.labelResolver
            .getClassificationService()
            .inferTypeFromDimensionLabel(dimRo.label);
          if (typeInfo) {
            classificationTypeId = await this.labelResolver
              .getClassificationService()
              .findOrCreateType(
                typeInfo.code,
                typeInfo.name,
                undefined,
                typeInfo.isHierarchical
              );
          } else {
            // Create a matrix-specific classification type
            const typeCode = `${matrixCode}_DIM${String(dimIndex)}`;
            classificationTypeId = await this.labelResolver
              .getClassificationService()
              .findOrCreateType(
                typeCode,
                dimRo.label,
                dimEn?.label,
                this.isHierarchicalDimension(dimRo.options)
              );
          }
        }

        // Insert dimension
        const dimLabels: BilingualText = {
          ro: dimRo.label,
          en: dimEn?.label,
        };

        const newDimension = {
          matrix_id: matrixId,
          dim_index: dimIndex,
          dimension_type: dimType,
          labels: jsonb(dimLabels),
          classification_type_id: classificationTypeId,
          is_hierarchical: this.isHierarchicalDimension(dimRo.options),
          option_count: dimRo.options.length,
        };

        await trx
          .insertInto("matrix_dimensions")
          .values(newDimension)
          .execute();
        totalDimensions++;

        // Process options (nom items)
        for (const optRo of dimRo.options) {
          // FIX: Match EN option by nomItemId instead of array index
          const optEn = enOptMap.get(
            `${String(dimRo.dimCode)}:${String(optRo.nomItemId)}`
          );

          const nomItemResult = await this.processNomItemInTrx(
            trx,
            matrixId,
            dimIndex,
            dimType,
            classificationTypeId,
            optRo,
            optEn
          );

          if (nomItemResult) {
            totalNomItems++;
          }
        }
      }

      return { dimensions: totalDimensions, nomItems: totalNomItems };
    }); // End transaction
  }

  /**
   * Process a single nom item within a transaction
   */
  private async processNomItemInTrx(
    trx: Kysely<Database>,
    matrixId: number,
    dimIndex: number,
    dimType: DimensionType,
    classificationTypeId: number | null,
    optRo: InsDimensionOption,
    optEn?: InsDimensionOption
  ): Promise<boolean> {
    let territoryId: number | null = null;
    let timePeriodId: number | null = null;
    let classificationValueId: number | null = null;
    let unitId: number | null = null;

    const labelRo = optRo.label;
    const labelEn = optEn?.label;

    // Resolve based on dimension type
    // Note: Label resolution uses main db, not trx (creates canonical entities outside matrix transaction)
    switch (dimType) {
      case "TERRITORIAL":
        territoryId = await this.labelResolver.resolveTerritory(
          labelRo,
          labelEn
        );
        break;
      case "TEMPORAL":
        timePeriodId = await this.labelResolver.resolveTimePeriod(
          labelRo,
          labelEn
        );
        break;
      case "UNIT_OF_MEASURE":
        unitId = await this.labelResolver.resolveUnit(labelRo, labelEn);
        break;
      case "CLASSIFICATION":
        if (classificationTypeId) {
          // Handle hierarchy
          let parentId: number | undefined;
          let level = 0;
          if (optRo.parentId !== undefined && optRo.parentId !== null) {
            // CRITICAL FIX: Include dim_index in lookup since nomItemId is unique per-dimension, not per-matrix
            // Use trx for lookups within the transaction
            const parentNomItem = await trx
              .selectFrom("matrix_nom_items")
              .select(["classification_value_id"])
              .where("matrix_id", "=", matrixId)
              .where("dim_index", "=", dimIndex)
              .where("nom_item_id", "=", optRo.parentId)
              .executeTakeFirst();
            parentId = parentNomItem?.classification_value_id ?? undefined;

            // Compute level from parent - if parent exists, get its level and add 1
            if (parentId) {
              const parentValue = await this.db
                .selectFrom("classification_values")
                .select("level")
                .where("id", "=", parentId)
                .executeTakeFirst();
              level = (parentValue?.level ?? 0) + 1;
            }
          }

          // CRITICAL FIX: Pass sortOrder correctly (offset is for ordering, level is computed from parent)
          classificationValueId =
            await this.labelResolver.resolveClassification(
              classificationTypeId,
              labelRo,
              labelEn,
              parentId,
              level,
              optRo.offset ?? 0 // sortOrder is the offset from API
            );
        }
        break;
    }

    // Insert nom item using transaction
    const labels: BilingualText = { ro: labelRo, en: labelEn };

    const newNomItem = {
      matrix_id: matrixId,
      dim_index: dimIndex,
      nom_item_id: optRo.nomItemId,
      dimension_type: dimType, // Denormalized for CHECK constraint validation
      territory_id: territoryId,
      time_period_id: timePeriodId,
      classification_value_id: classificationValueId,
      unit_id: unitId,
      labels: jsonb(labels),
      parent_nom_item_id: optRo.parentId ?? null,
      offset_order: optRo.offset ?? 0,
    };

    await trx.insertInto("matrix_nom_items").values(newNomItem).execute();
    return true;
  }

  // ============================================================================
  // Helper Methods
  // ============================================================================

  /**
   * Flatten the context hierarchy into a simple array
   * Note: InsContext structure is { parentCode, level, context: { code, name, childrenUrl } }
   * The API doesn't return children in the InsContext, so we just process the flat list
   */
  private flattenContexts(contexts: InsContext[]): InsContext[] {
    // The API returns a flat list already - contexts have parentCode="" for root
    return contexts;
  }

  private computeContextPath(
    ctx: InsContext,
    allContexts: InsContext[]
  ): string {
    const path: string[] = [ctx.context.code];
    let current = ctx;

    // parentCode is "" for root contexts, truthy check works
    while (current.parentCode) {
      path.unshift(current.parentCode);
      const parent = allContexts.find(
        (c) => c.context.code === current.parentCode
      );
      if (!parent) break;
      current = parent;
    }

    return path.join(".");
  }

  private buildFullMatrixMetadata(
    ro: InsMatrix,
    en: InsMatrix
  ): MatrixMetadata {
    // Parse periodicity from string array like ["Anuala", "Trimestriala"]
    const periodicity = this.parsePeriodicityFromStrings(ro.periodicitati);

    // Get year range from time dimension
    const yearRange = this.extractYearRange(ro.dimensionsMap, ro.details);

    return {
      names: { ro: ro.matrixName, en: en.matrixName },
      definitions: ro.definitie
        ? { ro: ro.definitie, en: en.definitie }
        : undefined,
      methodologies: ro.metodologie
        ? { ro: ro.metodologie, en: en.metodologie }
        : undefined,
      observations: ro.observatii
        ? { ro: ro.observatii, en: en.observatii }
        : undefined,
      seriesBreak: ro.intrerupere
        ? {
            ro: `${ro.intrerupere.lastPeriod} -> ${ro.intrerupere.nextMatrixCode}`,
          }
        : undefined,
      seriesContinuation: ro.continuareSerie
        ? {
            ro: ro.continuareSerie
              .map((c) => `${c.matCode}: ${c.lastPeriod}`)
              .join(", "),
          }
        : undefined,
      responsiblePersons: ro.persoaneResponsabile ?? undefined,
      lastUpdate: ro.ultimaActualizare ?? undefined,
      periodicity,
      yearRange,
      flags: {
        hasUatData: (ro.details?.nomLoc ?? 0) > 0,
        hasCountyData: (ro.details?.nomJud ?? 0) > 0,
        hasSiruta: (ro.details?.matSiruta ?? 0) > 0,
        hasCaenRev1: (ro.details?.matCaen1 ?? 0) > 0,
        hasCaenRev2: (ro.details?.matCaen2 ?? 0) > 0,
      },
      dataSources: ro.surseDeDate?.map((ds, i) => ({
        name: ds.nume,
        nameEn: en.surseDeDate?.[i]?.nume,
        sourceType: ds.tip ?? undefined,
        linkNumber: ds.linkNumber ?? undefined,
        sourceCode: ds.codTip ?? undefined,
      })),
      dimensionIndicators: {
        territorialDimIndex: this.findTerritorialDimIndex(
          ro.dimensionsMap,
          ro.details
        ),
        timeDimIndex: this.findTimeDimIndex(ro.dimensionsMap, ro.details),
      },
    };
  }

  private parsePeriodicityFromStrings(
    periodicitati?: string[]
  ): ("ANNUAL" | "QUARTERLY" | "MONTHLY")[] {
    if (!periodicitati) return [];
    const periods: ("ANNUAL" | "QUARTERLY" | "MONTHLY")[] = [];

    for (const p of periodicitati) {
      const lower = p.toLowerCase();
      if (lower.includes("anual")) periods.push("ANNUAL");
      if (lower.includes("trimestri")) periods.push("QUARTERLY");
      if (lower.includes("lunar")) periods.push("MONTHLY");
    }

    return periods;
  }

  private extractYearRange(
    dimensions: InsDimension[],
    details?: InsMatrixDetails
  ): [number, number] | undefined {
    // Find time dimension and extract year range from options
    const timeDimIdx = dimensions.findIndex(
      (d, i) => this.inferDimensionType(d, i, details) === "TEMPORAL"
    );
    const timeDim = timeDimIdx >= 0 ? dimensions[timeDimIdx] : undefined;
    if (!timeDim || timeDim.options.length === 0) return undefined;

    const years: number[] = [];
    for (const opt of timeDim.options) {
      const match = /\d{4}/.exec(opt.label);
      if (match) {
        years.push(parseInt(match[0], 10));
      }
    }

    if (years.length === 0) return undefined;
    return [Math.min(...years), Math.max(...years)];
  }

  private buildDimensionsSummary(
    dimsRo: InsDimension[],
    enDimMap: Map<number, InsDimension>,
    details?: InsMatrixDetails
  ): DimensionSummary[] {
    return dimsRo.map((dim, i) => ({
      index: i,
      labelRo: dim.label,
      // FIX: Match EN dimension by dimCode instead of array index
      labelEn: enDimMap.get(dim.dimCode)?.label,
      type: this.inferDimensionType(dim, i, details),
      optionCount: dim.options.length,
      isHierarchical: this.isHierarchicalDimension(dim.options),
    }));
  }

  private inferDimensionType(
    dim: InsDimension,
    dimIndex: number,
    details?: InsMatrixDetails
  ): DimensionType {
    const labelNormalized = this.normalize(dim.label);
    const labelLower = dim.label.toLowerCase();

    // 1. Use matrix metadata indicators (most reliable)
    if (details) {
      // Time dimension indicator (1-indexed in API)
      if (details.matTime > 0 && details.matTime === dimIndex + 1) {
        return "TEMPORAL";
      }
      // County/regional dimension indicator
      if (details.nomJud > 0 && details.nomJud === dimIndex + 1) {
        return "TERRITORIAL";
      }
      // Locality/UAT dimension indicator
      if (details.nomLoc > 0 && details.nomLoc === dimIndex + 1) {
        return "TERRITORIAL";
      }
      // Regional dimension indicator
      if (details.matRegJ > 0 && details.matRegJ === dimIndex + 1) {
        return "TERRITORIAL";
      }
    }

    // 2. Check dimension label for unit of measure
    if (
      labelLower.startsWith("um:") ||
      labelLower.includes("unitate de masura")
    ) {
      return "UNIT_OF_MEASURE";
    }

    // 3. Check dimension label for known classification patterns (before checking options!)
    const knownClassificationLabels = [
      "SEXE",
      "SEX",
      "MEDII DE REZIDENTA",
      "MEDII DE REZIDENȚĂ",
      "GRUPE DE VARSTA",
      "GRUPE DE VÂRSTA",
      "GRUPE DE VÂRSTE",
      "NIVEL DE EDUCATIE",
      "NIVEL DE INSTRUIRE",
      "STARE CIVILA",
      "STAREA CIVILA",
      "FORME DE PROPRIETATE",
      "CLASE DE MARIME",
      "CETATENIE",
      "CETĂȚENIE",
      "ETNIE",
      "NATIONALITATE",
      "RELIGIE",
      "CONFESIUNE",
      "ACTIVITATI",
      "ACTIVITĂȚI",
      "CAEN",
      "PRODUSE",
      "TIPURI",
    ];
    for (const pattern of knownClassificationLabels) {
      if (labelNormalized.includes(pattern)) {
        return "CLASSIFICATION";
      }
    }

    // 4. Check options for patterns (use multiple options, not just first)
    const options = dim.options.slice(0, 5); // Check first 5 options
    let territorialScore = 0;
    let temporalScore = 0;

    for (const opt of options) {
      const optLabel = opt.label;
      const optLabelLower = optLabel.toLowerCase();

      // Territorial patterns
      if (/^\d{4,6}\s+/.test(optLabel)) {
        // SIRUTA code prefix
        territorialScore += 3;
      }
      if (
        optLabelLower.includes("macroregiune") ||
        optLabelLower.includes("regiune")
      ) {
        territorialScore += 2;
      }
      if (
        optLabelLower.includes("bucuresti") ||
        optLabelLower.includes("bucurești")
      ) {
        territorialScore += 2;
      }
      if (optLabelLower.includes("judet") || optLabelLower.includes("județ")) {
        territorialScore += 2;
      }

      // Temporal patterns
      if (optLabelLower.includes("anul ") || optLabelLower.startsWith("anul")) {
        temporalScore += 3;
      }
      if (optLabelLower.includes("trimestrul ")) {
        temporalScore += 3;
      }
      if (optLabelLower.includes("luna ")) {
        temporalScore += 3;
      }
      if (/^\d{4}$/.test(optLabel)) {
        // Just a year like "2023"
        temporalScore += 2;
      }
    }

    // First option being "Total" is NOT enough to classify as territorial
    // Many classification dimensions also have "Total" as first option
    const firstOption = dim.options[0];
    if (
      firstOption?.label.toLowerCase() === "total" &&
      territorialScore === 0
    ) {
      // Only add score if we have other territorial indicators
      // Don't classify as territorial just because first option is "Total"
    }

    if (temporalScore > territorialScore && temporalScore >= 2) {
      return "TEMPORAL";
    }
    if (territorialScore > temporalScore && territorialScore >= 2) {
      return "TERRITORIAL";
    }

    // Default to classification
    return "CLASSIFICATION";
  }

  private isHierarchicalDimension(options: InsDimensionOption[]): boolean {
    return options.some(
      (opt) => opt.parentId !== undefined && opt.parentId !== null
    );
  }

  private findTerritorialDimIndex(
    dims: InsDimension[],
    details?: InsMatrixDetails
  ): number | undefined {
    const idx = dims.findIndex(
      (d, i) => this.inferDimensionType(d, i, details) === "TERRITORIAL"
    );
    return idx >= 0 ? idx : undefined;
  }

  private findTimeDimIndex(
    dims: InsDimension[],
    details?: InsMatrixDetails
  ): number | undefined {
    const idx = dims.findIndex(
      (d, i) => this.inferDimensionType(d, i, details) === "TEMPORAL"
    );
    return idx >= 0 ? idx : undefined;
  }

  private normalize(text: string): string {
    return text
      .toUpperCase()
      .normalize("NFD")
      .replaceAll(/[\u0300-\u036F]/g, "")
      .replaceAll(/\s+/g, " ")
      .trim();
  }

  /**
   * Build lookup maps for EN dimensions and options by ID (dimCode/nomItemId)
   * instead of relying on array index which may differ between RO and EN responses
   */
  private buildEnLookupMaps(dimsEn: InsDimension[]): {
    dimMap: Map<number, InsDimension>;
    optMap: Map<string, InsDimensionOption>; // key: "dimCode:nomItemId"
  } {
    const dimMap = new Map<number, InsDimension>();
    const optMap = new Map<string, InsDimensionOption>();

    for (const dim of dimsEn) {
      dimMap.set(dim.dimCode, dim);
      for (const opt of dim.options) {
        optMap.set(`${String(dim.dimCode)}:${String(opt.nomItemId)}`, opt);
      }
    }

    return { dimMap, optMap };
  }

  clearCache(): void {
    this.labelResolver.clearCache();
  }
}
