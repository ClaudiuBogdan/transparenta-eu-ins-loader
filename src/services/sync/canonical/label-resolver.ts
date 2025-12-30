import { ClassificationService } from "./classifications.js";
import { TerritoryService } from "./territories.js";
import { TimePeriodService } from "./time-periods.js";
import { UnitService } from "./units.js";
import { logger } from "../../../logger.js";

import type { Database, NewLabelMapping } from "../../../db/types.js";
import type { Kysely } from "kysely";

// ============================================================================
// Types
// ============================================================================

export type ContextType =
  | "TERRITORY"
  | "TIME_PERIOD"
  | "CLASSIFICATION"
  | "UNIT";

export interface ResolvedEntity {
  territoryId?: number;
  timePeriodId?: number;
  classificationValueId?: number;
  unitId?: number;
  method: "EXACT" | "PATTERN" | "FUZZY" | "MANUAL" | "SIRUTA" | null;
}

// ============================================================================
// Label Resolver
// ============================================================================

export class LabelResolver {
  private cache = new Map<string, ResolvedEntity | null>();
  private territoryService: TerritoryService;
  private timePeriodService: TimePeriodService;
  private classificationService: ClassificationService;
  private unitService: UnitService;

  constructor(private db: Kysely<Database>) {
    this.territoryService = new TerritoryService(db);
    this.timePeriodService = new TimePeriodService(db);
    this.classificationService = new ClassificationService(db);
    this.unitService = new UnitService(db);
  }

  /**
   * Resolve a territory label
   */
  async resolveTerritory(
    labelRo: string,
    labelEn?: string,
    hint?: string
  ): Promise<number | null> {
    const normalized = this.normalize(labelRo);
    const contextHint = hint ?? "";
    const cacheKey = `territory:${normalized}:${contextHint}`;

    if (this.cache.has(cacheKey)) {
      return this.cache.get(cacheKey)?.territoryId ?? null;
    }

    // Check existing mapping (include context_hint for disambiguation)
    const mapping = await this.db
      .selectFrom("label_mappings")
      .select("territory_id")
      .where("label_normalized", "=", normalized)
      .where("context_type", "=", "TERRITORY")
      .where("context_hint", "=", contextHint)
      .executeTakeFirst();

    if (mapping?.territory_id) {
      this.cache.set(cacheKey, {
        territoryId: mapping.territory_id,
        method: "EXACT",
      });
      return mapping.territory_id;
    }

    // Try to resolve
    const territoryId = await this.territoryService.findOrCreateFromLabel(
      labelRo,
      labelEn
    );

    // Save mapping
    await this.saveMapping({
      labelNormalized: normalized,
      labelOriginal: labelRo,
      contextType: "TERRITORY",
      contextHint: hint,
      territoryId,
      method: territoryId ? "PATTERN" : null,
    });

    this.cache.set(
      cacheKey,
      territoryId ? { territoryId, method: "PATTERN" } : null
    );
    return territoryId;
  }

  /**
   * Resolve a time period label
   */
  async resolveTimePeriod(
    labelRo: string,
    labelEn?: string
  ): Promise<number | null> {
    const normalized = this.normalize(labelRo);
    const cacheKey = `time:${normalized}`;

    if (this.cache.has(cacheKey)) {
      return this.cache.get(cacheKey)?.timePeriodId ?? null;
    }

    // Check existing mapping
    const mapping = await this.db
      .selectFrom("label_mappings")
      .select("time_period_id")
      .where("label_normalized", "=", normalized)
      .where("context_type", "=", "TIME_PERIOD")
      .executeTakeFirst();

    if (mapping?.time_period_id) {
      this.cache.set(cacheKey, {
        timePeriodId: mapping.time_period_id,
        method: "EXACT",
      });
      return mapping.time_period_id;
    }

    // Try to resolve
    const timePeriodId = await this.timePeriodService.findOrCreate(
      labelRo,
      labelEn
    );

    // Save mapping
    await this.saveMapping({
      labelNormalized: normalized,
      labelOriginal: labelRo,
      contextType: "TIME_PERIOD",
      timePeriodId,
      method: timePeriodId ? "PATTERN" : null,
    });

    this.cache.set(
      cacheKey,
      timePeriodId ? { timePeriodId, method: "PATTERN" } : null
    );
    return timePeriodId;
  }

  /**
   * Resolve a classification value label
   * FIX: Now checks existing mapping and saves mapping for auditability (like other resolve methods)
   */
  async resolveClassification(
    typeId: number,
    labelRo: string,
    labelEn?: string,
    parentId?: number,
    level = 0,
    sortOrder = 0
  ): Promise<number | null> {
    const normalized = this.normalize(labelRo);
    // Use typeId as context_hint to disambiguate between different classification types
    const contextHint = String(typeId);
    const cacheKey = `classification:${contextHint}:${normalized}`;

    if (this.cache.has(cacheKey)) {
      return this.cache.get(cacheKey)?.classificationValueId ?? null;
    }

    // Check existing mapping first (for consistency with other resolve methods)
    const mapping = await this.db
      .selectFrom("label_mappings")
      .select("classification_value_id")
      .where("label_normalized", "=", normalized)
      .where("context_type", "=", "CLASSIFICATION")
      .where("context_hint", "=", contextHint)
      .executeTakeFirst();

    if (mapping?.classification_value_id) {
      this.cache.set(cacheKey, {
        classificationValueId: mapping.classification_value_id,
        method: "EXACT",
      });
      return mapping.classification_value_id;
    }

    // Create the classification value
    const classificationValueId =
      await this.classificationService.findOrCreateValue(
        typeId,
        labelRo,
        labelEn,
        parentId,
        level,
        sortOrder
      );

    // Save mapping for auditability
    await this.saveMapping({
      labelNormalized: normalized,
      labelOriginal: labelRo,
      contextType: "CLASSIFICATION",
      contextHint,
      classificationValueId,
      method: classificationValueId ? "EXACT" : null,
    });

    this.cache.set(
      cacheKey,
      classificationValueId ? { classificationValueId, method: "EXACT" } : null
    );
    return classificationValueId;
  }

  /**
   * Resolve a unit of measure label
   */
  async resolveUnit(labelRo: string, labelEn?: string): Promise<number | null> {
    const normalized = this.normalize(labelRo);
    const cacheKey = `unit:${normalized}`;

    if (this.cache.has(cacheKey)) {
      return this.cache.get(cacheKey)?.unitId ?? null;
    }

    // Check existing mapping
    const mapping = await this.db
      .selectFrom("label_mappings")
      .select("unit_id")
      .where("label_normalized", "=", normalized)
      .where("context_type", "=", "UNIT")
      .executeTakeFirst();

    if (mapping?.unit_id) {
      this.cache.set(cacheKey, { unitId: mapping.unit_id, method: "EXACT" });
      return mapping.unit_id;
    }

    // Try to resolve
    const unitId = await this.unitService.findOrCreate(labelRo, labelEn);

    // Save mapping
    await this.saveMapping({
      labelNormalized: normalized,
      labelOriginal: labelRo,
      contextType: "UNIT",
      unitId,
      method: unitId ? "PATTERN" : null,
    });

    this.cache.set(cacheKey, unitId ? { unitId, method: "PATTERN" } : null);
    return unitId;
  }

  /**
   * Get the classification service for direct access
   */
  getClassificationService(): ClassificationService {
    return this.classificationService;
  }

  /**
   * Get the territory service for direct access
   */
  getTerritoryService(): TerritoryService {
    return this.territoryService;
  }

  /**
   * Get the time period service for direct access
   */
  getTimePeriodService(): TimePeriodService {
    return this.timePeriodService;
  }

  /**
   * Get the unit service for direct access
   */
  getUnitService(): UnitService {
    return this.unitService;
  }

  /**
   * Save a label mapping
   */
  private async saveMapping(params: {
    labelNormalized: string;
    labelOriginal: string;
    contextType: ContextType;
    contextHint?: string;
    territoryId?: number | null;
    timePeriodId?: number | null;
    classificationValueId?: number | null;
    unitId?: number | null;
    method: "EXACT" | "PATTERN" | "FUZZY" | "MANUAL" | "SIRUTA" | null;
  }): Promise<void> {
    const isResolved =
      params.territoryId != null ||
      params.timePeriodId != null ||
      params.classificationValueId != null ||
      params.unitId != null;

    const newMapping: NewLabelMapping = {
      label_normalized: params.labelNormalized,
      label_original: params.labelOriginal,
      context_type: params.contextType,
      context_hint: params.contextHint ?? "", // Use empty string, not null
      territory_id: params.territoryId ?? null,
      time_period_id: params.timePeriodId ?? null,
      classification_value_id: params.classificationValueId ?? null,
      unit_id: params.unitId ?? null,
      resolution_method: params.method,
      confidence: isResolved ? 1.0 : null,
      is_unresolvable: !isResolved,
      unresolvable_reason: isResolved ? null : "No matching pattern found",
      resolved_at: isResolved ? new Date() : null,
    };

    try {
      await this.db
        .insertInto("label_mappings")
        .values(newMapping)
        .onConflict((oc) =>
          oc
            .columns(["label_normalized", "context_type", "context_hint"])
            .doNothing()
        )
        .execute();
    } catch (error) {
      // Ignore duplicate key errors
      logger.debug({ params, error }, "Label mapping already exists");
    }
  }

  /**
   * Normalize text for consistent matching
   */
  private normalize(text: string): string {
    return text
      .toUpperCase()
      .normalize("NFD")
      .replaceAll(/[\u0300-\u036F]/g, "")
      .replaceAll(/\s+/g, " ")
      .trim();
  }

  /**
   * Clear all caches
   */
  clearCache(): void {
    this.cache.clear();
    this.territoryService.clearCache();
    this.timePeriodService.clearCache();
    this.classificationService.clearCache();
    this.unitService.clearCache();
  }
}
