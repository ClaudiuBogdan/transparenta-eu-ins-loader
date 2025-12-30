import { createHash } from "node:crypto";

import { jsonb, textArray } from "../../../db/connection.js";
import { logger } from "../../../logger.js";

import type { Database, BilingualText } from "../../../db/types.js";
import type { Kysely } from "kysely";

// ============================================================================
// Classification Service
// ============================================================================

export class ClassificationService {
  private typeCache = new Map<string, number>();
  private valueCache = new Map<string, number>();

  constructor(private db: Kysely<Database>) {}

  /**
   * Find or create a classification type
   */
  async findOrCreateType(
    code: string,
    nameRo: string,
    nameEn?: string,
    isHierarchical = false
  ): Promise<number> {
    const cacheKey = `type:${code}`;
    if (this.typeCache.has(cacheKey)) {
      return this.typeCache.get(cacheKey)!;
    }

    const existing = await this.db
      .selectFrom("classification_types")
      .select("id")
      .where("code", "=", code)
      .executeTakeFirst();

    if (existing) {
      this.typeCache.set(cacheKey, existing.id);
      return existing.id;
    }

    const names: BilingualText = {
      ro: nameRo,
      en: nameEn,
      normalized: this.normalize(nameRo),
    };

    const newType = {
      code,
      names: jsonb(names),
      is_hierarchical: isHierarchical,
      label_patterns: textArray([nameRo]),
    };

    const result = await this.db
      .insertInto("classification_types")
      .values(newType)
      .returning("id")
      .executeTakeFirst();

    const id = result!.id;
    this.typeCache.set(cacheKey, id);
    logger.debug({ code, nameRo, id }, "Created classification type");
    return id;
  }

  /**
   * Find or create a classification value using content-based deduplication
   */
  async findOrCreateValue(
    typeId: number,
    labelRo: string,
    labelEn?: string,
    parentId?: number,
    level = 0,
    sortOrder = 0
  ): Promise<number> {
    const normalized = this.normalize(labelRo);
    const contentHash = this.computeContentHash(normalized);
    const cacheKey = `value:${String(typeId)}:${contentHash}`;

    if (this.valueCache.has(cacheKey)) {
      return this.valueCache.get(cacheKey)!;
    }

    // Single lookup by content hash - no collision handling needed!
    const existing = await this.db
      .selectFrom("classification_values")
      .select("id")
      .where("type_id", "=", typeId)
      .where("content_hash", "=", contentHash)
      .executeTakeFirst();

    if (existing) {
      this.valueCache.set(cacheKey, existing.id);
      return existing.id;
    }

    // Generate a code from the label
    const code = this.generateCode(labelRo);

    const names: BilingualText = {
      ro: labelRo,
      en: labelEn,
      normalized,
    };

    // Compute path if hierarchical
    let path: string | null = null;
    if (parentId) {
      const parent = await this.db
        .selectFrom("classification_values")
        .select(["path", "code"])
        .where("id", "=", parentId)
        .executeTakeFirst();
      if (parent?.path) {
        path = `${parent.path}.${code}`;
      } else {
        path = `${parent?.code ?? "ROOT"}.${code}`;
      }
    } else {
      path = code;
    }

    const newValue = {
      type_id: typeId,
      code,
      content_hash: contentHash,
      path,
      parent_id: parentId ?? null,
      level,
      names: jsonb(names),
      sort_order: sortOrder,
    };

    const result = await this.db
      .insertInto("classification_values")
      .values(newValue)
      .returning("id")
      .executeTakeFirst();

    const id = result!.id;
    this.valueCache.set(cacheKey, id);
    logger.debug(
      { typeId, labelRo, contentHash, id },
      "Created classification value"
    );
    return id;
  }

  /**
   * Infer classification type from a dimension label
   * HIGH FIX: Uses normalize() to strip diacritics for consistent matching
   */
  inferTypeFromDimensionLabel(
    label: string
  ): { code: string; name: string; isHierarchical: boolean } | null {
    // Use normalize() to strip diacritics - "Medii de rezidență" becomes "MEDII DE REZIDENTA"
    const normalized = this.normalize(label);

    // Known classification types (patterns are already normalized/uppercase)
    if (normalized.includes("SEXE") || normalized.includes("SEX")) {
      return { code: "SEX", name: "Sexe", isHierarchical: false };
    }
    if (
      normalized.includes("MEDII DE REZIDENTA") ||
      normalized.includes("URBAN") ||
      normalized.includes("RURAL")
    ) {
      return {
        code: "RESIDENCE",
        name: "Medii de rezidență",
        isHierarchical: false,
      };
    }
    if (
      normalized.includes("GRUPE DE VARSTA") ||
      normalized.includes("VARSTE")
    ) {
      return {
        code: "AGE_GROUP",
        name: "Grupe de vârstă",
        isHierarchical: true,
      };
    }
    if (
      normalized.includes("CAEN") ||
      normalized.includes("ACTIVITATI ECONOMICE")
    ) {
      return {
        code: "ECONOMIC_ACTIVITY",
        name: "Activități economice",
        isHierarchical: true,
      };
    }
    if (
      normalized.includes("NIVEL DE EDUCATIE") ||
      normalized.includes("NIVEL DE INSTRUIRE")
    ) {
      return {
        code: "EDUCATION_LEVEL",
        name: "Niveluri de educație",
        isHierarchical: true,
      };
    }
    if (
      normalized.includes("STARE CIVILA") ||
      normalized.includes("STAREA CIVILA")
    ) {
      return {
        code: "MARITAL_STATUS",
        name: "Stare civilă",
        isHierarchical: false,
      };
    }
    if (normalized.includes("CETATENIE")) {
      return { code: "CITIZENSHIP", name: "Cetățenie", isHierarchical: false };
    }
    if (normalized.includes("ETNIE") || normalized.includes("NATIONALITATE")) {
      return { code: "ETHNICITY", name: "Etnie", isHierarchical: false };
    }
    if (normalized.includes("RELIGIE") || normalized.includes("CONFESIUNE")) {
      return { code: "RELIGION", name: "Religie", isHierarchical: false };
    }
    if (normalized.includes("FORME DE PROPRIETATE")) {
      return {
        code: "OWNERSHIP",
        name: "Forme de proprietate",
        isHierarchical: false,
      };
    }
    if (normalized.includes("CLASE DE MARIME")) {
      return {
        code: "SIZE_CLASS",
        name: "Clase de mărime",
        isHierarchical: true,
      };
    }

    return null;
  }

  /**
   * Generate a unique code for a classification type from dimension label
   */
  generateTypeCode(dimensionLabel: string): string {
    return this.normalize(dimensionLabel)
      .replaceAll(/\s+/g, "_")
      .replaceAll(/[^\dA-Z_]/g, "")
      .slice(0, 50);
  }

  /**
   * Compute content hash for deduplication
   */
  private computeContentHash(normalizedContent: string): string {
    return createHash("sha256").update(normalizedContent).digest("hex");
  }

  /**
   * Generate a code from a label
   */
  private generateCode(label: string): string {
    return this.normalize(label)
      .replaceAll(/\s+/g, "_")
      .replaceAll(/[^\dA-Z_]/g, "")
      .slice(0, 50);
  }

  /**
   * Normalize text
   */
  private normalize(text: string): string {
    return text
      .toUpperCase()
      .normalize("NFD")
      .replaceAll(/[\u0300-\u036F]/g, "")
      .replaceAll(/\s+/g, " ")
      .trim();
  }

  clearCache(): void {
    this.typeCache.clear();
    this.valueCache.clear();
  }
}
