import { logger } from "../../logger.js";

import type {
  Database,
  NewClassificationType,
  NewClassificationValue,
} from "../../db/types.js";
import type { Kysely } from "kysely";

// ============================================================================
// Classification Type Patterns
// ============================================================================

interface ClassificationPattern {
  code: string;
  name: string;
  patterns: RegExp[];
  isHierarchical: boolean;
}

const CLASSIFICATION_PATTERNS: ClassificationPattern[] = [
  {
    code: "SEX",
    name: "Sexe",
    patterns: [/^Sexe$/i],
    isHierarchical: false,
  },
  {
    code: "RESIDENCE",
    name: "Medii de rezidenta",
    patterns: [/medii\s+de\s+rezidenta/i],
    isHierarchical: false,
  },
  {
    code: "AGE_GROUP",
    name: "Grupe de varsta",
    patterns: [/varste?\s+(si|și)\s+grupe/i, /grupe?\s+de\s+varsta/i],
    isHierarchical: true,
  },
  {
    code: "CAEN_REV2",
    name: "CAEN Rev.2",
    patterns: [/caen\s+rev\.?2/i],
    isHierarchical: true,
  },
  {
    code: "CAEN_REV1",
    name: "CAEN Rev.1",
    patterns: [/caen\s+rev\.?1/i],
    isHierarchical: true,
  },
  {
    code: "OWNERSHIP",
    name: "Forme de proprietate",
    patterns: [/forme\s+de\s+proprietate/i],
    isHierarchical: true,
  },
  {
    code: "EDUCATION_LEVEL",
    name: "Niveluri de educatie",
    patterns: [/nivel\s+(de\s+)?educatie/i, /nivel\s+de\s+instruire/i],
    isHierarchical: true,
  },
  {
    code: "MARITAL_STATUS",
    name: "Stare civila",
    patterns: [/stare?\s+civila/i, /starea\s+civila/i],
    isHierarchical: false,
  },
  {
    code: "NATIONALITY",
    name: "Nationalitati",
    patterns: [/nationalitati/i, /etnie/i],
    isHierarchical: false,
  },
  {
    code: "ACCIDENT_CATEGORY",
    name: "Categorii de accidente",
    patterns: [/categorii\s+de\s+accidente/i],
    isHierarchical: true,
  },
  {
    code: "LAND_USE",
    name: "Modul de folosinta",
    patterns: [/modul\s+de\s+folosinta/i, /folosinta\s+fondului/i],
    isHierarchical: true,
  },
  {
    code: "TOURISM_STRUCTURE",
    name: "Tipuri de structuri turistice",
    patterns: [
      /structuri\s+de\s+primire\s+turistica/i,
      /tipuri?\s+de\s+structuri/i,
    ],
    isHierarchical: true,
  },
];

// ============================================================================
// Classification Service
// ============================================================================

export class ClassificationService {
  constructor(private db: Kysely<Database>) {}

  /**
   * Find or create a classification type from a dimension label
   * Returns the classification_type ID or null if not a classification
   */
  async findOrCreateType(dimensionLabel: string): Promise<number | null> {
    const trimmed = dimensionLabel.trim();

    // Try to match known pattern
    for (const pattern of CLASSIFICATION_PATTERNS) {
      if (pattern.patterns.some((p) => p.test(trimmed))) {
        return this.ensureTypeExists(
          pattern.code,
          pattern.name,
          trimmed,
          pattern.isHierarchical
        );
      }
    }

    // Unknown classification - create generic type from label
    const code = this.generateCode(trimmed);
    return this.ensureTypeExists(code, trimmed, trimmed, false);
  }

  /**
   * Ensure a classification type exists and return its ID
   */
  private async ensureTypeExists(
    code: string,
    name: string,
    insLabel: string,
    isHierarchical: boolean
  ): Promise<number> {
    // Check if exists
    const existing = await this.db
      .selectFrom("classification_types")
      .select(["id", "ins_labels"])
      .where("code", "=", code)
      .executeTakeFirst();

    if (existing) {
      // Add insLabel to ins_labels if not already present
      const labels = existing.ins_labels ?? [];
      if (!labels.includes(insLabel)) {
        await this.db
          .updateTable("classification_types")
          .set({
            ins_labels: [...labels, insLabel],
          })
          .where("id", "=", existing.id)
          .execute();
      }
      return existing.id;
    }

    // Create new
    const newType: NewClassificationType = {
      code,
      name,
      ins_labels: [insLabel],
      is_hierarchical: isHierarchical,
    };

    const result = await this.db
      .insertInto("classification_types")
      .values(newType)
      .returning("id")
      .executeTakeFirst();

    logger.debug({ code, name, insLabel }, "Created classification type");
    return result!.id;
  }

  /**
   * Find or create a classification value
   * Returns the classification_value ID
   */
  async findOrCreateValue(
    typeId: number,
    label: string,
    parentId: number | null,
    sortOrder: number
  ): Promise<number> {
    const code = this.generateValueCode(label);
    const normalized = this.normalize(label);

    // Check if exists
    const existing = await this.db
      .selectFrom("classification_values")
      .select("id")
      .where("classification_type_id", "=", typeId)
      .where("code", "=", code)
      .executeTakeFirst();

    if (existing) {
      return existing.id;
    }

    // Create new
    const newValue: NewClassificationValue = {
      classification_type_id: typeId,
      code,
      name: label.trim(),
      name_normalized: normalized,
      parent_id: parentId,
      level: parentId ? 1 : 0, // Simplified; path trigger will recompute
      sort_order: sortOrder,
    };

    const result = await this.db
      .insertInto("classification_values")
      .values(newValue)
      .returning("id")
      .executeTakeFirst();

    logger.debug({ typeId, code, label }, "Created classification value");
    return result!.id;
  }

  /**
   * Generate a code from a dimension label
   */
  private generateCode(label: string): string {
    // Remove diacritics and special chars, uppercase, underscores
    return this.normalize(label)
      .replaceAll(/\s+/g, "_")
      .replaceAll(/[^\dA-Z_]/g, "")
      .slice(0, 50);
  }

  /**
   * Generate a value code from a label
   */
  private generateValueCode(label: string): string {
    const trimmed = label.trim();

    // Common mappings
    if (/^Total$/i.test(trimmed)) return "TOTAL";
    if (/^Masculin$/i.test(trimmed)) return "M";
    if (/^Feminin$/i.test(trimmed)) return "F";
    if (/^Urban$/i.test(trimmed)) return "URBAN";
    if (/^Rural$/i.test(trimmed)) return "RURAL";

    // Otherwise, generate from label
    return this.normalize(trimmed)
      .replaceAll(/\s+/g, "_")
      .replaceAll(/[^\dA-Z_]/g, "")
      .slice(0, 50);
  }

  /**
   * Normalize a string (uppercase, no diacritics)
   */
  normalize(text: string): string {
    return text
      .toUpperCase()
      .normalize("NFD")
      .replaceAll(/[\u0300-\u036F]/g, "") // Remove diacritics
      .trim();
  }

  /**
   * Check if a dimension label is likely a classification
   */
  isClassificationLabel(label: string): boolean {
    const lower = label.toLowerCase().trim();

    // Exclude known non-classification types
    if (/^ani$/i.test(lower)) return false;
    if (/perioade/i.test(lower)) return false;
    if (/trimestre/i.test(lower)) return false;
    if (/^luni$/i.test(lower)) return false;
    if (/judete/i.test(lower)) return false;
    if (/regiuni/i.test(lower)) return false;
    if (/localitati/i.test(lower)) return false;
    if (/macroregiuni/i.test(lower)) return false;
    if (/^um:/i.test(lower)) return false;

    return true;
  }
}
