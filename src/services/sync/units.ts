import { logger } from "../../logger.js";

import type { Database, NewUnitOfMeasure } from "../../db/types.js";
import type { Kysely } from "kysely";

// ============================================================================
// Unit Pattern
// ============================================================================

const UNIT_PATTERN = /^UM:\s*(.+)$/i;

// Known unit mappings
const UNIT_MAPPINGS: Record<string, { code: string; symbol: string }> = {
  "numar persoane": { code: "PERSONS", symbol: "pers." },
  numar: { code: "NUMBER", symbol: "nr." },
  "mii persoane": { code: "THOUSAND_PERSONS", symbol: "mii pers." },
  procente: { code: "PERCENT", symbol: "%" },
  ha: { code: "HECTARES", symbol: "ha" },
  "m.p. arie desfasurata": { code: "SQM_AREA", symbol: "m²" },
  "mii lei": { code: "THOUSAND_LEI", symbol: "mii lei" },
  "milioane lei": { code: "MILLION_LEI", symbol: "mil. lei" },
  lei: { code: "LEI", symbol: "lei" },
  kg: { code: "KG", symbol: "kg" },
  tone: { code: "TONS", symbol: "t" },
  "mii tone": { code: "THOUSAND_TONS", symbol: "mii t" },
  litri: { code: "LITERS", symbol: "l" },
  "mii litri": { code: "THOUSAND_LITERS", symbol: "mii l" },
  "mii locuri": { code: "THOUSAND_PLACES", symbol: "mii loc." },
  "zile-turist": { code: "TOURIST_DAYS", symbol: "zile-turist" },
  "numar sosiri": { code: "ARRIVALS", symbol: "sosiri" },
  "numar innoptari": { code: "OVERNIGHT_STAYS", symbol: "înnoptări" },
};

// ============================================================================
// Unit of Measure Service
// ============================================================================

export class UnitOfMeasureService {
  constructor(private db: Kysely<Database>) {}

  /**
   * Find or create a unit of measure from a label
   * Returns the unit_of_measure ID or null if not a unit label
   */
  async findOrCreate(label: string): Promise<number | null> {
    const match = UNIT_PATTERN.exec(label);
    if (match?.[1] === undefined) {
      return null;
    }

    const unitName = match[1].trim();
    const normalizedName = this.normalize(unitName);

    // Try to find known mapping
    const mapping = UNIT_MAPPINGS[normalizedName.toLowerCase()];
    const code = mapping?.code ?? this.generateCode(unitName);
    const symbol = mapping?.symbol ?? null;

    // Check if exists
    const existing = await this.db
      .selectFrom("units_of_measure")
      .select(["id", "ins_labels"])
      .where("code", "=", code)
      .executeTakeFirst();

    if (existing !== undefined) {
      // Add label to ins_labels if not already present
      const labels = existing.ins_labels;
      if (!labels.includes(label)) {
        await this.db
          .updateTable("units_of_measure")
          .set({
            ins_labels: [...labels, label],
          })
          .where("id", "=", existing.id)
          .execute();
      }
      return existing.id;
    }

    // Create new
    const newUnit: NewUnitOfMeasure = {
      code,
      name: unitName,
      symbol,
      ins_labels: [label],
    };

    const result = await this.db
      .insertInto("units_of_measure")
      .values(newUnit)
      .returning("id")
      .executeTakeFirst();

    logger.debug({ code, unitName, label }, "Created unit of measure");
    return result?.id ?? null;
  }

  /**
   * Check if a label is a unit of measure
   */
  isUnitLabel(label: string): boolean {
    return UNIT_PATTERN.test(label.trim());
  }

  /**
   * Generate a code from a unit name
   */
  private generateCode(name: string): string {
    return this.normalize(name)
      .replaceAll(/\s+/g, "_")
      .replaceAll(/[^\dA-Z_]/g, "")
      .slice(0, 50);
  }

  /**
   * Normalize a string (uppercase, no diacritics)
   */
  private normalize(text: string): string {
    return text
      .toUpperCase()
      .normalize("NFD")
      .replaceAll(/[\u0300-\u036F]/g, "") // Remove diacritics
      .trim();
  }
}
