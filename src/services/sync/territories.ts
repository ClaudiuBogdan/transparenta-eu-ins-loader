import { logger } from "../../logger.js";

import type {
  Database,
  TerritorialLevel,
  NewTerritory,
} from "../../db/types.js";
import type { Kysely } from "kysely";

// ============================================================================
// SIRUTA Pattern
// ============================================================================

const SIRUTA_PATTERN = /^(\d{5,6})\s+(.+)$/;

// ============================================================================
// Static NUTS Hierarchy Data
// ============================================================================

interface MacroregionData {
  code: string;
  name: string;
}

interface RegionData {
  code: string;
  name: string;
  macroregion: string;
  counties: string[];
}

interface CountyData {
  code: string;
  name: string;
}

const MACROREGIONS: MacroregionData[] = [
  { code: "RO1", name: "MACROREGIUNEA UNU" },
  { code: "RO2", name: "MACROREGIUNEA DOI" },
  { code: "RO3", name: "MACROREGIUNEA TREI" },
  { code: "RO4", name: "MACROREGIUNEA PATRU" },
];

const REGIONS: RegionData[] = [
  {
    code: "RO11",
    name: "Nord-Vest",
    macroregion: "RO1",
    counties: ["BH", "BN", "CJ", "MM", "SJ", "SM"],
  },
  {
    code: "RO12",
    name: "Centru",
    macroregion: "RO1",
    counties: ["AB", "BV", "CV", "HR", "MS", "SB"],
  },
  {
    code: "RO21",
    name: "Nord-Est",
    macroregion: "RO2",
    counties: ["BC", "BT", "IS", "NT", "SV", "VS"],
  },
  {
    code: "RO22",
    name: "Sud-Est",
    macroregion: "RO2",
    counties: ["BR", "BZ", "CT", "GL", "TL", "VN"],
  },
  {
    code: "RO31",
    name: "Sud - Muntenia",
    macroregion: "RO3",
    counties: ["AG", "CL", "DB", "GR", "IL", "PH", "TR"],
  },
  {
    code: "RO32",
    name: "Bucuresti - Ilfov",
    macroregion: "RO3",
    counties: ["B", "IF"],
  },
  {
    code: "RO41",
    name: "Sud-Vest Oltenia",
    macroregion: "RO4",
    counties: ["DJ", "GJ", "MH", "OT", "VL"],
  },
  {
    code: "RO42",
    name: "Vest",
    macroregion: "RO4",
    counties: ["AR", "CS", "HD", "TM"],
  },
];

const COUNTIES: CountyData[] = [
  { code: "AB", name: "Alba" },
  { code: "AR", name: "Arad" },
  { code: "AG", name: "Arges" },
  { code: "BC", name: "Bacau" },
  { code: "BH", name: "Bihor" },
  { code: "BN", name: "Bistrita-Nasaud" },
  { code: "BT", name: "Botosani" },
  { code: "BV", name: "Brasov" },
  { code: "BR", name: "Braila" },
  { code: "B", name: "Bucuresti" },
  { code: "BZ", name: "Buzau" },
  { code: "CS", name: "Caras-Severin" },
  { code: "CL", name: "Calarasi" },
  { code: "CJ", name: "Cluj" },
  { code: "CT", name: "Constanta" },
  { code: "CV", name: "Covasna" },
  { code: "DB", name: "Dambovita" },
  { code: "DJ", name: "Dolj" },
  { code: "GL", name: "Galati" },
  { code: "GR", name: "Giurgiu" },
  { code: "GJ", name: "Gorj" },
  { code: "HR", name: "Harghita" },
  { code: "HD", name: "Hunedoara" },
  { code: "IL", name: "Ialomita" },
  { code: "IS", name: "Iasi" },
  { code: "IF", name: "Ilfov" },
  { code: "MM", name: "Maramures" },
  { code: "MH", name: "Mehedinti" },
  { code: "MS", name: "Mures" },
  { code: "NT", name: "Neamt" },
  { code: "OT", name: "Olt" },
  { code: "PH", name: "Prahova" },
  { code: "SM", name: "Satu Mare" },
  { code: "SJ", name: "Salaj" },
  { code: "SB", name: "Sibiu" },
  { code: "SV", name: "Suceava" },
  { code: "TR", name: "Teleorman" },
  { code: "TM", name: "Timis" },
  { code: "TL", name: "Tulcea" },
  { code: "VS", name: "Vaslui" },
  { code: "VL", name: "Valcea" },
  { code: "VN", name: "Vrancea" },
];

// ============================================================================
// Territory Service
// ============================================================================

export class TerritoryService {
  constructor(private db: Kysely<Database>) {}

  /**
   * Seed the static NUTS hierarchy (NATIONAL -> NUTS1 -> NUTS2 -> NUTS3)
   */
  async seedNutsHierarchy(): Promise<{ inserted: number; updated: number }> {
    let inserted = 0;
    let updated = 0;

    // 1. National level
    const national = await this.ensureTerritory({
      code: "RO",
      name: "TOTAL",
      level: "NATIONAL",
      parentId: null,
    });
    if (national.inserted) inserted++;
    else updated++;

    // 2. Macroregions (NUTS1)
    const macroregionIds = new Map<string, number>();
    for (const macro of MACROREGIONS) {
      const result = await this.ensureTerritory({
        code: macro.code,
        name: macro.name,
        level: "NUTS1",
        parentId: national.id,
      });
      macroregionIds.set(macro.code, result.id);
      if (result.inserted) inserted++;
      else updated++;
    }

    // 3. Regions (NUTS2)
    const regionIds = new Map<string, number>();
    for (const region of REGIONS) {
      const macroId = macroregionIds.get(region.macroregion);
      const result = await this.ensureTerritory({
        code: region.code,
        name: region.name,
        level: "NUTS2",
        parentId: macroId ?? null,
      });
      regionIds.set(region.code, result.id);
      if (result.inserted) inserted++;
      else updated++;
    }

    // 4. Counties (NUTS3)
    for (const county of COUNTIES) {
      // Find which region this county belongs to
      const region = REGIONS.find((r) => r.counties.includes(county.code));
      const regionId = region ? regionIds.get(region.code) : null;

      const result = await this.ensureTerritory({
        code: county.code,
        name: county.name,
        level: "NUTS3",
        parentId: regionId ?? null,
      });
      if (result.inserted) inserted++;
      else updated++;
    }

    logger.info({ inserted, updated }, "Seeded NUTS hierarchy");
    return { inserted, updated };
  }

  /**
   * Find or create a territory from a dimension option label
   * Returns the territory ID or null if not a territory label
   */
  async findOrCreateFromLabel(label: string): Promise<number | null> {
    const trimmed = label.trim();

    // Check for SIRUTA pattern: "38731 Ripiceni"
    const sirutaMatch = SIRUTA_PATTERN.exec(trimmed);
    if (sirutaMatch?.[1] && sirutaMatch[2]) {
      const [, sirutaCode, name] = sirutaMatch;
      return this.findOrCreateLocality(sirutaCode, name);
    }

    // Check for TOTAL (national)
    if (/^TOTAL$/i.test(trimmed)) {
      const national = await this.db
        .selectFrom("territories")
        .select("id")
        .where("code", "=", "RO")
        .executeTakeFirst();
      return national?.id ?? null;
    }

    // Check for macroregion
    const macroMatch = /MACROREGIUNEA\s+(UNU|DOI|TREI|PATRU)/i.exec(trimmed);
    if (macroMatch?.[1]) {
      const nameMap: Record<string, string> = {
        UNU: "RO1",
        DOI: "RO2",
        TREI: "RO3",
        PATRU: "RO4",
      };
      const code = nameMap[macroMatch[1].toUpperCase()];
      if (code) {
        const macro = await this.db
          .selectFrom("territories")
          .select("id")
          .where("code", "=", code)
          .executeTakeFirst();
        return macro?.id ?? null;
      }
    }

    // Check for region (by normalized name match)
    // Normalize: remove spaces around hyphens, lowercase
    const normalizedLabel = trimmed.toLowerCase().replaceAll(/\s*-\s*/g, "-");
    for (const region of REGIONS) {
      const normalizedRegionName = region.name
        .toLowerCase()
        .replaceAll(/\s*-\s*/g, "-");
      if (normalizedLabel.includes(normalizedRegionName)) {
        const reg = await this.db
          .selectFrom("territories")
          .select("id")
          .where("code", "=", region.code)
          .executeTakeFirst();
        return reg?.id ?? null;
      }
    }

    // Check for county (by name match, including "Municipiul" prefix)
    for (const county of COUNTIES) {
      const lowerTrimmed = trimmed.toLowerCase();
      const lowerCounty = county.name.toLowerCase();
      if (
        lowerTrimmed === lowerCounty ||
        lowerTrimmed.startsWith(lowerCounty + " ") ||
        lowerTrimmed === "municipiul " + lowerCounty ||
        lowerTrimmed.includes(lowerCounty)
      ) {
        const cty = await this.db
          .selectFrom("territories")
          .select("id")
          .where("code", "=", county.code)
          .executeTakeFirst();
        return cty?.id ?? null;
      }
    }

    // Not recognized as a territory
    return null;
  }

  /**
   * Find or create a locality (LAU level)
   */
  private async findOrCreateLocality(
    sirutaCode: string,
    name: string
  ): Promise<number> {
    // Check if exists
    const existing = await this.db
      .selectFrom("territories")
      .select("id")
      .where("siruta_code", "=", sirutaCode)
      .executeTakeFirst();

    if (existing) {
      return existing.id;
    }

    // TODO: Lookup parent county from SIRUTA data
    // For now, insert without parent (will be linked later)
    const newTerritory: NewTerritory = {
      code: sirutaCode,
      siruta_code: sirutaCode,
      name: name.trim(),
      name_normalized: this.normalize(name),
      level: "LAU",
      parent_id: null, // Would need SIRUTA lookup to determine county
      path: "", // Will be computed by trigger
    };

    const result = await this.db
      .insertInto("territories")
      .values(newTerritory)
      .returning("id")
      .executeTakeFirst();

    logger.debug({ sirutaCode, name }, "Created locality");
    return result!.id;
  }

  /**
   * Ensure a territory exists, return its ID
   */
  private async ensureTerritory(params: {
    code: string;
    name: string;
    level: TerritorialLevel;
    parentId: number | null;
    sirutaCode?: string;
  }): Promise<{ id: number; inserted: boolean }> {
    const existing = await this.db
      .selectFrom("territories")
      .select("id")
      .where("code", "=", params.code)
      .executeTakeFirst();

    if (existing) {
      // Update if needed
      await this.db
        .updateTable("territories")
        .set({
          name: params.name,
          parent_id: params.parentId,
          updated_at: new Date(),
        })
        .where("id", "=", existing.id)
        .execute();
      return { id: existing.id, inserted: false };
    }

    const newTerritory: NewTerritory = {
      code: params.code,
      siruta_code: params.sirutaCode ?? null,
      name: params.name,
      name_normalized: this.normalize(params.name),
      level: params.level,
      parent_id: params.parentId,
      path: "", // Will be computed by trigger
    };

    const result = await this.db
      .insertInto("territories")
      .values(newTerritory)
      .returning("id")
      .executeTakeFirst();

    return { id: result!.id, inserted: true };
  }

  /**
   * Check if a label is a territorial dimension value
   */
  isTerritorialLabel(label: string): boolean {
    const trimmed = label.trim();

    // SIRUTA pattern
    if (SIRUTA_PATTERN.test(trimmed)) return true;

    // TOTAL
    if (/^TOTAL$/i.test(trimmed)) return true;

    // Macroregion
    if (/MACROREGIUNEA/i.test(trimmed)) return true;

    // Region
    if (/Regiunea/i.test(trimmed)) return true;

    // Known counties
    for (const county of COUNTIES) {
      if (trimmed.toLowerCase() === county.name.toLowerCase()) return true;
    }

    return false;
  }

  /**
   * Normalize a string (uppercase, no diacritics)
   */
  private normalize(text: string): string {
    return text
      .toUpperCase()
      .normalize("NFD")
      .replaceAll(/[\u0300-\u036F]/g, "")
      .trim();
  }
}
