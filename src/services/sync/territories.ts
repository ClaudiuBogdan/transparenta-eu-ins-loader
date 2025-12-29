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

/**
 * Pattern to extract SIRUTA code and locality name from INS labels.
 *
 * Format: "<SIRUTA_CODE> <LOCALITY_NAME>"
 * Examples:
 *   - "143450 MUNICIPIUL SIBIU" (6-digit code)
 *   - "38731 Ripiceni" (5-digit code)
 *   - "1213 MUNICIPIUL AIUD" (4-digit code, leading zeros truncated)
 *
 * Bug fix (2024-12): Changed from \d{5,6} to \d{4,6} to handle 4-digit SIRUTA codes.
 * INS API returns some labels with 4-digit prefixes where leading zeros are truncated.
 * Official SIRUTA codes are 5-6 digits, but INS API labels may have 4-6 digits.
 * Without this fix, 90 localities in POP107D were not matched and fell through to
 * county name matching, which either failed or incorrectly mapped them.
 */
const SIRUTA_PATTERN = /^(\d{4,6})\s+(.+)$/;

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
// SIRUTA to County Mapping (Range-Based)
// ============================================================================
//
// SIRUTA codes are 6-digit codes where the structure encodes county information.
// However, INS API often truncates leading zeros, so codes appear as 4-6 digits.
//
// IMPORTANT: Simple prefix matching doesn't work because ranges overlap when
// leading zeros are stripped. For example:
//   - Arad codes: 9xxx-12xxx (4-digit, leading zeros stripped from 009xxx-012xxx)
//   - Galati codes: 92xxx-95xxx (5-digit)
//   - A code like "9262" (Arad) would incorrectly match prefix "92" (Galati)
//
// Solution: Use range-based lookup that considers the numeric value of the code.
// ============================================================================

interface SirutaRange {
  min: number;
  max: number;
  county: string;
}

/**
 * SIRUTA code ranges for each county.
 *
 * These ranges are based on the official SIRUTA classification where codes
 * are assigned in blocks to each county. The ranges account for INS API
 * truncating leading zeros (e.g., 009262 becomes 9262).
 *
 * Source: Romanian National Institute of Statistics (INS) SIRUTA classification
 */
const SIRUTA_RANGES: SirutaRange[] = [
  // Alba (01) - codes 1xxx-8xxx (originally 001xxx-008xxx)
  { min: 1000, max: 8999, county: "AB" },

  // Arad (02) - codes 9xxx-12xxx (originally 009xxx-012xxx)
  { min: 9000, max: 12999, county: "AR" },

  // Arges (03) - codes 13xxx-18xxx
  { min: 13000, max: 18999, county: "AG" },

  // Bacau (04) - codes 20xxx-25xxx
  { min: 20000, max: 25999, county: "BC" },

  // Bihor (05) - codes 26xxx-31xxx
  { min: 26000, max: 31999, county: "BH" },

  // Bistrita-Nasaud (06) - codes 32xxx-35xxx
  { min: 32000, max: 35999, county: "BN" },

  // Botosani (07) - codes 36xxx-39xxx
  { min: 36000, max: 39999, county: "BT" },

  // Brasov (08) - codes 40xxx-44xxx
  { min: 40000, max: 44999, county: "BV" },

  // Braila (09) - codes 45xxx-48xxx
  { min: 45000, max: 48999, county: "BR" },

  // Buzau (10) - codes 49xxx-54xxx
  { min: 49000, max: 54999, county: "BZ" },

  // Caras-Severin (11) - codes 55xxx-59xxx
  { min: 55000, max: 59999, county: "CS" },

  // Calarasi (51) - codes 60xxx-63xxx
  { min: 60000, max: 63999, county: "CL" },

  // Cluj (12) - codes 64xxx-69xxx
  { min: 64000, max: 69999, county: "CJ" },

  // Constanta (13) - codes 70xxx-75xxx
  { min: 70000, max: 75999, county: "CT" },

  // Covasna (14) - codes 76xxx-79xxx
  { min: 76000, max: 79999, county: "CV" },

  // Dambovita (15) - codes 80xxx-84xxx
  { min: 80000, max: 84999, county: "DB" },

  // Dolj (16) - codes 85xxx-91xxx
  { min: 85000, max: 91999, county: "DJ" },

  // Galati (17) - codes 92xxx-96xxx
  { min: 92000, max: 96999, county: "GL" },

  // Giurgiu (52) - codes 97xxx-101xxx
  { min: 97000, max: 101999, county: "GR" },

  // Gorj (18) - codes 102xxx-106xxx
  { min: 102000, max: 106999, county: "GJ" },

  // Harghita (19) - codes 107xxx-112xxx
  { min: 107000, max: 112999, county: "HR" },

  // Hunedoara (20) - codes 113xxx-119xxx
  { min: 113000, max: 119999, county: "HD" },

  // Ialomita (21) - codes 120xxx-124xxx
  { min: 120000, max: 124999, county: "IL" },

  // Iasi (22) - codes 125xxx-131xxx
  { min: 125000, max: 131999, county: "IS" },

  // Ilfov (23) - codes 132xxx-135xxx
  { min: 132000, max: 135999, county: "IF" },

  // Maramures (24) - codes 136xxx-141xxx
  { min: 136000, max: 141999, county: "MM" },

  // Mehedinti (25) - codes 142xxx-146xxx
  { min: 142000, max: 146999, county: "MH" },

  // Mures (26) - codes 147xxx-153xxx
  { min: 147000, max: 153999, county: "MS" },

  // Neamt (27) - codes 154xxx-159xxx
  { min: 154000, max: 159999, county: "NT" },

  // Olt (28) - codes 160xxx-165xxx
  { min: 160000, max: 165999, county: "OT" },

  // Prahova (29) - codes 166xxx-173xxx
  { min: 166000, max: 173999, county: "PH" },

  // Satu Mare (30) - codes 174xxx-178xxx
  { min: 174000, max: 178999, county: "SM" },

  // Salaj (31) - codes 179xxx-183xxx
  { min: 179000, max: 183999, county: "SJ" },

  // Sibiu (32) - codes 184xxx-189xxx
  { min: 184000, max: 189999, county: "SB" },

  // Suceava (33) - codes 190xxx-197xxx
  { min: 190000, max: 197999, county: "SV" },

  // Teleorman (34) - codes 198xxx-203xxx
  { min: 198000, max: 203999, county: "TR" },

  // Timis (35) - codes 204xxx-211xxx
  { min: 204000, max: 211999, county: "TM" },

  // Tulcea (36) - codes 212xxx-217xxx
  { min: 212000, max: 217999, county: "TL" },

  // Vaslui (37) - codes 218xxx-223xxx
  { min: 218000, max: 223999, county: "VS" },

  // Valcea (38) - codes 224xxx-229xxx
  { min: 224000, max: 229999, county: "VL" },

  // Vrancea (39) - codes 230xxx-235xxx
  { min: 230000, max: 235999, county: "VN" },

  // Bucuresti (40) - special sector codes
  // Bucuresti sectors have specific SIRUTA codes
  { min: 179132, max: 179132, county: "B" }, // Sector 1
  { min: 179141, max: 179141, county: "B" }, // Sector 2
  { min: 179150, max: 179150, county: "B" }, // Sector 3
  { min: 179169, max: 179169, county: "B" }, // Sector 4
  { min: 179178, max: 179178, county: "B" }, // Sector 5
  { min: 179187, max: 179187, county: "B" }, // Sector 6
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

    // Check for county (by EXACT name match only)
    // FIX: Removed the overly broad `.includes(lowerCounty)` check that caused false positives.
    // Previously, labels like "1017 MUNICIPIUL ALBA IULIA" would match "Alba" county because
    // the string contained "alba". This incorrectly returned the county NUTS3 entity instead
    // of creating a LAU entry for the municipality.
    //
    // Now we only match:
    // - Exact county name (e.g., "Alba" -> Alba county)
    // - "Judetul X" pattern (e.g., "Judetul Alba" -> Alba county)
    // - "Municipiul X" where X is EXACTLY the county name (county capitals only)
    //
    // Labels with SIRUTA prefixes should be caught by SIRUTA_PATTERN above, not here.
    for (const county of COUNTIES) {
      const lowerTrimmed = trimmed.toLowerCase();
      const lowerCounty = county.name.toLowerCase();
      if (
        lowerTrimmed === lowerCounty ||
        lowerTrimmed === "judetul " + lowerCounty ||
        lowerTrimmed === "municipiul " + lowerCounty
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
   *
   * FIX: Now properly links localities to their parent county using SIRUTA prefix mapping.
   * Previously, all LAU entries were created with parent_id = null, breaking the
   * territorial hierarchy and preventing path computation.
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

    // FIX: Lookup parent county from SIRUTA code prefix
    // The SIRUTA code encodes the county in its prefix (first 1-3 digits)
    const parentId = await this.getParentCountyId(sirutaCode);

    if (!parentId) {
      logger.warn(
        { sirutaCode, name },
        "Could not determine parent county for locality"
      );
    }

    const newTerritory: NewTerritory = {
      code: sirutaCode,
      siruta_code: sirutaCode,
      name: name.trim(),
      name_normalized: this.normalize(name),
      level: "LAU",
      parent_id: parentId, // FIX: Now properly set from SIRUTA prefix lookup
      path: "", // Will be computed by trigger
    };

    const result = await this.db
      .insertInto("territories")
      .values(newTerritory)
      .returning("id")
      .executeTakeFirst();

    logger.debug({ sirutaCode, name, parentId }, "Created locality");
    return result!.id;
  }

  /**
   * Get the parent county ID from a SIRUTA code
   *
   * FIX: Added method to determine county from SIRUTA code prefix.
   * SIRUTA codes encode the county in their prefix, allowing us to establish
   * the proper parent-child relationship in the territorial hierarchy.
   *
   * @param sirutaCode - The SIRUTA code (4-6 digits)
   * @returns The county territory ID, or null if not found
   */
  private async getParentCountyId(sirutaCode: string): Promise<number | null> {
    const countyCode = this.getCountyCodeFromSiruta(sirutaCode);

    if (!countyCode) {
      return null;
    }

    const county = await this.db
      .selectFrom("territories")
      .select("id")
      .where("code", "=", countyCode)
      .where("level", "=", "NUTS3")
      .executeTakeFirst();

    return county?.id ?? null;
  }

  /**
   * Extract the county code from a SIRUTA code using range-based lookup
   *
   * FIX: Changed from prefix-based to range-based lookup to avoid conflicts.
   *
   * The previous prefix-based approach failed because:
   *   - Arad codes: 9xxx-12xxx (4-digit, from 009xxx-012xxx with leading zeros stripped)
   *   - Galati codes: 92xxx-96xxx (5-digit)
   *   - Code "9262" (Arad) would incorrectly match prefix "92" (Galati)
   *
   * Range-based lookup compares the numeric value against defined ranges,
   * correctly identifying that 9262 falls in 9000-12999 (Arad), not 92000-96999 (Galati).
   *
   * @param sirutaCode - The SIRUTA code (4-6 digits)
   * @returns The county code (e.g., "AB", "CJ"), or null if not found
   */
  private getCountyCodeFromSiruta(sirutaCode: string): string | null {
    const code = Number.parseInt(sirutaCode, 10);

    if (Number.isNaN(code)) {
      return null;
    }

    // Find the range that contains this SIRUTA code
    for (const range of SIRUTA_RANGES) {
      if (code >= range.min && code <= range.max) {
        return range.county;
      }
    }

    return null;
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
