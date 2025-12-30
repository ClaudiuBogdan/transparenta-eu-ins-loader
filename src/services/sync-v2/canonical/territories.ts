import { jsonb } from "../../../db/connection.js";
import { logger } from "../../../logger.js";

import type {
  Database,
  TerritoryLevel,
  BilingualText,
} from "../../../db/types-v2.js";
import type { Kysely } from "kysely";

// ============================================================================
// Static Data
// ============================================================================

interface MacroregionData {
  code: string;
  names: BilingualText;
}

interface RegionData {
  code: string;
  names: BilingualText;
  macroregion: string;
  counties: string[];
}

interface CountyData {
  code: string;
  names: BilingualText;
}

const MACROREGIONS: MacroregionData[] = [
  {
    code: "RO1",
    names: {
      ro: "MACROREGIUNEA UNU",
      en: "Macroregion One",
      normalized: "MACROREGIUNEA UNU",
    },
  },
  {
    code: "RO2",
    names: {
      ro: "MACROREGIUNEA DOI",
      en: "Macroregion Two",
      normalized: "MACROREGIUNEA DOI",
    },
  },
  {
    code: "RO3",
    names: {
      ro: "MACROREGIUNEA TREI",
      en: "Macroregion Three",
      normalized: "MACROREGIUNEA TREI",
    },
  },
  {
    code: "RO4",
    names: {
      ro: "MACROREGIUNEA PATRU",
      en: "Macroregion Four",
      normalized: "MACROREGIUNEA PATRU",
    },
  },
];

const REGIONS: RegionData[] = [
  {
    code: "RO11",
    names: { ro: "Nord-Vest", en: "North-West", normalized: "NORD VEST" },
    macroregion: "RO1",
    counties: ["BH", "BN", "CJ", "MM", "SJ", "SM"],
  },
  {
    code: "RO12",
    names: { ro: "Centru", en: "Center", normalized: "CENTRU" },
    macroregion: "RO1",
    counties: ["AB", "BV", "CV", "HR", "MS", "SB"],
  },
  {
    code: "RO21",
    names: { ro: "Nord-Est", en: "North-East", normalized: "NORD EST" },
    macroregion: "RO2",
    counties: ["BC", "BT", "IS", "NT", "SV", "VS"],
  },
  {
    code: "RO22",
    names: { ro: "Sud-Est", en: "South-East", normalized: "SUD EST" },
    macroregion: "RO2",
    counties: ["BR", "BZ", "CT", "GL", "TL", "VN"],
  },
  {
    code: "RO31",
    names: {
      ro: "Sud - Muntenia",
      en: "South - Muntenia",
      normalized: "SUD MUNTENIA",
    },
    macroregion: "RO3",
    counties: ["AG", "CL", "DB", "GR", "IL", "PH", "TR"],
  },
  {
    code: "RO32",
    names: {
      ro: "București - Ilfov",
      en: "Bucharest - Ilfov",
      normalized: "BUCURESTI ILFOV",
    },
    macroregion: "RO3",
    counties: ["B", "IF"],
  },
  {
    code: "RO41",
    names: {
      ro: "Sud-Vest Oltenia",
      en: "South-West Oltenia",
      normalized: "SUD VEST OLTENIA",
    },
    macroregion: "RO4",
    counties: ["DJ", "GJ", "MH", "OT", "VL"],
  },
  {
    code: "RO42",
    names: { ro: "Vest", en: "West", normalized: "VEST" },
    macroregion: "RO4",
    counties: ["AR", "CS", "HD", "TM"],
  },
];

export const COUNTIES: CountyData[] = [
  { code: "AB", names: { ro: "Alba", en: "Alba", normalized: "ALBA" } },
  { code: "AR", names: { ro: "Arad", en: "Arad", normalized: "ARAD" } },
  { code: "AG", names: { ro: "Argeș", en: "Arges", normalized: "ARGES" } },
  { code: "BC", names: { ro: "Bacău", en: "Bacau", normalized: "BACAU" } },
  { code: "BH", names: { ro: "Bihor", en: "Bihor", normalized: "BIHOR" } },
  {
    code: "BN",
    names: {
      ro: "Bistrița-Năsăud",
      en: "Bistrita-Nasaud",
      normalized: "BISTRITA NASAUD",
    },
  },
  {
    code: "BT",
    names: { ro: "Botoșani", en: "Botosani", normalized: "BOTOSANI" },
  },
  { code: "BV", names: { ro: "Brașov", en: "Brasov", normalized: "BRASOV" } },
  { code: "BR", names: { ro: "Brăila", en: "Braila", normalized: "BRAILA" } },
  {
    code: "B",
    names: { ro: "București", en: "Bucharest", normalized: "BUCURESTI" },
  },
  { code: "BZ", names: { ro: "Buzău", en: "Buzau", normalized: "BUZAU" } },
  {
    code: "CS",
    names: {
      ro: "Caraș-Severin",
      en: "Caras-Severin",
      normalized: "CARAS SEVERIN",
    },
  },
  {
    code: "CL",
    names: { ro: "Călărași", en: "Calarasi", normalized: "CALARASI" },
  },
  { code: "CJ", names: { ro: "Cluj", en: "Cluj", normalized: "CLUJ" } },
  {
    code: "CT",
    names: { ro: "Constanța", en: "Constanta", normalized: "CONSTANTA" },
  },
  {
    code: "CV",
    names: { ro: "Covasna", en: "Covasna", normalized: "COVASNA" },
  },
  {
    code: "DB",
    names: { ro: "Dâmbovița", en: "Dambovita", normalized: "DAMBOVITA" },
  },
  { code: "DJ", names: { ro: "Dolj", en: "Dolj", normalized: "DOLJ" } },
  { code: "GL", names: { ro: "Galați", en: "Galati", normalized: "GALATI" } },
  {
    code: "GR",
    names: { ro: "Giurgiu", en: "Giurgiu", normalized: "GIURGIU" },
  },
  { code: "GJ", names: { ro: "Gorj", en: "Gorj", normalized: "GORJ" } },
  {
    code: "HR",
    names: { ro: "Harghita", en: "Harghita", normalized: "HARGHITA" },
  },
  {
    code: "HD",
    names: { ro: "Hunedoara", en: "Hunedoara", normalized: "HUNEDOARA" },
  },
  {
    code: "IL",
    names: { ro: "Ialomița", en: "Ialomita", normalized: "IALOMITA" },
  },
  { code: "IS", names: { ro: "Iași", en: "Iasi", normalized: "IASI" } },
  { code: "IF", names: { ro: "Ilfov", en: "Ilfov", normalized: "ILFOV" } },
  {
    code: "MM",
    names: { ro: "Maramureș", en: "Maramures", normalized: "MARAMURES" },
  },
  {
    code: "MH",
    names: { ro: "Mehedinți", en: "Mehedinti", normalized: "MEHEDINTI" },
  },
  { code: "MS", names: { ro: "Mureș", en: "Mures", normalized: "MURES" } },
  { code: "NT", names: { ro: "Neamț", en: "Neamt", normalized: "NEAMT" } },
  { code: "OT", names: { ro: "Olt", en: "Olt", normalized: "OLT" } },
  {
    code: "PH",
    names: { ro: "Prahova", en: "Prahova", normalized: "PRAHOVA" },
  },
  {
    code: "SM",
    names: { ro: "Satu Mare", en: "Satu Mare", normalized: "SATU MARE" },
  },
  { code: "SJ", names: { ro: "Sălaj", en: "Salaj", normalized: "SALAJ" } },
  { code: "SB", names: { ro: "Sibiu", en: "Sibiu", normalized: "SIBIU" } },
  {
    code: "SV",
    names: { ro: "Suceava", en: "Suceava", normalized: "SUCEAVA" },
  },
  {
    code: "TR",
    names: { ro: "Teleorman", en: "Teleorman", normalized: "TELEORMAN" },
  },
  { code: "TM", names: { ro: "Timiș", en: "Timis", normalized: "TIMIS" } },
  { code: "TL", names: { ro: "Tulcea", en: "Tulcea", normalized: "TULCEA" } },
  { code: "VS", names: { ro: "Vaslui", en: "Vaslui", normalized: "VASLUI" } },
  { code: "VL", names: { ro: "Vâlcea", en: "Valcea", normalized: "VALCEA" } },
  {
    code: "VN",
    names: { ro: "Vrancea", en: "Vrancea", normalized: "VRANCEA" },
  },
];

// SIRUTA code ranges for county lookup
interface SirutaRange {
  min: number;
  max: number;
  county: string;
}

const SIRUTA_RANGES: SirutaRange[] = [
  { min: 1000, max: 8999, county: "AB" },
  { min: 9000, max: 12999, county: "AR" },
  { min: 13000, max: 18999, county: "AG" },
  { min: 20000, max: 25999, county: "BC" },
  { min: 26000, max: 31999, county: "BH" },
  { min: 32000, max: 35999, county: "BN" },
  { min: 36000, max: 39999, county: "BT" },
  { min: 40000, max: 44999, county: "BV" },
  { min: 45000, max: 48999, county: "BR" },
  { min: 49000, max: 54999, county: "BZ" },
  { min: 55000, max: 59999, county: "CS" },
  { min: 60000, max: 63999, county: "CL" },
  { min: 64000, max: 69999, county: "CJ" },
  { min: 70000, max: 75999, county: "CT" },
  { min: 76000, max: 79999, county: "CV" },
  { min: 80000, max: 84999, county: "DB" },
  { min: 85000, max: 91999, county: "DJ" },
  { min: 92000, max: 96999, county: "GL" },
  { min: 97000, max: 101999, county: "GR" },
  { min: 102000, max: 106999, county: "GJ" },
  { min: 107000, max: 112999, county: "HR" },
  { min: 113000, max: 119999, county: "HD" },
  { min: 120000, max: 124999, county: "IL" },
  { min: 125000, max: 131999, county: "IS" },
  { min: 132000, max: 135999, county: "IF" },
  { min: 136000, max: 141999, county: "MM" },
  { min: 142000, max: 146999, county: "MH" },
  { min: 147000, max: 153999, county: "MS" },
  { min: 154000, max: 159999, county: "NT" },
  { min: 160000, max: 165999, county: "OT" },
  { min: 166000, max: 173999, county: "PH" },
  { min: 174000, max: 178999, county: "SM" },
  // HIGH FIX: Bucharest sectors MUST come BEFORE SJ range since their codes (179132-179187)
  // fall within SJ's range (179000-183999). Ranges are checked in order!
  { min: 179132, max: 179132, county: "B" }, // Sector 1
  { min: 179141, max: 179141, county: "B" }, // Sector 2
  { min: 179150, max: 179150, county: "B" }, // Sector 3
  { min: 179169, max: 179169, county: "B" }, // Sector 4
  { min: 179178, max: 179178, county: "B" }, // Sector 5
  { min: 179187, max: 179187, county: "B" }, // Sector 6
  { min: 179000, max: 183999, county: "SJ" },
  { min: 184000, max: 189999, county: "SB" },
  { min: 190000, max: 197999, county: "SV" },
  { min: 198000, max: 203999, county: "TR" },
  { min: 204000, max: 211999, county: "TM" },
  { min: 212000, max: 217999, county: "TL" },
  { min: 218000, max: 223999, county: "VS" },
  { min: 224000, max: 229999, county: "VL" },
  { min: 230000, max: 235999, county: "VN" },
];

// Pattern to extract SIRUTA code from labels
export const SIRUTA_PATTERN = /^(\d{4,6})\s+(.+)$/;

// ============================================================================
// Territory Service
// ============================================================================

export class TerritoryService {
  private cache = new Map<string, number>();

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
      names: { ro: "TOTAL", en: "Total", normalized: "TOTAL" },
      level: "NATIONAL",
      parentId: null,
      path: "RO",
    });
    if (national.inserted) inserted++;
    else updated++;

    // 2. Macroregions (NUTS1)
    const macroregionIds = new Map<string, number>();
    for (const macro of MACROREGIONS) {
      const result = await this.ensureTerritory({
        code: macro.code,
        names: macro.names,
        level: "NUTS1",
        parentId: national.id,
        path: `RO.${macro.code}`,
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
        names: region.names,
        level: "NUTS2",
        parentId: macroId ?? null,
        path: `RO.${region.macroregion}.${region.code}`,
      });
      regionIds.set(region.code, result.id);
      if (result.inserted) inserted++;
      else updated++;
    }

    // 4. Counties (NUTS3)
    for (const county of COUNTIES) {
      const region = REGIONS.find((r) => r.counties.includes(county.code));
      const regionId = region ? regionIds.get(region.code) : null;

      const result = await this.ensureTerritory({
        code: county.code,
        names: county.names,
        level: "NUTS3",
        parentId: regionId ?? null,
        path: region
          ? `RO.${region.macroregion}.${region.code}.${county.code}`
          : `RO.${county.code}`,
      });
      if (result.inserted) inserted++;
      else updated++;
    }

    logger.info({ inserted, updated }, "Seeded NUTS hierarchy");
    return { inserted, updated };
  }

  /**
   * Find or create a territory from a label
   */
  async findOrCreateFromLabel(
    labelRo: string,
    labelEn?: string
  ): Promise<number | null> {
    const trimmed = labelRo.trim();
    const normalized = this.normalize(trimmed);
    const normalizedLower = normalized.toLowerCase();

    // Check cache first
    const cacheKey = `territory:${normalized}`;
    if (this.cache.has(cacheKey)) {
      return this.cache.get(cacheKey) ?? null;
    }

    // Special cases
    if (
      normalizedLower === "extra-regiuni" ||
      normalizedLower === "extra regiuni"
    ) {
      return null;
    }

    // Multi-county aggregates
    if (trimmed.includes(",") && /[A-Z][a-z]+,\s*[A-Z][a-z]+/.test(trimmed)) {
      return null;
    }

    // Bucharest special patterns
    if (
      normalizedLower.includes("bucuresti") &&
      (normalizedLower.includes("sai") || normalizedLower.includes("incl"))
    ) {
      const bucharest = await this.db
        .selectFrom("territories")
        .select("id")
        .where("code", "=", "B")
        .executeTakeFirst();
      const id = bucharest?.id ?? null;
      if (id) this.cache.set(cacheKey, id);
      return id;
    }

    // SIRUTA pattern: "38731 Ripiceni"
    const sirutaMatch = SIRUTA_PATTERN.exec(trimmed);
    if (sirutaMatch?.[1] && sirutaMatch[2]) {
      const id = await this.findOrCreateLocality(
        sirutaMatch[1],
        sirutaMatch[2],
        labelEn
      );
      if (id) this.cache.set(cacheKey, id);
      return id;
    }

    // TOTAL or national
    if (/^TOTAL$/i.test(trimmed) || normalizedLower === "nivel national") {
      const national = await this.db
        .selectFrom("territories")
        .select("id")
        .where("code", "=", "RO")
        .executeTakeFirst();
      const id = national?.id ?? null;
      if (id) this.cache.set(cacheKey, id);
      return id;
    }

    // Macroregion
    const macroMatch = /MACROREGIUNEA\s+(UNU|DOI|TREI|PATRU)/i.exec(trimmed);
    if (macroMatch?.[1]) {
      const codeMap: Record<string, string> = {
        UNU: "RO1",
        DOI: "RO2",
        TREI: "RO3",
        PATRU: "RO4",
      };
      const code = codeMap[macroMatch[1].toUpperCase()];
      if (code) {
        const macro = await this.db
          .selectFrom("territories")
          .select("id")
          .where("code", "=", code)
          .executeTakeFirst();
        const id = macro?.id ?? null;
        if (id) this.cache.set(cacheKey, id);
        return id;
      }
    }

    // Region - check with normalized names
    for (const region of REGIONS) {
      const regionNorm = region.names.normalized?.toLowerCase() ?? "";
      if (
        normalizedLower.includes(regionNorm) ||
        this.matchesRegionPattern(normalizedLower, regionNorm)
      ) {
        const reg = await this.db
          .selectFrom("territories")
          .select("id")
          .where("code", "=", region.code)
          .executeTakeFirst();
        const id = reg?.id ?? null;
        if (id) this.cache.set(cacheKey, id);
        return id;
      }
    }

    // County - exact match only
    for (const county of COUNTIES) {
      const countyNorm = county.names.normalized?.toLowerCase() ?? "";
      if (
        normalizedLower === countyNorm ||
        normalizedLower === `judetul ${countyNorm}` ||
        normalizedLower === `municipiul ${countyNorm}`
      ) {
        const cty = await this.db
          .selectFrom("territories")
          .select("id")
          .where("code", "=", county.code)
          .executeTakeFirst();
        const id = cty?.id ?? null;
        if (id) this.cache.set(cacheKey, id);
        return id;
      }
    }

    return null;
  }

  /**
   * Get county code from SIRUTA code using range lookup
   */
  getCountyCodeFromSiruta(sirutaCode: string): string | null {
    const code = Number.parseInt(sirutaCode, 10);
    if (Number.isNaN(code)) return null;

    for (const range of SIRUTA_RANGES) {
      if (code >= range.min && code <= range.max) {
        return range.county;
      }
    }
    return null;
  }

  private async findOrCreateLocality(
    sirutaCode: string,
    nameRo: string,
    nameEn?: string
  ): Promise<number> {
    const existing = await this.db
      .selectFrom("territories")
      .select("id")
      .where("siruta_code", "=", sirutaCode)
      .executeTakeFirst();
    if (existing) return existing.id;

    const countyCode = this.getCountyCodeFromSiruta(sirutaCode);
    let parentId: number | null = null;
    let path = sirutaCode;

    if (countyCode) {
      const county = await this.db
        .selectFrom("territories")
        .select(["id", "path"])
        .where("code", "=", countyCode)
        .where("level", "=", "NUTS3")
        .executeTakeFirst();
      if (county) {
        parentId = county.id;
        path = `${county.path}.${sirutaCode}`;
      }
    }

    const names: BilingualText = {
      ro: nameRo.trim(),
      en: nameEn?.trim(),
      normalized: this.normalize(nameRo),
    };

    const newTerritory = {
      code: sirutaCode,
      siruta_code: sirutaCode,
      level: "LAU" as const,
      path,
      parent_id: parentId,
      names: jsonb(names),
      siruta_metadata: null,
    };

    const result = await this.db
      .insertInto("territories")
      .values(newTerritory)
      .returning("id")
      .executeTakeFirst();
    logger.debug({ sirutaCode, nameRo, parentId }, "Created locality");
    return result!.id;
  }

  private async ensureTerritory(params: {
    code: string;
    names: BilingualText;
    level: TerritoryLevel;
    parentId: number | null;
    path: string;
    sirutaCode?: string;
  }): Promise<{ id: number; inserted: boolean }> {
    const existing = await this.db
      .selectFrom("territories")
      .select("id")
      .where("code", "=", params.code)
      .executeTakeFirst();

    if (existing) {
      await this.db
        .updateTable("territories")
        .set({
          names: jsonb(params.names),
          parent_id: params.parentId,
          path: params.path,
          updated_at: new Date(),
        })
        .where("id", "=", existing.id)
        .execute();
      return { id: existing.id, inserted: false };
    }

    const newTerritory = {
      code: params.code,
      siruta_code: params.sirutaCode ?? null,
      level: params.level,
      path: params.path,
      parent_id: params.parentId,
      names: jsonb(params.names),
      siruta_metadata: null,
    };

    const result = await this.db
      .insertInto("territories")
      .values(newTerritory)
      .returning("id")
      .executeTakeFirst();
    return { id: result!.id, inserted: true };
  }

  private matchesRegionPattern(label: string, regionName: string): boolean {
    const words = regionName.split(/\s+/);
    const pattern = words
      .map((w) => w.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"))
      .join("[\\s-]+");
    const regex = new RegExp(`(?:^|regiunea\\s+|\\s)${pattern}(?:\\s|$)`, "i");
    return regex.test(label);
  }

  private normalize(text: string): string {
    return text
      .toUpperCase()
      .normalize("NFD")
      .replaceAll(/[\u0300-\u036F]/g, "")
      .replaceAll(/[\s-]+/g, " ")
      .trim();
  }

  clearCache(): void {
    this.cache.clear();
  }
}
