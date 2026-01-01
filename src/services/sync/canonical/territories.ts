import { logger } from "../../../logger.js";

import type { Database } from "../../../db/types.js";
import type { Kysely } from "kysely";

// ============================================================================
// Static Data (kept for label matching patterns)
// ============================================================================

interface RegionData {
  code: string;
  name: string;
  normalized: string;
}

interface CountyData {
  code: string;
  name: string;
  normalized: string;
}

const REGIONS: RegionData[] = [
  { code: "RO11", name: "Nord-Vest", normalized: "NORD VEST" },
  { code: "RO12", name: "Centru", normalized: "CENTRU" },
  { code: "RO21", name: "Nord-Est", normalized: "NORD EST" },
  { code: "RO22", name: "Sud-Est", normalized: "SUD EST" },
  { code: "RO31", name: "Sud - Muntenia", normalized: "SUD MUNTENIA" },
  { code: "RO32", name: "București - Ilfov", normalized: "BUCURESTI ILFOV" },
  { code: "RO41", name: "Sud-Vest Oltenia", normalized: "SUD VEST OLTENIA" },
  { code: "RO42", name: "Vest", normalized: "VEST" },
];

export const COUNTIES: CountyData[] = [
  { code: "AB", name: "Alba", normalized: "ALBA" },
  { code: "AR", name: "Arad", normalized: "ARAD" },
  { code: "AG", name: "Argeș", normalized: "ARGES" },
  { code: "BC", name: "Bacău", normalized: "BACAU" },
  { code: "BH", name: "Bihor", normalized: "BIHOR" },
  { code: "BN", name: "Bistrița-Năsăud", normalized: "BISTRITA NASAUD" },
  { code: "BT", name: "Botoșani", normalized: "BOTOSANI" },
  { code: "BV", name: "Brașov", normalized: "BRASOV" },
  { code: "BR", name: "Brăila", normalized: "BRAILA" },
  { code: "B", name: "București", normalized: "BUCURESTI" },
  { code: "BZ", name: "Buzău", normalized: "BUZAU" },
  { code: "CS", name: "Caraș-Severin", normalized: "CARAS SEVERIN" },
  { code: "CL", name: "Călărași", normalized: "CALARASI" },
  { code: "CJ", name: "Cluj", normalized: "CLUJ" },
  { code: "CT", name: "Constanța", normalized: "CONSTANTA" },
  { code: "CV", name: "Covasna", normalized: "COVASNA" },
  { code: "DB", name: "Dâmbovița", normalized: "DAMBOVITA" },
  { code: "DJ", name: "Dolj", normalized: "DOLJ" },
  { code: "GL", name: "Galați", normalized: "GALATI" },
  { code: "GR", name: "Giurgiu", normalized: "GIURGIU" },
  { code: "GJ", name: "Gorj", normalized: "GORJ" },
  { code: "HR", name: "Harghita", normalized: "HARGHITA" },
  { code: "HD", name: "Hunedoara", normalized: "HUNEDOARA" },
  { code: "IL", name: "Ialomița", normalized: "IALOMITA" },
  { code: "IS", name: "Iași", normalized: "IASI" },
  { code: "IF", name: "Ilfov", normalized: "ILFOV" },
  { code: "MM", name: "Maramureș", normalized: "MARAMURES" },
  { code: "MH", name: "Mehedinți", normalized: "MEHEDINTI" },
  { code: "MS", name: "Mureș", normalized: "MURES" },
  { code: "NT", name: "Neamț", normalized: "NEAMT" },
  { code: "OT", name: "Olt", normalized: "OLT" },
  { code: "PH", name: "Prahova", normalized: "PRAHOVA" },
  { code: "SM", name: "Satu Mare", normalized: "SATU MARE" },
  { code: "SJ", name: "Sălaj", normalized: "SALAJ" },
  { code: "SB", name: "Sibiu", normalized: "SIBIU" },
  { code: "SV", name: "Suceava", normalized: "SUCEAVA" },
  { code: "TR", name: "Teleorman", normalized: "TELEORMAN" },
  { code: "TM", name: "Timiș", normalized: "TIMIS" },
  { code: "TL", name: "Tulcea", normalized: "TULCEA" },
  { code: "VS", name: "Vaslui", normalized: "VASLUI" },
  { code: "VL", name: "Vâlcea", normalized: "VALCEA" },
  { code: "VN", name: "Vrancea", normalized: "VRANCEA" },
];

// Pattern to extract SIRUTA code from labels like "38731 Ripiceni"
export const SIRUTA_PATTERN = /^(\d{4,6})\s+(.+)$/;

// ============================================================================
// Territory Service (Lookup Only - No Creation)
// ============================================================================

/**
 * TerritoryService provides lookup-only access to territories.
 * Territories must be pre-seeded using `pnpm cli seed territories`.
 * This service never creates territories - it only looks them up.
 */
export class TerritoryService {
  private cache = new Map<string, number>();

  constructor(private db: Kysely<Database>) {}

  /**
   * Find a territory from a label (lookup only, never creates)
   * @returns Territory ID if found, null if not found
   */
  async findFromLabel(
    labelRo: string,
    _labelEn?: string
  ): Promise<number | null> {
    const trimmed = labelRo.trim();
    const normalized = this.normalize(trimmed);
    const normalizedLower = normalized.toLowerCase();

    // Check cache first
    const cacheKey = `territory:${normalized}`;
    if (this.cache.has(cacheKey)) {
      return this.cache.get(cacheKey) ?? null;
    }

    // Extra-regiuni: Special EU statistical category for extra-territorial regions
    // (e.g., embassies, consulates). Stored as NUTS1 level in territories table.
    if (
      normalizedLower === "extra-regiuni" ||
      normalizedLower === "extra regiuni"
    ) {
      const extra = await this.db
        .selectFrom("territories")
        .select("id")
        .where("code", "=", "EXTRA")
        .executeTakeFirst();
      const id = extra?.id ?? null;
      if (id !== null) this.cache.set(cacheKey, id);
      return id;
    }

    // Multi-county aggregates - not a single territory
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
      if (id !== null) this.cache.set(cacheKey, id);
      return id;
    }

    // SIRUTA pattern: "38731 Ripiceni" - lookup by siruta_code
    const sirutaMatch = SIRUTA_PATTERN.exec(trimmed);
    if (sirutaMatch?.[1]) {
      const sirutaCode = sirutaMatch[1];
      const territory = await this.db
        .selectFrom("territories")
        .select("id")
        .where("siruta_code", "=", sirutaCode)
        .executeTakeFirst();

      if (territory) {
        this.cache.set(cacheKey, territory.id);
        return territory.id;
      }

      // Also check by code (siruta_code might be stored as code for some entries)
      const byCode = await this.db
        .selectFrom("territories")
        .select("id")
        .where("code", "=", sirutaCode)
        .executeTakeFirst();

      if (byCode) {
        this.cache.set(cacheKey, byCode.id);
        return byCode.id;
      }

      // SIRUTA pattern but not found - this is a missing territory
      logger.debug(
        { sirutaCode, label: trimmed },
        "Territory with SIRUTA code not found in seed data"
      );
      return null;
    }

    // TOTAL or national
    if (/^TOTAL$/i.test(trimmed) || normalizedLower === "nivel national") {
      const national = await this.db
        .selectFrom("territories")
        .select("id")
        .where("code", "=", "RO")
        .executeTakeFirst();
      const id = national?.id ?? null;
      if (id !== null) this.cache.set(cacheKey, id);
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
        if (id !== null) this.cache.set(cacheKey, id);
        return id;
      }
    }

    // Region - check with normalized names
    for (const region of REGIONS) {
      const regionNorm = region.normalized.toLowerCase();
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
        if (id !== null) this.cache.set(cacheKey, id);
        return id;
      }
    }

    // County - exact match only
    for (const county of COUNTIES) {
      const countyNorm = county.normalized.toLowerCase();
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
        if (id !== null) this.cache.set(cacheKey, id);
        return id;
      }
    }

    return null;
  }

  /**
   * @deprecated Use findFromLabel instead. This alias exists for backwards compatibility.
   */
  async findOrCreateFromLabel(
    labelRo: string,
    labelEn?: string
  ): Promise<number | null> {
    return this.findFromLabel(labelRo, labelEn);
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
