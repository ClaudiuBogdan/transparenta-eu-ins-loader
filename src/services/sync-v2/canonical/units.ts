import { jsonb, textArray } from "../../../db/connection.js";
import { logger } from "../../../logger.js";

import type { Database, BilingualText } from "../../../db/types-v2.js";
import type { Kysely } from "kysely";

// ============================================================================
// Unit Pattern
// ============================================================================

const UNIT_PATTERN = /^UM:\s*(.+)$/i;

// Known unit mappings with bilingual names (seeded from old database)
const UNIT_MAPPINGS: Record<
  string,
  { code: string; symbol: string; nameEn: string }
> = {
  // === Basic Counts ===
  "numar persoane": { code: "PERSONS", symbol: "pers.", nameEn: "Persons" },
  persoane: { code: "PERSONS", symbol: "pers.", nameEn: "Persons" },
  numar: { code: "NUMBER", symbol: "nr.", nameEn: "Number" },
  "mii persoane": {
    code: "THOUSAND_PERSONS",
    symbol: "mii pers.",
    nameEn: "Thousands of persons",
  },
  mii: { code: "MII", symbol: "", nameEn: "Thousands" },
  bucati: { code: "BUCATI", symbol: "buc.", nameEn: "Pieces" },
  "mii bucati": {
    code: "MII_BUCATI",
    symbol: "mii buc.",
    nameEn: "Thousands of pieces",
  },
  locuri: { code: "LOCURI", symbol: "", nameEn: "Places/Seats" },
  "mii locuri": {
    code: "THOUSAND_PLACES",
    symbol: "mii loc.",
    nameEn: "Thousands of places",
  },
  "numar linii": { code: "NUMAR_LINII", symbol: "", nameEn: "Number of lines" },
  "numar locuri-zile": {
    code: "NUMAR_LOCURIZILE",
    symbol: "",
    nameEn: "Place-days",
  },
  "numar spectacole": {
    code: "NUMAR_SPECTACOLE",
    symbol: "",
    nameEn: "Number of shows",
  },
  "numar spectatori": {
    code: "NUMAR_SPECTATORI",
    symbol: "",
    nameEn: "Number of spectators",
  },
  "mii spectatori": {
    code: "MII_SPECTATORI",
    symbol: "",
    nameEn: "Thousands of spectators",
  },
  "mii exemplare": {
    code: "MII_EXEMPLARE",
    symbol: "",
    nameEn: "Thousands of copies",
  },
  "numar mediu la 100 gospodarii": {
    code: "NUMAR_MEDIU_LA_100_GOSPODARII",
    symbol: "",
    nameEn: "Average per 100 households",
  },

  // === Percentages & Rates ===
  procente: { code: "PERCENT", symbol: "%", nameEn: "Percent" },
  procent: { code: "PERCENT", symbol: "%", nameEn: "Percent" },
  "%": { code: "PERCENT", symbol: "%", nameEn: "Percent" },
  promile: { code: "PROMILE", symbol: "‰", nameEn: "Per mille" },
  "la mie": { code: "PROMILE", symbol: "‰", nameEn: "Per mille" },
  indice: { code: "INDEX", symbol: "", nameEn: "Index" },
  rata: { code: "RATE", symbol: "", nameEn: "Rate" },
  coeficient: { code: "COEFFICIENT", symbol: "", nameEn: "Coefficient" },
  "anul 2020": { code: "ANUL_2020", symbol: "", nameEn: "Year 2020 (base)" },

  // === Demographic Rates ===
  "nascuti vii la 1000 locuitori": {
    code: "NASCUTI_VII_LA_1000_LOCUITORI",
    symbol: "",
    nameEn: "Live births per 1000 inhabitants",
  },
  "nascuti vii la 1000 femei in varsta fertila": {
    code: "NASCUTI_VII_LA_1000_FEMEI_IN_VARSTA_FERTILA",
    symbol: "",
    nameEn: "Live births per 1000 women of fertile age",
  },
  "nascuti morti la 1000 nascuti": {
    code: "NASCUTI_MORTI_LA_1000_NASCUTI",
    symbol: "",
    nameEn: "Stillbirths per 1000 births",
  },
  "decedati la 1000 locuitori": {
    code: "DECEDATI_LA_1000_LOCUITORI",
    symbol: "",
    nameEn: "Deaths per 1000 inhabitants",
  },
  "decedati sub 1 an la 1000 nascuti vii": {
    code: "DECEDATI_SUB_1_AN_LA_1000_NASCUTI_VII",
    symbol: "",
    nameEn: "Infant deaths per 1000 live births",
  },
  "casatorii la 1000 locuitori": {
    code: "CASATORII_LA_1000_LOCUITORI",
    symbol: "",
    nameEn: "Marriages per 1000 inhabitants",
  },
  "divorturi la 1000 locuitori": {
    code: "DIVORTURI_LA_1000_LOCUITORI",
    symbol: "",
    nameEn: "Divorces per 1000 inhabitants",
  },
  "spor natural la 1000 locuitori": {
    code: "SPOR_NATURAL_LA_1000_LOCUITORI",
    symbol: "",
    nameEn: "Natural increase per 1000 inhabitants",
  },
  "la 1000 femei": {
    code: "LA_1000_FEMEI",
    symbol: "",
    nameEn: "Per 1000 women",
  },
  "la 1000 nascuti-vii": {
    code: "LA_1000_NASCUTIVII",
    symbol: "",
    nameEn: "Per 1000 live births",
  },
  "cazuri noi la 100000 locuitori": {
    code: "CAZURI_NOI_LA_100000_LOCUITORI",
    symbol: "",
    nameEn: "New cases per 100000 inhabitants",
  },
  "rate la 1000 locuitori": {
    code: "RATE_LA_1000_LOCUITORI",
    symbol: "",
    nameEn: "Rates per 1000 inhabitants",
  },

  // === Area & Distance ===
  ha: { code: "HECTARES", symbol: "ha", nameEn: "Hectares" },
  hectare: { code: "HECTARES", symbol: "ha", nameEn: "Hectares" },
  "mii hectare": {
    code: "MII_HECTARE",
    symbol: "mii ha",
    nameEn: "Thousands of hectares",
  },
  kilometri: { code: "KILOMETRI", symbol: "km", nameEn: "Kilometers" },
  "metri patrati": {
    code: "METRI_PATRATI",
    symbol: "m²",
    nameEn: "Square meters",
  },
  "m.p. arie desfasurata": {
    code: "METRI_PATRATI_ARIE_DESFASURATA",
    symbol: "m²",
    nameEn: "Square meters (developed area)",
  },
  "metri patrati arie desfasurata": {
    code: "METRI_PATRATI_ARIE_DESFASURATA",
    symbol: "m²",
    nameEn: "Square meters (developed area)",
  },
  "metri patrati suprafata utila": {
    code: "METRI_PATRATI_SUPRAFATA_UTILA",
    symbol: "m²",
    nameEn: "Square meters (usable area)",
  },

  // === Volume ===
  litri: { code: "LITERS", symbol: "l", nameEn: "Liters" },
  "mii litri": {
    code: "THOUSAND_LITERS",
    symbol: "mii l",
    nameEn: "Thousands of liters",
  },
  "metri cubi": { code: "METRI_CUBI", symbol: "m³", nameEn: "Cubic meters" },
  "mii metri cubi": {
    code: "MII_METRI_CUBI",
    symbol: "mii m³",
    nameEn: "Thousands of cubic meters",
  },
  "milioane metri cubi": {
    code: "MILIOANE_METRI_CUBI",
    symbol: "mil. m³",
    nameEn: "Millions of cubic meters",
  },
  "milioane metri cubi 15 gr. c 760 mm hg": {
    code: "MILIOANE_METRI_CUBI_15_GR_C_760_MM_HG",
    symbol: "",
    nameEn: "Millions of cubic meters (15°C, 760mmHg)",
  },
  "metri cubi pe zi": {
    code: "METRI_CUBI_PE_ZI",
    symbol: "m³/zi",
    nameEn: "Cubic meters per day",
  },
  "10000 hl": {
    code: "10000_HL",
    symbol: "10000 hl",
    nameEn: "10000 hectoliters",
  },

  // === Weight ===
  kg: { code: "KG", symbol: "kg", nameEn: "Kilograms" },
  kilograme: { code: "KG", symbol: "kg", nameEn: "Kilograms" },
  "kilograme substanta activa": {
    code: "KILOGRAME_SUBSTANTA_ACTIVA",
    symbol: "kg",
    nameEn: "Kilograms of active substance",
  },
  tone: { code: "TONS", symbol: "t", nameEn: "Tons" },
  "mii tone": {
    code: "THOUSAND_TONS",
    symbol: "mii t",
    nameEn: "Thousands of tons",
  },
  "mii tone (gg)": {
    code: "MII_TONE_GG",
    symbol: "Gg",
    nameEn: "Thousands of tons (Gg)",
  },
  "tone (mg)": { code: "TONE_MG", symbol: "Mg", nameEn: "Tons (Mg)" },
  "tone (mg) echivalent co2": {
    code: "TONE_MG_ECHIVALENT_CO2",
    symbol: "Mg CO2eq",
    nameEn: "Tons (Mg) CO2 equivalent",
  },
  "tone (mg) echivalent n2o": {
    code: "TONE_MG_ECHIVALENT_N2O",
    symbol: "Mg N2O eq",
    nameEn: "Tons (Mg) N2O equivalent",
  },
  "tone 100% substanta activa": {
    code: "TONE_100_SUBSTANTA_ACTIVA",
    symbol: "t",
    nameEn: "Tons 100% active substance",
  },
  "tone dw": { code: "TONE_DW", symbol: "t DW", nameEn: "Tons (dry weight)" },
  "tone o2/ zi": {
    code: "TONE_O2_ZI",
    symbol: "t O2/zi",
    nameEn: "Tons O2 per day",
  },
  "tone registru brut": {
    code: "TONE_REGISTRU_BRUT",
    symbol: "TRB",
    nameEn: "Gross register tons",
  },
  "tone/tone": {
    code: "TONETONE",
    symbol: "",
    nameEn: "Tons per tons (ratio)",
  },
  "tone/mii lei preturile anului 2020": {
    code: "TONEMII_LEI_PRETURILE_ANULUI_2020",
    symbol: "",
    nameEn: "Tons per thousand lei (2020 prices)",
  },

  // === Currency - Romanian Lei ===
  lei: { code: "LEI", symbol: "lei", nameEn: "Lei (RON)" },
  "lei ron": { code: "LEI_RON", symbol: "lei", nameEn: "Lei (RON)" },
  "mii lei": {
    code: "THOUSAND_LEI",
    symbol: "mii lei",
    nameEn: "Thousands of lei",
  },
  "mii lei ron": {
    code: "MII_LEI_RON",
    symbol: "mii lei",
    nameEn: "Thousands of lei (RON)",
  },
  "milioane lei": {
    code: "MILLION_LEI",
    symbol: "mil. lei",
    nameEn: "Millions of lei",
  },
  "milioane lei ron": {
    code: "MILIOANE_LEI_RON",
    symbol: "mil. lei",
    nameEn: "Millions of lei (RON)",
  },
  "miliarde lei": {
    code: "MILIARDE_LEI",
    symbol: "mld. lei",
    nameEn: "Billions of lei",
  },
  "lei / buc": { code: "LEI__BUC", symbol: "lei/buc", nameEn: "Lei per piece" },
  "lei / kg": { code: "LEI__KG", symbol: "lei/kg", nameEn: "Lei per kilogram" },
  "lei / litru": {
    code: "LEI__LITRU",
    symbol: "lei/l",
    nameEn: "Lei per liter",
  },
  "lei / ora": { code: "LEI__ORA", symbol: "lei/ora", nameEn: "Lei per hour" },
  "lei / persoana": {
    code: "LEI__PERSOANA",
    symbol: "lei/pers",
    nameEn: "Lei per person",
  },
  "lei  / tona": { code: "LEI__TONA", symbol: "lei/t", nameEn: "Lei per ton" },
  "lei  / 10 hl": {
    code: "LEI__10_HL",
    symbol: "lei/10hl",
    nameEn: "Lei per 10 hectoliters",
  },
  "lei/ fir": { code: "LEI_FIR", symbol: "lei/fir", nameEn: "Lei per strand" },
  "mii lei preturile anului 2020/tona": {
    code: "MII_LEI_PRETURILE_ANULUI_2020TONA",
    symbol: "",
    nameEn: "Thousands of lei (2020 prices) per ton",
  },

  // === Currency - Foreign ===
  euro: { code: "EURO", symbol: "€", nameEn: "Euro" },
  "mii euro": {
    code: "THOUSAND_EURO",
    symbol: "mii €",
    nameEn: "Thousands of euro",
  },
  "milioane euro": {
    code: "MILLION_EURO",
    symbol: "mil. €",
    nameEn: "Millions of euro",
  },
  "mii dolari (usd)": {
    code: "MII_DOLARI_USD",
    symbol: "mii $",
    nameEn: "Thousands of USD",
  },
  "milioane dolari (usd)": {
    code: "MILIOANE_DOLARI_USD",
    symbol: "mil. $",
    nameEn: "Millions of USD",
  },

  // === Time ===
  ani: { code: "YEARS", symbol: "ani", nameEn: "Years" },
  luni: { code: "MONTHS", symbol: "luni", nameEn: "Months" },
  zile: { code: "DAYS", symbol: "zile", nameEn: "Days" },
  ore: { code: "ORE", symbol: "ore", nameEn: "Hours" },
  "mii ore": {
    code: "MII_ORE",
    symbol: "mii ore",
    nameEn: "Thousands of hours",
  },
  "ore - om": { code: "ORE__OM", symbol: "ore-om", nameEn: "Man-hours" },
  "zile-turist": {
    code: "TOURIST_DAYS",
    symbol: "zile-turist",
    nameEn: "Tourist days",
  },

  // === Energy ===
  gigacalorii: { code: "GIGACALORII", symbol: "Gcal", nameEn: "Gigacalories" },
  "mii gigacalorii": {
    code: "MII_GIGACALORII",
    symbol: "mii Gcal",
    nameEn: "Thousands of gigacalories",
  },
  terrajouli: { code: "TERRAJOULI", symbol: "TJ", nameEn: "Terajoules" },
  "mii kilowatti": {
    code: "MII_KILOWATTI",
    symbol: "mii kW",
    nameEn: "Thousands of kilowatts",
  },
  "milioane kilowatti-ora": {
    code: "MILIOANE_KILOWATTIORA",
    symbol: "mil. kWh",
    nameEn: "Millions of kilowatt-hours",
  },
  "kg echivalent petrol": {
    code: "KG_ECHIVALENT_PETROL",
    symbol: "kgep",
    nameEn: "Kilograms of oil equivalent",
  },
  "mii tone echivalent petrol": {
    code: "MII_TONE_ECHIVALENT_PETROL",
    symbol: "mii tep",
    nameEn: "Thousands of tons of oil equivalent",
  },

  // === Transport ===
  "mii pasageri": {
    code: "MII_PASAGERI",
    symbol: "mii pas.",
    nameEn: "Thousands of passengers",
  },
  "milioane pasageri-km": {
    code: "MILIOANE_PASAGERIKM",
    symbol: "mil. pas-km",
    nameEn: "Millions of passenger-kilometers",
  },
  "mii tone-kilometru": {
    code: "MII_TONEKILOMETRU",
    symbol: "mii t-km",
    nameEn: "Thousands of ton-kilometers",
  },
  "milioane tone-km": {
    code: "MILIOANE_TONEKM",
    symbol: "mil. t-km",
    nameEn: "Millions of ton-kilometers",
  },
  "mii vehicule-km": {
    code: "MII_VEHICULEKM",
    symbol: "mii veh-km",
    nameEn: "Thousands of vehicle-kilometers",
  },
  "numar sosiri": { code: "ARRIVALS", symbol: "sosiri", nameEn: "Arrivals" },
  "numar innoptari": {
    code: "OVERNIGHT_STAYS",
    symbol: "înnoptări",
    nameEn: "Overnight stays",
  },

  // === Technical/Other ===
  "cai putere": { code: "CAI_PUTERE", symbol: "CP", nameEn: "Horsepower" },
  "milioane linii echivalente echipate": {
    code: "MILIOANE_LINII_ECHIVALENTE_ECHIPATE",
    symbol: "",
    nameEn: "Millions of equipped equivalent lines",
  },
  "echivalent norma intreaga(eni)": {
    code: "ECHIVALENT_NORMA_INTREAGAENI",
    symbol: "ENI",
    nameEn: "Full-time equivalent (FTE)",
  },
  "1000 unitati anuale de munca (uam)": {
    code: "1000_UNITATI_ANUALE_DE_MUNCA_UAM",
    symbol: "mii UAM",
    nameEn: "Thousands of annual work units (AWU)",
  },
};

// ============================================================================
// Unit Service
// ============================================================================

export class UnitService {
  private cache = new Map<string, number>();

  constructor(private db: Kysely<Database>) {}

  /**
   * Find or create a unit of measure from a label
   */
  async findOrCreate(
    labelRo: string,
    labelEn?: string
  ): Promise<number | null> {
    // Extract unit name from "UM: ..." pattern or use label directly
    const match = UNIT_PATTERN.exec(labelRo);
    const unitName = match?.[1]?.trim() ?? labelRo.trim();

    if (!unitName) return null;

    const normalizedName = this.normalize(unitName);
    const cacheKey = `unit:${normalizedName}`;

    if (this.cache.has(cacheKey)) {
      return this.cache.get(cacheKey) ?? null;
    }

    // Try to find known mapping
    const mapping = UNIT_MAPPINGS[normalizedName.toLowerCase()];
    const code = mapping?.code ?? this.generateCode(unitName);
    const symbol = mapping?.symbol ?? null;
    const nameEn = labelEn ?? mapping?.nameEn ?? unitName;

    // Check if exists
    const existing = await this.db
      .selectFrom("units_of_measure")
      .select(["id", "label_patterns"])
      .where("code", "=", code)
      .executeTakeFirst();

    if (existing) {
      // Add label to patterns if not present
      const patterns = existing.label_patterns;
      if (!patterns.includes(labelRo)) {
        const newPatterns = [...patterns, labelRo];
        await this.db
          .updateTable("units_of_measure")
          .set({ label_patterns: textArray(newPatterns) })
          .where("id", "=", existing.id)
          .execute();
      }
      this.cache.set(cacheKey, existing.id);
      return existing.id;
    }

    // Create new unit
    const names: BilingualText = {
      ro: unitName,
      en: nameEn,
      normalized: normalizedName,
    };

    const newUnit = {
      code,
      symbol,
      names: jsonb(names),
      label_patterns: textArray([labelRo]),
    };

    const result = await this.db
      .insertInto("units_of_measure")
      .values(newUnit)
      .returning("id")
      .executeTakeFirst();

    const id = result?.id ?? null;
    if (id) {
      this.cache.set(cacheKey, id);
      logger.debug({ code, unitName, id }, "Created unit of measure");
    }
    return id;
  }

  /**
   * Check if a label is a unit dimension label
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
    this.cache.clear();
  }
}
