import { jsonb } from "../../../db/connection.js";
import { logger } from "../../../logger.js";

import type {
  Database,
  Periodicity,
  BilingualText,
} from "../../../db/types.js";
import type { Kysely } from "kysely";

// ============================================================================
// Parsing Patterns
// ============================================================================

const ANNUAL_PATTERN = /Anul\s+(\d{4})/i;
const QUARTERLY_PATTERN = /Trimestrul\s+(I{1,3}V?|IV)\s+(\d{4})/i;
const MONTHLY_PATTERN = /Luna\s+(\w+)\s+(\d{4})/i;
const YEAR_ONLY_PATTERN = /^(\d{4})$/;
const SHORT_QUARTER_PATTERN = /^T([1-4])\s+(\d{4})$/i;
const ALT_MONTHLY_PATTERN = /^(\w{3})\s+(\d{4})$/i;
const YEAR_RANGE_START_PATTERN = /^(\d{4})\s*[-–]\s*\d{4}$/;
const YEAR_RANGE_ANII_PATTERN = /^Ani+i?\s+(\d{4})\s*[-–]\s*(\d{4})$/i;
const MONTH_ONLY_PATTERN =
  /^(Ianuarie|Februarie|Martie|Aprilie|Mai|Iunie|Iulie|August|Septembrie|Octombrie|Noiembrie|Decembrie)$/i;

const MONTHS_RO = [
  "Ianuarie",
  "Februarie",
  "Martie",
  "Aprilie",
  "Mai",
  "Iunie",
  "Iulie",
  "August",
  "Septembrie",
  "Octombrie",
  "Noiembrie",
  "Decembrie",
];

const MONTHS_EN = [
  "January",
  "February",
  "March",
  "April",
  "May",
  "June",
  "July",
  "August",
  "September",
  "October",
  "November",
  "December",
];

const MONTHS_RO_SHORT = [
  "Ian",
  "Feb",
  "Mar",
  "Apr",
  "Mai",
  "Iun",
  "Iul",
  "Aug",
  "Sep",
  "Oct",
  "Nov",
  "Dec",
];

const QUARTER_MAP: Record<string, number> = { I: 1, II: 2, III: 3, IV: 4 };
const QUARTER_NAMES_EN = ["Quarter 1", "Quarter 2", "Quarter 3", "Quarter 4"];

// ============================================================================
// Types
// ============================================================================

interface ParsedTimePeriod {
  year: number;
  quarter?: number;
  month?: number;
  periodicity: Periodicity;
}

// ============================================================================
// Time Period Service
// ============================================================================

export class TimePeriodService {
  private cache = new Map<string, number | null>();

  constructor(private db: Kysely<Database>) {}

  /**
   * Find or create a time period from an INS label
   */
  async findOrCreate(
    labelRo: string,
    labelEn?: string
  ): Promise<number | null> {
    const cacheKey = `time:${labelRo}`;
    if (this.cache.has(cacheKey)) {
      return this.cache.get(cacheKey) ?? null;
    }

    const parsed = this.parseLabel(labelRo);
    if (parsed === null) {
      logger.warn({ labelRo }, "Could not parse time period label");
      this.cache.set(cacheKey, null);
      return null;
    }

    // Check existing
    let query = this.db
      .selectFrom("time_periods")
      .select("id")
      .where("year", "=", parsed.year)
      .where("periodicity", "=", parsed.periodicity);

    if (parsed.quarter !== undefined) {
      query = query.where("quarter", "=", parsed.quarter);
    } else {
      query = query.where("quarter", "is", null);
    }

    if (parsed.month !== undefined) {
      query = query.where("month", "=", parsed.month);
    } else {
      query = query.where("month", "is", null);
    }

    const existing = await query.executeTakeFirst();

    if (existing !== undefined) {
      this.cache.set(cacheKey, existing.id);
      return existing.id;
    }

    // Compute period bounds and labels
    const { periodStart, periodEnd } = this.computePeriodBounds(parsed);
    const labels = this.generateLabels(parsed, labelRo, labelEn);

    const newPeriod = {
      year: parsed.year,
      quarter: parsed.quarter ?? null,
      month: parsed.month ?? null,
      periodicity: parsed.periodicity,
      period_start: periodStart,
      period_end: periodEnd,
      labels: jsonb(labels),
    };

    const result = await this.db
      .insertInto("time_periods")
      .values(newPeriod)
      .returning("id")
      .executeTakeFirst();

    const id = result?.id ?? null;
    if (id) {
      this.cache.set(cacheKey, id);
      logger.debug({ labelRo, parsed, id }, "Created time period");
    }
    return id;
  }

  /**
   * Parse an INS time label into components
   */
  parseLabel(label: string): ParsedTimePeriod | null {
    const trimmed = label.trim();

    // Skip non-time-period labels
    if (MONTH_ONLY_PATTERN.test(trimmed)) return null;
    if (/^Total$/i.test(trimmed)) return null;

    // Year range with "Anii": use end year
    const yearRangeAniiMatch = YEAR_RANGE_ANII_PATTERN.exec(trimmed);
    if (yearRangeAniiMatch?.[2]) {
      return {
        year: Number.parseInt(yearRangeAniiMatch[2], 10),
        periodicity: "ANNUAL",
      };
    }

    // Annual: "Anul 2023"
    const annualMatch = ANNUAL_PATTERN.exec(trimmed);
    if (annualMatch?.[1]) {
      return {
        year: Number.parseInt(annualMatch[1], 10),
        periodicity: "ANNUAL",
      };
    }

    // Quarterly: "Trimestrul I 2024"
    const quarterlyMatch = QUARTERLY_PATTERN.exec(trimmed);
    if (quarterlyMatch?.[1] && quarterlyMatch[2]) {
      const quarter = QUARTER_MAP[quarterlyMatch[1]];
      if (quarter) {
        return {
          year: Number.parseInt(quarterlyMatch[2], 10),
          quarter,
          periodicity: "QUARTERLY",
        };
      }
    }

    // Monthly: "Luna Ianuarie 2024"
    const monthlyMatch = MONTHLY_PATTERN.exec(trimmed);
    if (monthlyMatch?.[1] && monthlyMatch[2]) {
      const monthName = monthlyMatch[1];
      const yearStr = monthlyMatch[2];
      const monthIndex = MONTHS_RO.findIndex(
        (m) => m.toLowerCase() === monthName.toLowerCase()
      );
      if (monthIndex >= 0) {
        return {
          year: Number.parseInt(yearStr, 10),
          month: monthIndex + 1,
          periodicity: "MONTHLY",
        };
      }
    }

    // Year only: "2023"
    const yearOnlyMatch = YEAR_ONLY_PATTERN.exec(trimmed);
    if (yearOnlyMatch?.[1]) {
      return {
        year: Number.parseInt(yearOnlyMatch[1], 10),
        periodicity: "ANNUAL",
      };
    }

    // Short quarter: "T1 2024"
    const shortQuarterMatch = SHORT_QUARTER_PATTERN.exec(trimmed);
    if (shortQuarterMatch?.[1] && shortQuarterMatch[2]) {
      return {
        year: Number.parseInt(shortQuarterMatch[2], 10),
        quarter: Number.parseInt(shortQuarterMatch[1], 10),
        periodicity: "QUARTERLY",
      };
    }

    // Alternative monthly: "Ian 2024"
    const altMonthlyMatch = ALT_MONTHLY_PATTERN.exec(trimmed);
    if (altMonthlyMatch?.[1] && altMonthlyMatch[2]) {
      const monthName = altMonthlyMatch[1];
      const yearStr = altMonthlyMatch[2];
      const monthIndex = MONTHS_RO_SHORT.findIndex(
        (m) => m.toLowerCase() === monthName.toLowerCase()
      );
      if (monthIndex >= 0) {
        return {
          year: Number.parseInt(yearStr, 10),
          month: monthIndex + 1,
          periodicity: "MONTHLY",
        };
      }
    }

    // Year range: "2020-2024" -> use start year
    const yearRangeMatch = YEAR_RANGE_START_PATTERN.exec(trimmed);
    if (yearRangeMatch?.[1]) {
      return {
        year: Number.parseInt(yearRangeMatch[1], 10),
        periodicity: "ANNUAL",
      };
    }

    return null;
  }

  /**
   * Compute period bounds using UTC to avoid timezone issues
   * FIX: Using Date.UTC() ensures dates are consistent regardless of server timezone
   */
  private computePeriodBounds(parsed: ParsedTimePeriod): {
    periodStart: Date;
    periodEnd: Date;
  } {
    const year = parsed.year;

    if (parsed.periodicity === "ANNUAL") {
      return {
        periodStart: new Date(Date.UTC(year, 0, 1)),
        periodEnd: new Date(Date.UTC(year, 11, 31)),
      };
    }

    if (parsed.periodicity === "QUARTERLY" && parsed.quarter != null) {
      const startMonth = (parsed.quarter - 1) * 3;
      const endMonth = startMonth + 2;
      // Get last day of quarter using UTC
      const lastDay = new Date(Date.UTC(year, endMonth + 1, 0)).getUTCDate();
      return {
        periodStart: new Date(Date.UTC(year, startMonth, 1)),
        periodEnd: new Date(Date.UTC(year, endMonth, lastDay)),
      };
    }

    if (parsed.periodicity === "MONTHLY" && parsed.month != null) {
      const monthIndex = parsed.month - 1;
      // Get last day of month using UTC
      const lastDay = new Date(Date.UTC(year, monthIndex + 1, 0)).getUTCDate();
      return {
        periodStart: new Date(Date.UTC(year, monthIndex, 1)),
        periodEnd: new Date(Date.UTC(year, monthIndex, lastDay)),
      };
    }

    return {
      periodStart: new Date(Date.UTC(year, 0, 1)),
      periodEnd: new Date(Date.UTC(year, 11, 31)),
    };
  }

  private generateLabels(
    parsed: ParsedTimePeriod,
    labelRo: string,
    labelEn?: string
  ): BilingualText {
    const ro = labelRo.trim();
    let en: string | undefined = labelEn?.trim();

    // Generate English label if not provided
    if (!en) {
      if (parsed.periodicity === "ANNUAL") {
        en = `Year ${String(parsed.year)}`;
      } else if (parsed.periodicity === "QUARTERLY" && parsed.quarter) {
        const quarterName =
          QUARTER_NAMES_EN[parsed.quarter - 1] ??
          `Quarter ${String(parsed.quarter)}`;
        en = `${quarterName} ${String(parsed.year)}`;
      } else if (parsed.periodicity === "MONTHLY" && parsed.month) {
        const monthName =
          MONTHS_EN[parsed.month - 1] ?? `Month ${String(parsed.month)}`;
        en = `${monthName} ${String(parsed.year)}`;
      }
    }

    return { ro, en };
  }

  isTimePeriodLabel(label: string): boolean {
    return this.parseLabel(label) !== null;
  }

  clearCache(): void {
    this.cache.clear();
  }
}
