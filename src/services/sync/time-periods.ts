import { logger } from "../../logger.js";

import type {
  Database,
  PeriodicityType,
  NewTimePeriod,
} from "../../db/types.js";
import type { Kysely } from "kysely";

// ============================================================================
// Parsing Patterns
// ============================================================================

// Standard patterns
const ANNUAL_PATTERN = /Anul\s+(\d{4})/i;
const QUARTERLY_PATTERN = /Trimestrul\s+(I{1,3}V?|IV)\s+(\d{4})/i;
const MONTHLY_PATTERN = /Luna\s+(\w+)\s+(\d{4})/i;

// Additional patterns for flexibility
const YEAR_ONLY_PATTERN = /^(\d{4})$/; // "2023"
const SHORT_QUARTER_PATTERN = /^T([1-4])\s+(\d{4})$/i; // "T1 2024"
const ALT_MONTHLY_PATTERN = /^(\w{3})\s+(\d{4})$/i; // "Ian 2024"
const YEAR_RANGE_START_PATTERN = /^(\d{4})\s*[-–]\s*\d{4}$/; // "2020-2024" -> use start year
const YEAR_RANGE_ANII_PATTERN = /^Ani+i?\s+(\d{4})\s*[-–]\s*(\d{4})$/i; // "Anii 1901 - 2000" -> use end year

// Patterns for labels that are NOT time periods (return null)
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

const QUARTER_MAP: Record<string, number> = {
  I: 1,
  II: 2,
  III: 3,
  IV: 4,
};

// ============================================================================
// Types
// ============================================================================

interface ParsedTimePeriod {
  year: number;
  quarter?: number;
  month?: number;
  periodicity: PeriodicityType;
}

// ============================================================================
// Time Period Service
// ============================================================================

export class TimePeriodService {
  constructor(private db: Kysely<Database>) {}

  /**
   * Find or create a time period from an INS label
   * Returns the time_period ID
   */
  async findOrCreate(insLabel: string): Promise<number | null> {
    const parsed = this.parseLabel(insLabel);
    if (parsed === null) {
      logger.warn({ insLabel }, "Could not parse time period label");
      return null;
    }

    // Check for existing
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
      return existing.id;
    }

    // Compute period start and end
    const { periodStart, periodEnd } = this.computePeriodBounds(parsed);

    // Insert new
    const newPeriod: NewTimePeriod = {
      year: parsed.year,
      quarter: parsed.quarter ?? null,
      month: parsed.month ?? null,
      periodicity: parsed.periodicity,
      ins_label: insLabel.trim(),
      period_start: periodStart,
      period_end: periodEnd,
    };

    const result = await this.db
      .insertInto("time_periods")
      .values(newPeriod)
      .returning("id")
      .executeTakeFirst();

    logger.debug({ insLabel, parsed, id: result?.id }, "Created time period");
    return result?.id ?? null;
  }

  /**
   * Parse an INS time label into components
   */
  parseLabel(label: string): ParsedTimePeriod | null {
    const trimmed = label.trim();

    // Skip labels that are NOT time periods
    if (MONTH_ONLY_PATTERN.test(trimmed)) {
      // Month without year is ambiguous - not a valid time period
      return null;
    }

    if (/^Total$/i.test(trimmed)) {
      // "Total" is not a time period
      return null;
    }

    // Year range with "Anii": "Anii 1901 - 2000" -> use end year
    const yearRangeAniiMatch = YEAR_RANGE_ANII_PATTERN.exec(trimmed);
    if (yearRangeAniiMatch?.[2] !== undefined) {
      return {
        year: Number.parseInt(yearRangeAniiMatch[2], 10),
        periodicity: "ANNUAL",
      };
    }

    // Annual: "Anul 2023"
    const annualMatch = ANNUAL_PATTERN.exec(trimmed);
    if (annualMatch?.[1] !== undefined) {
      return {
        year: Number.parseInt(annualMatch[1], 10),
        periodicity: "ANNUAL",
      };
    }

    // Quarterly: "Trimestrul I 2024", "Trimestrul IV 2023"
    const quarterlyMatch = QUARTERLY_PATTERN.exec(trimmed);
    if (quarterlyMatch?.[1] !== undefined && quarterlyMatch[2] !== undefined) {
      const quarterStr = quarterlyMatch[1];
      const quarter = QUARTER_MAP[quarterStr];
      if (quarter !== undefined) {
        return {
          year: Number.parseInt(quarterlyMatch[2], 10),
          quarter,
          periodicity: "QUARTERLY",
        };
      }
    }

    // Monthly: "Luna Ianuarie 2024"
    const monthlyMatch = MONTHLY_PATTERN.exec(trimmed);
    if (monthlyMatch?.[1] !== undefined && monthlyMatch[2] !== undefined) {
      const monthName = monthlyMatch[1];
      const monthIndex = MONTHS_RO.findIndex(
        (m) => m.toLowerCase() === monthName.toLowerCase()
      );
      if (monthIndex >= 0) {
        return {
          year: Number.parseInt(monthlyMatch[2], 10),
          month: monthIndex + 1,
          periodicity: "MONTHLY",
        };
      }
    }

    // Year only: "2023"
    const yearOnlyMatch = YEAR_ONLY_PATTERN.exec(trimmed);
    if (yearOnlyMatch?.[1] !== undefined) {
      return {
        year: Number.parseInt(yearOnlyMatch[1], 10),
        periodicity: "ANNUAL",
      };
    }

    // Short quarter: "T1 2024", "T2 2023"
    const shortQuarterMatch = SHORT_QUARTER_PATTERN.exec(trimmed);
    if (
      shortQuarterMatch?.[1] !== undefined &&
      shortQuarterMatch[2] !== undefined
    ) {
      return {
        year: Number.parseInt(shortQuarterMatch[2], 10),
        quarter: Number.parseInt(shortQuarterMatch[1], 10),
        periodicity: "QUARTERLY",
      };
    }

    // Alternative monthly: "Ian 2024", "Feb 2023"
    const altMonthlyMatch = ALT_MONTHLY_PATTERN.exec(trimmed);
    if (
      altMonthlyMatch?.[1] !== undefined &&
      altMonthlyMatch[2] !== undefined
    ) {
      const monthName = altMonthlyMatch[1];
      const monthIndex = MONTHS_RO_SHORT.findIndex(
        (m) => m.toLowerCase() === monthName.toLowerCase()
      );
      if (monthIndex >= 0) {
        return {
          year: Number.parseInt(altMonthlyMatch[2], 10),
          month: monthIndex + 1,
          periodicity: "MONTHLY",
        };
      }
    }

    // Year range: "2020-2024" -> use start year as annual
    const yearRangeMatch = YEAR_RANGE_START_PATTERN.exec(trimmed);
    if (yearRangeMatch?.[1] !== undefined) {
      return {
        year: Number.parseInt(yearRangeMatch[1], 10),
        periodicity: "ANNUAL",
      };
    }

    return null;
  }

  /**
   * Compute period start and end dates
   */
  private computePeriodBounds(parsed: ParsedTimePeriod): {
    periodStart: Date;
    periodEnd: Date;
  } {
    const year = parsed.year;

    if (parsed.periodicity === "ANNUAL") {
      return {
        periodStart: new Date(year, 0, 1), // January 1
        periodEnd: new Date(year, 11, 31), // December 31
      };
    }

    if (parsed.periodicity === "QUARTERLY" && parsed.quarter != null) {
      const quarterStartMonth = (parsed.quarter - 1) * 3;
      const quarterEndMonth = quarterStartMonth + 2;
      const lastDay = new Date(year, quarterEndMonth + 1, 0).getDate();

      return {
        periodStart: new Date(year, quarterStartMonth, 1),
        periodEnd: new Date(year, quarterEndMonth, lastDay),
      };
    }

    if (parsed.periodicity === "MONTHLY" && parsed.month != null) {
      const monthIndex = parsed.month - 1;
      const lastDay = new Date(year, monthIndex + 1, 0).getDate();

      return {
        periodStart: new Date(year, monthIndex, 1),
        periodEnd: new Date(year, monthIndex, lastDay),
      };
    }

    // Fallback (should not happen)
    return {
      periodStart: new Date(year, 0, 1),
      periodEnd: new Date(year, 11, 31),
    };
  }

  /**
   * Check if a label is a time period
   */
  isTimePeriodLabel(label: string): boolean {
    return this.parseLabel(label) !== null;
  }
}
