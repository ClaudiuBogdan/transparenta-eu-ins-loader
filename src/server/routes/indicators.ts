/**
 * Indicators Routes - /api/v1/indicators
 * Composite indicators calculated from multiple matrices
 */

import { Type, type Static } from "@sinclair/typebox";
import { sql } from "kysely";

import { db } from "../../db/connection.js";
import { NotFoundError } from "../plugins/error-handler.js";
import { LocaleSchema, type Locale } from "../schemas/common.js";

import type { CompositeIndicatorConfig } from "../../db/types.js";
import type { FastifyInstance } from "fastify";

// ============================================================================
// Schemas
// ============================================================================

const ListIndicatorsQuerySchema = Type.Object({
  locale: Type.Optional(LocaleSchema),
  category: Type.Optional(Type.String({ description: "Filter by category" })),
  activeOnly: Type.Optional(
    Type.Boolean({ description: "Only active indicators", default: true })
  ),
});

type ListIndicatorsQuery = Static<typeof ListIndicatorsQuerySchema>;

const IndicatorCodeParamSchema = Type.Object({
  code: Type.String({ description: "Indicator code" }),
});

const CalculateIndicatorQuerySchema = Type.Object({
  locale: Type.Optional(LocaleSchema),
  territoryId: Type.Optional(Type.Number({ description: "Territory ID" })),
  year: Type.Number({ description: "Year for calculation" }),
});

type CalculateIndicatorQuery = Static<typeof CalculateIndicatorQuerySchema>;

// ============================================================================
// Helper Functions
// ============================================================================

function getLocalizedName(
  name: string,
  nameEn: string | null,
  locale: Locale
): string {
  return locale === "en" && nameEn ? nameEn : name;
}

// ============================================================================
// Routes
// ============================================================================

export function registerIndicatorRoutes(app: FastifyInstance): void {
  /**
   * GET /api/v1/indicators
   * List all composite indicators
   */
  app.get<{ Querystring: ListIndicatorsQuery }>(
    "/indicators",
    {
      schema: {
        summary: "List composite indicators",
        description:
          "Get all available composite indicators with their formulas and configurations.",
        tags: ["Indicators"],
        querystring: ListIndicatorsQuerySchema,
      },
    },
    async (request) => {
      const { locale = "ro", category, activeOnly = true } = request.query;

      let query = db
        .selectFrom("composite_indicators")
        .select([
          "id",
          "code",
          "name",
          "name_en",
          "formula",
          "unit_code",
          "config",
          "category",
          "is_active",
          "created_at",
        ]);

      if (activeOnly) {
        query = query.where("is_active", "=", true);
      }

      if (category) {
        query = query.where("category", "=", category);
      }

      const rows = await query.orderBy("category").orderBy("name").execute();

      const indicators = rows.map((r) => ({
        id: r.id,
        code: r.code,
        name: getLocalizedName(r.name, r.name_en, locale),
        formula: r.formula,
        unitCode: r.unit_code,
        category: r.category,
        isActive: r.is_active,
        config: {
          description: describeConfig(r.config),
          requiredMatrices: getRequiredMatrices(r.config),
        },
      }));

      // Group by category
      const byCategory: Record<string, typeof indicators> = {};
      for (const ind of indicators) {
        const cat = ind.category ?? "other";
        byCategory[cat] ??= [];
        byCategory[cat].push(ind);
      }

      return {
        data: {
          indicators,
          byCategory,
          total: indicators.length,
        },
      };
    }
  );

  /**
   * GET /api/v1/indicators/:code
   * Calculate a specific indicator
   */
  app.get<{
    Params: Static<typeof IndicatorCodeParamSchema>;
    Querystring: CalculateIndicatorQuery;
  }>(
    "/indicators/:code",
    {
      schema: {
        summary: "Calculate indicator",
        description:
          "Calculate a composite indicator for a specific territory and year. " +
          "Example: `/indicators/population-density?territoryId=1&year=2023`",
        tags: ["Indicators"],
        params: IndicatorCodeParamSchema,
        querystring: CalculateIndicatorQuerySchema,
      },
    },
    async (request) => {
      const { code } = request.params;
      const { locale = "ro", territoryId, year } = request.query;

      // Get indicator definition
      const indicator = await db
        .selectFrom("composite_indicators")
        .select([
          "id",
          "code",
          "name",
          "name_en",
          "formula",
          "unit_code",
          "config",
          "category",
        ])
        .where("code", "=", code)
        .where("is_active", "=", true)
        .executeTakeFirst();

      if (!indicator) {
        throw new NotFoundError(`Indicator with code ${code} not found`);
      }

      const config = indicator.config;

      // Calculate indicator based on config
      const result = await calculateIndicator(config, year, territoryId);

      // Get territory info if specified
      let territory = null;
      if (territoryId) {
        const t = await db
          .selectFrom("territories")
          .select(["id", "code", "name", "level"])
          .where("id", "=", territoryId)
          .executeTakeFirst();

        if (t) {
          territory = {
            id: t.id,
            code: t.code,
            name: t.name,
            level: t.level,
          };
        }
      }

      // Get unit info if specified
      let unit = null;
      if (indicator.unit_code) {
        const u = await db
          .selectFrom("units_of_measure")
          .select(["code", "symbol", "names"])
          .where("code", "=", indicator.unit_code)
          .executeTakeFirst();

        if (u) {
          unit = {
            code: u.code,
            symbol: u.symbol,
            name: locale === "en" && u.names.en ? u.names.en : u.names.ro,
          };
        }
      }

      return {
        data: {
          indicator: {
            code: indicator.code,
            name: getLocalizedName(indicator.name, indicator.name_en, locale),
            formula: indicator.formula,
            category: indicator.category,
          },
          calculation: {
            year,
            territory,
            value: result.value,
            components: result.components,
            unit,
          },
        },
      };
    }
  );
}

// ============================================================================
// Helper Functions for Indicator Calculation
// ============================================================================

function describeConfig(config: CompositeIndicatorConfig): string {
  const parts: string[] = [];

  if (Array.isArray(config.numerator)) {
    parts.push(
      `Numerator: Sum of ${String(config.numerator.length)} components`
    );
  } else {
    parts.push(
      `Numerator: ${config.numerator.description ?? config.numerator.matrixCode}`
    );
  }

  if (config.denominator) {
    parts.push(
      `Denominator: ${config.denominator.description ?? config.denominator.matrixCode}`
    );
  }

  if (config.multiplier && config.multiplier !== 1) {
    parts.push(`Multiplier: ${String(config.multiplier)}`);
  }

  return parts.join("; ");
}

function getRequiredMatrices(config: CompositeIndicatorConfig): string[] {
  const matrices: string[] = [];

  if (Array.isArray(config.numerator)) {
    for (const n of config.numerator) {
      if (!matrices.includes(n.matrixCode)) {
        matrices.push(n.matrixCode);
      }
    }
  } else {
    matrices.push(config.numerator.matrixCode);
  }

  if (config.denominator) {
    if (!matrices.includes(config.denominator.matrixCode)) {
      matrices.push(config.denominator.matrixCode);
    }
  }

  return matrices;
}

interface CalculationResult {
  value: number | null;
  components: {
    numerator: number | null;
    denominator: number | null;
    multiplier: number;
  };
}

async function calculateIndicator(
  config: CompositeIndicatorConfig,
  year: number,
  territoryId?: number
): Promise<CalculationResult> {
  const multiplier = config.multiplier ?? 1;

  // Calculate numerator
  let numeratorValue: number | null = null;

  if (Array.isArray(config.numerator)) {
    // Sum of multiple components
    let sum = 0;
    let hasValue = false;

    for (const comp of config.numerator) {
      const value = await getMatrixValue(
        comp.matrixCode,
        year,
        territoryId,
        comp.filter
      );
      if (value !== null) {
        sum += value;
        hasValue = true;
      }
    }

    numeratorValue = hasValue ? sum : null;
  } else {
    numeratorValue = await getMatrixValue(
      config.numerator.matrixCode,
      year,
      territoryId,
      config.numerator.filter
    );
  }

  // Calculate denominator
  let denominatorValue: number | null = null;

  if (config.denominator) {
    denominatorValue = await getMatrixValue(
      config.denominator.matrixCode,
      year,
      territoryId,
      config.denominator.filter
    );
  }

  // Calculate result
  let value: number | null = null;

  if (numeratorValue !== null) {
    if (config.denominator) {
      if (denominatorValue !== null && denominatorValue !== 0) {
        value = (numeratorValue / denominatorValue) * multiplier;
        value = Math.round(value * 100) / 100;
      }
    } else {
      value = numeratorValue * multiplier;
      value = Math.round(value * 100) / 100;
    }
  }

  return {
    value,
    components: {
      numerator: numeratorValue,
      denominator: denominatorValue,
      multiplier,
    },
  };
}

async function getMatrixValue(
  matrixCode: string,
  year: number,
  territoryId?: number,
  filter?: Record<string, unknown>
): Promise<number | null> {
  // Get matrix ID
  const matrix = await db
    .selectFrom("matrices")
    .select("id")
    .where("ins_code", "=", matrixCode)
    .executeTakeFirst();

  if (!matrix) {
    return null;
  }

  // Build query
  let query = db
    .selectFrom("statistics")
    .innerJoin("time_periods", "statistics.time_period_id", "time_periods.id")
    .select(sql<number>`SUM(statistics.value)`.as("total"))
    .where("statistics.matrix_id", "=", matrix.id)
    .where("time_periods.year", "=", year)
    .where("time_periods.periodicity", "=", "ANNUAL");

  if (territoryId) {
    query = query.where("statistics.territory_id", "=", territoryId);
  }

  // Apply classification filters if specified
  if (filter) {
    // This is a simplified filter - in production you'd want more sophisticated filtering
    // based on classification values
  }

  const result = await query.executeTakeFirst();

  if (result?.total == null) {
    return null;
  }
  return result.total;
}
