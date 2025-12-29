/**
 * Time Period Routes - /api/v1/time-periods
 */

import { Type, type Static } from "@sinclair/typebox";

import { db } from "../../db/connection.js";
import {
  parseLimit,
  validateCursor,
  createPaginationMeta,
} from "../../utils/pagination.js";
import {
  PaginationQuerySchema,
  PeriodicitySchema,
  YearRangeQuerySchema,
} from "../schemas/common.js";
import { TimePeriodListResponseSchema } from "../schemas/responses.js";

import type { TimePeriodDto } from "../../types/api.js";
import type { FastifyInstance } from "fastify";

// ============================================================================
// Schemas
// ============================================================================

const ListTimePeriodsQuerySchema = Type.Intersect([
  PaginationQuerySchema,
  YearRangeQuerySchema,
  Type.Object({
    periodicity: Type.Optional(PeriodicitySchema),
    matrixCode: Type.Optional(
      Type.String({
        description: "Filter by matrix code to get available periods",
      })
    ),
  }),
]);

type ListTimePeriodsQuery = Static<typeof ListTimePeriodsQuerySchema>;

// ============================================================================
// Routes
// ============================================================================

export function registerTimePeriodRoutes(app: FastifyInstance): void {
  /**
   * GET /api/v1/time-periods
   * List time periods with filtering
   */
  app.get<{ Querystring: ListTimePeriodsQuery }>(
    "/time-periods",
    {
      schema: {
        summary: "List time periods",
        description:
          "Get available time periods with filtering. Periods can be annual, quarterly, or monthly. " +
          "Examples:\n- `/time-periods?periodicity=ANNUAL&yearFrom=2020` - Annual periods from 2020\n" +
          "- `/time-periods?matrixCode=POP105A` - Available periods for a specific matrix",
        tags: ["Time Periods"],
        querystring: ListTimePeriodsQuerySchema,
        response: {
          200: TimePeriodListResponseSchema,
        },
      },
    },
    async (request) => {
      const {
        periodicity,
        yearFrom,
        yearTo,
        matrixCode,
        limit: rawLimit,
        cursor,
      } = request.query;

      const limit = parseLimit(rawLimit, 100, 500);
      const cursorPayload = validateCursor(cursor);

      let query = db
        .selectFrom("time_periods")
        .select([
          "id",
          "year",
          "quarter",
          "month",
          "periodicity",
          "ins_label",
          "period_start",
          "period_end",
        ]);

      // Filter by matrix if specified
      if (matrixCode) {
        const matrix = await db
          .selectFrom("matrices")
          .select("id")
          .where("ins_code", "=", matrixCode)
          .executeTakeFirst();

        if (matrix) {
          query = query.where(
            "id",
            "in",
            db
              .selectFrom("matrix_dimension_options")
              .innerJoin(
                "matrix_dimensions",
                "matrix_dimension_options.matrix_dimension_id",
                "matrix_dimensions.id"
              )
              .select("matrix_dimension_options.time_period_id")
              .where("matrix_dimensions.matrix_id", "=", matrix.id)
              .where("matrix_dimension_options.time_period_id", "is not", null)
          );
        }
      }

      // Apply filters
      if (periodicity) {
        query = query.where("periodicity", "=", periodicity);
      }

      if (yearFrom !== undefined) {
        query = query.where("year", ">=", yearFrom);
      }

      if (yearTo !== undefined) {
        query = query.where("year", "<=", yearTo);
      }

      // Apply cursor
      if (cursorPayload) {
        query = query.where((eb) =>
          eb.or([
            eb("year", ">", cursorPayload.sortValue as number),
            eb.and([
              eb("year", "=", cursorPayload.sortValue as number),
              eb("id", ">", cursorPayload.id),
            ]),
          ])
        );
      }

      const rows = await query
        .orderBy("year", "asc")
        .orderBy("quarter", "asc")
        .orderBy("month", "asc")
        .orderBy("id", "asc")
        .limit(limit + 1)
        .execute();

      const hasMore = rows.length > limit;
      const items: TimePeriodDto[] = rows.slice(0, limit).map((tp) => ({
        id: tp.id,
        year: tp.year,
        quarter: tp.quarter,
        month: tp.month,
        periodicity: tp.periodicity,
        insLabel: tp.ins_label,
        periodStart: tp.period_start?.toISOString().split("T")[0] ?? "",
        periodEnd: tp.period_end?.toISOString().split("T")[0] ?? "",
      }));

      return {
        data: items,
        meta: {
          pagination: createPaginationMeta(items, limit, "year", hasMore),
        },
      };
    }
  );
}
