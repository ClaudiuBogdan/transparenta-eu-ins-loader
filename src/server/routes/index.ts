/**
 * API Routes Registration
 */

import { Type } from "@sinclair/typebox";

import { registerClassificationRoutes } from "./classifications.js";
import { registerContextRoutes } from "./contexts.js";
import { registerMatrixRoutes } from "./matrices.js";
import { registerStatisticsRoutes } from "./statistics.js";
import { registerTerritoryRoutes } from "./territories.js";
import { registerTimePeriodRoutes } from "./time-periods.js";

import type { FastifyInstance } from "fastify";

// Health check response schema
const HealthResponseSchema = Type.Object(
  {
    status: Type.Literal("ok"),
  },
  {
    examples: [{ status: "ok" }],
  }
);

/**
 * Register all API v1 routes
 */
export async function registerApiRoutes(app: FastifyInstance): Promise<void> {
  // Health check (no version prefix)
  app.get(
    "/health",
    {
      schema: {
        summary: "Health check",
        description: "Returns the health status of the API",
        tags: ["Health"],
        response: {
          200: HealthResponseSchema,
        },
      },
    },
    () => ({ status: "ok" as const })
  );

  // API v1 routes
  await app.register(
    (api) => {
      // Discovery endpoints
      registerContextRoutes(api);
      registerMatrixRoutes(api);

      // Dimension endpoints
      registerTerritoryRoutes(api);
      registerTimePeriodRoutes(api);
      registerClassificationRoutes(api);

      // Data query endpoints
      registerStatisticsRoutes(api);
    },
    { prefix: "/api/v1" }
  );
}
