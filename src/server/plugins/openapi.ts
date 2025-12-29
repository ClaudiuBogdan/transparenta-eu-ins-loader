/**
 * OpenAPI Plugin - Generates OpenAPI 3.0 specification
 */

import swagger from "@fastify/swagger";
import fp from "fastify-plugin";

import type { FastifyInstance } from "fastify";

function openapiPlugin(
  fastify: FastifyInstance,
  _opts: Record<string, unknown>,
  done: () => void
): void {
  void fastify.register(swagger, {
    openapi: {
      openapi: "3.0.3",
      info: {
        title: "INS Tempo API",
        description:
          "REST API for Romanian statistical data from INS (Institutul Național de Statistică) Tempo Online. " +
          "This API provides access to demographic, economic, and social statistics organized in a hierarchical " +
          "context structure with matrices (datasets) containing multi-dimensional statistical data.",
        version: "1.0.0",
        contact: {
          name: "INS Tempo API",
        },
      },
      servers: [
        {
          url: "http://localhost:3000",
          description: "Local development server",
        },
      ],
      tags: [
        {
          name: "Discovery",
          description:
            "Browse the hierarchical context structure and discover available matrices (datasets)",
        },
        {
          name: "Matrices",
          description:
            "Get matrix details, dimensions, and available dimension values",
        },
        {
          name: "Territories",
          description:
            "NUTS/LAU territorial hierarchy - regions, counties, and localities (UATs)",
        },
        {
          name: "Time Periods",
          description:
            "Available time periods (years, months, quarters) for statistical data",
        },
        {
          name: "Classifications",
          description:
            "Classification systems used to categorize statistical dimensions",
        },
        {
          name: "Statistics",
          description:
            "Query statistical data from matrices with filtering and time series output",
        },
      ],
    },
  });

  done();
}

export const openapi = fp(openapiPlugin, { name: "openapi" });
