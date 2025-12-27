import cors from "@fastify/cors";
import Fastify from "fastify";

import { fastifyLoggerConfig } from "../logger.js";
import {
  fetchContexts,
  fetchContext,
  fetchMatricesList,
  fetchMatrix,
} from "../scraper/client.js";

const PORT = parseInt(process.env.PORT ?? "3000", 10);
const HOST = process.env.HOST ?? "0.0.0.0";

const app = Fastify({
  logger: fastifyLoggerConfig,
});

await app.register(cors, {
  origin: true,
});

// Health check
app.get("/health", () => {
  return { status: "ok" };
});

// Navigation endpoints

app.get("/api/contexts", async () => {
  const contexts = await fetchContexts();
  return { data: contexts };
});

app.get<{ Params: { id: string } }>("/api/contexts/:id", async (request) => {
  const { id } = request.params;
  const context = await fetchContext(parseInt(id, 10));
  return { data: context };
});

app.get<{ Querystring: { uatOnly?: string } }>(
  "/api/matrices",
  async (request) => {
    const matrices = await fetchMatricesList();

    // If uatOnly filter is requested, we'd need to fetch each matrix's metadata
    // For now, return the full list
    if (request.query.uatOnly === "true") {
      // TODO: Filter by UAT capability (requires fetching metadata for each)
      return {
        data: matrices,
        warning: "UAT filtering requires matrix metadata - returning all",
      };
    }

    return { data: matrices };
  }
);

app.get<{ Params: { code: string } }>(
  "/api/matrices/:code",
  async (request) => {
    const { code } = request.params;
    const matrix = await fetchMatrix(code);
    return { data: matrix };
  }
);

// Scraping endpoints (placeholder)

app.post<{ Params: { code: string } }>(
  "/api/scrape/matrix/:code",
  async (request, reply) => {
    const { code } = request.params;

    // TODO: Implement actual scraping logic
    return reply.status(501).send({
      error: "Not implemented",
      message: `Scraping for matrix ${code} is not yet implemented`,
    });
  }
);

// Data endpoints (placeholder)

app.get<{ Params: { matrixCode: string } }>(
  "/api/data/:matrixCode",
  async (request, reply) => {
    const { matrixCode } = request.params;

    // TODO: Query from local database
    return reply.status(501).send({
      error: "Not implemented",
      message: `Data query for matrix ${matrixCode} is not yet implemented`,
    });
  }
);

app.get("/api/siruta", async (_request, reply) => {
  // TODO: Query SIRUTA from local database
  return reply.status(501).send({
    error: "Not implemented",
    message: "SIRUTA query is not yet implemented",
  });
});

// Start server
try {
  await app.listen({ port: PORT, host: HOST });
  app.log.info({ host: HOST, port: PORT }, "Server started");
} catch (err) {
  app.log.error(err, "Failed to start server");
  process.exit(1);
}
