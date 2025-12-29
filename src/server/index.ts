import cors from "@fastify/cors";
import Fastify from "fastify";

import { fastifyLoggerConfig } from "../logger.js";
import { errorHandler } from "./plugins/error-handler.js";
import { openapi } from "./plugins/openapi.js";
import { registerApiRoutes } from "./routes/index.js";

const PORT = Number.parseInt(process.env.PORT ?? "3000", 10);
const HOST = process.env.HOST ?? "0.0.0.0";

const app = Fastify({
  logger: fastifyLoggerConfig,
});

// Register plugins
await app.register(cors, {
  origin: true,
});

// Register OpenAPI (must be before routes)
await app.register(openapi);

// Register error handler
await app.register(errorHandler);

// Register API routes
await registerApiRoutes(app);

// OpenAPI spec endpoint
app.get("/openapi.json", { schema: { hide: true } }, () => app.swagger());

// Start server
try {
  await app.listen({ port: PORT, host: HOST });
  app.log.info({ host: HOST, port: PORT }, "Server started");
} catch (err) {
  app.log.error(err, "Failed to start server");
  process.exit(1);
}
