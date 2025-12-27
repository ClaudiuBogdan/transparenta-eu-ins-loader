import pino from "pino";

const LOG_LEVEL = process.env.LOG_LEVEL ?? "info";

export const logger = pino({
  level: LOG_LEVEL,
  // In production, use JSON format for log aggregation
  // In development, pino-pretty is piped via npm script
});

// Child loggers for different modules
export const apiLogger = logger.child({ module: "ins-api" });
export const dbLogger = logger.child({ module: "database" });
export const serverLogger = logger.child({ module: "server" });
