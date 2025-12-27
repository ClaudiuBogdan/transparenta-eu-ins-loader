import "dotenv/config";

import { existsSync, mkdirSync } from "node:fs";
import { dirname } from "node:path";

import pino, { type DestinationStream, type LoggerOptions } from "pino";

const LOG_LEVEL = process.env.LOG_LEVEL ?? "info";
const LOG_FILE = process.env.LOG_FILE;

// Build the destination stream
function createDestination(): DestinationStream | undefined {
  if (LOG_FILE === undefined || LOG_FILE === "") {
    return undefined;
  }

  // Ensure log directory exists
  const logDir = dirname(LOG_FILE);
  if (logDir !== "." && !existsSync(logDir)) {
    mkdirSync(logDir, { recursive: true });
  }

  // Create streams for both stdout and file
  const streams: pino.StreamEntry[] = [
    { level: LOG_LEVEL as pino.Level, stream: process.stdout },
    {
      level: LOG_LEVEL as pino.Level,
      stream: pino.destination({
        dest: LOG_FILE,
        sync: false,
      }),
    },
  ];

  return pino.multistream(streams) as DestinationStream;
}

const destination = createDestination();

// Base logger options
export const loggerOptions: LoggerOptions = {
  level: LOG_LEVEL,
};

// Create the main logger
export const logger =
  destination !== undefined
    ? pino(loggerOptions, destination)
    : pino(loggerOptions);

// For Fastify: export config that Fastify can use directly
export const fastifyLoggerConfig =
  destination !== undefined
    ? { level: LOG_LEVEL, stream: destination }
    : { level: LOG_LEVEL };

// Child loggers for different modules
export const apiLogger = logger.child({ module: "ins-api" });
export const dbLogger = logger.child({ module: "database" });
export const serverLogger = logger.child({ module: "server" });

// Log startup info
if (LOG_FILE !== undefined && LOG_FILE !== "") {
  logger.info(
    { logFile: LOG_FILE, logLevel: LOG_LEVEL },
    "Logging to file enabled"
  );
}
