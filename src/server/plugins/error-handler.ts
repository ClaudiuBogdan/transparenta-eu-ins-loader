/**
 * Fastify error handler plugin
 */

import fp from "fastify-plugin";

import type { ApiError } from "../../types/api.js";
import type {
  FastifyInstance,
  FastifyError,
  FastifyRequest,
  FastifyReply,
} from "fastify";

// ============================================================================
// Custom Error Classes
// ============================================================================

export class NotFoundError extends Error {
  code = "NOT_FOUND" as const;
  statusCode = 404;

  constructor(message: string) {
    super(message);
    this.name = "NotFoundError";
  }
}

export class ValidationError extends Error {
  code = "VALIDATION_ERROR" as const;
  statusCode = 400;
  details?: Record<string, unknown>;

  constructor(message: string, details?: Record<string, unknown>) {
    super(message);
    this.name = "ValidationError";
    this.details = details;
  }
}

export class QueryTooLargeError extends Error {
  code = "QUERY_TOO_LARGE" as const;
  statusCode = 400;

  constructor(message: string) {
    super(message);
    this.name = "QueryTooLargeError";
  }
}

export class NoDataError extends Error {
  code = "NO_DATA" as const;
  statusCode = 404;

  constructor(message: string) {
    super(message);
    this.name = "NoDataError";
  }
}

export class DatabaseError extends Error {
  code = "DATABASE_ERROR" as const;
  statusCode = 503;

  constructor(message: string) {
    super(message);
    this.name = "DatabaseError";
  }
}

// ============================================================================
// Error Handler Plugin
// ============================================================================

function errorHandlerPlugin(fastify: FastifyInstance): void {
  fastify.setErrorHandler(
    (error: FastifyError, request: FastifyRequest, reply: FastifyReply) => {
      const requestId = request.id;

      // Handle Fastify validation errors
      if (error.validation) {
        const response: ApiError = {
          error: "VALIDATION_ERROR",
          message: "Invalid request parameters",
          details: {
            validation: error.validation,
          },
          requestId,
        };
        return reply.status(400).send(response);
      }

      // Handle custom errors
      if (error instanceof NotFoundError) {
        const response: ApiError = {
          error: error.code,
          message: error.message,
          requestId,
        };
        return reply.status(error.statusCode).send(response);
      }

      if (error instanceof ValidationError) {
        const response: ApiError = {
          error: error.code,
          message: error.message,
          details: error.details,
          requestId,
        };
        return reply.status(error.statusCode).send(response);
      }

      if (error instanceof QueryTooLargeError) {
        const response: ApiError = {
          error: error.code,
          message: error.message,
          requestId,
        };
        return reply.status(error.statusCode).send(response);
      }

      if (error instanceof NoDataError) {
        const response: ApiError = {
          error: error.code,
          message: error.message,
          requestId,
        };
        return reply.status(error.statusCode).send(response);
      }

      if (error instanceof DatabaseError) {
        const response: ApiError = {
          error: error.code,
          message: error.message,
          requestId,
        };
        return reply.status(error.statusCode).send(response);
      }

      // Handle 404 errors
      if (error.statusCode === 404) {
        const response: ApiError = {
          error: "NOT_FOUND",
          message: error.message || "Resource not found",
          requestId,
        };
        return reply.status(404).send(response);
      }

      // Log unexpected errors
      request.log.error(error, "Unhandled error");

      // Return generic error for unexpected errors
      const response: ApiError = {
        error: "INTERNAL_ERROR",
        message: "An unexpected error occurred",
        requestId,
      };
      return reply.status(500).send(response);
    }
  );

  // Handle 404 for unknown routes
  fastify.setNotFoundHandler((request: FastifyRequest, reply: FastifyReply) => {
    const response: ApiError = {
      error: "NOT_FOUND",
      message: `Route ${request.method} ${request.url} not found`,
      requestId: request.id,
    };
    return reply.status(404).send(response);
  });
}

export const errorHandler = fp(errorHandlerPlugin, {
  name: "error-handler",
});
