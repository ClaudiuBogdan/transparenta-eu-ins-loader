import Fastify, { type FastifyInstance } from "fastify";
import { describe, it, expect, beforeAll, afterAll } from "vitest";

import {
  errorHandler,
  NotFoundError,
  ValidationError,
  QueryTooLargeError,
  NoDataError,
  DatabaseError,
} from "../../../../src/server/plugins/error-handler.js";

describe("server/plugins/error-handler", () => {
  // ============================================================================
  // Custom Error Classes Tests
  // ============================================================================

  describe("NotFoundError", () => {
    it("should create error with correct properties", () => {
      const error = new NotFoundError("Resource not found");

      expect(error).toBeInstanceOf(Error);
      expect(error).toBeInstanceOf(NotFoundError);
      expect(error.name).toBe("NotFoundError");
      expect(error.message).toBe("Resource not found");
      expect(error.code).toBe("NOT_FOUND");
      expect(error.statusCode).toBe(404);
    });

    it("should preserve stack trace", () => {
      const error = new NotFoundError("Test error");
      expect(error.stack).toBeDefined();
      expect(error.stack).toContain("NotFoundError");
    });
  });

  describe("ValidationError", () => {
    it("should create error with correct properties", () => {
      const error = new ValidationError("Invalid input");

      expect(error).toBeInstanceOf(Error);
      expect(error).toBeInstanceOf(ValidationError);
      expect(error.name).toBe("ValidationError");
      expect(error.message).toBe("Invalid input");
      expect(error.code).toBe("VALIDATION_ERROR");
      expect(error.statusCode).toBe(400);
      expect(error.details).toBeUndefined();
    });

    it("should accept optional details", () => {
      const details = { field: "email", reason: "Invalid format" };
      const error = new ValidationError("Invalid input", details);

      expect(error.details).toEqual(details);
    });

    it("should preserve details object", () => {
      const details = {
        fields: ["name", "email"],
        constraints: { minLength: 3 },
      };
      const error = new ValidationError("Multiple errors", details);

      expect(error.details).toEqual(details);
    });
  });

  describe("QueryTooLargeError", () => {
    it("should create error with correct properties", () => {
      const error = new QueryTooLargeError("Query exceeds 30,000 cell limit");

      expect(error).toBeInstanceOf(Error);
      expect(error).toBeInstanceOf(QueryTooLargeError);
      expect(error.name).toBe("QueryTooLargeError");
      expect(error.message).toBe("Query exceeds 30,000 cell limit");
      expect(error.code).toBe("QUERY_TOO_LARGE");
      expect(error.statusCode).toBe(400);
    });
  });

  describe("NoDataError", () => {
    it("should create error with correct properties", () => {
      const error = new NoDataError("No data found for the specified criteria");

      expect(error).toBeInstanceOf(Error);
      expect(error).toBeInstanceOf(NoDataError);
      expect(error.name).toBe("NoDataError");
      expect(error.message).toBe("No data found for the specified criteria");
      expect(error.code).toBe("NO_DATA");
      expect(error.statusCode).toBe(404);
    });
  });

  describe("DatabaseError", () => {
    it("should create error with correct properties", () => {
      const error = new DatabaseError("Database connection failed");

      expect(error).toBeInstanceOf(Error);
      expect(error).toBeInstanceOf(DatabaseError);
      expect(error.name).toBe("DatabaseError");
      expect(error.message).toBe("Database connection failed");
      expect(error.code).toBe("DATABASE_ERROR");
      expect(error.statusCode).toBe(503);
    });
  });

  // ============================================================================
  // Error Handler Plugin Tests
  // ============================================================================

  describe("errorHandler plugin", () => {
    let app: FastifyInstance;

    beforeAll(async () => {
      app = Fastify({ logger: false });
      await app.register(errorHandler);

      // Test routes that throw different errors
      app.get("/not-found", async () => {
        throw new NotFoundError("Resource not found");
      });

      app.get("/validation-error", async () => {
        throw new ValidationError("Invalid parameters", { field: "id" });
      });

      app.get("/validation-error-no-details", async () => {
        throw new ValidationError("Invalid parameters");
      });

      app.get("/query-too-large", async () => {
        throw new QueryTooLargeError("Query exceeds limit");
      });

      app.get("/no-data", async () => {
        throw new NoDataError("No data available");
      });

      app.get("/database-error", async () => {
        throw new DatabaseError("Connection failed");
      });

      app.get("/generic-error", async () => {
        throw new Error("Something went wrong");
      });

      app.get("/fastify-404", async () => {
        const error = new Error("Not found") as Error & { statusCode: number };
        error.statusCode = 404;
        throw error;
      });

      app.get("/fastify-validation", async () => {
        const error = new Error("Validation failed") as Error & {
          validation: { keyword: string; message: string }[];
        };
        error.validation = [{ keyword: "type", message: "must be string" }];
        throw error;
      });

      await app.ready();
    });

    afterAll(async () => {
      await app.close();
    });

    it("should handle NotFoundError", async () => {
      const response = await app.inject({
        method: "GET",
        url: "/not-found",
      });

      expect(response.statusCode).toBe(404);
      const body = response.json();
      expect(body.error).toBe("NOT_FOUND");
      expect(body.message).toBe("Resource not found");
      expect(body.requestId).toBeDefined();
    });

    it("should handle ValidationError with details", async () => {
      const response = await app.inject({
        method: "GET",
        url: "/validation-error",
      });

      expect(response.statusCode).toBe(400);
      const body = response.json();
      expect(body.error).toBe("VALIDATION_ERROR");
      expect(body.message).toBe("Invalid parameters");
      expect(body.details).toEqual({ field: "id" });
      expect(body.requestId).toBeDefined();
    });

    it("should handle ValidationError without details", async () => {
      const response = await app.inject({
        method: "GET",
        url: "/validation-error-no-details",
      });

      expect(response.statusCode).toBe(400);
      const body = response.json();
      expect(body.error).toBe("VALIDATION_ERROR");
      expect(body.message).toBe("Invalid parameters");
      expect(body.details).toBeUndefined();
    });

    it("should handle QueryTooLargeError", async () => {
      const response = await app.inject({
        method: "GET",
        url: "/query-too-large",
      });

      expect(response.statusCode).toBe(400);
      const body = response.json();
      expect(body.error).toBe("QUERY_TOO_LARGE");
      expect(body.message).toBe("Query exceeds limit");
      expect(body.requestId).toBeDefined();
    });

    it("should handle NoDataError", async () => {
      const response = await app.inject({
        method: "GET",
        url: "/no-data",
      });

      expect(response.statusCode).toBe(404);
      const body = response.json();
      expect(body.error).toBe("NO_DATA");
      expect(body.message).toBe("No data available");
      expect(body.requestId).toBeDefined();
    });

    it("should handle DatabaseError", async () => {
      const response = await app.inject({
        method: "GET",
        url: "/database-error",
      });

      expect(response.statusCode).toBe(503);
      const body = response.json();
      expect(body.error).toBe("DATABASE_ERROR");
      expect(body.message).toBe("Connection failed");
      expect(body.requestId).toBeDefined();
    });

    it("should handle generic errors with 500 status", async () => {
      const response = await app.inject({
        method: "GET",
        url: "/generic-error",
      });

      expect(response.statusCode).toBe(500);
      const body = response.json();
      expect(body.error).toBe("INTERNAL_ERROR");
      expect(body.message).toBe("An unexpected error occurred");
      expect(body.requestId).toBeDefined();
    });

    it("should handle Fastify 404 errors", async () => {
      const response = await app.inject({
        method: "GET",
        url: "/fastify-404",
      });

      expect(response.statusCode).toBe(404);
      const body = response.json();
      expect(body.error).toBe("NOT_FOUND");
      expect(body.requestId).toBeDefined();
    });

    it("should handle Fastify validation errors", async () => {
      const response = await app.inject({
        method: "GET",
        url: "/fastify-validation",
      });

      expect(response.statusCode).toBe(400);
      const body = response.json();
      expect(body.error).toBe("VALIDATION_ERROR");
      expect(body.message).toBe("Invalid request parameters");
      expect(body.details).toBeDefined();
      expect(body.details.validation).toBeDefined();
    });

    it("should handle unknown routes with 404", async () => {
      const response = await app.inject({
        method: "GET",
        url: "/unknown-route",
      });

      expect(response.statusCode).toBe(404);
      const body = response.json();
      expect(body.error).toBe("NOT_FOUND");
      expect(body.message).toContain("Route GET /unknown-route not found");
      expect(body.requestId).toBeDefined();
    });

    it("should handle unknown POST routes with 404", async () => {
      const response = await app.inject({
        method: "POST",
        url: "/unknown-route",
      });

      expect(response.statusCode).toBe(404);
      const body = response.json();
      expect(body.error).toBe("NOT_FOUND");
      expect(body.message).toContain("Route POST /unknown-route not found");
    });
  });
});
