/**
 * Unit tests for pagination utilities
 */

import { describe, it, expect } from "vitest";

import {
  encodeCursor,
  decodeCursor,
  parseLimit,
  validateCursor,
  createPaginationMeta,
  type CursorPayload,
} from "../../../src/utils/pagination.js";

describe("Pagination Utils", () => {
  describe("encodeCursor", () => {
    it("should encode payload to base64url string", () => {
      const payload: CursorPayload = {
        sortValue: "test",
        id: 123,
        direction: "forward",
      };

      const encoded = encodeCursor(payload);

      expect(typeof encoded).toBe("string");
      expect(encoded.length).toBeGreaterThan(0);
      // Base64url should not contain + or /
      expect(encoded).not.toMatch(/[+/]/);
    });

    it("should encode numeric sortValue correctly", () => {
      const payload: CursorPayload = {
        sortValue: 2023,
        id: 456,
        direction: "forward",
      };

      const encoded = encodeCursor(payload);
      const decoded = decodeCursor(encoded);

      expect(decoded?.sortValue).toBe(2023);
      expect(decoded?.id).toBe(456);
    });

    it("should produce consistent encoding", () => {
      const payload: CursorPayload = {
        sortValue: "consistent",
        id: 789,
        direction: "backward",
      };

      const encoded1 = encodeCursor(payload);
      const encoded2 = encodeCursor(payload);

      expect(encoded1).toBe(encoded2);
    });
  });

  describe("decodeCursor", () => {
    it("should decode valid cursor back to payload", () => {
      const original: CursorPayload = {
        sortValue: "test-value",
        id: 100,
        direction: "forward",
      };

      const encoded = encodeCursor(original);
      const decoded = decodeCursor(encoded);

      expect(decoded).toEqual(original);
    });

    it("should return null for invalid base64", () => {
      const result = decodeCursor("not-valid-base64!!!");

      expect(result).toBeNull();
    });

    it("should return null for invalid JSON", () => {
      // Valid base64 but not valid JSON
      const invalidJson = Buffer.from("not json").toString("base64url");
      const result = decodeCursor(invalidJson);

      expect(result).toBeNull();
    });

    it("should return null for empty string", () => {
      const result = decodeCursor("");

      expect(result).toBeNull();
    });

    it("should handle special characters in sortValue", () => {
      const payload: CursorPayload = {
        sortValue: "Special chars: äöü é ñ",
        id: 1,
        direction: "forward",
      };

      const encoded = encodeCursor(payload);
      const decoded = decodeCursor(encoded);

      expect(decoded?.sortValue).toBe(payload.sortValue);
    });
  });

  describe("parseLimit", () => {
    it("should return default limit when value is undefined", () => {
      const result = parseLimit(undefined);

      expect(result).toBe(50); // default
    });

    it("should return default limit when value is undefined with custom default", () => {
      const result = parseLimit(undefined, 25, 100);

      expect(result).toBe(25);
    });

    it("should parse string limit correctly", () => {
      const result = parseLimit("30");

      expect(result).toBe(30);
    });

    it("should accept numeric limit directly", () => {
      const result = parseLimit(42);

      expect(result).toBe(42);
    });

    it("should cap limit at maxLimit", () => {
      const result = parseLimit(500, 50, 100);

      expect(result).toBe(100);
    });

    it("should return default for NaN", () => {
      const result = parseLimit("not-a-number", 50, 100);

      expect(result).toBe(50);
    });

    it("should return default for zero", () => {
      const result = parseLimit(0, 50, 100);

      expect(result).toBe(50);
    });

    it("should return default for negative numbers", () => {
      const result = parseLimit(-10, 50, 100);

      expect(result).toBe(50);
    });

    it("should handle edge case: limit equals maxLimit", () => {
      const result = parseLimit(100, 50, 100);

      expect(result).toBe(100);
    });

    it("should handle string '0'", () => {
      const result = parseLimit("0", 50, 100);

      expect(result).toBe(50);
    });
  });

  describe("validateCursor", () => {
    it("should return null for undefined cursor", () => {
      const result = validateCursor(undefined);

      expect(result).toBeNull();
    });

    it("should return null for empty string cursor", () => {
      const result = validateCursor("");

      expect(result).toBeNull();
    });

    it("should return valid payload for correct cursor", () => {
      const payload: CursorPayload = {
        sortValue: "test",
        id: 123,
        direction: "forward",
      };
      const encoded = encodeCursor(payload);

      const result = validateCursor(encoded);

      expect(result).toEqual(payload);
    });

    it("should return null for cursor with invalid sortValue type", () => {
      const invalidPayload = {
        sortValue: { nested: "object" },
        id: 123,
        direction: "forward",
      };
      const encoded = Buffer.from(JSON.stringify(invalidPayload)).toString(
        "base64url"
      );

      const result = validateCursor(encoded);

      expect(result).toBeNull();
    });

    it("should return null for cursor with invalid id type", () => {
      const invalidPayload = {
        sortValue: "test",
        id: "not-a-number",
        direction: "forward",
      };
      const encoded = Buffer.from(JSON.stringify(invalidPayload)).toString(
        "base64url"
      );

      const result = validateCursor(encoded);

      expect(result).toBeNull();
    });

    it("should accept string sortValue", () => {
      const payload: CursorPayload = {
        sortValue: "string-value",
        id: 1,
        direction: "forward",
      };
      const encoded = encodeCursor(payload);

      const result = validateCursor(encoded);

      expect(result?.sortValue).toBe("string-value");
    });

    it("should accept number sortValue", () => {
      const payload: CursorPayload = {
        sortValue: 12345,
        id: 1,
        direction: "forward",
      };
      const encoded = encodeCursor(payload);

      const result = validateCursor(encoded);

      expect(result?.sortValue).toBe(12345);
    });
  });

  describe("createPaginationMeta", () => {
    it("should return null cursor when hasMore is false", () => {
      const items = [{ id: 1, name: "test" }];

      const result = createPaginationMeta(items, 10, "name", false);

      expect(result.cursor).toBeNull();
      expect(result.hasMore).toBe(false);
      expect(result.limit).toBe(10);
    });

    it("should create cursor from last item when hasMore is true", () => {
      const items = [
        { id: 1, name: "first" },
        { id: 2, name: "second" },
        { id: 3, name: "third" },
      ];

      const result = createPaginationMeta(items, 3, "name", true);

      expect(result.cursor).not.toBeNull();
      expect(result.hasMore).toBe(true);

      // Decode and verify cursor
      const decoded = decodeCursor(result.cursor!);
      expect(decoded?.sortValue).toBe("third");
      expect(decoded?.id).toBe(3);
      expect(decoded?.direction).toBe("forward");
    });

    it("should return null cursor for empty items array", () => {
      const items: { id: number; name: string }[] = [];

      const result = createPaginationMeta(items, 10, "name", false);

      expect(result.cursor).toBeNull();
    });

    it("should include total when provided", () => {
      const items = [{ id: 1, name: "test" }];

      const result = createPaginationMeta(items, 10, "name", false, 100);

      expect(result.total).toBe(100);
    });

    it("should not include total when not provided", () => {
      const items = [{ id: 1, name: "test" }];

      const result = createPaginationMeta(items, 10, "name", false);

      expect(result.total).toBeUndefined();
    });

    it("should use correct sortField for cursor", () => {
      const items = [
        { id: 1, year: 2020, name: "a" },
        { id: 2, year: 2021, name: "b" },
        { id: 3, year: 2022, name: "c" },
      ];

      const result = createPaginationMeta(items, 3, "year", true);

      const decoded = decodeCursor(result.cursor!);
      expect(decoded?.sortValue).toBe(2022);
    });

    it("should handle numeric sortField", () => {
      const items = [
        { id: 1, value: 100 },
        { id: 2, value: 200 },
      ];

      const result = createPaginationMeta(items, 2, "value", true);

      const decoded = decodeCursor(result.cursor!);
      expect(decoded?.sortValue).toBe(200);
    });
  });
});
