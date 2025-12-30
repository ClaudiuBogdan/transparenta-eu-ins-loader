import { describe, it, expect } from "vitest";

import {
  buildEncQuery,
  estimateCellCount,
  wouldExceedLimit,
  parsePivotResponse,
} from "../../../src/scraper/client.js";

describe("scraper/client", () => {
  // ============================================================================
  // Pure utility functions (no API calls)
  // ============================================================================

  describe("buildEncQuery", () => {
    it("should build query string from single dimension with single value", () => {
      expect(buildEncQuery([[100]])).toBe("100");
    });

    it("should build query string from single dimension with multiple values", () => {
      expect(buildEncQuery([[100, 101, 102]])).toBe("100,101,102");
    });

    it("should build query string from multiple dimensions", () => {
      expect(buildEncQuery([[1, 2], [105], [108, 109], [112]])).toBe(
        "1,2:105:108,109:112"
      );
    });

    it("should handle empty selections", () => {
      expect(buildEncQuery([])).toBe("");
    });

    it("should handle empty dimension arrays", () => {
      expect(buildEncQuery([[], [100], []])).toBe(":100:");
    });

    it("should handle typical INS query with 6 dimensions", () => {
      expect(
        buildEncQuery([[1, 2], [105], [108], [112], [4494], [9685, 9686]])
      ).toBe("1,2:105:108:112:4494:9685,9686");
    });
  });

  describe("estimateCellCount", () => {
    it("should return 1 for single value in single dimension", () => {
      expect(estimateCellCount([[100]])).toBe(1);
    });

    it("should multiply values across dimensions", () => {
      expect(
        estimateCellCount([
          [1, 2],
          [3, 4, 5],
        ])
      ).toBe(6);
    });

    it("should handle many dimensions", () => {
      expect(
        estimateCellCount([
          [1, 2],
          [3, 4, 5],
          [6, 7, 8, 9],
          [10, 11],
        ])
      ).toBe(48);
    });

    it("should return 1 for empty selections", () => {
      expect(estimateCellCount([])).toBe(1);
    });

    it("should return 0 if any dimension is empty", () => {
      expect(estimateCellCount([[1, 2], [], [3, 4]])).toBe(0);
    });

    it("should handle large selection counts", () => {
      expect(
        estimateCellCount([
          Array.from({ length: 100 }, (_, i) => i),
          Array.from({ length: 50 }, (_, i) => i),
          Array.from({ length: 10 }, (_, i) => i),
        ])
      ).toBe(50000);
    });
  });

  describe("wouldExceedLimit", () => {
    it("should return false when under default limit", () => {
      expect(
        wouldExceedLimit([
          [1, 2],
          [3, 4, 5],
        ])
      ).toBe(false);
    });

    it("should return true when exceeding default 30000 limit", () => {
      expect(
        wouldExceedLimit([
          Array.from({ length: 200 }, (_, i) => i),
          Array.from({ length: 200 }, (_, i) => i),
        ])
      ).toBe(true);
    });

    it("should return false when exactly at limit", () => {
      expect(
        wouldExceedLimit([
          Array.from({ length: 100 }, (_, i) => i),
          Array.from({ length: 300 }, (_, i) => i),
        ])
      ).toBe(false);
    });

    it("should respect custom limit", () => {
      expect(
        wouldExceedLimit(
          [
            [1, 2, 3],
            [4, 5, 6],
          ],
          5
        )
      ).toBe(true);
    });

    it("should return false for small query with custom limit", () => {
      expect(
        wouldExceedLimit(
          [
            [1, 2],
            [3, 4],
          ],
          10
        )
      ).toBe(false);
    });
  });

  describe("parsePivotResponse", () => {
    it("should parse simple CSV response", () => {
      const csv = "Header1, Header2\nValue1, Value2\nValue3, Value4";
      expect(parsePivotResponse(csv)).toEqual([
        ["Header1", "Header2"],
        ["Value1", "Value2"],
        ["Value3", "Value4"],
      ]);
    });

    it("should filter out empty lines", () => {
      const csv = "Header1, Header2\n\nValue1, Value2\n\n";
      expect(parsePivotResponse(csv)).toEqual([
        ["Header1", "Header2"],
        ["Value1", "Value2"],
      ]);
    });

    it("should handle single row", () => {
      expect(parsePivotResponse("SingleValue")).toEqual([["SingleValue"]]);
    });

    it("should handle empty input", () => {
      expect(parsePivotResponse("")).toEqual([]);
    });

    it("should handle whitespace-only lines", () => {
      const csv = "Header\n   \nValue\n\t\n";
      expect(parsePivotResponse(csv)).toEqual([["Header"], ["Value"]]);
    });

    it("should handle typical INS pivot response", () => {
      const csv = `Judet, Varsta, An, Valoare
Alba, 0-14 ani, 2023, 50000
Alba, 15-64 ani, 2023, 250000
Arad, 0-14 ani, 2023, 45000`;
      const result = parsePivotResponse(csv);
      expect(result).toHaveLength(4);
      expect(result[0]).toEqual(["Judet", "Varsta", "An", "Valoare"]);
      expect(result[1]).toEqual(["Alba", "0-14 ani", "2023", "50000"]);
    });
  });
});
