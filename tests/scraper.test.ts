import { describe, it, expect } from "vitest";

import { estimateCellCount, wouldExceedLimit } from "../src/scraper/client.js";

describe("Scraper Utils", () => {
  describe("estimateCellCount", () => {
    it("should calculate cell count correctly", () => {
      const selections = [[1, 2, 3], [4, 5], [6]];
      expect(estimateCellCount(selections)).toBe(6); // 3 * 2 * 1
    });

    it("should return 1 for single selections", () => {
      const selections = [[1], [2], [3]];
      expect(estimateCellCount(selections)).toBe(1);
    });
  });

  describe("wouldExceedLimit", () => {
    it("should return false for small queries", () => {
      const selections = [
        [1, 2, 3],
        [4, 5],
      ];
      expect(wouldExceedLimit(selections)).toBe(false);
    });

    it("should return true for large queries", () => {
      // 100 * 100 * 100 = 1,000,000 > 30,000
      const selections = [
        Array.from({ length: 100 }, (_, i) => i),
        Array.from({ length: 100 }, (_, i) => i),
        Array.from({ length: 100 }, (_, i) => i),
      ];
      expect(wouldExceedLimit(selections)).toBe(true);
    });
  });
});
