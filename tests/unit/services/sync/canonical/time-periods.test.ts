import { describe, it, expect, vi, beforeEach } from "vitest";

import { TimePeriodService } from "../../../../../src/services/sync/canonical/time-periods.js";

import type { Database } from "../../../../../src/db/types.js";
import type { Kysely } from "kysely";

// Mock the logger
vi.mock("../../../../../src/logger.js", () => ({
  logger: {
    debug: vi.fn(),
    warn: vi.fn(),
    info: vi.fn(),
    error: vi.fn(),
  },
}));

// Mock db connection jsonb helper
vi.mock("../../../../../src/db/connection.js", () => ({
  jsonb: (val: unknown) => JSON.stringify(val),
}));

// Create mock database
function createMockDb() {
  const mockExecuteTakeFirst = vi.fn();
  const mockReturning = vi.fn(() => ({
    executeTakeFirst: mockExecuteTakeFirst,
  }));
  const mockValues = vi.fn(() => ({ returning: mockReturning }));
  const mockInsertInto = vi.fn(() => ({ values: mockValues }));

  const mockWhere = vi.fn().mockReturnThis();
  const mockSelect = vi.fn(() => ({
    where: mockWhere,
    executeTakeFirst: mockExecuteTakeFirst,
  }));
  const mockSelectFrom = vi.fn(() => ({ select: mockSelect }));

  const db = {
    selectFrom: mockSelectFrom,
    insertInto: mockInsertInto,
    mocks: {
      executeTakeFirst: mockExecuteTakeFirst,
      where: mockWhere,
      values: mockValues,
    },
  } as unknown as Kysely<Database> & {
    mocks: {
      executeTakeFirst: ReturnType<typeof vi.fn>;
      where: ReturnType<typeof vi.fn>;
      values: ReturnType<typeof vi.fn>;
    };
  };

  return db;
}

describe("services/sync/canonical/time-periods", () => {
  let service: TimePeriodService;
  let mockDb: ReturnType<typeof createMockDb>;

  beforeEach(() => {
    mockDb = createMockDb();
    service = new TimePeriodService(mockDb);
  });

  // ============================================================================
  // parseLabel() Tests
  // ============================================================================

  describe("parseLabel", () => {
    describe("Annual patterns", () => {
      it("should parse 'Anul 2023'", () => {
        const result = service.parseLabel("Anul 2023");
        expect(result).toEqual({ year: 2023, periodicity: "ANNUAL" });
      });

      it("should parse 'Anul 2024' (case insensitive)", () => {
        const result = service.parseLabel("anul 2024");
        expect(result).toEqual({ year: 2024, periodicity: "ANNUAL" });
      });

      it("should parse 'Anul 1990'", () => {
        const result = service.parseLabel("Anul 1990");
        expect(result).toEqual({ year: 1990, periodicity: "ANNUAL" });
      });
    });

    describe("Year-only patterns", () => {
      it("should parse '2023'", () => {
        const result = service.parseLabel("2023");
        expect(result).toEqual({ year: 2023, periodicity: "ANNUAL" });
      });

      it("should parse '1995'", () => {
        const result = service.parseLabel("1995");
        expect(result).toEqual({ year: 1995, periodicity: "ANNUAL" });
      });

      it("should parse '2030'", () => {
        const result = service.parseLabel("2030");
        expect(result).toEqual({ year: 2030, periodicity: "ANNUAL" });
      });
    });

    describe("Year range patterns", () => {
      it("should parse '2020-2024' using start year", () => {
        const result = service.parseLabel("2020-2024");
        expect(result).toEqual({ year: 2020, periodicity: "ANNUAL" });
      });

      it("should parse '2018–2022' with en-dash", () => {
        const result = service.parseLabel("2018–2022");
        expect(result).toEqual({ year: 2018, periodicity: "ANNUAL" });
      });

      it("should parse 'Anii 2015-2020' using end year", () => {
        const result = service.parseLabel("Anii 2015-2020");
        expect(result).toEqual({ year: 2020, periodicity: "ANNUAL" });
      });

      it("should parse 'Anii 2010–2015' with en-dash using end year", () => {
        const result = service.parseLabel("Anii 2010–2015");
        expect(result).toEqual({ year: 2015, periodicity: "ANNUAL" });
      });
    });

    describe("Quarterly patterns", () => {
      it("should parse 'Trimestrul I 2024'", () => {
        const result = service.parseLabel("Trimestrul I 2024");
        expect(result).toEqual({
          year: 2024,
          quarter: 1,
          periodicity: "QUARTERLY",
        });
      });

      it("should parse 'Trimestrul II 2023'", () => {
        const result = service.parseLabel("Trimestrul II 2023");
        expect(result).toEqual({
          year: 2023,
          quarter: 2,
          periodicity: "QUARTERLY",
        });
      });

      it("should parse 'Trimestrul III 2022'", () => {
        const result = service.parseLabel("Trimestrul III 2022");
        expect(result).toEqual({
          year: 2022,
          quarter: 3,
          periodicity: "QUARTERLY",
        });
      });

      it("should parse 'Trimestrul IV 2021'", () => {
        const result = service.parseLabel("Trimestrul IV 2021");
        expect(result).toEqual({
          year: 2021,
          quarter: 4,
          periodicity: "QUARTERLY",
        });
      });

      it("should parse 'TRIMESTRUL I 2020' (case insensitive for word)", () => {
        // Note: Roman numerals must be uppercase (I, II, III, IV)
        const result = service.parseLabel("TRIMESTRUL I 2020");
        expect(result).toEqual({
          year: 2020,
          quarter: 1,
          periodicity: "QUARTERLY",
        });
      });

      it("should return null for lowercase roman numerals 'trimestrul i 2020'", () => {
        // Roman numerals are case-sensitive in the pattern
        const result = service.parseLabel("trimestrul i 2020");
        expect(result).toBeNull();
      });
    });

    describe("Short quarterly patterns", () => {
      it("should parse 'T1 2024'", () => {
        const result = service.parseLabel("T1 2024");
        expect(result).toEqual({
          year: 2024,
          quarter: 1,
          periodicity: "QUARTERLY",
        });
      });

      it("should parse 'T2 2023'", () => {
        const result = service.parseLabel("T2 2023");
        expect(result).toEqual({
          year: 2023,
          quarter: 2,
          periodicity: "QUARTERLY",
        });
      });

      it("should parse 'T3 2022'", () => {
        const result = service.parseLabel("T3 2022");
        expect(result).toEqual({
          year: 2022,
          quarter: 3,
          periodicity: "QUARTERLY",
        });
      });

      it("should parse 'T4 2021'", () => {
        const result = service.parseLabel("T4 2021");
        expect(result).toEqual({
          year: 2021,
          quarter: 4,
          periodicity: "QUARTERLY",
        });
      });

      it("should parse lowercase 't1 2020'", () => {
        const result = service.parseLabel("t1 2020");
        expect(result).toEqual({
          year: 2020,
          quarter: 1,
          periodicity: "QUARTERLY",
        });
      });
    });

    describe("Monthly patterns", () => {
      it("should parse 'Luna Ianuarie 2024'", () => {
        const result = service.parseLabel("Luna Ianuarie 2024");
        expect(result).toEqual({
          year: 2024,
          month: 1,
          periodicity: "MONTHLY",
        });
      });

      it("should parse 'Luna Februarie 2024'", () => {
        const result = service.parseLabel("Luna Februarie 2024");
        expect(result).toEqual({
          year: 2024,
          month: 2,
          periodicity: "MONTHLY",
        });
      });

      it("should parse 'Luna Martie 2024'", () => {
        const result = service.parseLabel("Luna Martie 2024");
        expect(result).toEqual({
          year: 2024,
          month: 3,
          periodicity: "MONTHLY",
        });
      });

      it("should parse 'Luna Aprilie 2024'", () => {
        const result = service.parseLabel("Luna Aprilie 2024");
        expect(result).toEqual({
          year: 2024,
          month: 4,
          periodicity: "MONTHLY",
        });
      });

      it("should parse 'Luna Mai 2024'", () => {
        const result = service.parseLabel("Luna Mai 2024");
        expect(result).toEqual({
          year: 2024,
          month: 5,
          periodicity: "MONTHLY",
        });
      });

      it("should parse 'Luna Iunie 2024'", () => {
        const result = service.parseLabel("Luna Iunie 2024");
        expect(result).toEqual({
          year: 2024,
          month: 6,
          periodicity: "MONTHLY",
        });
      });

      it("should parse 'Luna Iulie 2024'", () => {
        const result = service.parseLabel("Luna Iulie 2024");
        expect(result).toEqual({
          year: 2024,
          month: 7,
          periodicity: "MONTHLY",
        });
      });

      it("should parse 'Luna August 2024'", () => {
        const result = service.parseLabel("Luna August 2024");
        expect(result).toEqual({
          year: 2024,
          month: 8,
          periodicity: "MONTHLY",
        });
      });

      it("should parse 'Luna Septembrie 2024'", () => {
        const result = service.parseLabel("Luna Septembrie 2024");
        expect(result).toEqual({
          year: 2024,
          month: 9,
          periodicity: "MONTHLY",
        });
      });

      it("should parse 'Luna Octombrie 2024'", () => {
        const result = service.parseLabel("Luna Octombrie 2024");
        expect(result).toEqual({
          year: 2024,
          month: 10,
          periodicity: "MONTHLY",
        });
      });

      it("should parse 'Luna Noiembrie 2024'", () => {
        const result = service.parseLabel("Luna Noiembrie 2024");
        expect(result).toEqual({
          year: 2024,
          month: 11,
          periodicity: "MONTHLY",
        });
      });

      it("should parse 'Luna Decembrie 2024'", () => {
        const result = service.parseLabel("Luna Decembrie 2024");
        expect(result).toEqual({
          year: 2024,
          month: 12,
          periodicity: "MONTHLY",
        });
      });

      it("should parse lowercase 'luna ianuarie 2023'", () => {
        const result = service.parseLabel("luna ianuarie 2023");
        expect(result).toEqual({
          year: 2023,
          month: 1,
          periodicity: "MONTHLY",
        });
      });
    });

    describe("Alternative monthly patterns (short)", () => {
      it("should parse 'Ian 2024'", () => {
        const result = service.parseLabel("Ian 2024");
        expect(result).toEqual({
          year: 2024,
          month: 1,
          periodicity: "MONTHLY",
        });
      });

      it("should parse 'Feb 2024'", () => {
        const result = service.parseLabel("Feb 2024");
        expect(result).toEqual({
          year: 2024,
          month: 2,
          periodicity: "MONTHLY",
        });
      });

      it("should parse 'Mar 2024'", () => {
        const result = service.parseLabel("Mar 2024");
        expect(result).toEqual({
          year: 2024,
          month: 3,
          periodicity: "MONTHLY",
        });
      });

      it("should parse 'Apr 2024'", () => {
        const result = service.parseLabel("Apr 2024");
        expect(result).toEqual({
          year: 2024,
          month: 4,
          periodicity: "MONTHLY",
        });
      });

      it("should parse 'Mai 2024' (short same as full)", () => {
        const result = service.parseLabel("Mai 2024");
        expect(result).toEqual({
          year: 2024,
          month: 5,
          periodicity: "MONTHLY",
        });
      });

      it("should parse 'Iun 2024'", () => {
        const result = service.parseLabel("Iun 2024");
        expect(result).toEqual({
          year: 2024,
          month: 6,
          periodicity: "MONTHLY",
        });
      });

      it("should parse 'Iul 2024'", () => {
        const result = service.parseLabel("Iul 2024");
        expect(result).toEqual({
          year: 2024,
          month: 7,
          periodicity: "MONTHLY",
        });
      });

      it("should parse 'Aug 2024'", () => {
        const result = service.parseLabel("Aug 2024");
        expect(result).toEqual({
          year: 2024,
          month: 8,
          periodicity: "MONTHLY",
        });
      });

      it("should parse 'Sep 2024'", () => {
        const result = service.parseLabel("Sep 2024");
        expect(result).toEqual({
          year: 2024,
          month: 9,
          periodicity: "MONTHLY",
        });
      });

      it("should parse 'Oct 2024'", () => {
        const result = service.parseLabel("Oct 2024");
        expect(result).toEqual({
          year: 2024,
          month: 10,
          periodicity: "MONTHLY",
        });
      });

      it("should parse 'Nov 2024'", () => {
        const result = service.parseLabel("Nov 2024");
        expect(result).toEqual({
          year: 2024,
          month: 11,
          periodicity: "MONTHLY",
        });
      });

      it("should parse 'Dec 2024'", () => {
        const result = service.parseLabel("Dec 2024");
        expect(result).toEqual({
          year: 2024,
          month: 12,
          periodicity: "MONTHLY",
        });
      });
    });

    describe("Invalid/non-time labels", () => {
      it("should return null for 'Total'", () => {
        const result = service.parseLabel("Total");
        expect(result).toBeNull();
      });

      it("should return null for 'total' (lowercase)", () => {
        const result = service.parseLabel("total");
        expect(result).toBeNull();
      });

      it("should return null for month-only label 'Ianuarie'", () => {
        const result = service.parseLabel("Ianuarie");
        expect(result).toBeNull();
      });

      it("should return null for month-only label 'Decembrie'", () => {
        const result = service.parseLabel("Decembrie");
        expect(result).toBeNull();
      });

      it("should return null for random text", () => {
        const result = service.parseLabel("Some random text");
        expect(result).toBeNull();
      });

      it("should return null for empty string", () => {
        const result = service.parseLabel("");
        expect(result).toBeNull();
      });

      it("should return null for invalid year format", () => {
        const result = service.parseLabel("Anul ABC");
        expect(result).toBeNull();
      });
    });

    describe("Edge cases", () => {
      it("should handle leading/trailing whitespace", () => {
        const result = service.parseLabel("  Anul 2023  ");
        expect(result).toEqual({ year: 2023, periodicity: "ANNUAL" });
      });

      it("should handle mixed case", () => {
        const result = service.parseLabel("ANUL 2023");
        expect(result).toEqual({ year: 2023, periodicity: "ANNUAL" });
      });
    });
  });

  // ============================================================================
  // isTimePeriodLabel() Tests
  // ============================================================================

  describe("isTimePeriodLabel", () => {
    it("should return true for valid time period labels", () => {
      expect(service.isTimePeriodLabel("Anul 2023")).toBe(true);
      expect(service.isTimePeriodLabel("2024")).toBe(true);
      expect(service.isTimePeriodLabel("Trimestrul I 2024")).toBe(true);
      expect(service.isTimePeriodLabel("Luna Ianuarie 2024")).toBe(true);
    });

    it("should return false for non-time labels", () => {
      expect(service.isTimePeriodLabel("Total")).toBe(false);
      expect(service.isTimePeriodLabel("Ianuarie")).toBe(false);
      expect(service.isTimePeriodLabel("Random text")).toBe(false);
    });
  });

  // ============================================================================
  // clearCache() Tests
  // ============================================================================

  describe("clearCache", () => {
    it("should clear the internal cache", async () => {
      // Set up mock to return existing period
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce({ id: 1 });

      // First call - should query db and cache
      await service.findOrCreate("Anul 2023");

      // Clear cache
      service.clearCache();

      // Set up mock for second query
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce({ id: 1 });

      // Second call - should query db again after cache clear
      await service.findOrCreate("Anul 2023");

      // Database should have been called twice
      expect(mockDb.selectFrom).toHaveBeenCalledTimes(2);
    });
  });

  // ============================================================================
  // findOrCreate() Tests
  // ============================================================================

  describe("findOrCreate", () => {
    it("should return null for unparseable label", async () => {
      const result = await service.findOrCreate("Invalid Label");
      expect(result).toBeNull();
    });

    it("should return cached value on second call", async () => {
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce({ id: 42 });

      const first = await service.findOrCreate("Anul 2023");
      const second = await service.findOrCreate("Anul 2023");

      expect(first).toBe(42);
      expect(second).toBe(42);
      // Should only call db once due to caching
      expect(mockDb.selectFrom).toHaveBeenCalledTimes(1);
    });

    it("should return existing period from database", async () => {
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce({ id: 100 });

      const result = await service.findOrCreate("Anul 2024");

      expect(result).toBe(100);
      expect(mockDb.selectFrom).toHaveBeenCalledWith("time_periods");
    });

    it("should create new period when not found", async () => {
      // First call returns undefined (not found)
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce(undefined);
      // Second call returns the inserted id
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce({ id: 200 });

      const result = await service.findOrCreate("Anul 2025");

      expect(result).toBe(200);
      expect(mockDb.insertInto).toHaveBeenCalledWith("time_periods");
    });

    it("should use English label when provided", async () => {
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce(undefined);
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce({ id: 300 });

      await service.findOrCreate("Anul 2023", "Year 2023");

      expect(mockDb.mocks.values).toHaveBeenCalled();
    });
  });

  // ============================================================================
  // computePeriodBounds Tests (via findOrCreate)
  // ============================================================================

  describe("period bounds computation", () => {
    it("should compute annual bounds correctly", async () => {
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce(undefined);
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce({ id: 1 });

      await service.findOrCreate("Anul 2024");

      expect(mockDb.mocks.values).toHaveBeenCalledWith(
        expect.objectContaining({
          year: 2024,
          periodicity: "ANNUAL",
          period_start: new Date(Date.UTC(2024, 0, 1)),
          period_end: new Date(Date.UTC(2024, 11, 31)),
        })
      );
    });

    it("should compute Q1 bounds correctly", async () => {
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce(undefined);
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce({ id: 1 });

      await service.findOrCreate("Trimestrul I 2024");

      expect(mockDb.mocks.values).toHaveBeenCalledWith(
        expect.objectContaining({
          year: 2024,
          quarter: 1,
          periodicity: "QUARTERLY",
          period_start: new Date(Date.UTC(2024, 0, 1)),
          period_end: new Date(Date.UTC(2024, 2, 31)),
        })
      );
    });

    it("should compute Q2 bounds correctly", async () => {
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce(undefined);
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce({ id: 1 });

      await service.findOrCreate("Trimestrul II 2024");

      expect(mockDb.mocks.values).toHaveBeenCalledWith(
        expect.objectContaining({
          year: 2024,
          quarter: 2,
          period_start: new Date(Date.UTC(2024, 3, 1)),
          period_end: new Date(Date.UTC(2024, 5, 30)),
        })
      );
    });

    it("should compute Q3 bounds correctly", async () => {
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce(undefined);
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce({ id: 1 });

      await service.findOrCreate("Trimestrul III 2024");

      expect(mockDb.mocks.values).toHaveBeenCalledWith(
        expect.objectContaining({
          year: 2024,
          quarter: 3,
          period_start: new Date(Date.UTC(2024, 6, 1)),
          period_end: new Date(Date.UTC(2024, 8, 30)),
        })
      );
    });

    it("should compute Q4 bounds correctly", async () => {
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce(undefined);
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce({ id: 1 });

      await service.findOrCreate("Trimestrul IV 2024");

      expect(mockDb.mocks.values).toHaveBeenCalledWith(
        expect.objectContaining({
          year: 2024,
          quarter: 4,
          period_start: new Date(Date.UTC(2024, 9, 1)),
          period_end: new Date(Date.UTC(2024, 11, 31)),
        })
      );
    });

    it("should compute January bounds correctly", async () => {
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce(undefined);
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce({ id: 1 });

      await service.findOrCreate("Luna Ianuarie 2024");

      expect(mockDb.mocks.values).toHaveBeenCalledWith(
        expect.objectContaining({
          year: 2024,
          month: 1,
          periodicity: "MONTHLY",
          period_start: new Date(Date.UTC(2024, 0, 1)),
          period_end: new Date(Date.UTC(2024, 0, 31)),
        })
      );
    });

    it("should compute February bounds correctly (leap year)", async () => {
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce(undefined);
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce({ id: 1 });

      await service.findOrCreate("Luna Februarie 2024");

      expect(mockDb.mocks.values).toHaveBeenCalledWith(
        expect.objectContaining({
          year: 2024,
          month: 2,
          period_end: new Date(Date.UTC(2024, 1, 29)), // Leap year
        })
      );
    });

    it("should compute February bounds correctly (non-leap year)", async () => {
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce(undefined);
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce({ id: 1 });

      await service.findOrCreate("Luna Februarie 2023");

      expect(mockDb.mocks.values).toHaveBeenCalledWith(
        expect.objectContaining({
          year: 2023,
          month: 2,
          period_end: new Date(Date.UTC(2023, 1, 28)), // Non-leap year
        })
      );
    });

    it("should compute December bounds correctly", async () => {
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce(undefined);
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce({ id: 1 });

      await service.findOrCreate("Luna Decembrie 2024");

      expect(mockDb.mocks.values).toHaveBeenCalledWith(
        expect.objectContaining({
          year: 2024,
          month: 12,
          period_start: new Date(Date.UTC(2024, 11, 1)),
          period_end: new Date(Date.UTC(2024, 11, 31)),
        })
      );
    });
  });

  // ============================================================================
  // Label Generation Tests
  // ============================================================================

  describe("label generation", () => {
    it("should generate English label for annual period", async () => {
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce(undefined);
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce({ id: 1 });

      await service.findOrCreate("Anul 2024");

      expect(mockDb.mocks.values).toHaveBeenCalledWith(
        expect.objectContaining({
          labels: expect.stringContaining('"en":"Year 2024"'),
        })
      );
    });

    it("should generate English label for quarterly period", async () => {
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce(undefined);
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce({ id: 1 });

      await service.findOrCreate("Trimestrul II 2024");

      expect(mockDb.mocks.values).toHaveBeenCalledWith(
        expect.objectContaining({
          labels: expect.stringContaining('"en":"Quarter 2 2024"'),
        })
      );
    });

    it("should generate English label for monthly period", async () => {
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce(undefined);
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce({ id: 1 });

      await service.findOrCreate("Luna Martie 2024");

      expect(mockDb.mocks.values).toHaveBeenCalledWith(
        expect.objectContaining({
          labels: expect.stringContaining('"en":"March 2024"'),
        })
      );
    });

    it("should use provided English label", async () => {
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce(undefined);
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce({ id: 1 });

      await service.findOrCreate("Anul 2024", "Year of 2024");

      expect(mockDb.mocks.values).toHaveBeenCalledWith(
        expect.objectContaining({
          labels: expect.stringContaining('"en":"Year of 2024"'),
        })
      );
    });
  });
});
