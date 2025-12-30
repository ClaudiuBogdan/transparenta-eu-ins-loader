import Fastify, { type FastifyInstance } from "fastify";
import {
  describe,
  it,
  expect,
  vi,
  beforeAll,
  afterAll,
  beforeEach,
} from "vitest";

// Mock the database module before importing routes
vi.mock("../../../../src/db/connection.js", () => {
  const mockExecuteTakeFirst = vi.fn();
  const mockExecute = vi.fn();
  const mockLimit = vi.fn(() => ({ execute: mockExecute }));
  const mockOrderBy = vi.fn().mockReturnThis();
  const mockGroupBy = vi.fn(() => ({ execute: mockExecute }));
  const mockWhere = vi.fn().mockReturnThis();
  const mockSelect = vi.fn(() => ({
    where: mockWhere,
    groupBy: mockGroupBy,
    orderBy: mockOrderBy,
    limit: mockLimit,
    execute: mockExecute,
    executeTakeFirst: mockExecuteTakeFirst,
  }));
  const mockInnerJoin = vi.fn().mockReturnThis();
  const mockLeftJoin = vi.fn().mockReturnThis();
  const mockSelectFrom = vi.fn(() => ({
    select: mockSelect,
    innerJoin: mockInnerJoin,
    leftJoin: mockLeftJoin,
  }));

  const mockFn = {
    count: vi.fn(() => ({ as: vi.fn() })),
    min: vi.fn(() => ({ as: vi.fn() })),
    max: vi.fn(() => ({ as: vi.fn() })),
    avg: vi.fn(() => ({ as: vi.fn() })),
    sum: vi.fn(() => ({ as: vi.fn() })),
  };

  const db = {
    selectFrom: mockSelectFrom,
    fn: mockFn,
    mocks: {
      executeTakeFirst: mockExecuteTakeFirst,
      execute: mockExecute,
      where: mockWhere,
      selectFrom: mockSelectFrom,
      leftJoin: mockLeftJoin,
      innerJoin: mockInnerJoin,
      select: mockSelect,
    },
  };

  return { db, jsonb: (val: unknown) => JSON.stringify(val) };
});

import { db } from "../../../../src/db/connection.js";
import { errorHandler } from "../../../../src/server/plugins/error-handler.js";
import { registerStatisticsRoutes } from "../../../../src/server/routes/statistics.js";

// Get mocks from the mocked module
const mockDb = db as unknown as {
  mocks: {
    executeTakeFirst: ReturnType<typeof vi.fn>;
    execute: ReturnType<typeof vi.fn>;
    where: ReturnType<typeof vi.fn>;
    selectFrom: ReturnType<typeof vi.fn>;
    leftJoin: ReturnType<typeof vi.fn>;
    innerJoin: ReturnType<typeof vi.fn>;
    select: ReturnType<typeof vi.fn>;
  };
  selectFrom: ReturnType<typeof vi.fn>;
};

// Sample fixtures
const sampleMatrix = {
  id: 1,
  ins_code: "POP105A",
  metadata: {
    names: { ro: "Populatia", en: "Population" },
    periodicity: ["ANNUAL"],
    flags: { hasUatData: true, hasCountyData: true },
    yearRange: [2000, 2024],
    lastUpdate: "2024-01-15",
  },
  dimensions: [
    { code: "DIM1", label: "Dimension 1" },
    { code: "DIM2", label: "Dimension 2" },
  ],
  sync_status: "SYNCED",
  last_sync_at: new Date("2024-01-15"),
  context_path: "1.10",
  context_names: { ro: "Populatie", en: "Population" },
};

const sampleStatistics = [
  {
    id: 1,
    value: 19000000,
    value_status: null,
    tp_id: 1,
    year: 2020,
    quarter: null,
    month: null,
    periodicity: "ANNUAL",
    tp_labels: { ro: "Anul 2020", en: "Year 2020" },
    terr_id: 1,
    terr_code: "RO",
    terr_names: { ro: "Romania", en: "Romania" },
    terr_level: "NATIONAL",
    terr_path: "RO",
    unit_id: 1,
    unit_code: "NR",
    unit_names: { ro: "Numar", en: "Number" },
    unit_symbol: null,
  },
  {
    id: 2,
    value: 19100000,
    value_status: null,
    tp_id: 2,
    year: 2021,
    quarter: null,
    month: null,
    periodicity: "ANNUAL",
    tp_labels: { ro: "Anul 2021", en: "Year 2021" },
    terr_id: 1,
    terr_code: "RO",
    terr_names: { ro: "Romania", en: "Romania" },
    terr_level: "NATIONAL",
    terr_path: "RO",
    unit_id: 1,
    unit_code: "NR",
    unit_names: { ro: "Numar", en: "Number" },
    unit_symbol: null,
  },
];

describe("server/routes/statistics", () => {
  let app: FastifyInstance;

  beforeAll(async () => {
    app = Fastify({ logger: false });
    await app.register(errorHandler);

    // Register routes under /api/v1 prefix
    app.register(
      async (instance) => {
        registerStatisticsRoutes(instance);
      },
      { prefix: "/api/v1" }
    );

    await app.ready();
  });

  afterAll(async () => {
    await app.close();
  });

  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe("GET /api/v1/statistics/:matrixCode", () => {
    it("should return 404 when matrix not found", async () => {
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce(undefined);

      const response = await app.inject({
        method: "GET",
        url: "/api/v1/statistics/INVALID",
      });

      expect(response.statusCode).toBe(404);
      const body = response.json();
      expect(body.error).toBe("NOT_FOUND");
      expect(body.message).toContain("Matrix INVALID not found");
    });

    it("should return 404 when no statistics found", async () => {
      // Matrix found
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce(sampleMatrix);
      // Empty statistics
      mockDb.mocks.execute.mockResolvedValueOnce([]);
      // Empty classification values for TOTAL filter
      mockDb.mocks.execute.mockResolvedValueOnce([]);

      const response = await app.inject({
        method: "GET",
        url: "/api/v1/statistics/POP105A",
      });

      expect(response.statusCode).toBe(404);
      const body = response.json();
      expect(body.error).toBe("NO_DATA");
    });

    it("should return statistics with groupBy=none", async () => {
      // Matrix found
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce(sampleMatrix);
      // Classification values for TOTAL filter
      mockDb.mocks.execute.mockResolvedValueOnce([{ id: 1 }]);
      // Statistics data
      mockDb.mocks.execute.mockResolvedValueOnce(sampleStatistics);

      const response = await app.inject({
        method: "GET",
        url: "/api/v1/statistics/POP105A?groupBy=none",
      });

      expect(response.statusCode).toBe(200);
      const body = response.json();
      expect(body.data).toBeDefined();
      expect(body.data.matrix).toBeDefined();
      expect(body.data.matrix.insCode).toBe("POP105A");
      expect(body.data.series).toBeDefined();
      expect(body.data.series.length).toBeGreaterThan(0);
    });

    it("should return statistics grouped by territory", async () => {
      // Matrix found
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce(sampleMatrix);
      // Classification values for TOTAL filter
      mockDb.mocks.execute.mockResolvedValueOnce([{ id: 1 }]);
      // Statistics data
      mockDb.mocks.execute.mockResolvedValueOnce(sampleStatistics);

      const response = await app.inject({
        method: "GET",
        url: "/api/v1/statistics/POP105A?groupBy=territory",
      });

      expect(response.statusCode).toBe(200);
      const body = response.json();
      expect(body.data.series).toBeDefined();
    });

    it("should return statistics grouped by classification", async () => {
      // Matrix found
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce(sampleMatrix);
      // Classification values for TOTAL filter
      mockDb.mocks.execute.mockResolvedValueOnce([{ id: 1 }]);
      // Statistics data
      mockDb.mocks.execute.mockResolvedValueOnce(sampleStatistics);
      // Classification values for grouping
      mockDb.mocks.execute.mockResolvedValueOnce([
        {
          statistic_id: 1,
          type_code: "SEX",
          value_id: 1,
          value_code: "TOTAL",
          value_names: { ro: "Total", en: "Total" },
        },
      ]);

      const response = await app.inject({
        method: "GET",
        url: "/api/v1/statistics/POP105A?groupBy=classification",
      });

      expect(response.statusCode).toBe(200);
      const body = response.json();
      expect(body.data.series).toBeDefined();
    });

    it("should apply territory filters", async () => {
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce(sampleMatrix);
      mockDb.mocks.execute.mockResolvedValueOnce([{ id: 1 }]);
      mockDb.mocks.execute.mockResolvedValueOnce(sampleStatistics);

      const response = await app.inject({
        method: "GET",
        url: "/api/v1/statistics/POP105A?territoryCode=RO&territoryLevel=NATIONAL",
      });

      expect(response.statusCode).toBe(200);
      expect(mockDb.mocks.where).toHaveBeenCalled();
    });

    it("should apply time filters", async () => {
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce(sampleMatrix);
      mockDb.mocks.execute.mockResolvedValueOnce([{ id: 1 }]);
      mockDb.mocks.execute.mockResolvedValueOnce(sampleStatistics);

      const response = await app.inject({
        method: "GET",
        url: "/api/v1/statistics/POP105A?yearFrom=2020&yearTo=2023&periodicity=ANNUAL",
      });

      expect(response.statusCode).toBe(200);
    });

    it("should use English locale when specified", async () => {
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce(sampleMatrix);
      mockDb.mocks.execute.mockResolvedValueOnce([{ id: 1 }]);
      mockDb.mocks.execute.mockResolvedValueOnce(sampleStatistics);

      const response = await app.inject({
        method: "GET",
        url: "/api/v1/statistics/POP105A?locale=en",
      });

      expect(response.statusCode).toBe(200);
      const body = response.json();
      expect(body.data.matrix.name).toBe("Population");
    });

    it("should include all classifications when includeAll=true", async () => {
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce(sampleMatrix);
      // No TOTAL filter query when includeAll=true
      mockDb.mocks.execute.mockResolvedValueOnce(sampleStatistics);

      const response = await app.inject({
        method: "GET",
        url: "/api/v1/statistics/POP105A?includeAll=true",
      });

      expect(response.statusCode).toBe(200);
    });

    it("should apply custom classification filters", async () => {
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce(sampleMatrix);
      mockDb.mocks.execute.mockResolvedValueOnce([{ id: 2 }]); // Filter values
      mockDb.mocks.execute.mockResolvedValueOnce(sampleStatistics);

      const response = await app.inject({
        method: "GET",
        url: `/api/v1/statistics/POP105A?classificationFilters=${encodeURIComponent('{"SEX":["M"]}')}`,
      });

      expect(response.statusCode).toBe(200);
    });

    it("should handle pagination correctly", async () => {
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce(sampleMatrix);
      mockDb.mocks.execute.mockResolvedValueOnce([{ id: 1 }]);
      mockDb.mocks.execute.mockResolvedValueOnce(sampleStatistics);

      const response = await app.inject({
        method: "GET",
        url: "/api/v1/statistics/POP105A?limit=10",
      });

      expect(response.statusCode).toBe(200);
      const body = response.json();
      expect(body.meta.pagination).toBeDefined();
    });
  });

  describe("GET /api/v1/statistics/:matrixCode/summary", () => {
    it("should return 404 when matrix not found", async () => {
      mockDb.mocks.executeTakeFirst.mockResolvedValueOnce(undefined);

      const response = await app.inject({
        method: "GET",
        url: "/api/v1/statistics/INVALID/summary",
      });

      expect(response.statusCode).toBe(404);
      const body = response.json();
      expect(body.error).toBe("NOT_FOUND");
    });

    it("should return statistics summary", async () => {
      // Matrix
      mockDb.mocks.executeTakeFirst
        .mockResolvedValueOnce(sampleMatrix)
        // Stats aggregates
        .mockResolvedValueOnce({
          total_count: 1000,
          min_value: 1000,
          max_value: 20000000,
          avg_value: 5000000,
          sum_value: 5000000000,
        })
        // Time range
        .mockResolvedValueOnce({ min_year: 2000, max_year: 2024 })
        // Null count
        .mockResolvedValueOnce({ count: 50 });

      // Territory levels
      mockDb.mocks.execute.mockResolvedValueOnce([
        { level: "NATIONAL", count: 100 },
        { level: "NUTS3", count: 900 },
      ]);

      const response = await app.inject({
        method: "GET",
        url: "/api/v1/statistics/POP105A/summary",
      });

      expect(response.statusCode).toBe(200);
      const body = response.json();
      expect(body.data).toBeDefined();
      expect(body.data.matrix).toBeDefined();
      expect(body.data.matrix.insCode).toBe("POP105A");
      expect(body.data.summary).toBeDefined();
      expect(body.data.summary.totalRecords).toBe(1000);
      expect(body.data.summary.timeRange).toEqual({ from: 2000, to: 2024 });
      expect(body.data.summary.territoryLevels).toBeDefined();
      expect(body.data.summary.valueStats).toBeDefined();
    });

    it("should use English locale when specified", async () => {
      mockDb.mocks.executeTakeFirst
        .mockResolvedValueOnce(sampleMatrix)
        .mockResolvedValueOnce({
          total_count: 100,
          min_value: 1000,
          max_value: 2000,
          avg_value: 1500,
          sum_value: 150000,
        })
        .mockResolvedValueOnce({ min_year: 2020, max_year: 2024 })
        .mockResolvedValueOnce({ count: 0 });
      mockDb.mocks.execute.mockResolvedValueOnce([]);

      const response = await app.inject({
        method: "GET",
        url: "/api/v1/statistics/POP105A/summary?locale=en",
      });

      expect(response.statusCode).toBe(200);
      const body = response.json();
      expect(body.data.matrix.name).toBe("Population");
    });

    it("should handle null value statistics", async () => {
      mockDb.mocks.executeTakeFirst
        .mockResolvedValueOnce(sampleMatrix)
        .mockResolvedValueOnce({
          total_count: 0,
          min_value: null,
          max_value: null,
          avg_value: null,
          sum_value: null,
        })
        .mockResolvedValueOnce({ min_year: null, max_year: null })
        .mockResolvedValueOnce({ count: 0 });
      mockDb.mocks.execute.mockResolvedValueOnce([]);

      const response = await app.inject({
        method: "GET",
        url: "/api/v1/statistics/POP105A/summary",
      });

      expect(response.statusCode).toBe(200);
      const body = response.json();
      expect(body.data.summary.valueStats.min).toBeNull();
      expect(body.data.summary.valueStats.max).toBeNull();
      expect(body.data.summary.timeRange).toBeNull();
    });
  });
});
