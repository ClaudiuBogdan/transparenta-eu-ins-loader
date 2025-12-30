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

// Create comprehensive mock chain
function createMockChain() {
  const executeTakeFirst = vi.fn();
  const executeTakeFirstOrThrow = vi.fn();
  const execute = vi.fn();

  const chainMethods = {
    select: vi.fn(),
    selectAll: vi.fn(),
    where: vi.fn(),
    groupBy: vi.fn(),
    orderBy: vi.fn(),
    limit: vi.fn(),
    innerJoin: vi.fn(),
    leftJoin: vi.fn(),
    as: vi.fn(),
    values: vi.fn(),
    set: vi.fn(),
    returningAll: vi.fn(),
    execute,
    executeTakeFirst,
    executeTakeFirstOrThrow,
  };

  // Make all chain methods return chainMethods (fluent interface)
  for (const key of Object.keys(chainMethods)) {
    if (typeof chainMethods[key as keyof typeof chainMethods] === "function") {
      const fn = chainMethods[key as keyof typeof chainMethods];
      if (
        fn !== execute &&
        fn !== executeTakeFirst &&
        fn !== executeTakeFirstOrThrow
      ) {
        fn.mockReturnValue(chainMethods);
      }
    }
  }

  return {
    chainMethods,
    executeTakeFirst,
    executeTakeFirstOrThrow,
    execute,
  };
}

const { chainMethods, executeTakeFirst, executeTakeFirstOrThrow, execute } =
  createMockChain();

// Mock the database module
vi.mock("../../../../src/db/connection.js", () => ({
  db: {
    selectFrom: vi.fn(() => chainMethods),
    insertInto: vi.fn(() => chainMethods),
    updateTable: vi.fn(() => chainMethods),
  },
}));

// Mock sql template tag - partial mock to override only sql function
vi.mock("kysely", async (importOriginal) => {
  const original = await importOriginal();
  return {
    ...(original as object),
    sql: Object.assign(
      (_strings: TemplateStringsArray, ..._values: unknown[]) => ({
        as: vi.fn(() => "mocked_sql"),
        execute: vi.fn().mockResolvedValue({ rows: [{ exists: true }] }),
      }),
      { raw: vi.fn() }
    ),
  };
});

import { errorHandler } from "../../../../src/server/plugins/error-handler.js";
import { registerSyncRoutes } from "../../../../src/server/routes/sync.js";

// Sample fixtures
const sampleMatrix = {
  id: 1,
  ins_code: "POP105A",
  sync_status: "SYNCED",
  last_sync_at: new Date("2024-01-15"),
  sync_error: null,
  metadata: {
    names: {
      ro: "Populatia pe sexe si grupe de varsta",
      en: "Population by sex and age",
    },
    periodicity: ["ANNUAL"],
  },
  data_count: 100,
};

const sampleSyncJob = {
  id: 1,
  matrix_id: 1,
  status: "PENDING" as const,
  year_from: 2020,
  year_to: 2024,
  priority: 0,
  created_at: new Date("2024-01-15T10:00:00Z"),
  started_at: null,
  completed_at: null,
  rows_inserted: 0,
  rows_updated: 0,
  error_message: null,
  created_by: "api",
};

const sampleCompletedJob = {
  ...sampleSyncJob,
  id: 2,
  status: "COMPLETED" as const,
  started_at: new Date("2024-01-15T10:05:00Z"),
  completed_at: new Date("2024-01-15T10:10:00Z"),
  rows_inserted: 1000,
  rows_updated: 50,
};

describe("server/routes/sync", () => {
  let app: FastifyInstance;

  beforeAll(async () => {
    app = Fastify({ logger: false });
    await app.register(errorHandler);

    app.register(
      async (instance) => {
        registerSyncRoutes(instance);
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
    // Reset chain methods to return chainMethods
    for (const key of Object.keys(chainMethods)) {
      const fn = chainMethods[key as keyof typeof chainMethods];
      if (
        fn !== execute &&
        fn !== executeTakeFirst &&
        fn !== executeTakeFirstOrThrow
      ) {
        fn.mockReturnValue(chainMethods);
      }
    }
  });

  describe("GET /api/v1/sync/status", () => {
    it("should return sync status summary", async () => {
      // Setup mock responses in order
      execute
        // Status counts
        .mockResolvedValueOnce([
          { sync_status: "PENDING", count: 100 },
          { sync_status: "SYNCED", count: 1500 },
          { sync_status: "FAILED", count: 50 },
        ])
        // Queue counts
        .mockResolvedValueOnce([
          { status: "PENDING", count: 5 },
          { status: "RUNNING", count: 1 },
        ])
        // Matrix list
        .mockResolvedValueOnce([sampleMatrix]);

      executeTakeFirst.mockResolvedValueOnce({ count: 500 });

      const response = await app.inject({
        method: "GET",
        url: "/api/v1/sync/status",
      });

      expect(response.statusCode).toBe(200);
      const body = response.json();
      expect(body.data).toBeDefined();
      expect(body.data.summary).toBeDefined();
      expect(body.data.queue).toBeDefined();
      expect(body.data.matrices).toBeDefined();
    });

    it("should handle empty results", async () => {
      execute
        .mockResolvedValueOnce([])
        .mockResolvedValueOnce([])
        .mockResolvedValueOnce([]);
      executeTakeFirst.mockResolvedValueOnce({ count: 0 });

      const response = await app.inject({
        method: "GET",
        url: "/api/v1/sync/status",
      });

      expect(response.statusCode).toBe(200);
      const body = response.json();
      expect(body.data.summary.total).toBe(0);
      expect(body.data.matrices).toEqual([]);
    });
  });

  describe("POST /api/v1/sync/data/:matrixCode", () => {
    it("should return 404 when matrix not found", async () => {
      executeTakeFirst.mockResolvedValueOnce(undefined);

      const response = await app.inject({
        method: "POST",
        url: "/api/v1/sync/data/INVALID",
        payload: { yearFrom: 2020, yearTo: 2024 },
      });

      expect(response.statusCode).toBe(404);
      const body = response.json();
      expect(body.error).toBe("NOT_FOUND");
    });

    it("should return 400 when matrix metadata not synced", async () => {
      executeTakeFirst.mockResolvedValueOnce({
        ...sampleMatrix,
        sync_status: "PENDING",
      });

      const response = await app.inject({
        method: "POST",
        url: "/api/v1/sync/data/POP105A",
        payload: {},
      });

      expect(response.statusCode).toBe(400);
      const body = response.json();
      expect(body.error).toBe("VALIDATION_ERROR");
      expect(body.message).toContain("metadata not synced");
    });

    it("should return 400 when yearFrom > yearTo", async () => {
      const response = await app.inject({
        method: "POST",
        url: "/api/v1/sync/data/POP105A",
        payload: { yearFrom: 2024, yearTo: 2020 },
      });

      expect(response.statusCode).toBe(400);
      const body = response.json();
      expect(body.error).toBe("VALIDATION_ERROR");
    });

    it("should return existing job if one is pending", async () => {
      executeTakeFirst
        .mockResolvedValueOnce(sampleMatrix) // Matrix found
        .mockResolvedValueOnce(sampleSyncJob); // Existing job found

      const response = await app.inject({
        method: "POST",
        url: "/api/v1/sync/data/POP105A",
        payload: {},
      });

      expect(response.statusCode).toBe(200);
      const body = response.json();
      expect(body.data.isNewJob).toBe(false);
      expect(body.data.message).toContain("already pending");
    });

    it("should create new job when no active job exists", async () => {
      executeTakeFirst
        .mockResolvedValueOnce(sampleMatrix) // Matrix found
        .mockResolvedValueOnce(undefined); // No existing job

      executeTakeFirstOrThrow.mockResolvedValueOnce(sampleSyncJob);

      const response = await app.inject({
        method: "POST",
        url: "/api/v1/sync/data/POP105A",
        payload: { yearFrom: 2020, yearTo: 2024 },
      });

      expect(response.statusCode).toBe(201);
      const body = response.json();
      expect(body.data.isNewJob).toBe(true);
      expect(body.data.message).toContain("Sync job created");
    });
  });

  describe("GET /api/v1/sync/jobs/:jobId", () => {
    it("should return job details", async () => {
      executeTakeFirst.mockResolvedValueOnce({
        ...sampleCompletedJob,
        ins_code: "POP105A",
        metadata: sampleMatrix.metadata,
      });

      const response = await app.inject({
        method: "GET",
        url: "/api/v1/sync/jobs/2",
      });

      expect(response.statusCode).toBe(200);
      const body = response.json();
      expect(body.data.id).toBe(2);
      expect(body.data.matrixCode).toBe("POP105A");
      expect(body.data.status).toBe("COMPLETED");
    });

    it("should return 404 when job not found", async () => {
      executeTakeFirst.mockResolvedValueOnce(undefined);

      const response = await app.inject({
        method: "GET",
        url: "/api/v1/sync/jobs/999",
      });

      expect(response.statusCode).toBe(404);
      const body = response.json();
      expect(body.error).toBe("NOT_FOUND");
    });

    it("should return 400 for invalid job ID", async () => {
      const response = await app.inject({
        method: "GET",
        url: "/api/v1/sync/jobs/invalid",
      });

      expect(response.statusCode).toBe(400);
      const body = response.json();
      expect(body.error).toBe("VALIDATION_ERROR");
    });
  });

  describe("GET /api/v1/sync/jobs", () => {
    it("should return paginated list of jobs", async () => {
      execute.mockResolvedValueOnce([
        {
          ...sampleCompletedJob,
          ins_code: "POP105A",
          metadata: sampleMatrix.metadata,
        },
        {
          ...sampleSyncJob,
          ins_code: "POP106A",
          metadata: sampleMatrix.metadata,
        },
      ]);
      executeTakeFirst.mockResolvedValueOnce({ count: 2 });

      const response = await app.inject({
        method: "GET",
        url: "/api/v1/sync/jobs",
      });

      expect(response.statusCode).toBe(200);
      const body = response.json();
      expect(body.data.length).toBe(2);
      expect(body.meta.pagination).toBeDefined();
    });

    it("should return empty list when no jobs", async () => {
      execute.mockResolvedValueOnce([]);
      executeTakeFirst.mockResolvedValueOnce({ count: 0 });

      const response = await app.inject({
        method: "GET",
        url: "/api/v1/sync/jobs",
      });

      expect(response.statusCode).toBe(200);
      const body = response.json();
      expect(body.data).toEqual([]);
      expect(body.meta.pagination.total).toBe(0);
    });
  });

  describe("DELETE /api/v1/sync/jobs/:jobId", () => {
    it("should cancel a pending job", async () => {
      executeTakeFirst.mockResolvedValueOnce({
        id: 1,
        status: "PENDING",
        ins_code: "POP105A",
        metadata: sampleMatrix.metadata,
      });

      executeTakeFirstOrThrow.mockResolvedValueOnce({
        ...sampleSyncJob,
        status: "CANCELLED",
        completed_at: new Date(),
      });

      const response = await app.inject({
        method: "DELETE",
        url: "/api/v1/sync/jobs/1",
      });

      expect(response.statusCode).toBe(200);
      const body = response.json();
      expect(body.data.status).toBe("CANCELLED");
    });

    it("should return 404 when job not found", async () => {
      executeTakeFirst.mockResolvedValueOnce(undefined);

      const response = await app.inject({
        method: "DELETE",
        url: "/api/v1/sync/jobs/999",
      });

      expect(response.statusCode).toBe(404);
    });

    it("should return 400 when cancelling running job", async () => {
      executeTakeFirst.mockResolvedValueOnce({
        id: 2,
        status: "RUNNING",
        ins_code: "POP105A",
        metadata: sampleMatrix.metadata,
      });

      const response = await app.inject({
        method: "DELETE",
        url: "/api/v1/sync/jobs/2",
      });

      expect(response.statusCode).toBe(400);
      const body = response.json();
      expect(body.error).toBe("VALIDATION_ERROR");
      expect(body.message).toContain("Cannot cancel job");
    });

    it("should return 400 for invalid job ID", async () => {
      const response = await app.inject({
        method: "DELETE",
        url: "/api/v1/sync/jobs/invalid",
      });

      expect(response.statusCode).toBe(400);
      const body = response.json();
      expect(body.error).toBe("VALIDATION_ERROR");
    });
  });
});
