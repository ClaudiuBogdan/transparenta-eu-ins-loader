/**
 * Kysely Database Mock Utilities
 *
 * Provides mock implementations for Kysely query builders
 * to enable unit testing without a real database.
 */

import { vi } from "vitest";

import type { Database } from "../../src/db/types.js";
import type { Kysely } from "kysely";

/**
 * Creates a mock select query builder that returns specified data
 */
export function createMockSelectBuilder<T>(data: T[]): MockSelectBuilder<T> {
  const builder: MockSelectBuilder<T> = {
    select: vi.fn().mockReturnThis(),
    selectAll: vi.fn().mockReturnThis(),
    where: vi.fn().mockReturnThis(),
    whereRef: vi.fn().mockReturnThis(),
    orderBy: vi.fn().mockReturnThis(),
    groupBy: vi.fn().mockReturnThis(),
    having: vi.fn().mockReturnThis(),
    limit: vi.fn().mockReturnThis(),
    offset: vi.fn().mockReturnThis(),
    leftJoin: vi.fn().mockReturnThis(),
    innerJoin: vi.fn().mockReturnThis(),
    rightJoin: vi.fn().mockReturnThis(),
    fullJoin: vi.fn().mockReturnThis(),
    execute: vi.fn().mockResolvedValue(data),
    executeTakeFirst: vi.fn().mockResolvedValue(data[0]),
    executeTakeFirstOrThrow: vi.fn().mockImplementation(async () => {
      if (data.length === 0) {
        throw new Error("No result found");
      }
      return data[0];
    }),
    // Additional methods
    distinctOn: vi.fn().mockReturnThis(),
    distinct: vi.fn().mockReturnThis(),
    forUpdate: vi.fn().mockReturnThis(),
    forShare: vi.fn().mockReturnThis(),
    union: vi.fn().mockReturnThis(),
    unionAll: vi.fn().mockReturnThis(),
    intersect: vi.fn().mockReturnThis(),
    except: vi.fn().mockReturnThis(),
    as: vi.fn().mockReturnThis(),
    mockData: data,
    setMockData: (newData: unknown[]) => {
      builder.mockData = newData as T[];
      builder.execute.mockResolvedValue(newData);
      builder.executeTakeFirst.mockResolvedValue(newData[0]);
    },
  };

  return builder;
}

export interface MockSelectBuilder<T = unknown> {
  select: ReturnType<typeof vi.fn>;
  selectAll: ReturnType<typeof vi.fn>;
  where: ReturnType<typeof vi.fn>;
  whereRef: ReturnType<typeof vi.fn>;
  orderBy: ReturnType<typeof vi.fn>;
  groupBy: ReturnType<typeof vi.fn>;
  having: ReturnType<typeof vi.fn>;
  limit: ReturnType<typeof vi.fn>;
  offset: ReturnType<typeof vi.fn>;
  leftJoin: ReturnType<typeof vi.fn>;
  innerJoin: ReturnType<typeof vi.fn>;
  rightJoin: ReturnType<typeof vi.fn>;
  fullJoin: ReturnType<typeof vi.fn>;
  execute: ReturnType<typeof vi.fn>;
  executeTakeFirst: ReturnType<typeof vi.fn>;
  executeTakeFirstOrThrow: ReturnType<typeof vi.fn>;
  distinctOn: ReturnType<typeof vi.fn>;
  distinct: ReturnType<typeof vi.fn>;
  forUpdate: ReturnType<typeof vi.fn>;
  forShare: ReturnType<typeof vi.fn>;
  union: ReturnType<typeof vi.fn>;
  unionAll: ReturnType<typeof vi.fn>;
  intersect: ReturnType<typeof vi.fn>;
  except: ReturnType<typeof vi.fn>;
  as: ReturnType<typeof vi.fn>;
  mockData: T[];
  setMockData: (data: unknown[]) => void;
}

/**
 * Creates a mock insert query builder
 */
export function createMockInsertBuilder(returnData?: unknown) {
  const builder = {
    values: vi.fn().mockReturnThis(),
    onConflict: vi.fn().mockReturnValue({
      column: vi.fn().mockReturnThis(),
      columns: vi.fn().mockReturnThis(),
      doNothing: vi.fn().mockReturnThis(),
      doUpdateSet: vi.fn().mockReturnThis(),
    }),
    returning: vi.fn().mockReturnThis(),
    execute: vi.fn().mockResolvedValue([returnData]),
    executeTakeFirst: vi.fn().mockResolvedValue(returnData),
    executeTakeFirstOrThrow: vi.fn().mockResolvedValue(returnData),
  };

  return builder;
}

/**
 * Creates a mock update query builder
 */
export function createMockUpdateBuilder() {
  const builder = {
    set: vi.fn().mockReturnThis(),
    where: vi.fn().mockReturnThis(),
    whereRef: vi.fn().mockReturnThis(),
    returning: vi.fn().mockReturnThis(),
    execute: vi.fn().mockResolvedValue([]),
    executeTakeFirst: vi.fn().mockResolvedValue(undefined),
    executeTakeFirstOrThrow: vi.fn().mockResolvedValue({}),
  };

  return builder;
}

/**
 * Creates a mock delete query builder
 */
export function createMockDeleteBuilder() {
  const builder = {
    where: vi.fn().mockReturnThis(),
    whereRef: vi.fn().mockReturnThis(),
    returning: vi.fn().mockReturnThis(),
    execute: vi.fn().mockResolvedValue([]),
    executeTakeFirst: vi.fn().mockResolvedValue(undefined),
  };

  return builder;
}

/**
 * Creates a mock transaction
 */
export function createMockTransaction() {
  return {
    execute: vi
      .fn()
      .mockImplementation(
        async (callback: (trx: MockDb) => Promise<unknown>) => {
          const trx = createMockDb();
          return callback(trx);
        }
      ),
  };
}

/**
 * Mock function builder (for aggregate functions)
 */
export function createMockFn() {
  return {
    count: vi.fn().mockReturnValue({ as: vi.fn() }),
    sum: vi.fn().mockReturnValue({ as: vi.fn() }),
    avg: vi.fn().mockReturnValue({ as: vi.fn() }),
    min: vi.fn().mockReturnValue({ as: vi.fn() }),
    max: vi.fn().mockReturnValue({ as: vi.fn() }),
    coalesce: vi.fn().mockReturnValue({ as: vi.fn() }),
  };
}

/**
 * Table query builders store
 */
type TableBuilders = Record<
  string,
  {
    select?: MockSelectBuilder;
    insert?: ReturnType<typeof createMockInsertBuilder>;
    update?: ReturnType<typeof createMockUpdateBuilder>;
    delete?: ReturnType<typeof createMockDeleteBuilder>;
  }
>;

/**
 * Creates a complete mock Kysely database instance
 */
export function createMockDb(tableBuilders: TableBuilders = {}) {
  const getOrCreateSelectBuilder = (table: string) => {
    tableBuilders[table] ??= {};
    tableBuilders[table].select ??= createMockSelectBuilder([]);
    return tableBuilders[table].select;
  };

  const getOrCreateInsertBuilder = (table: string) => {
    tableBuilders[table] ??= {};
    tableBuilders[table].insert ??= createMockInsertBuilder();
    return tableBuilders[table].insert;
  };

  const getOrCreateUpdateBuilder = (table: string) => {
    tableBuilders[table] ??= {};
    tableBuilders[table].update ??= createMockUpdateBuilder();
    return tableBuilders[table].update;
  };

  const getOrCreateDeleteBuilder = (table: string) => {
    tableBuilders[table] ??= {};
    tableBuilders[table].delete ??= createMockDeleteBuilder();
    return tableBuilders[table].delete;
  };

  const db = {
    selectFrom: vi.fn().mockImplementation((table: string) => {
      return getOrCreateSelectBuilder(table);
    }),
    insertInto: vi.fn().mockImplementation((table: string) => {
      return getOrCreateInsertBuilder(table);
    }),
    updateTable: vi.fn().mockImplementation((table: string) => {
      return getOrCreateUpdateBuilder(table);
    }),
    deleteFrom: vi.fn().mockImplementation((table: string) => {
      return getOrCreateDeleteBuilder(table);
    }),
    transaction: vi.fn().mockReturnValue(createMockTransaction()),
    fn: createMockFn(),
    dynamic: {
      ref: vi.fn().mockImplementation((col: string) => col),
    },
    tableBuilders: tableBuilders,
    getSelectBuilder: getOrCreateSelectBuilder,
    getInsertBuilder: getOrCreateInsertBuilder,
    getUpdateBuilder: getOrCreateUpdateBuilder,
    getDeleteBuilder: getOrCreateDeleteBuilder,
  };

  return db as unknown as MockDb;
}

export type MockDb = Kysely<Database> & {
  tableBuilders: TableBuilders;
  getSelectBuilder: (table: string) => MockSelectBuilder;
  getInsertBuilder: (
    table: string
  ) => ReturnType<typeof createMockInsertBuilder>;
  getUpdateBuilder: (
    table: string
  ) => ReturnType<typeof createMockUpdateBuilder>;
  getDeleteBuilder: (
    table: string
  ) => ReturnType<typeof createMockDeleteBuilder>;
};

/**
 * Helper to setup mock data for a specific table
 */
export function setupMockTable<T>(
  db: MockDb,
  tableName: string,
  data: T[]
): MockSelectBuilder<T> {
  const builder = createMockSelectBuilder(data);
  db.tableBuilders[tableName] ??= {};
  // Cast to any first to avoid type incompatibility
  db.tableBuilders[tableName].select = builder as unknown as MockSelectBuilder;
  return builder;
}

/**
 * Reset all mock calls
 */
export function resetMockDb(db: MockDb) {
  vi.clearAllMocks();
  // Clear all table builders by setting to empty object
  const tables = Object.keys(db.tableBuilders);
  for (const table of tables) {
    db.tableBuilders[table] = {};
  }
}
