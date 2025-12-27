import { existsSync, mkdirSync } from "node:fs";
import { dirname } from "node:path";

import SQLite from "better-sqlite3";
import { Kysely, SqliteDialect } from "kysely";

import type { Database } from "./schema.js";

const DB_PATH = process.env.DB_PATH ?? "./data/ins.db";

// Ensure data directory exists
const dataDir = dirname(DB_PATH);
if (!existsSync(dataDir)) {
  mkdirSync(dataDir, { recursive: true });
}

const dialect = new SqliteDialect({
  database: new SQLite(DB_PATH),
});

export const db = new Kysely<Database>({
  dialect,
});

export * from "./schema.js";
