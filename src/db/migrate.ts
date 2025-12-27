import { sql } from "kysely";

import { db } from "./index.js";

async function migrate() {
  console.log("Running migrations...");

  // Create ins_contexts table
  await db.schema
    .createTable("ins_contexts")
    .ifNotExists()
    .addColumn("id", "integer", (col) => col.primaryKey())
    .addColumn("code", "text", (col) => col.notNull())
    .addColumn("name", "text", (col) => col.notNull())
    .addColumn("level", "integer", (col) => col.notNull())
    .addColumn("parent_code", "text")
    .execute();

  // Create ins_matrices table
  await db.schema
    .createTable("ins_matrices")
    .ifNotExists()
    .addColumn("code", "text", (col) => col.primaryKey())
    .addColumn("name", "text", (col) => col.notNull())
    .addColumn("description", "text")
    .addColumn("context_id", "integer", (col) =>
      col.references("ins_contexts.id")
    )
    .addColumn("start_year", "integer")
    .addColumn("end_year", "integer")
    .addColumn("last_update", "text")
    .addColumn("has_county_data", "integer", (col) => col.defaultTo(0))
    .addColumn("has_uat_data", "integer", (col) => col.defaultTo(0))
    .execute();

  // Create ins_dimensions table
  await db.schema
    .createTable("ins_dimensions")
    .ifNotExists()
    .addColumn("id", "integer", (col) => col.primaryKey().autoIncrement())
    .addColumn("matrix_code", "text", (col) =>
      col.notNull().references("ins_matrices.code")
    )
    .addColumn("dimension_id", "integer", (col) => col.notNull())
    .addColumn("dimension_name", "text", (col) => col.notNull())
    .addColumn("is_territorial", "integer", (col) => col.defaultTo(0))
    .addColumn("is_temporal", "integer", (col) => col.defaultTo(0))
    .execute();

  // Create ins_dimension_options table
  await db.schema
    .createTable("ins_dimension_options")
    .ifNotExists()
    .addColumn("id", "integer", (col) => col.primaryKey().autoIncrement())
    .addColumn("dimension_id", "integer", (col) =>
      col.notNull().references("ins_dimensions.id")
    )
    .addColumn("nom_item_id", "integer", (col) => col.notNull())
    .addColumn("label", "text", (col) => col.notNull())
    .addColumn("offset", "integer", (col) => col.notNull())
    .addColumn("parent_nom_item_id", "integer")
    .execute();

  // Create ins_statistics table
  await db.schema
    .createTable("ins_statistics")
    .ifNotExists()
    .addColumn("id", "integer", (col) => col.primaryKey().autoIncrement())
    .addColumn("matrix_code", "text", (col) =>
      col.notNull().references("ins_matrices.code")
    )
    .addColumn("siruta_code", "text")
    .addColumn("period_year", "integer")
    .addColumn("period_quarter", "integer")
    .addColumn("period_month", "integer")
    .addColumn("indicator_value", "real")
    .addColumn("dimension_values", "text")
    .execute();

  // Create siruta table
  await db.schema
    .createTable("siruta")
    .ifNotExists()
    .addColumn("siruta", "text", (col) => col.primaryKey())
    .addColumn("denloc", "text", (col) => col.notNull())
    .addColumn("jud", "integer", (col) => col.notNull())
    .addColumn("sirsup", "text")
    .addColumn("tip", "integer", (col) => col.notNull())
    .addColumn("niv", "integer", (col) => col.notNull())
    .addColumn("med", "integer", (col) => col.notNull())
    .execute();

  // Create indexes
  await sql`CREATE INDEX IF NOT EXISTS idx_stats_siruta ON ins_statistics(siruta_code)`.execute(
    db
  );
  await sql`CREATE INDEX IF NOT EXISTS idx_stats_period ON ins_statistics(period_year, matrix_code)`.execute(
    db
  );
  await sql`CREATE INDEX IF NOT EXISTS idx_siruta_jud ON siruta(jud)`.execute(
    db
  );

  console.log("Migrations completed successfully!");
  await db.destroy();
}

migrate().catch((err: unknown) => {
  console.error("Migration failed:", err);
  process.exit(1);
});
