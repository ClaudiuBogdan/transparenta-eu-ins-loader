/**
 * Display utilities for CLI output formatting
 */

import chalk from "chalk";
import CliTable3 from "cli-table3";

import type {
  InsContext,
  InsDimension,
  InsDimensionOption,
  InsMatrix,
  InsMatrixListItem,
} from "../../types/index.js";

/**
 * Display contexts in a formatted table
 */
export function displayContextsTable(contexts: InsContext[]): void {
  const table = new CliTable3({
    head: [chalk.cyan("Code"), chalk.cyan("Name"), chalk.cyan("Level")],
    colWidths: [10, 70, 8],
    wordWrap: true,
  });

  for (const ctx of contexts) {
    table.push([ctx.context.code, ctx.context.name, String(ctx.level)]);
  }

  console.log(table.toString());
}

/**
 * Display root domains (level 0 contexts)
 */
export function displayDomains(contexts: InsContext[]): void {
  const domains = contexts.filter((c) => c.level === 0);

  console.log(chalk.bold("\nStatistical Domains:\n"));

  for (const domain of domains) {
    console.log(
      `  ${chalk.cyan(domain.context.code.padStart(2))} - ${domain.context.name}`
    );
  }
  console.log();
}

/**
 * Display matrices in a formatted table
 */
export function displayMatricesTable(
  matrices: InsMatrixListItem[],
  showIndex = false
): void {
  const headers = showIndex
    ? [chalk.cyan("#"), chalk.cyan("Code"), chalk.cyan("Name")]
    : [chalk.cyan("Code"), chalk.cyan("Name")];

  const colWidths = showIndex ? [5, 12, 70] : [12, 75];

  const table = new CliTable3({
    head: headers,
    colWidths,
    wordWrap: true,
  });

  for (const [index, matrix] of matrices.entries()) {
    const row = showIndex
      ? [String(index + 1), chalk.green(matrix.code), matrix.name]
      : [chalk.green(matrix.code), matrix.name];
    table.push(row);
  }

  console.log(table.toString());
}

/**
 * Display matrix details
 */
export function displayMatrixDetails(matrix: InsMatrix, code: string): void {
  console.log(chalk.bold.underline(`\nMatrix: ${code}\n`));

  console.log(chalk.bold("Name:"));
  console.log(`  ${matrix.matrixName}\n`);

  if (matrix.definitie !== undefined && matrix.definitie !== "") {
    console.log(chalk.bold("Definition:"));
    console.log(`  ${matrix.definitie}\n`);
  }

  console.log(chalk.bold("Metadata:"));
  console.log(
    `  Last Update: ${matrix.ultimaActualizare ?? chalk.gray("N/A")}`
  );
  if (matrix.periodicitati !== undefined && matrix.periodicitati.length > 0) {
    console.log(`  Periodicity: ${matrix.periodicitati.join(", ")}`);
  }
  console.log();

  console.log(chalk.bold("Data Availability:"));
  console.log(
    `  County level:   ${matrix.details.nomJud > 0 ? chalk.green("Yes") : chalk.gray("No")}`
  );
  console.log(
    `  UAT level:      ${matrix.details.nomLoc > 0 ? chalk.green("Yes") : chalk.gray("No")}`
  );
  console.log(
    `  SIRUTA codes:   ${matrix.details.matSiruta > 0 ? chalk.green("Yes") : chalk.gray("No")}`
  );
  console.log();

  console.log(
    chalk.bold(`Dimensions (${String(matrix.dimensionsMap.length)}):`)
  );
  for (const dim of matrix.dimensionsMap) {
    console.log(
      `  [${chalk.cyan(String(dim.dimCode))}] ${dim.label} (${String(dim.options.length)} options)`
    );
  }
  console.log();

  if (matrix.observatii !== undefined && matrix.observatii !== "") {
    console.log(chalk.bold("Notes:"));
    console.log(`  ${matrix.observatii}\n`);
  }
}

/**
 * Display dimensions list for a matrix
 */
export function displayDimensionsList(dimensions: InsDimension[]): void {
  const table = new CliTable3({
    head: [
      chalk.cyan("Index"),
      chalk.cyan("Code"),
      chalk.cyan("Label"),
      chalk.cyan("Options"),
    ],
    colWidths: [8, 8, 50, 10],
  });

  for (const [index, dim] of dimensions.entries()) {
    table.push([
      String(index),
      String(dim.dimCode),
      dim.label,
      String(dim.options.length),
    ]);
  }

  console.log(table.toString());
}

/**
 * Display dimension options (with optional hierarchy support)
 */
export function displayDimensionOptions(
  dimension: InsDimension,
  limit?: number
): void {
  console.log(chalk.bold(`\n${dimension.label}:\n`));

  const options =
    limit !== undefined ? dimension.options.slice(0, limit) : dimension.options;

  // Check if hierarchical (has parentId values)
  const hasHierarchy = options.some((opt) => opt.parentId !== null);

  if (hasHierarchy) {
    displayHierarchicalOptions(options);
  } else {
    displayFlatOptions(options);
  }

  if (limit !== undefined && dimension.options.length > limit) {
    const remaining = dimension.options.length - limit;
    console.log(chalk.gray(`\n  ... and ${String(remaining)} more options`));
  }
}

function displayFlatOptions(options: InsDimensionOption[]): void {
  const table = new CliTable3({
    head: [chalk.cyan("ID"), chalk.cyan("Label")],
    colWidths: [10, 70],
    wordWrap: true,
  });

  for (const opt of options) {
    table.push([String(opt.nomItemId), opt.label]);
  }

  console.log(table.toString());
}

function displayHierarchicalOptions(options: InsDimensionOption[]): void {
  // Build parent-child relationships
  const byParent = new Map<number | null, InsDimensionOption[]>();

  for (const opt of options) {
    const parentId = opt.parentId ?? null;
    const children = byParent.get(parentId) ?? [];
    children.push(opt);
    byParent.set(parentId, children);
  }

  // Display tree structure
  const printLevel = (parentId: number | null, indent: number): void => {
    const children = byParent.get(parentId) ?? [];
    for (const child of children) {
      const prefix = "  ".repeat(indent);
      const idStr = chalk.gray(`[${String(child.nomItemId)}]`);
      console.log(`${prefix}${idStr} ${child.label}`);
      printLevel(child.nomItemId, indent + 1);
    }
  };

  printLevel(null, 0);
}

/**
 * Display query results as a table
 */
export function displayQueryResults(rows: string[][]): void {
  if (rows.length === 0) {
    console.log(chalk.yellow("No data returned"));
    return;
  }

  const headerRow = rows[0];
  if (headerRow === undefined) {
    console.log(chalk.yellow("No data returned"));
    return;
  }

  const headers = headerRow.map((h) => chalk.cyan(h.trim()));
  const dataRows = rows.slice(1);

  // Calculate column widths based on content
  const colWidths = headers.map((header, i) => {
    const headerLen = header.length;
    const dataLens = dataRows.map((row) => (row[i] ?? "").length);
    const maxLen = Math.max(headerLen, ...dataLens);
    return Math.min(Math.max(maxLen + 2, 8), 30);
  });

  const table = new CliTable3({
    head: headers,
    colWidths,
    wordWrap: true,
  });

  for (const row of dataRows) {
    table.push(row.map((cell) => cell.trim()));
  }

  console.log(table.toString());
  console.log(chalk.gray(`\n${String(dataRows.length)} row(s) returned\n`));
}

/**
 * Display a spinner-like message (for long operations)
 */
export function showLoading(message: string): () => void {
  const frames = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
  let i = 0;

  const interval = setInterval(() => {
    process.stdout.write(`\r${chalk.cyan(frames[i])} ${message}`);
    i = (i + 1) % frames.length;
  }, 80);

  return () => {
    clearInterval(interval);
    process.stdout.write("\r" + " ".repeat(message.length + 3) + "\r");
  };
}

/**
 * Print error message
 */
export function printError(message: string): void {
  console.error(chalk.red("Error:"), message);
}

/**
 * Print success message
 */
export function printSuccess(message: string): void {
  console.log(chalk.green("Success:"), message);
}

/**
 * Print warning message
 */
export function printWarning(message: string): void {
  console.log(chalk.yellow("Warning:"), message);
}
