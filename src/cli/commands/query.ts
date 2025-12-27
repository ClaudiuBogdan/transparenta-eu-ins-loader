/**
 * Query command - Fetch data from a matrix
 */

import { checkbox, confirm, select } from "@inquirer/prompts";
import chalk from "chalk";

import {
  buildEncQuery,
  estimateCellCount,
  fetchMatrix,
  parsePivotResponse,
  queryMatrixData,
} from "../utils/api.js";
import {
  displayQueryResults,
  printError,
  printWarning,
  showLoading,
} from "../utils/display.js";

import type { InsDimensionOption } from "../../types/index.js";
import type { Command } from "commander";

const CELL_LIMIT = 30000;

export function registerQueryCommand(program: Command): void {
  program
    .command("query <matrixCode>")
    .description("Query data from a matrix with interactive selection")
    .option("-c, --csv", "Output raw CSV instead of table")
    .option("-a, --all", "Select all options for each dimension (limited)")
    .action(
      async (
        matrixCode: string,
        options?: { csv?: boolean; all?: boolean }
      ) => {
        const stopLoading = showLoading(`Fetching matrix ${matrixCode}...`);

        try {
          const matrix = await fetchMatrix(matrixCode);
          stopLoading();

          console.log(chalk.bold(`\nQuery: ${matrix.matrixName}\n`));

          // Build selections for each dimension
          const selections: number[][] = [];

          for (const dim of matrix.dimensionsMap) {
            console.log(chalk.cyan(`\n${dim.label}:`));
            console.log(
              chalk.gray(`  ${String(dim.options.length)} options available`)
            );

            let selectedIds: number[];

            if (options?.all === true) {
              // Auto-select first 5 options (to avoid huge queries)
              const autoSelect = dim.options.slice(0, 5);
              selectedIds = autoSelect.map((o) => o.nomItemId);
              console.log(
                chalk.gray(
                  `  Auto-selected first ${String(autoSelect.length)} options`
                )
              );
            } else {
              // Interactive selection
              selectedIds = await selectDimensionOptions(
                dim.options,
                dim.label
              );
            }

            if (selectedIds.length === 0) {
              printError(`Must select at least one option for ${dim.label}`);
              process.exit(1);
            }

            selections.push(selectedIds);
          }

          // Check cell count
          const cellCount = estimateCellCount(selections);
          console.log(chalk.gray(`\nEstimated cells: ${String(cellCount)}`));

          if (cellCount > CELL_LIMIT) {
            printWarning(
              `Query would return ${String(cellCount)} cells, exceeding the ${String(CELL_LIMIT)} limit.`
            );
            const proceed = await confirm({
              message: "Try anyway? (API may reject)",
              default: false,
            });
            if (!proceed) {
              console.log("Query cancelled.");
              return;
            }
          }

          // Build and execute query
          const encQuery = buildEncQuery(selections);

          const request = {
            encQuery,
            language: "ro" as const,
            matCode: matrixCode,
            matMaxDim: matrix.details.matMaxDim,
            matRegJ: matrix.details.matRegJ,
            matUMSpec: matrix.details.matUMSpec,
          };

          const stopQueryLoading = showLoading("Querying data...");
          const csvData = await queryMatrixData(request);
          stopQueryLoading();

          if (options?.csv === true) {
            console.log("\n" + csvData);
          } else {
            const rows = parsePivotResponse(csvData);
            displayQueryResults(rows);
          }
        } catch (error) {
          printError(error instanceof Error ? error.message : "Unknown error");
          process.exit(1);
        }
      }
    );
}

async function selectDimensionOptions(
  options: InsDimensionOption[],
  dimensionLabel: string
): Promise<number[]> {
  if (options.length <= 10) {
    // Small number of options - use checkbox
    const selected = await checkbox({
      message: `Select ${dimensionLabel}:`,
      choices: options.map((opt) => ({
        name: opt.label,
        value: opt.nomItemId,
      })),
    });
    return selected;
  }

  // Many options - offer selection modes
  const mode = await select({
    message: `${String(options.length)} options available. How to select?`,
    choices: [
      { name: "Select first N options", value: "first" },
      { name: "Select last N options", value: "last" },
      { name: "Select all (careful with limits!)", value: "all" },
      { name: "Search and select", value: "search" },
    ],
  });

  if (mode === "all") {
    return options.map((o) => o.nomItemId);
  }

  if (mode === "first" || mode === "last") {
    const count = await select({
      message: "How many?",
      choices: [1, 3, 5, 10, 20].map((n) => ({
        name: String(n),
        value: n,
      })),
    });

    const slice =
      mode === "first" ? options.slice(0, count) : options.slice(-count);

    return slice.map((o) => o.nomItemId);
  }

  if (mode === "search") {
    // Simple search - show matching options
    const { input } = await import("@inquirer/prompts");
    const term = await input({ message: "Search term:" });
    const termLower = term.toLowerCase();

    const matches = options.filter((o) =>
      o.label.toLowerCase().includes(termLower)
    );

    if (matches.length === 0) {
      console.log(chalk.yellow("No matches found. Selecting first option."));
      const firstOption = options[0];
      return firstOption !== undefined ? [firstOption.nomItemId] : [];
    }

    const selected = await checkbox({
      message: `Found ${String(matches.length)} matches:`,
      choices: matches.slice(0, 20).map((opt) => ({
        name: opt.label,
        value: opt.nomItemId,
      })),
    });

    return selected;
  }

  const firstOption = options[0];
  return firstOption !== undefined ? [firstOption.nomItemId] : [];
}
