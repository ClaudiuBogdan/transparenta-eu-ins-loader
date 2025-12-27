/**
 * Dimensions command - Browse matrix dimensions and options
 */

import { fetchMatrix } from "../utils/api.js";
import {
  displayDimensionOptions,
  displayDimensionsList,
  printError,
  showLoading,
} from "../utils/display.js";

import type { Command } from "commander";

export function registerDimensionsCommand(program: Command): void {
  program
    .command("dimensions <matrixCode> [dimIndex]")
    .alias("dims")
    .description("Browse dimensions and their options for a matrix")
    .option("-l, --limit <n>", "Limit number of options shown", "50")
    .action(
      async (
        matrixCode: string,
        dimIndex?: string,
        options?: { limit?: string }
      ) => {
        const stopLoading = showLoading(`Fetching matrix ${matrixCode}...`);

        try {
          const matrix = await fetchMatrix(matrixCode);
          stopLoading();

          if (dimIndex !== undefined && dimIndex !== "") {
            // Show options for specific dimension
            const idx = parseInt(dimIndex, 10);

            if (idx < 0 || idx >= matrix.dimensionsMap.length) {
              printError(
                `Invalid dimension index. Valid range: 0-${String(matrix.dimensionsMap.length - 1)}`
              );
              process.exit(1);
            }

            const dimension = matrix.dimensionsMap[idx];
            if (dimension === undefined) {
              printError(`Dimension at index ${String(idx)} not found`);
              process.exit(1);
            }

            const limit =
              options?.limit !== undefined
                ? parseInt(options.limit, 10)
                : undefined;

            displayDimensionOptions(dimension, limit);
          } else {
            // List all dimensions
            console.log(`\nDimensions for ${matrixCode}:\n`);
            displayDimensionsList(matrix.dimensionsMap);
            console.log(
              `\nUse 'ins-cli dimensions ${matrixCode} <index>' to see options for a dimension.\n`
            );
          }
        } catch (error) {
          stopLoading();
          printError(error instanceof Error ? error.message : "Unknown error");
          process.exit(1);
        }
      }
    );
}
