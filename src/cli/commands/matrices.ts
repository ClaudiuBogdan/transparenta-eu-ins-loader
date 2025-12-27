/**
 * Matrices command - List and search datasets
 */

import { fetchMatrix, fetchMatricesList } from "../utils/api.js";
import {
  displayMatricesTable,
  printError,
  printWarning,
  showLoading,
} from "../utils/display.js";

import type { Command } from "commander";

interface MatricesOptions {
  search?: string;
  uat?: boolean;
  county?: boolean;
  limit?: string;
}

export function registerMatricesCommand(program: Command): void {
  program
    .command("matrices")
    .description("List available datasets (matrices)")
    .option("-s, --search <term>", "Search by name or code")
    .option("-u, --uat", "Only show matrices with UAT-level data")
    .option("-c, --county", "Only show matrices with county-level data")
    .option("-l, --limit <n>", "Limit results", "50")
    .action(async (options: MatricesOptions) => {
      const stopLoading = showLoading("Fetching matrices...");

      try {
        let matrices = await fetchMatricesList();
        stopLoading();

        // Apply search filter
        if (options.search !== undefined && options.search !== "") {
          const searchTerm = options.search.toLowerCase();
          matrices = matrices.filter(
            (m) =>
              m.code.toLowerCase().includes(searchTerm) ||
              m.name.toLowerCase().includes(searchTerm)
          );
        }

        // Apply UAT/County filters (requires fetching each matrix metadata)
        if (options.uat === true || options.county === true) {
          printWarning(
            "Filtering by UAT/county requires fetching metadata for each matrix. This may take a while..."
          );

          const stopFilterLoading = showLoading(
            "Checking matrix capabilities..."
          );
          const filtered = [];

          // Only check first 100 to avoid excessive API calls
          const toCheck = matrices.slice(0, 100);

          for (const m of toCheck) {
            try {
              const matrix = await fetchMatrix(m.code);

              const hasUat = matrix.details.nomLoc > 0;
              const hasCounty = matrix.details.nomJud > 0;

              if (options.uat === true && !hasUat) continue;
              if (options.county === true && !hasCounty) continue;

              filtered.push(m);
            } catch {
              // Skip matrices that fail to fetch
            }
          }

          stopFilterLoading();
          matrices = filtered;

          if (toCheck.length < matrices.length) {
            printWarning(
              `Only checked first 100 matrices. Use --search to narrow down first.`
            );
          }
        }

        const limit = parseInt(options.limit ?? "50", 10);
        const displayed = matrices.slice(0, limit);

        console.log(
          `\nMatrices (showing ${String(displayed.length)} of ${String(matrices.length)}):\n`
        );

        if (displayed.length > 0) {
          displayMatricesTable(displayed, true);
        } else {
          console.log("No matrices found matching your criteria.\n");
        }
      } catch (error) {
        printError(error instanceof Error ? error.message : "Unknown error");
        process.exit(1);
      }
    });
}
