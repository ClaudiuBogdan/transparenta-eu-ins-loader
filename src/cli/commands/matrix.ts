/**
 * Matrix command - View dataset details
 */

import { fetchMatrix } from "../utils/api.js";
import {
  displayMatrixDetails,
  printError,
  showLoading,
} from "../utils/display.js";

import type { Command } from "commander";

export function registerMatrixCommand(program: Command): void {
  program
    .command("matrix <code>")
    .description("Show details for a specific dataset")
    .option("-j, --json", "Output as JSON")
    .action(async (code: string, options?: { json?: boolean }) => {
      const stopLoading = showLoading(`Fetching matrix ${code}...`);

      try {
        const matrix = await fetchMatrix(code);
        stopLoading();

        if (options?.json === true) {
          console.log(JSON.stringify(matrix, null, 2));
        } else {
          displayMatrixDetails(matrix, code);
        }
      } catch (error) {
        stopLoading();
        printError(error instanceof Error ? error.message : "Unknown error");
        process.exit(1);
      }
    });
}
