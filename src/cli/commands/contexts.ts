/**
 * Contexts command - Browse statistical domains
 */

import { fetchContext, fetchContexts } from "../utils/api.js";
import {
  displayContextsTable,
  displayDomains,
  printError,
  showLoading,
} from "../utils/display.js";

import type { Command } from "commander";

export function registerContextsCommand(program: Command): void {
  program
    .command("contexts [code]")
    .description("List statistical domains and categories")
    .option("-a, --all", "Show all contexts in hierarchy")
    .option("-l, --level <n>", "Filter by hierarchy level")
    .action(
      async (code?: string, options?: { all?: boolean; level?: string }) => {
        const stopLoading = showLoading("Fetching contexts...");

        try {
          if (code !== undefined && code !== "") {
            // Fetch children of specific context
            const contexts = await fetchContext(code);
            stopLoading();

            if (contexts.length === 0) {
              console.log(`No children found for context ${code}`);
              return;
            }

            console.log(`\nChildren of context ${code}:\n`);
            displayContextsTable(contexts);
          } else {
            // Fetch all contexts
            const contexts = await fetchContexts();
            stopLoading();

            if (options?.all === true) {
              // Show all in table format
              const levelFilter = options.level;
              const filtered =
                levelFilter !== undefined
                  ? contexts.filter(
                      (c) => c.level === parseInt(levelFilter, 10)
                    )
                  : contexts;

              console.log(`\nAll contexts (${String(filtered.length)}):\n`);
              displayContextsTable(filtered);
            } else {
              // Show just root domains
              displayDomains(contexts);
            }
          }
        } catch (error) {
          stopLoading();
          printError(error instanceof Error ? error.message : "Unknown error");
          process.exit(1);
        }
      }
    );
}
