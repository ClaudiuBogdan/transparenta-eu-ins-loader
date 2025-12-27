/**
 * Explore command - Interactive dataset browser
 */

import { confirm, select } from "@inquirer/prompts";
import chalk from "chalk";

import {
  fetchContext,
  fetchContexts,
  fetchMatrix,
  fetchMatricesList,
} from "../utils/api.js";
import {
  displayDimensionOptions,
  displayMatrixDetails,
  printError,
  showLoading,
} from "../utils/display.js";

import type {
  InsContext,
  InsMatrix,
  InsMatrixListItem,
} from "../../types/index.js";
import type { Command } from "commander";

export function registerExploreCommand(program: Command): void {
  program
    .command("explore")
    .description("Interactive dataset browser")
    .action(async () => {
      try {
        await runExplorer();
      } catch (error) {
        if ((error as { name?: string }).name === "ExitPromptError") {
          console.log("\nGoodbye!");
          return;
        }
        printError(error instanceof Error ? error.message : "Unknown error");
        process.exit(1);
      }
    });
}

async function runExplorer(): Promise<void> {
  console.log(chalk.bold("\nINS Dataset Explorer\n"));
  console.log(
    chalk.gray("Navigate through statistical domains to find datasets.\n")
  );
  console.log(chalk.gray("Press Ctrl+C at any time to exit.\n"));

  const mode = await select({
    message: "How would you like to explore?",
    choices: [
      { name: "Browse by domain hierarchy", value: "hierarchy" },
      { name: "Search all matrices", value: "search" },
      { name: "Quick pick popular matrices", value: "popular" },
    ],
  });

  if (mode === "hierarchy") {
    await exploreByHierarchy();
  } else if (mode === "search") {
    await searchMatrices();
  } else {
    await showPopularMatrices();
  }
}

async function exploreByHierarchy(): Promise<void> {
  const stopLoading = showLoading("Loading domains...");
  const allContexts = await fetchContexts();
  stopLoading();

  // Start with root domains
  const domains = allContexts.filter((c) => c.level === 0);

  let currentContext: InsContext | null = null;

  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- Intentional infinite loop for interactive navigation
  while (true) {
    let choices;

    if (currentContext === null) {
      // Show root domains
      choices = domains.map((d) => ({
        name: d.context.name,
        value: d.context.code,
        description: `Code: ${d.context.code}`,
      }));
    } else {
      // Fetch children
      const stopChildLoading = showLoading("Loading...");
      const children = await fetchContext(currentContext.context.code);
      stopChildLoading();

      if (children.length === 0) {
        // Leaf node - might have matrices
        console.log(
          chalk.yellow("\nNo sub-categories. Checking for matrices...")
        );
        await showMatricesForContext(currentContext.context.code, allContexts);
        currentContext = null;
        continue;
      }

      // Check if children are matrices (childrenUrl = "matrix")
      if (currentContext.context.childrenUrl === "matrix") {
        await showMatricesForContext(currentContext.context.code, allContexts);
        currentContext = null;
        continue;
      }

      choices = [
        { name: chalk.gray("← Back"), value: "__back__" },
        ...children.map((c) => ({
          name: c.context.name,
          value: c.context.code,
          description:
            c.context.childrenUrl === "matrix" ? "Contains datasets" : "",
        })),
      ];
    }

    const selected: string = await select({
      message:
        currentContext !== null
          ? `${currentContext.context.name}:`
          : "Select a domain:",
      choices,
    });

    if (selected === "__back__") {
      currentContext = null;
    } else {
      const ctx: InsContext | undefined = allContexts.find(
        (c) => c.context.code === selected
      );
      if (ctx !== undefined) {
        currentContext = ctx;
      }
    }
  }
}

async function showMatricesForContext(
  contextCode: string,
  allContexts: InsContext[]
): Promise<void> {
  // Find all contexts that are children/descendants of this context
  const childCodes = new Set<string>();
  const queue = [contextCode];

  while (queue.length > 0) {
    const code = queue.shift();
    if (code === undefined) continue;

    childCodes.add(code);

    const children = allContexts.filter((c) => c.parentCode === code);
    for (const child of children) {
      queue.push(child.context.code);
    }
  }

  // Get matrices list
  const stopLoading = showLoading("Loading matrices...");
  const allMatrices = await fetchMatricesList();
  stopLoading();

  // Filter - unfortunately the list doesn't include context info
  // So we just show all and let user pick
  console.log(
    chalk.yellow(`\nFound ${String(allMatrices.length)} total matrices.`)
  );
  console.log(chalk.gray("Enter a search term to filter:\n"));

  const { input } = await import("@inquirer/prompts");
  const term = await input({ message: "Search:" });

  const termLower = term.toLowerCase();
  const matches = allMatrices.filter(
    (m) =>
      m.code.toLowerCase().includes(termLower) ||
      m.name.toLowerCase().includes(termLower)
  );

  if (matches.length === 0) {
    console.log(chalk.yellow("No matches found."));
    return;
  }

  await selectAndExploreMatrix(matches.slice(0, 20));
}

async function searchMatrices(): Promise<void> {
  const stopLoading = showLoading("Loading matrices...");
  const allMatrices = await fetchMatricesList();
  stopLoading();

  const { input } = await import("@inquirer/prompts");
  const term = await input({
    message: `Search ${String(allMatrices.length)} matrices:`,
  });

  const termLower = term.toLowerCase();
  const matches = allMatrices.filter(
    (m) =>
      m.code.toLowerCase().includes(termLower) ||
      m.name.toLowerCase().includes(termLower)
  );

  if (matches.length === 0) {
    console.log(chalk.yellow("No matches found."));
    return;
  }

  console.log(chalk.green(`\nFound ${String(matches.length)} matches:\n`));
  await selectAndExploreMatrix(matches.slice(0, 30));
}

async function showPopularMatrices(): Promise<void> {
  const popular: { code: string; name: string }[] = [
    { code: "POP105A", name: "Populatia rezidenta (1 ianuarie)" },
    { code: "POP107D", name: "Populatia dupa domiciliu (UAT)" },
    { code: "SOM101E", name: "Someri inregistrati" },
    { code: "TUR104E", name: "Capacitate turistica" },
    { code: "LOC103B", name: "Locuinte" },
  ];

  console.log(chalk.bold("\nPopular Datasets:\n"));
  await selectAndExploreMatrixSimple(popular);
}

async function selectAndExploreMatrix(
  matrices: InsMatrixListItem[]
): Promise<void> {
  await selectAndExploreMatrixSimple(matrices);
}

async function selectAndExploreMatrixSimple(
  matrices: { code: string; name: string }[]
): Promise<void> {
  const selected = await select({
    message: "Select a matrix:",
    choices: [
      ...matrices.map((m) => ({
        name: `${chalk.green(m.code)} - ${m.name}`,
        value: m.code,
      })),
      { name: chalk.gray("← Back"), value: "__back__" },
    ],
  });

  if (selected === "__back__") {
    await runExplorer();
    return;
  }

  await exploreMatrix(selected);
}

async function exploreMatrix(code: string): Promise<void> {
  const stopLoading = showLoading(`Loading ${code}...`);
  const matrix = await fetchMatrix(code);
  stopLoading();

  displayMatrixDetails(matrix, code);

  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- Intentional infinite loop for menu navigation
  while (true) {
    const action = await select({
      message: "What would you like to do?",
      choices: [
        { name: "Browse dimensions", value: "dims" },
        { name: "View dimension options", value: "opts" },
        { name: "Export metadata as JSON", value: "json" },
        { name: chalk.gray("← Back to search"), value: "__back__" },
      ],
    });

    if (action === "__back__") {
      await runExplorer();
      return;
    }

    if (action === "json") {
      console.log(JSON.stringify(matrix, null, 2));
    }

    if (action === "dims") {
      console.log(chalk.bold("\nDimensions:\n"));
      for (const [i, dim] of matrix.dimensionsMap.entries()) {
        console.log(
          `  [${String(i)}] ${chalk.cyan(dim.label)} - ${String(dim.options.length)} options`
        );
      }
      console.log();
    }

    if (action === "opts") {
      await browseDimensionOptions(matrix);
    }
  }
}

async function browseDimensionOptions(matrix: InsMatrix): Promise<void> {
  const dimChoice = await select({
    message: "Select a dimension:",
    choices: matrix.dimensionsMap.map((dim, i) => ({
      name: `${dim.label} (${String(dim.options.length)} options)`,
      value: i,
    })),
  });

  const dimension = matrix.dimensionsMap[dimChoice];
  if (dimension === undefined) {
    console.log(chalk.yellow("Dimension not found"));
    return;
  }

  displayDimensionOptions(dimension, 30);

  const showMore = await confirm({
    message: "Show all options?",
    default: false,
  });

  if (showMore) {
    displayDimensionOptions(dimension);
  }
}
