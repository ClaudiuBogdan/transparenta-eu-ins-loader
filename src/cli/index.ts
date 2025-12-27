#!/usr/bin/env node

import chalk from "chalk";
import { Command } from "commander";

const API_URL = process.env.API_URL ?? "http://localhost:3000";

const program = new Command();

program
  .name("ins-cli")
  .description("CLI client for INS Tempo Scraper")
  .version("0.1.0");

// Navigation commands

program
  .command("contexts")
  .description("List all statistical domains")
  .action(async () => {
    try {
      const response = await fetch(`${API_URL}/api/contexts`);
      const { data } = (await response.json()) as { data: unknown[] };

      console.log(chalk.bold("\nStatistical Domains:\n"));
      for (const ctx of data as {
        code: string;
        name: string;
        id: number;
      }[]) {
        console.log(
          `  ${chalk.cyan(ctx.code)} - ${ctx.name} (id: ${String(ctx.id)})`
        );
      }
      console.log();
    } catch (error) {
      console.error(
        chalk.red("Error:"),
        error instanceof Error ? error.message : "Unknown error"
      );
      process.exit(1);
    }
  });

program
  .command("matrices")
  .description("List available matrices")
  .option("--uat-only", "Only show matrices with UAT-level data")
  .option("--limit <n>", "Limit results", "20")
  .action(async (options: { uatOnly?: boolean; limit: string }) => {
    try {
      const url =
        options.uatOnly === true
          ? `${API_URL}/api/matrices?uatOnly=true`
          : `${API_URL}/api/matrices`;

      const response = await fetch(url);
      const { data, warning } = (await response.json()) as {
        data: unknown[];
        warning?: string;
      };

      if (warning !== undefined && warning !== "") {
        console.log(chalk.yellow(`\nWarning: ${warning}`));
      }

      const limit = parseInt(options.limit, 10);
      const matrices = (data as { code: string; name: string }[]).slice(
        0,
        limit
      );

      console.log(
        chalk.bold(
          `\nMatrices (showing ${String(matrices.length)} of ${String(data.length)}):\n`
        )
      );
      for (const matrix of matrices) {
        console.log(`  ${chalk.green(matrix.code)} - ${matrix.name}`);
      }
      console.log();
    } catch (error) {
      console.error(
        chalk.red("Error:"),
        error instanceof Error ? error.message : "Unknown error"
      );
      process.exit(1);
    }
  });

program
  .command("matrix <code>")
  .description("Show details for a specific matrix")
  .action(async (code: string) => {
    try {
      const response = await fetch(`${API_URL}/api/matrices/${code}`);

      if (!response.ok) {
        console.error(chalk.red(`Error: Matrix ${code} not found`));
        process.exit(1);
      }

      const { data } = (await response.json()) as {
        data: {
          matrixName: string;
          matrixDescription: string;
          startYear: number;
          endYear: number;
          lastUpdate: string;
          matrixDetails: { nomJud: number; nomLoc: number };
          dimensions: {
            dimensionId: number;
            dimensionName: string;
            options: unknown[];
          }[];
        };
      };

      console.log(chalk.bold(`\nMatrix: ${data.matrixName}\n`));
      console.log(`  Description: ${data.matrixDescription}`);
      console.log(
        `  Period: ${String(data.startYear)} - ${String(data.endYear)}`
      );
      console.log(`  Last Update: ${data.lastUpdate}`);
      console.log(
        `  County Data: ${data.matrixDetails.nomJud !== 0 ? chalk.green("Yes") : chalk.gray("No")}`
      );
      console.log(
        `  UAT Data: ${data.matrixDetails.nomLoc !== 0 ? chalk.green("Yes") : chalk.gray("No")}`
      );

      console.log(chalk.bold("\n  Dimensions:"));
      for (const dim of data.dimensions) {
        console.log(
          `    [${String(dim.dimensionId)}] ${dim.dimensionName} (${String(dim.options.length)} options)`
        );
      }
      console.log();
    } catch (error) {
      console.error(
        chalk.red("Error:"),
        error instanceof Error ? error.message : "Unknown error"
      );
      process.exit(1);
    }
  });

// Scraping commands

program
  .command("scrape <code>")
  .description("Scrape data for a specific matrix")
  .option("--year <year>", "Limit to specific year")
  .action((code: string, options: { year?: string }) => {
    console.log(chalk.yellow(`\nScraping ${code}...`));
    if (options.year !== undefined && options.year !== "") {
      console.log(chalk.gray(`  Year filter: ${options.year}`));
    }
    console.log(chalk.red("\n  Not yet implemented\n"));
  });

// Data query commands

program
  .command("query <matrixCode>")
  .description("Query scraped data")
  .option("--siruta <code>", "Filter by SIRUTA code")
  .option("--year <year>", "Filter by year")
  .action((matrixCode: string, options: { siruta?: string; year?: string }) => {
    console.log(chalk.yellow(`\nQuerying ${matrixCode}...`));
    if (options.siruta !== undefined && options.siruta !== "") {
      console.log(chalk.gray(`  SIRUTA filter: ${options.siruta}`));
    }
    if (options.year !== undefined && options.year !== "") {
      console.log(chalk.gray(`  Year filter: ${options.year}`));
    }
    console.log(chalk.red("\n  Not yet implemented\n"));
  });

program.parse();
