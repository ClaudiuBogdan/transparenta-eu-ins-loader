#!/usr/bin/env node

/**
 * INS Dataset Explorer CLI
 *
 * Interactive CLI for exploring Romanian statistical datasets from INS Tempo.
 */

import { Command } from "commander";

import { registerContextsCommand } from "./commands/contexts.js";
import { registerDimensionsCommand } from "./commands/dimensions.js";
import { registerExploreCommand } from "./commands/explore.js";
import { registerMatricesCommand } from "./commands/matrices.js";
import { registerMatrixCommand } from "./commands/matrix.js";
import { registerQueryCommand } from "./commands/query.js";

const program = new Command();

program
  .name("ins-cli")
  .description("CLI for exploring INS Tempo statistical datasets")
  .version("0.2.0");

// Register all commands
registerExploreCommand(program);
registerContextsCommand(program);
registerMatricesCommand(program);
registerMatrixCommand(program);
registerDimensionsCommand(program);
registerQueryCommand(program);

// Default to explore if no command specified
program.action(() => {
  // Show help by default
  program.outputHelp();
});

program.parse();
