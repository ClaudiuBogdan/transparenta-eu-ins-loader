#!/usr/bin/env node

/**
 * Transparenta EU INS Loader CLI
 *
 * CLI for loading Romanian statistical datasets from INS Tempo for Transparenta.eu.
 */

import { Command } from "commander";

import { registerContextsCommand } from "./commands/contexts.js";
import { registerDbCommand } from "./commands/db.js";
import { registerDimensionsCommand } from "./commands/dimensions.js";
import { registerExploreCommand } from "./commands/explore.js";
import { registerMatricesCommand } from "./commands/matrices.js";
import { registerMatrixCommand } from "./commands/matrix.js";
import { registerQueryCommand } from "./commands/query.js";
import { registerSyncCommand } from "./commands/sync.js";

const program = new Command();

program
  .name("transparenta-ins")
  .description("Transparenta EU - INS Tempo statistical data loader CLI")
  .version("0.3.0");

// Register all commands
registerDbCommand(program);
registerSyncCommand(program);
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
