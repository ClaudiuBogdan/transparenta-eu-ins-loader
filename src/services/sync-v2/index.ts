/**
 * Sync V2 Services
 *
 * Three-layer sync architecture:
 * 1. Raw Layer - Fetch and preserve exact INS API responses
 * 2. Canonical Layer - Normalize and deduplicate entities
 * 3. Process Layer - Build relationships and fact tables
 */

// Canonical services
export { TerritoryService } from "./canonical/territories.js";
export { TimePeriodService } from "./canonical/time-periods.js";
export { ClassificationService } from "./canonical/classifications.js";
export { UnitService } from "./canonical/units.js";
export { LabelResolver } from "./canonical/label-resolver.js";

// Orchestration
export { SyncOrchestrator } from "./orchestrator.js";
export type { SyncOptions, SyncProgress } from "./orchestrator.js";
