/**
 * Territory fixtures for testing
 */

import type { BilingualText } from "../../src/db/types.js";

export const sampleTerritories = [
  {
    id: 1,
    code: "RO",
    siruta_code: null,
    names: { ro: "ROMANIA", en: "ROMANIA" } as BilingualText,
    level: "NATIONAL" as const,
    parent_id: null,
    path: "RO",
    created_at: new Date(),
    updated_at: new Date(),
  },
  {
    id: 2,
    code: "RO1",
    siruta_code: null,
    names: { ro: "MACROREGIUNEA UNU", en: "MACROREGION ONE" } as BilingualText,
    level: "NUTS1" as const,
    parent_id: 1,
    path: "RO.RO1",
    created_at: new Date(),
    updated_at: new Date(),
  },
  {
    id: 5,
    code: "RO11",
    siruta_code: null,
    names: { ro: "NORD-VEST", en: "NORTH-WEST" } as BilingualText,
    level: "NUTS2" as const,
    parent_id: 2,
    path: "RO.RO1.RO11",
    created_at: new Date(),
    updated_at: new Date(),
  },
  {
    id: 14,
    code: "AB",
    siruta_code: "10001",
    names: { ro: "Alba", en: "Alba" } as BilingualText,
    level: "NUTS3" as const,
    parent_id: 5,
    path: "RO.RO1.RO12.AB",
    created_at: new Date(),
    updated_at: new Date(),
  },
  {
    id: 15,
    code: "AR",
    siruta_code: "10002",
    names: { ro: "Arad", en: "Arad" } as BilingualText,
    level: "NUTS3" as const,
    parent_id: 5,
    path: "RO.RO1.RO11.AR",
    created_at: new Date(),
    updated_at: new Date(),
  },
  {
    id: 20,
    code: "CJ",
    siruta_code: "10003",
    names: { ro: "Cluj", en: "Cluj" } as BilingualText,
    level: "NUTS3" as const,
    parent_id: 5,
    path: "RO.RO1.RO11.CJ",
    created_at: new Date(),
    updated_at: new Date(),
  },
];

export const nationalTerritory = sampleTerritories[0]!;
export const countyTerritories = sampleTerritories.filter(
  (t) => t.level === "NUTS3"
);

export function createTerritory(
  overrides: Partial<(typeof sampleTerritories)[0]> = {}
) {
  return { ...sampleTerritories[0]!, ...overrides };
}

export function getTerritoryById(id: number) {
  return sampleTerritories.find((t) => t.id === id);
}

export function getTerritoriesByLevel(level: string) {
  return sampleTerritories.filter((t) => t.level === level);
}
