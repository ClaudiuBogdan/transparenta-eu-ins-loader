/**
 * Direct INS Tempo API client for CLI
 * Bypasses the REST server - calls INS API directly
 */

import type {
  InsContext,
  InsMatrix,
  InsMatrixListItem,
  InsQueryRequest,
} from "../../types/index.js";

const BASE_URL = "http://statistici.insse.ro:8077/tempo-ins";
const RATE_LIMIT_MS = 750;

let lastRequestTime = 0;

async function rateLimitedFetch(
  url: string,
  options?: RequestInit
): Promise<Response> {
  const now = Date.now();
  const elapsed = now - lastRequestTime;

  if (elapsed < RATE_LIMIT_MS) {
    const waitTime = RATE_LIMIT_MS - elapsed;
    await new Promise((resolve) => setTimeout(resolve, waitTime));
  }

  lastRequestTime = Date.now();
  return fetch(url, options);
}

/**
 * Fetch all contexts (full hierarchy)
 */
export async function fetchContexts(): Promise<InsContext[]> {
  const response = await rateLimitedFetch(`${BASE_URL}/context/`);
  if (!response.ok) {
    throw new Error(`Failed to fetch contexts: ${response.statusText}`);
  }
  return (await response.json()) as InsContext[];
}

/**
 * Fetch children of a specific context
 */
export async function fetchContext(code: string): Promise<InsContext[]> {
  const response = await rateLimitedFetch(`${BASE_URL}/context/${code}`);
  if (!response.ok) {
    throw new Error(`Failed to fetch context ${code}: ${response.statusText}`);
  }
  return (await response.json()) as InsContext[];
}

/**
 * Fetch all matrices list
 */
export async function fetchMatricesList(
  lang: "ro" | "en" = "ro"
): Promise<InsMatrixListItem[]> {
  const response = await rateLimitedFetch(
    `${BASE_URL}/matrix/matrices?lang=${lang}`
  );
  if (!response.ok) {
    throw new Error(`Failed to fetch matrices: ${response.statusText}`);
  }
  return (await response.json()) as InsMatrixListItem[];
}

/**
 * Fetch matrix metadata
 */
export async function fetchMatrix(code: string): Promise<InsMatrix> {
  const response = await rateLimitedFetch(`${BASE_URL}/matrix/${code}`);
  if (!response.ok) {
    throw new Error(`Failed to fetch matrix ${code}: ${response.statusText}`);
  }
  return (await response.json()) as InsMatrix;
}

/**
 * Query matrix data using /pivot endpoint
 * Returns CSV text
 */
export async function queryMatrixData(
  request: InsQueryRequest
): Promise<string> {
  const response = await rateLimitedFetch(`${BASE_URL}/pivot`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(request),
  });

  if (!response.ok) {
    throw new Error(
      `Failed to query matrix ${request.matCode}: ${response.statusText}`
    );
  }

  return response.text();
}

/**
 * Build encQuery string from nomItemId selections
 */
export function buildEncQuery(selections: number[][]): string {
  return selections.map((dimSelections) => dimSelections.join(",")).join(":");
}

/**
 * Estimate cell count for a query
 */
export function estimateCellCount(selections: number[][]): number {
  return selections.reduce((acc, dim) => acc * dim.length, 1);
}

/**
 * Parse CSV pivot response into rows
 */
export function parsePivotResponse(csvText: string): string[][] {
  return csvText
    .split("\n")
    .filter((row) => row.trim() !== "")
    .map((row) => row.split(", "));
}
