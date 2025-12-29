import { apiLogger } from "../logger.js";

import type {
  InsContext,
  InsMatrix,
  InsQueryRequest,
  InsMatrixListItem,
} from "../types/index.js";

const BASE_URL = "http://statistici.insse.ro:8077/tempo-ins";
const RATE_LIMIT_MS = 750; // 0.75 seconds between requests

/**
 * Supported locales for INS API
 */
export type Locale = "ro" | "en";

let lastRequestTime = 0;

async function rateLimitedFetch(
  url: string,
  options?: RequestInit
): Promise<Response> {
  const now = Date.now();
  const elapsed = now - lastRequestTime;

  if (elapsed < RATE_LIMIT_MS) {
    const waitTime = RATE_LIMIT_MS - elapsed;
    apiLogger.debug({ waitTime }, "Rate limiting: waiting before request");
    await new Promise((resolve) => setTimeout(resolve, waitTime));
  }

  lastRequestTime = Date.now();

  const method = options?.method ?? "GET";
  apiLogger.debug({ method, url }, "Sending request to INS API");

  const startTime = performance.now();
  const response = await fetch(url, options);
  const duration = Math.round(performance.now() - startTime);

  apiLogger.debug(
    {
      method,
      url,
      status: response.status,
      statusText: response.statusText,
      duration: `${String(duration)}ms`,
    },
    "Received response from INS API"
  );

  return response;
}

/**
 * Fetch all top-level statistical domains (contexts)
 * @param lang - Language for context names ('ro' or 'en'), defaults to 'ro'
 */
export async function fetchContexts(
  lang: Locale = "ro"
): Promise<InsContext[]> {
  const url = `${BASE_URL}/context/?lang=${lang}`;
  apiLogger.info({ url, lang }, "Fetching all contexts");

  const response = await rateLimitedFetch(url);
  if (!response.ok) {
    apiLogger.error(
      { status: response.status, statusText: response.statusText, lang },
      "Failed to fetch contexts"
    );
    throw new Error(`Failed to fetch contexts: ${response.statusText}`);
  }

  const data = (await response.json()) as InsContext[];
  apiLogger.debug(
    { contextCount: data.length, lang },
    "Successfully fetched contexts"
  );

  // Log each context at trace level for detailed inspection
  for (const ctx of data) {
    apiLogger.trace({ context: ctx }, "Context data");
  }

  return data;
}

/**
 * Fetch contexts in both RO and EN languages in parallel
 * @returns Object with Romanian and English context lists
 */
export async function fetchContextsBilingual(): Promise<{
  ro: InsContext[];
  en: InsContext[];
}> {
  apiLogger.info("Fetching contexts in both RO and EN languages");

  const [ro, en] = await Promise.all([
    fetchContexts("ro"),
    fetchContexts("en"),
  ]);

  apiLogger.debug(
    { roCount: ro.length, enCount: en.length },
    "Successfully fetched bilingual contexts"
  );

  return { ro, en };
}

/**
 * Fetch a specific context and its children by code
 */
export async function fetchContext(code: string): Promise<InsContext[]> {
  const url = `${BASE_URL}/context/${code}`;
  apiLogger.info({ url, contextCode: code }, "Fetching context by code");

  const response = await rateLimitedFetch(url);
  if (!response.ok) {
    apiLogger.error(
      {
        contextCode: code,
        status: response.status,
        statusText: response.statusText,
      },
      "Failed to fetch context"
    );
    throw new Error(`Failed to fetch context ${code}: ${response.statusText}`);
  }

  const data = (await response.json()) as InsContext[];
  apiLogger.debug({ contextCode: code, data }, "Successfully fetched context");

  return data;
}

/**
 * Fetch all available matrices
 * @param lang - Language for matrix names ('ro' or 'en'), defaults to 'ro'
 */
export async function fetchMatricesList(
  lang: Locale = "ro"
): Promise<InsMatrixListItem[]> {
  const url = `${BASE_URL}/matrix/matrices?lang=${lang}`;
  apiLogger.info({ url, lang }, "Fetching all matrices list");

  const response = await rateLimitedFetch(url);
  if (!response.ok) {
    apiLogger.error(
      { status: response.status, statusText: response.statusText, lang },
      "Failed to fetch matrices list"
    );
    throw new Error(`Failed to fetch matrices: ${response.statusText}`);
  }

  const data = (await response.json()) as InsMatrixListItem[];
  apiLogger.debug(
    { matrixCount: data.length, lang },
    "Successfully fetched matrices list"
  );

  return data;
}

/**
 * Fetch matrices list in both RO and EN languages in parallel
 * @returns Object with Romanian and English matrix lists
 */
export async function fetchMatricesListBilingual(): Promise<{
  ro: InsMatrixListItem[];
  en: InsMatrixListItem[];
}> {
  apiLogger.info("Fetching matrices list in both RO and EN languages");

  const [ro, en] = await Promise.all([
    fetchMatricesList("ro"),
    fetchMatricesList("en"),
  ]);

  apiLogger.debug(
    { roCount: ro.length, enCount: en.length },
    "Successfully fetched bilingual matrices list"
  );

  return { ro, en };
}

/**
 * Fetch metadata for a specific matrix (dimensions, options, etc.)
 * @param lang - Language for matrix metadata ('ro' or 'en'), defaults to 'ro'
 */
export async function fetchMatrix(
  code: string,
  lang: Locale = "ro"
): Promise<InsMatrix> {
  const url = `${BASE_URL}/matrix/${code}?lang=${lang}`;
  apiLogger.info({ url, matrixCode: code, lang }, "Fetching matrix metadata");

  const response = await rateLimitedFetch(url);
  if (!response.ok) {
    apiLogger.error(
      {
        matrixCode: code,
        lang,
        status: response.status,
        statusText: response.statusText,
      },
      "Failed to fetch matrix"
    );
    throw new Error(`Failed to fetch matrix ${code}: ${response.statusText}`);
  }

  const data = (await response.json()) as InsMatrix;
  apiLogger.debug(
    { matrixCode: code, lang },
    "Successfully fetched matrix metadata"
  );

  // Log dimensions at debug level
  for (const dim of data.dimensionsMap) {
    apiLogger.debug(
      {
        matrixCode: code,
        dimCode: dim.dimCode,
        label: dim.label,
        optionCount: dim.options.length,
      },
      "Matrix dimension"
    );

    // Log dimension options at trace level
    for (const opt of dim.options) {
      apiLogger.trace(
        { matrixCode: code, dimCode: dim.dimCode, option: opt },
        "Dimension option"
      );
    }
  }

  return data;
}

/**
 * Fetch matrix metadata in both RO and EN languages in parallel
 * @param code - Matrix code
 * @returns Object with Romanian and English matrix metadata
 */
export async function fetchMatrixBilingual(code: string): Promise<{
  ro: InsMatrix;
  en: InsMatrix;
}> {
  apiLogger.info(
    { matrixCode: code },
    "Fetching matrix in both RO and EN languages"
  );

  const [ro, en] = await Promise.all([
    fetchMatrix(code, "ro"),
    fetchMatrix(code, "en"),
  ]);

  apiLogger.debug(
    { matrixCode: code },
    "Successfully fetched bilingual matrix metadata"
  );

  return { ro, en };
}

/**
 * Query data from a matrix using the /pivot endpoint
 * Returns CSV-formatted text data
 */
export async function queryMatrix(request: InsQueryRequest): Promise<string> {
  const url = `${BASE_URL}/pivot`;

  apiLogger.info(
    {
      url,
      matCode: request.matCode,
      language: request.language,
      matMaxDim: request.matMaxDim,
    },
    "Querying matrix data"
  );

  // Log request body at debug level
  apiLogger.debug({ requestBody: request }, "Matrix query request body");

  const response = await rateLimitedFetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(request),
  });

  if (!response.ok) {
    apiLogger.error(
      {
        matCode: request.matCode,
        status: response.status,
        statusText: response.statusText,
      },
      "Failed to query matrix"
    );
    throw new Error(
      `Failed to query matrix ${request.matCode}: ${response.statusText}`
    );
  }

  // The pivot endpoint returns CSV text, not JSON
  const data = await response.text();
  const rowCount = data.split("\n").filter((row) => row.trim() !== "").length;

  apiLogger.debug(
    {
      matCode: request.matCode,
      rowCount,
      sampleData: data.slice(0, 200),
    },
    "Successfully queried matrix data"
  );

  return data;
}

/**
 * Build an encQuery string from selected nomItemIds
 * @param selections Array of nomItemId arrays, one per dimension
 * @returns Colon-separated query string (e.g., "1,2:105:108:112:4494:9685")
 */
export function buildEncQuery(selections: number[][]): string {
  return selections.map((dimSelections) => dimSelections.join(",")).join(":");
}

/**
 * Estimate the number of cells a query will return
 */
export function estimateCellCount(selections: number[][]): number {
  return selections.reduce((acc, dim) => acc * dim.length, 1);
}

/**
 * Check if a query would exceed the 30,000 cell limit
 */
export function wouldExceedLimit(
  selections: number[][],
  limit = 30000
): boolean {
  const cellCount = estimateCellCount(selections);
  const exceeds = cellCount > limit;

  if (exceeds) {
    apiLogger.warn(
      { cellCount, limit },
      "Query would exceed INS API cell limit"
    );
  }

  return exceeds;
}

/**
 * Parse CSV response from pivot endpoint into rows
 */
export function parsePivotResponse(csvText: string): string[][] {
  return csvText
    .split("\n")
    .filter((row) => row.trim() !== "")
    .map((row) => row.split(", "));
}
