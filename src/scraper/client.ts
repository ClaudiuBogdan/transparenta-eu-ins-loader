import { apiLogger } from "../logger.js";

import type { InsContext, InsMatrix, InsQueryRequest } from "../types/index.js";

const BASE_URL = "http://statistici.insse.ro:8077/tempo-ins";
const RATE_LIMIT_MS = 750; // 0.75 seconds between requests

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
 */
export async function fetchContexts(): Promise<InsContext[]> {
  const url = `${BASE_URL}/context/`;
  apiLogger.info({ url }, "Fetching all contexts");

  const response = await rateLimitedFetch(url);
  if (!response.ok) {
    apiLogger.error(
      { status: response.status, statusText: response.statusText },
      "Failed to fetch contexts"
    );
    throw new Error(`Failed to fetch contexts: ${response.statusText}`);
  }

  const data = (await response.json()) as InsContext[];
  apiLogger.debug(
    { contextCount: data.length, data },
    "Successfully fetched contexts"
  );

  // Log each context at trace level for detailed inspection
  for (const ctx of data) {
    apiLogger.trace({ context: ctx }, "Context data");
  }

  return data;
}

/**
 * Fetch a specific context and its children
 */
export async function fetchContext(id: number): Promise<InsContext> {
  const url = `${BASE_URL}/context/${String(id)}`;
  apiLogger.info({ url, contextId: id }, "Fetching context by ID");

  const response = await rateLimitedFetch(url);
  if (!response.ok) {
    apiLogger.error(
      {
        contextId: id,
        status: response.status,
        statusText: response.statusText,
      },
      "Failed to fetch context"
    );
    throw new Error(
      `Failed to fetch context ${String(id)}: ${response.statusText}`
    );
  }

  const data = (await response.json()) as InsContext;
  apiLogger.debug({ contextId: id, data }, "Successfully fetched context");

  return data;
}

/**
 * Fetch all available matrices
 */
export async function fetchMatricesList(): Promise<
  { code: string; name: string }[]
> {
  const url = `${BASE_URL}/matrix/matrices?lang=ro`;
  apiLogger.info({ url }, "Fetching all matrices list");

  const response = await rateLimitedFetch(url);
  if (!response.ok) {
    apiLogger.error(
      { status: response.status, statusText: response.statusText },
      "Failed to fetch matrices list"
    );
    throw new Error(`Failed to fetch matrices: ${response.statusText}`);
  }

  const data = (await response.json()) as { code: string; name: string }[];
  apiLogger.debug(
    { matrixCount: data.length, data },
    "Successfully fetched matrices list"
  );

  return data;
}

/**
 * Fetch metadata for a specific matrix (dimensions, options, etc.)
 */
export async function fetchMatrix(code: string): Promise<InsMatrix> {
  const url = `${BASE_URL}/matrix/${code}`;
  apiLogger.info({ url, matrixCode: code }, "Fetching matrix metadata");

  const response = await rateLimitedFetch(url);
  if (!response.ok) {
    apiLogger.error(
      {
        matrixCode: code,
        status: response.status,
        statusText: response.statusText,
      },
      "Failed to fetch matrix"
    );
    throw new Error(`Failed to fetch matrix ${code}: ${response.statusText}`);
  }

  const data = (await response.json()) as InsMatrix;
  apiLogger.debug(
    { matrixCode: code, data },
    "Successfully fetched matrix metadata"
  );

  // Log dimensions at debug level
  for (const dim of data.dimensions) {
    apiLogger.debug(
      {
        matrixCode: code,
        dimensionId: dim.dimensionId,
        dimensionName: dim.dimensionName,
        optionCount: dim.options.length,
      },
      "Matrix dimension"
    );

    // Log dimension options at trace level
    for (const opt of dim.options) {
      apiLogger.trace(
        { matrixCode: code, dimensionId: dim.dimensionId, option: opt },
        "Dimension option"
      );
    }
  }

  return data;
}

/**
 * Query data from a matrix
 */
export async function queryMatrix(
  request: InsQueryRequest
): Promise<unknown[][]> {
  const url = `${BASE_URL}/matrix/dataSet/${request.matrixName}`;
  const estimatedCells = estimateCellCount(request.arr);

  apiLogger.info(
    {
      url,
      matrixName: request.matrixName,
      language: request.language,
      dimensionCount: request.arr.length,
      estimatedCells,
      nomJud: request.matrixDetails.nomJud,
      nomLoc: request.matrixDetails.nomLoc,
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
        matrixName: request.matrixName,
        status: response.status,
        statusText: response.statusText,
      },
      "Failed to query matrix"
    );
    throw new Error(
      `Failed to query matrix ${request.matrixName}: ${response.statusText}`
    );
  }

  const data = (await response.json()) as unknown[][];
  apiLogger.debug(
    {
      matrixName: request.matrixName,
      rowCount: data.length,
      sampleRow: data[0],
    },
    "Successfully queried matrix data"
  );

  return data;
}

/**
 * Estimate the number of cells a query will return
 */
export function estimateCellCount(dimensionSelections: unknown[][]): number {
  return dimensionSelections.reduce((acc, dim) => acc * dim.length, 1);
}

/**
 * Check if a query would exceed the 30,000 cell limit
 */
export function wouldExceedLimit(
  dimensionSelections: unknown[][],
  limit = 30000
): boolean {
  const cellCount = estimateCellCount(dimensionSelections);
  const exceeds = cellCount > limit;

  if (exceeds) {
    apiLogger.warn(
      { cellCount, limit },
      "Query would exceed INS API cell limit"
    );
  }

  return exceeds;
}
