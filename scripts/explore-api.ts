/**
 * INS Tempo API Exploration Script
 *
 * This script systematically explores the INS Tempo API to document:
 * - All endpoint request/response schemas
 * - Error conditions and edge cases
 * - Rate limiting behavior
 * - Dimension types and hierarchies
 *
 * Output: JSON files in scripts/responses/ and console report
 */

import { writeFileSync, mkdirSync, existsSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const currentDir = dirname(fileURLToPath(import.meta.url));
const RESPONSES_DIR = join(currentDir, "responses");

// Ensure responses directory exists
if (!existsSync(RESPONSES_DIR)) {
  mkdirSync(RESPONSES_DIR, { recursive: true });
}

const BASE_URL = "http://statistici.insse.ro:8077/tempo-ins";
const RATE_LIMIT_MS = 750;

let lastRequestTime = 0;
let requestCount = 0;

interface ApiResponse<T = unknown> {
  endpoint: string;
  method: string;
  status: number;
  statusText: string;
  duration: number;
  data?: T;
  error?: string;
  headers?: Record<string, string>;
}

interface ExplorationReport {
  timestamp: string;
  totalRequests: number;
  totalDuration: number;
  endpoints: {
    contexts: ApiResponse[];
    matrices: ApiResponse[];
    queries: ApiResponse[];
    errors: ApiResponse[];
  };
  findings: {
    contextFields: string[];
    matrixFields: string[];
    dimensionTypes: string[];
    matrixDetailsFlags: string[];
    errorFormats: string[];
  };
}

async function rateLimitedFetch(
  url: string,
  options?: RequestInit
): Promise<Response> {
  const now = Date.now();
  const elapsed = now - lastRequestTime;

  if (elapsed < RATE_LIMIT_MS) {
    const waitTime = RATE_LIMIT_MS - elapsed;
    console.log(`  [Rate limit] Waiting ${String(waitTime)}ms...`);
    await new Promise((resolve) => setTimeout(resolve, waitTime));
  }

  lastRequestTime = Date.now();
  requestCount++;

  return fetch(url, options);
}

async function exploreEndpoint<T>(
  endpoint: string,
  method: "GET" | "POST" = "GET",
  body?: unknown
): Promise<ApiResponse<T>> {
  const url = `${BASE_URL}${endpoint}`;
  console.log(`[${String(requestCount + 1)}] ${method} ${endpoint}`);

  const startTime = performance.now();

  try {
    const options: RequestInit = { method };
    if (body !== undefined && body !== null) {
      options.headers = { "Content-Type": "application/json" };
      options.body = JSON.stringify(body);
    }

    const response = await rateLimitedFetch(url, options);
    const duration = Math.round(performance.now() - startTime);

    // Extract relevant headers
    const headers: Record<string, string> = {};
    response.headers.forEach((value, key) => {
      if (
        ["content-type", "content-length", "date", "server"].includes(
          key.toLowerCase()
        )
      ) {
        headers[key] = value;
      }
    });

    let data: T | undefined;
    let error: string | undefined;

    if (response.ok) {
      try {
        data = (await response.json()) as T;
      } catch {
        error = "Failed to parse JSON response";
      }
    } else {
      try {
        const errorBody = await response.text();
        error = errorBody !== "" ? errorBody : response.statusText;
      } catch {
        error = response.statusText;
      }
    }

    console.log(
      `  Status: ${String(response.status)} | Duration: ${String(duration)}ms`
    );

    return {
      endpoint,
      method,
      status: response.status,
      statusText: response.statusText,
      duration,
      data,
      error,
      headers,
    };
  } catch (err) {
    const duration = Math.round(performance.now() - startTime);
    const error = err instanceof Error ? err.message : String(err);
    console.log(`  ERROR: ${error}`);

    return {
      endpoint,
      method,
      status: 0,
      statusText: "Network Error",
      duration,
      error,
    };
  }
}

function saveResponse(filename: string, data: unknown): void {
  const filepath = join(RESPONSES_DIR, filename);
  writeFileSync(filepath, JSON.stringify(data, null, 2));
  console.log(`  Saved: ${filename}`);
}

function extractKeys(obj: unknown, prefix = ""): string[] {
  if (obj === null || obj === undefined || typeof obj !== "object") return [];

  const keys: string[] = [];
  for (const [key, value] of Object.entries(obj)) {
    const fullKey = prefix !== "" ? `${prefix}.${key}` : key;
    keys.push(fullKey);

    if (
      value !== null &&
      value !== undefined &&
      typeof value === "object" &&
      !Array.isArray(value)
    ) {
      keys.push(...extractKeys(value, fullKey));
    }
  }
  return keys;
}

// =====================
// EXPLORATION FUNCTIONS
// =====================

interface InsContextItem {
  name: string;
  code: string;
  childrenUrl: string;
  comment: string | null;
  url: string;
}

interface InsContextRaw {
  parentCode: string;
  level: number;
  context: InsContextItem;
}

interface InsMatrixListItem {
  code: string;
  name: string;
}

interface InsMatrixRaw {
  matrixName: string;
  ancestors?: InsContextItem[];
  periodicitati?: string[];
  surseDeDate?: unknown[];
  definitie?: string;
  metodologie?: string;
  ultimaActualizare?: string;
  observatii?: string;
  persoaneResponsabile?: string | null;
  dimensionsMap: InsMatrixDimensionRaw[];
  intrerupere?: string | null;
  continuareSerie?: string | null;
  details: {
    nomJud: number;
    nomLoc: number;
    matMaxDim: number;
    matUMSpec: number;
    matSiruta: number;
    matCaen1: number;
    matCaen2: number;
    matRegJ: number;
    matCharge: number;
    matViews: number;
    matDownloads: number;
    matActive: number;
    matTime: number;
  };
  [key: string]: unknown;
}

interface InsMatrixDimensionRaw {
  dimCode: number;
  label: string;
  options: InsMatrixDimensionOptionRaw[];
}

interface InsMatrixDimensionOptionRaw {
  label: string;
  nomItemId: number;
  offset: number;
  parentId?: number | null;
}

async function exploreContexts(): Promise<ApiResponse[]> {
  console.log("\n=== EXPLORING CONTEXT ENDPOINTS ===\n");
  const results: ApiResponse[] = [];

  // 1. Get root contexts
  const rootContexts = await exploreEndpoint<InsContextRaw[]>("/context/");
  results.push(rootContexts);
  saveResponse("01-root-contexts.json", rootContexts);

  if (rootContexts.data !== undefined && Array.isArray(rootContexts.data)) {
    // Group by level 0 (top-level domains)
    const topLevel = rootContexts.data.filter((c) => c.level === 0);
    console.log(
      `\nFound ${String(rootContexts.data.length)} total contexts (${String(topLevel.length)} top-level domains):`
    );
    for (const ctx of topLevel) {
      console.log(`  - [${ctx.context.code}] ${ctx.context.name}`);
    }

    // 2. Explore a specific context to understand structure
    // Use code "1" for first domain
    const contextDetail = await exploreEndpoint<InsContextRaw[]>(`/context/1`);
    results.push(contextDetail);
    saveResponse(`02-context-1-social.json`, contextDetail);

    // Explore code "10" for population subcategory
    const subContextDetail =
      await exploreEndpoint<InsContextRaw[]>(`/context/10`);
    results.push(subContextDetail);
    saveResponse(`02-context-10-population.json`, subContextDetail);

    // Explore code "1010" for resident population
    const leafContextDetail =
      await exploreEndpoint<InsContextRaw[]>(`/context/1010`);
    results.push(leafContextDetail);
    saveResponse(`02-context-1010-resident-pop.json`, leafContextDetail);
  }

  return results;
}

async function exploreMatrices(): Promise<ApiResponse[]> {
  console.log("\n=== EXPLORING MATRIX ENDPOINTS ===\n");
  const results: ApiResponse[] = [];

  // 1. Get matrices list
  const matricesList = await exploreEndpoint<InsMatrixListItem[]>(
    "/matrix/matrices?lang=ro"
  );
  results.push(matricesList);
  saveResponse("03-matrices-list.json", matricesList);

  if (matricesList.data !== undefined && Array.isArray(matricesList.data)) {
    console.log(`\nFound ${String(matricesList.data.length)} matrices total`);

    // Sample matrices to explore (different types)
    const samplesToExplore = [
      "POP105A", // Population by county (known county-level)
      "POP107D", // Population by domicile (known UAT-level)
      "SOM101E", // Unemployment (likely quarterly)
      "TUR104E", // Tourism (likely UAT-level)
      "AGR101A", // Agriculture (likely annual)
      "LOC103B", // Housing (UAT-level)
    ];

    // Also add first 3 matrices from the list to see variety
    const firstThree = matricesList.data.slice(0, 3).map((m) => m.code);
    const allSamples = [...new Set([...samplesToExplore, ...firstThree])];

    console.log(
      `\nExploring ${String(allSamples.length)} sample matrices...\n`
    );

    const matrixDetailsFlags = new Set<string>();
    const dimensionTypes = new Set<string>();

    for (const code of allSamples) {
      const matrix = await exploreEndpoint<InsMatrixRaw>(`/matrix/${code}`);
      results.push(matrix);
      saveResponse(`04-matrix-${code}.json`, matrix);

      if (matrix.data !== undefined) {
        // Collect details flags
        const details = matrix.data.details;
        for (const key of Object.keys(details)) {
          matrixDetailsFlags.add(key);
        }

        // Collect dimension labels
        const dimensionsMap = matrix.data.dimensionsMap;
        for (const dim of dimensionsMap) {
          dimensionTypes.add(dim.label);
        }

        console.log(
          `  Dimensions: ${String(dimensionsMap.length)}, Updated: ${matrix.data.ultimaActualizare ?? "N/A"}`
        );
        console.log(
          `    nomJud: ${String(details.nomJud)}, nomLoc: ${String(details.nomLoc)}, matSiruta: ${String(details.matSiruta)}`
        );
      }
    }

    console.log("\n--- Discovered matrixDetails flags ---");
    console.log([...matrixDetailsFlags].sort().join(", "));

    console.log("\n--- Discovered dimension types ---");
    console.log([...dimensionTypes].sort().join("\n"));

    saveResponse("05-discovered-matrixDetails-flags.json", [
      ...matrixDetailsFlags,
    ]);
    saveResponse("05-discovered-dimension-types.json", [...dimensionTypes]);
  }

  return results;
}

async function exploreQueries(): Promise<ApiResponse[]> {
  console.log("\n=== EXPLORING QUERY ENDPOINT ===\n");
  const results: ApiResponse[] = [];

  // First get a matrix to query
  const matrix = await exploreEndpoint<InsMatrixRaw>("/matrix/POP105A");

  if (matrix.data?.dimensionsMap === undefined) {
    console.log("Failed to get matrix for query exploration");
    return results;
  }

  console.log(
    `Matrix has ${String(matrix.data.dimensionsMap.length)} dimensions:`
  );
  for (const dim of matrix.data.dimensionsMap) {
    console.log(
      `  - [${String(dim.dimCode)}] ${dim.label}: ${String(dim.options.length)} options`
    );
  }

  // Build a small query (select first option from each dimension)
  const queryArr = matrix.data.dimensionsMap.map((dim) => [dim.options[0]]);

  const queryRequest = {
    language: "ro",
    arr: queryArr,
    matrixName: "POP105A",
    matrixDetails: {
      nomJud: matrix.data.details.nomJud,
      nomLoc: matrix.data.details.nomLoc,
    },
  };

  console.log("\nSending query request...");
  saveResponse("06-query-request-sample.json", queryRequest);

  const queryResult = await exploreEndpoint<unknown[][]>(
    "/matrix/dataSet/POP105A",
    "POST",
    queryRequest
  );
  results.push(queryResult);
  saveResponse("06-query-response-sample.json", queryResult);

  if (queryResult.data !== undefined && Array.isArray(queryResult.data)) {
    console.log(`\nQuery returned ${String(queryResult.data.length)} rows`);
    console.log("First row (headers):", JSON.stringify(queryResult.data[0]));
    if (queryResult.data.length > 1) {
      console.log("Second row (data):", JSON.stringify(queryResult.data[1]));
    }
  }

  // Try a larger query to see response format better
  console.log("\n--- Testing larger query ---");

  // Select first 2 options from each dimension
  const largerQueryArr = matrix.data.dimensionsMap.map((dim) =>
    dim.options.slice(0, Math.min(2, dim.options.length))
  );

  const largerQueryRequest = {
    language: "ro",
    arr: largerQueryArr,
    matrixName: "POP105A",
    matrixDetails: {
      nomJud: matrix.data.details.nomJud,
      nomLoc: matrix.data.details.nomLoc,
    },
  };

  const largerQueryResult = await exploreEndpoint<unknown[][]>(
    "/matrix/dataSet/POP105A",
    "POST",
    largerQueryRequest
  );
  results.push(largerQueryResult);
  saveResponse("06-query-response-larger.json", largerQueryResult);

  if (
    largerQueryResult.data !== undefined &&
    Array.isArray(largerQueryResult.data)
  ) {
    console.log(
      `Larger query returned ${String(largerQueryResult.data.length)} rows`
    );
    console.log("Sample row:", JSON.stringify(largerQueryResult.data[1]));
  }

  return results;
}

async function exploreErrors(): Promise<ApiResponse[]> {
  console.log("\n=== EXPLORING ERROR CONDITIONS ===\n");
  const results: ApiResponse[] = [];

  // 1. Invalid context ID
  console.log("Testing invalid context ID...");
  const invalidContext = await exploreEndpoint("/context/999999");
  results.push(invalidContext);
  saveResponse("07-error-invalid-context.json", invalidContext);

  // 2. Invalid matrix code
  console.log("Testing invalid matrix code...");
  const invalidMatrix = await exploreEndpoint("/matrix/INVALID123");
  results.push(invalidMatrix);
  saveResponse("07-error-invalid-matrix.json", invalidMatrix);

  // 3. Malformed query (empty arr)
  console.log("Testing malformed query (empty arr)...");
  const malformedQuery = await exploreEndpoint(
    "/matrix/dataSet/POP105A",
    "POST",
    {
      language: "ro",
      arr: [],
      matrixName: "POP105A",
      matrixDetails: { nomJud: 0, nomLoc: 0 },
    }
  );
  results.push(malformedQuery);
  saveResponse("07-error-malformed-query.json", malformedQuery);

  // 4. Missing required fields
  console.log("Testing missing required fields...");
  const missingFields = await exploreEndpoint(
    "/matrix/dataSet/POP105A",
    "POST",
    {
      language: "ro",
    }
  );
  results.push(missingFields);
  saveResponse("07-error-missing-fields.json", missingFields);

  return results;
}

async function testRateLimiting(): Promise<{
  serverEnforced: boolean;
  responseTimes: number[];
}> {
  console.log("\n=== TESTING RATE LIMITING ===\n");

  // Temporarily disable our rate limiting
  const originalRateLimit = lastRequestTime;
  lastRequestTime = 0;

  const responseTimes: number[] = [];
  const testCount = 5;

  console.log(`Sending ${String(testCount)} rapid requests (no delay)...`);

  for (let i = 0; i < testCount; i++) {
    const start = performance.now();
    try {
      const response = await fetch(`${BASE_URL}/context/`);
      const duration = Math.round(performance.now() - start);
      responseTimes.push(duration);
      console.log(
        `  Request ${String(i + 1)}: ${String(response.status)} in ${String(duration)}ms`
      );

      if (response.status === 429) {
        console.log("  Server enforced rate limit detected!");
        lastRequestTime = originalRateLimit;
        return { serverEnforced: true, responseTimes };
      }
    } catch (err) {
      const duration = Math.round(performance.now() - start);
      console.log(
        `  Request ${String(i + 1)}: ERROR in ${String(duration)}ms - ${err instanceof Error ? err.message : String(err)}`
      );
      responseTimes.push(duration);
    }
  }

  // Restore rate limiting
  lastRequestTime = Date.now();

  const avgTime =
    responseTimes.reduce((a, b) => a + b, 0) / responseTimes.length;
  console.log(`\nAverage response time: ${String(Math.round(avgTime))}ms`);

  saveResponse("08-rate-limiting-test.json", {
    testCount,
    responseTimes,
    averageTime: avgTime,
    serverEnforced: false,
  });

  return { serverEnforced: false, responseTimes };
}

// =====================
// MAIN EXPLORATION
// =====================

async function main() {
  console.log("========================================");
  console.log("   INS TEMPO API EXPLORATION");
  console.log("========================================");
  console.log(`Base URL: ${BASE_URL}`);
  console.log(`Started: ${new Date().toISOString()}`);
  console.log(`Responses saved to: ${RESPONSES_DIR}`);

  const startTime = performance.now();

  const report: ExplorationReport = {
    timestamp: new Date().toISOString(),
    totalRequests: 0,
    totalDuration: 0,
    endpoints: {
      contexts: [],
      matrices: [],
      queries: [],
      errors: [],
    },
    findings: {
      contextFields: [],
      matrixFields: [],
      dimensionTypes: [],
      matrixDetailsFlags: [],
      errorFormats: [],
    },
  };

  try {
    // 1. Explore contexts
    report.endpoints.contexts = await exploreContexts();

    // 2. Explore matrices
    report.endpoints.matrices = await exploreMatrices();

    // 3. Explore queries
    report.endpoints.queries = await exploreQueries();

    // 4. Explore errors
    report.endpoints.errors = await exploreErrors();

    // 5. Test rate limiting
    await testRateLimiting();

    // Extract findings
    const firstContextResponse = report.endpoints.contexts[0];
    if (firstContextResponse?.data !== undefined) {
      const firstContext: unknown = Array.isArray(firstContextResponse.data)
        ? firstContextResponse.data[0]
        : firstContextResponse.data;
      report.findings.contextFields = extractKeys(firstContext);
    }

    // Find a successful matrix response
    const successfulMatrix = report.endpoints.matrices.find(
      (m) =>
        m.data !== undefined &&
        m.data !== null &&
        typeof m.data === "object" &&
        "matrixName" in m.data
    );
    if (successfulMatrix?.data !== undefined) {
      report.findings.matrixFields = extractKeys(successfulMatrix.data);
      const matrixData = successfulMatrix.data as InsMatrixRaw;
      report.findings.matrixDetailsFlags = Object.keys(matrixData.details);
      report.findings.dimensionTypes = matrixData.dimensionsMap.map(
        (d) => d.label
      );
    }
  } catch (err) {
    console.error("\nExploration failed:", err);
  }

  report.totalRequests = requestCount;
  report.totalDuration = Math.round(performance.now() - startTime);

  // Save final report
  saveResponse("00-exploration-report.json", report);

  console.log("\n========================================");
  console.log("   EXPLORATION COMPLETE");
  console.log("========================================");
  console.log(`Total requests: ${String(report.totalRequests)}`);
  console.log(
    `Total duration: ${String(Math.round(report.totalDuration / 1000))}s`
  );
  console.log(`Responses saved to: ${RESPONSES_DIR}`);
}

main().catch(console.error);
