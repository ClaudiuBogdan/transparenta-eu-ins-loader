# INS Tempo API Specification

This document provides a formal specification for the INS (Institutul Național de Statistică) Tempo API, based on systematic exploration and validation of the live endpoints.

**Base URL:** `http://statistici.insse.ro:8077/tempo-ins`

**Last Validated:** 2025-12-27

---

## Table of Contents

1. [Overview](#1-overview)
2. [Endpoint Reference](#2-endpoint-reference)
3. [Data Model](#3-data-model)
4. [Dimension Types](#4-dimension-types)
5. [Error Handling](#5-error-handling)
6. [Rate Limiting](#6-rate-limiting)
7. [Usage Examples](#7-usage-examples)
8. [Appendices](#8-appendices)

---

## 1. Overview

### 1.1 Authentication

No authentication required. The API is publicly accessible.

### 1.2 Content Types

- **Request:** `application/json`
- **Response:**
  - Context/Matrix endpoints: `application/json;charset=UTF-8`
  - Pivot (query) endpoint: `text/plain` (CSV format)

### 1.3 API Constraints

| Constraint | Value | Notes |
|------------|-------|-------|
| Cell Limit | 30,000 | Maximum cells per query |
| Recommended Delay | 750ms | Client-side rate limiting (server does not enforce) |
| Total Matrices | ~1,898 | As of 2025-12-27 |
| Total Contexts | ~340 | Hierarchical categories |
| Top-Level Domains | 8 | A-H (Social, Economic, etc.) |

---

## 2. Endpoint Reference

### 2.1 Context Endpoints

#### GET `/context/`

Returns the complete hierarchy of statistical domains and categories.

**Response:** `InsContextItem[]`

```json
[
  {
    "parentCode": "0",
    "level": 0,
    "context": {
      "name": "A. STATISTICA SOCIALA",
      "code": "1",
      "childrenUrl": "context",
      "comment": null,
      "url": "context"
    }
  },
  {
    "parentCode": "1",
    "level": 1,
    "context": {
      "name": "A.1 POPULATIE SI STRUCTURA DEMOGRAFICA",
      "code": "10",
      "childrenUrl": "context",
      "comment": "",
      "url": "context"
    }
  }
]
```

**Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `parentCode` | string | Parent context code ("0" for root) |
| `level` | number | Hierarchy depth (0=domain, 1=subcategory, 2+=leaf) |
| `context.name` | string | Display name (may contain HTML links) |
| `context.code` | string | Unique identifier |
| `context.childrenUrl` | string | "context" or "matrix" (indicates child type) |
| `context.comment` | string\|null | Additional notes |
| `context.url` | string | Navigation hint |

**Top-Level Domains (level=0):**

| Code | Name |
|------|------|
| 1 | A. STATISTICA SOCIALA |
| 2 | B. STATISTICA ECONOMICA |
| 3 | C. FINANTE |
| 4 | D. JUSTITIE |
| 5 | E. MEDIU INCONJURATOR |
| 6 | F. UTILITATI PUBLICE SI ADMINISTRAREA TERITORIULUI |
| 7 | G. DEZVOLTARE DURABILA - Orizont 2020 |
| 8 | H. DEZVOLTARE DURABILA - Tinte 2030 |

---

#### GET `/context/{code}`

Returns children of a specific context.

**Parameters:**
- `code` (path, required): Context code (e.g., "1", "10", "1010")

**Response:** Same structure as `/context/` but filtered to children of the specified context.

**Error Response (Invalid Code):**
- HTTP 400 Bad Request
- Body: Empty or error message

---

### 2.2 Matrix Endpoints

#### GET `/matrix/matrices?lang=ro`

Returns the complete catalog of available datasets.

**Query Parameters:**
- `lang` (optional): "ro" (Romanian) or "en" (English). Default: "ro"

**Response:** `InsMatrixListItem[]`

```json
[
  {
    "name": "Populatia rezidenta la 1 ianuarie...",
    "code": "POP105A",
    "childrenUrl": "matrix",
    "comment": null,
    "url": "matrix"
  }
]
```

**Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Full dataset description |
| `code` | string | Unique matrix identifier (e.g., "POP105A") |
| `childrenUrl` | string | Always "matrix" |
| `comment` | string\|null | Additional notes |
| `url` | string | Always "matrix" |

---

#### GET `/matrix/{code}`

Returns complete metadata for a specific matrix including dimensions.

**Parameters:**
- `code` (path, required): Matrix code (e.g., "POP105A")

**Response:** `InsMatrixMetadata`

```json
{
  "ancestors": [
    {
      "name": "home",
      "code": "",
      "childrenUrl": "context",
      "comment": null,
      "url": "context"
    },
    {
      "name": "A. STATISTICA SOCIALA",
      "code": "1",
      "childrenUrl": "context",
      "comment": null,
      "url": "context"
    }
  ],
  "matrixName": "Populatia rezidenta la 1 ianuarie pe grupe de varsta...",
  "periodicitati": ["Anuala"],
  "surseDeDate": [
    {
      "nume": "Populatia rezidenta a Romaniei...",
      "tip": "Surse statistice (INS)",
      "linkNumber": 4057,
      "codTip": 1
    }
  ],
  "definitie": "Populatia rezidenta reprezinta...",
  "metodologie": "Sursa datelor o constituie...",
  "ultimaActualizare": "02-09-2025",
  "observatii": "Datele sunt disponibile incepand cu anul 2003...",
  "persoaneResponsabile": null,
  "dimensionsMap": [
    {
      "dimCode": 1,
      "label": "Varste si grupe de varsta",
      "options": [
        {
          "label": "Total",
          "nomItemId": 1,
          "offset": 1,
          "parentId": null
        },
        {
          "label": "   0- 4 ani",
          "nomItemId": 2,
          "offset": 2,
          "parentId": null
        }
      ]
    }
  ],
  "intrerupere": null,
  "continuareSerie": null,
  "details": {
    "nomJud": 0,
    "nomLoc": 0,
    "matMaxDim": 6,
    "matUMSpec": 0,
    "matSiruta": 0,
    "matCaen1": 0,
    "matCaen2": 0,
    "matRegJ": 4,
    "matCharge": 0,
    "matViews": 0,
    "matDownloads": 0,
    "matActive": 1,
    "matTime": 5
  }
}
```

**Key Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `ancestors` | array | Breadcrumb path from root to this matrix |
| `matrixName` | string | Full matrix description/name |
| `periodicitati` | string[] | Periodicity: "Anuala", "Trimestriala", "Lunara" |
| `surseDeDate` | array | Data sources |
| `definitie` | string | Definition/description |
| `metodologie` | string | Methodology notes |
| `ultimaActualizare` | string | Last update date (DD-MM-YYYY) |
| `observatii` | string | Observations/notes |
| `dimensionsMap` | array | Dimension definitions with options |
| `details` | object | Matrix capability flags |

**Details Flags:**

| Flag | Type | Description |
|------|------|-------------|
| `nomJud` | number | Has county dimension (0=no, >0=dimension index) |
| `nomLoc` | number | Has locality/UAT dimension (0=no, >0=dimension index) |
| `matMaxDim` | number | Number of dimensions |
| `matSiruta` | number | Uses SIRUTA codes (0/1) |
| `matCaen1` | number | Uses CAEN Rev.1 classification (0/1) |
| `matCaen2` | number | Uses CAEN Rev.2 classification (0/1) |
| `matRegJ` | number | Regional dimension index |
| `matTime` | number | Time dimension index |
| `matActive` | number | Dataset is active (1) or discontinued (0) |
| `matUMSpec` | number | Has special unit of measure |
| `matViews` | number | View counter |
| `matDownloads` | number | Download counter |
| `matCharge` | number | Complexity/cost indicator |

**Dimension Option Structure:**

| Field | Type | Description |
|-------|------|-------------|
| `label` | string | Display label (may include SIRUTA code for localities) |
| `nomItemId` | number | Unique option identifier (used in queries) |
| `offset` | number | Order index (1-based) |
| `parentId` | number\|null | Parent option ID for hierarchical dimensions |

---

### 2.3 Data Query Endpoint

#### POST `/pivot`

Query data from a matrix. Returns CSV-formatted text.

**Request Body:**

```json
{
  "encQuery": "1:105:108:112:4494:9685",
  "language": "ro",
  "matCode": "POP105A",
  "matMaxDim": 6,
  "matRegJ": 4,
  "matUMSpec": 0
}
```

**Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `encQuery` | string | Colon-separated `nomItemId` values (one per dimension) |
| `language` | string | "ro" or "en" |
| `matCode` | string | Matrix code |
| `matMaxDim` | number | From `details.matMaxDim` |
| `matRegJ` | number | From `details.matRegJ` |
| `matUMSpec` | number | From `details.matUMSpec` |

**encQuery Format:**

The `encQuery` is built by joining `nomItemId` values with colons, one group per dimension:
- Single selection: `"1:105:108:112:4494:9685"`
- Multiple selections per dimension: `"1,2,3:105:108:112:4494,4513:9685"`

**Response:** CSV-formatted text

```
Varste si grupe de varsta, Sexe, Medii de rezidenta, Macroregiuni  regiuni de dezvoltare si judete, Ani, UM: Numar persoane, Valoare
Total, Total, Total, TOTAL, Anul 2003, Numar persoane, 21627509
```

**Response Format:**
- First row: Column headers (dimension labels + "Valoare")
- Subsequent rows: Data values
- Separator: Comma with space (`, `)

**Special Values:**
- `:` - Data not available
- `-` - No data
- `*` - Confidential data
- `<0.5` - Value less than 0.5

---

## 3. Data Model

### 3.1 OLAP Cube Structure

The INS Tempo API uses a multi-dimensional OLAP cube model:

```
Context (Domain)
  └── Context (Category)
        └── Context (Subcategory)
              └── Matrix (Dataset)
                    └── Dimension 1
                    │     └── Option 1, Option 2, ...
                    └── Dimension 2
                    │     └── Option 1, Option 2, ...
                    └── Dimension N
                          └── Option 1, Option 2, ...
```

### 3.2 Matrix Identification

- **Code Pattern:** 3 letters + 3 digits + optional letter (e.g., "POP105A", "LOC103B")
- **Domain Prefixes:**
  - `POP` - Population
  - `LOC` - Housing/Localities
  - `AGR` - Agriculture
  - `SOM` - Unemployment
  - `TUR` - Tourism
  - `ACC` - Accidents
  - etc.

### 3.3 UAT-Level Data Identification

Matrices with locality-level data have:
- `details.nomLoc > 0`
- `details.matSiruta = 1`

Key UAT-level matrices:
- `POP107D` - Population by domicile
- `LOC103B` - Housing stock
- `TUR104E` / `TUR105H` - Tourism capacity
- `SOM101E` - Unemployment

---

## 4. Dimension Types

### 4.1 Temporal Dimensions

**Label patterns:**
- Annual: `"Anul 2023"`, `"Anul 2024"`
- Quarterly: `"Trimestrul I 2024"`, `"Trimestrul II 2024"`
- Monthly: `"Luna Ianuarie 2024"`, `"Luna Februarie 2024"`

**Dimension labels:** `"Ani"`, `"Perioade"`, `"Trimestre"`, `"Luni"`

### 4.2 Territorial Dimensions

**Hierarchy levels:**

| Level | Example | Notes |
|-------|---------|-------|
| National | TOTAL | Always available |
| Macroregion | MACROREGIUNEA UNU | NUTS I level |
| Development Region | Regiunea NORD-VEST | NUTS II level |
| County | Bihor, Cluj | NUTS III level (42 units) |
| Locality | 38731 Ripiceni | UAT level (3,222 units) |

**Dimension labels:**
- `"Macroregiuni, regiuni de dezvoltare si judete"` - Combined regional
- `"Judete"` - Counties only
- `"Localitati"` - Localities with SIRUTA codes

**SIRUTA Code Extraction:**

For locality-level data, SIRUTA codes are embedded in labels:
```
"38731 Ripiceni" → SIRUTA: 38731
"179195 Bucuresti" → SIRUTA: 179195
```

### 4.3 Classification Dimensions

| Label | Options Example |
|-------|-----------------|
| `"Sexe"` | Total, Masculin, Feminin |
| `"Medii de rezidenta"` | Total, Urban, Rural |
| `"Varste si grupe de varsta"` | Total, 0-4 ani, 5-9 ani, ... |
| `"CAEN Rev.2"` | Economic activity codes |
| `"Forme de proprietate"` | Ownership types |

### 4.4 Unit of Measure Dimensions

Always a single-option dimension indicating the measurement unit:
- `"UM: Numar persoane"` - Number of persons
- `"UM: Numar"` - Count
- `"UM: Ha"` - Hectares
- `"UM: M.p. arie desfasurata"` - Square meters

---

## 5. Error Handling

### 5.1 HTTP Status Codes

| Code | Meaning | Typical Cause |
|------|---------|---------------|
| 200 | Success | Request processed |
| 400 | Bad Request | Invalid context/matrix code |
| 500 | Server Error | Malformed query, cell limit exceeded |

### 5.2 Error Response Format

```json
{
  "timestamp": 1766839181796,
  "status": 500,
  "error": "Internal Server Error",
  "exception": "java.lang.ArrayIndexOutOfBoundsException",
  "message": "-1",
  "path": "/tempo-ins/matrix/dataSet/POP105A"
}
```

### 5.3 Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| ArrayIndexOutOfBoundsException | Empty or malformed query | Ensure all dimensions have at least one selection |
| Cell limit exceeded | Query returns >30,000 cells | Reduce selections or chunk by time/territory |
| Invalid matrix code | Non-existent code | Verify code exists in matrices list |

---

## 6. Rate Limiting

### 6.1 Server Behavior

**Finding:** The INS server does NOT enforce rate limiting at the server level.

Testing with 5 rapid sequential requests (no delay):
- All returned HTTP 200
- Average response time: ~71ms
- No 429 (Too Many Requests) responses observed

### 6.2 Recommended Client Behavior

Despite no server enforcement, implement client-side rate limiting:
- **Recommended delay:** 750ms between requests
- **Rationale:** Be a good API citizen, prevent potential IP blocking

### 6.3 Response Times (Observed)

| Endpoint | Typical Response Time |
|----------|----------------------|
| `/context/` | 70-100ms |
| `/matrix/matrices` | 700-900ms |
| `/matrix/{code}` | 100-200ms |
| `/pivot` (small query) | 200-400ms |
| `/pivot` (large query) | 5-20s |

---

## 7. Usage Examples

### 7.1 List All UAT-Level Matrices

```typescript
// 1. Fetch all matrices
const matrices = await fetch(`${BASE_URL}/matrix/matrices?lang=ro`).then(r => r.json());

// 2. For each matrix, fetch details and check for UAT data
const uatMatrices = [];
for (const m of matrices) {
  const details = await fetch(`${BASE_URL}/matrix/${m.code}`).then(r => r.json());
  if (details.details.nomLoc > 0 || details.details.matSiruta > 0) {
    uatMatrices.push({
      code: m.code,
      name: m.name,
      nomLoc: details.details.nomLoc,
      matSiruta: details.details.matSiruta
    });
  }
  await sleep(750); // Rate limiting
}
```

### 7.2 Query Population Data

```typescript
// 1. Fetch matrix metadata
const matrix = await fetch(`${BASE_URL}/matrix/POP105A`).then(r => r.json());

// 2. Build encQuery (select first option from each dimension)
const encQuery = matrix.dimensionsMap
  .map(dim => dim.options[0].nomItemId)
  .join(':');

// 3. Execute query
const response = await fetch(`${BASE_URL}/pivot`, {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    encQuery,
    language: 'ro',
    matCode: 'POP105A',
    matMaxDim: matrix.details.matMaxDim,
    matRegJ: matrix.details.matRegJ,
    matUMSpec: matrix.details.matUMSpec
  })
});

// 4. Parse CSV response
const csvText = await response.text();
const rows = csvText.split('\n').map(row => row.split(', '));
const headers = rows[0];
const data = rows.slice(1);
```

### 7.3 Handle Large Queries (Chunking)

```typescript
function estimateCellCount(selections: number[][]): number {
  return selections.reduce((acc, sel) => acc * sel.length, 1);
}

// If query would exceed limit, chunk by time dimension
const timeOptions = matrix.dimensionsMap
  .find(d => d.label.includes('Ani'))?.options ?? [];

for (const year of timeOptions) {
  const yearQuery = buildQueryWithSingleYear(year.nomItemId);
  const result = await executeQuery(yearQuery);
  await sleep(750);
}
```

---

## 8. Appendices

### 8.1 Complete Details Flags Reference

| Flag | Type | Description | Values |
|------|------|-------------|--------|
| `nomJud` | number | County dimension presence | 0=no, >0=dim index |
| `nomLoc` | number | Locality dimension presence | 0=no, >0=dim index |
| `matMaxDim` | number | Total dimension count | 3-6 typical |
| `matSiruta` | number | Uses SIRUTA codes | 0=no, 1=yes |
| `matCaen1` | number | CAEN Rev.1 classification | 0=no, 1=yes |
| `matCaen2` | number | CAEN Rev.2 classification | 0=no, 1=yes |
| `matRegJ` | number | Regional dimension index | 0-6 |
| `matTime` | number | Time dimension index | 0-6 |
| `matActive` | number | Dataset active status | 0=discontinued, 1=active |
| `matUMSpec` | number | Special unit of measure | 0=no, 1=yes |
| `matViews` | number | View counter | Integer |
| `matDownloads` | number | Download counter | Integer |
| `matCharge` | number | Query complexity cost | Integer |

### 8.2 Discovered Dimension Types

From exploration of sample matrices:

**Temporal:**
- Ani
- Perioade

**Territorial:**
- Macroregiuni, regiuni de dezvoltare si judete
- Judete
- Localitati

**Classification:**
- Sexe
- Medii de rezidenta
- Varste si grupe de varsta
- CAEN Rev.1 (activitati ale economiei nationale)
- CAEN Rev.2 (activitati ale economiei nationale)
- Forme de proprietate
- Categorii de accidente de munca
- Modul de folosinta a fondului funciar
- Tipuri de structuri de primire turistica

**Units:**
- UM: Numar persoane
- UM: Numar
- UM: Ha
- UM: M.p. arie desfasurata

### 8.3 TypeScript Type Definitions

See `src/types/index.ts` for complete type definitions. Key corrections from exploration:

```typescript
// Context structure (different from original types)
interface InsContextResponse {
  parentCode: string;
  level: number;
  context: {
    name: string;
    code: string;
    childrenUrl: "context" | "matrix";
    comment: string | null;
    url: string;
  };
}

// Matrix metadata (corrected field names)
interface InsMatrixMetadata {
  matrixName: string;
  ancestors?: InsContextItem[];
  periodicitati?: string[];
  surseDeDate?: InsDataSource[];
  definitie?: string;
  metodologie?: string;
  ultimaActualizare?: string;  // Not "lastUpdate"
  observatii?: string;
  persoaneResponsabile?: string | null;
  dimensionsMap: InsDimension[];  // Not "dimensions"
  intrerupere?: string | null;
  continuareSerie?: string | null;
  details: InsMatrixDetails;  // Not "matrixDetails"
}

// Dimension structure (corrected field names)
interface InsDimension {
  dimCode: number;  // Not "dimensionId"
  label: string;    // Not "dimensionName"
  options: InsDimensionOption[];
}

// Complete details object
interface InsMatrixDetails {
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
}

// Pivot query request
interface InsPivotRequest {
  encQuery: string;
  language: "ro" | "en";
  matCode: string;
  matMaxDim: number;
  matRegJ: number;
  matUMSpec: number;
}
```

---

## Changelog

- **2025-12-27:** Initial specification created from live API exploration
