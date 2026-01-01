# INS Tempo CLI Usage Guide

This guide explains how to extract Romanian statistical data from the INS (Institutul Național de Statistică) Tempo API using the CLI or direct API calls.

## Quick Reference

```bash
# Run CLI commands
pnpm cli contexts              # List statistical domains
pnpm cli matrices --search pop # Search matrices by name
pnpm cli matrix POP107D        # View matrix details
pnpm cli dimensions POP107D 2  # View dimension options
pnpm cli query POP107D         # Interactive data query
pnpm cli explore               # Interactive browser
```

## Core Concepts

### 1. Matrices (Datasets)

Matrices are statistical datasets identified by codes like `POP107D`, `SCL103B`. Each matrix contains multi-dimensional data (e.g., population by county, year, gender).

### 2. Dimensions

Each matrix has multiple dimensions. Every dimension has options with `nomItemId` values used for querying.

Example dimensions for `POP107D` (Population by localities):

- Dimension 0: Sexes (Total, Male, Female)
- Dimension 1: Age groups (Total, 0-4, 5-9, ...)
- Dimension 2: Counties (Sibiu, Cluj, ...)
- Dimension 3: Localities (with SIRUTA codes)
- Dimension 4: Years (1992-2025)
- Dimension 5: Unit of measure

### 3. The encQuery Format

Queries use colon-separated `nomItemId` values, one group per dimension:

```
encQuery: "1:105:3095:2444:4247,4266,4285:9685"
         [sex]:[age]:[county]:[locality]:[years]:[unit]
```

- Single value: `1`
- Multiple values: `4247,4266,4285`
- Values are `nomItemId` from dimension options

---

## Step-by-Step: How to Extract Data

### Step 1: Find the Right Matrix

Search for matrices by topic:

```bash
pnpm cli matrices --search "populatie"   # Population data
pnpm cli matrices --search "elevi"       # Student data
pnpm cli matrices --search "somaj"       # Unemployment
pnpm cli matrices --uat                  # Only UAT-level data
```

Or use the API directly:

```javascript
const response = await fetch('http://statistici.insse.ro:8077/tempo-ins/matrix/matrices?lang=ro');
const matrices = await response.json();
// Filter: matrices.filter(m => m.name.toLowerCase().includes('populatie'))
```

**Common matrices:**

| Code | Description |
|------|-------------|
| `POP107D` | Population by localities (with SIRUTA) |
| `POP105A` | Population by counties |
| `SCL103B` | Pre-university students by county |
| `SOM101B` | Unemployment by county |
| `FOR101B` | Labor force by county |

### Step 2: Examine Matrix Structure

Get matrix metadata to understand its dimensions:

```bash
pnpm cli matrix POP107D
pnpm cli matrix POP107D --json  # Full JSON output
```

Or via API:

```javascript
const response = await fetch('http://statistici.insse.ro:8077/tempo-ins/matrix/POP107D');
const matrix = await response.json();

// Key fields:
// matrix.dimensionsMap - array of dimensions
// matrix.details.nomJud - has county data (>0 = yes)
// matrix.details.nomLoc - has UAT/locality data (>0 = yes)
```

### Step 3: Get Dimension Options

List options for each dimension to find `nomItemId` values:

```bash
pnpm cli dimensions POP107D      # List all dimensions
pnpm cli dimensions POP107D 2    # Options for dimension 2 (counties)
pnpm cli dimensions POP107D 3    # Options for dimension 3 (localities)
```

Or via API:

```javascript
// Dimensions are in matrix.dimensionsMap
const matrix = await fetch('http://statistici.insse.ro:8077/tempo-ins/matrix/POP107D').then(r => r.json());

for (const dim of matrix.dimensionsMap) {
  console.log(`Dimension ${dim.dimCode}: ${dim.label}`);
  for (const opt of dim.options) {
    console.log(`  ${opt.nomItemId}: ${opt.label}`);
  }
}
```

**Important:** Localities include SIRUTA codes in their labels:

```
"143450 MUNICIPIUL SIBIU" -> nomItemId: 2444
"125150 MUNICIPIUL CLUJ-NAPOCA" -> nomItemId: 1920
```

### Step 4: Build and Execute Query

#### Using CLI (Interactive)

```bash
pnpm cli query POP107D
# Follow prompts to select options for each dimension
```

#### Using API (Direct)

```javascript
const request = {
  encQuery: '1:105:3095:2444:4247,4266,4285,4304,4323:9685',
  // Format: dim0:dim1:dim2:dim3:dim4:dim5
  // 1 = Total (sex)
  // 105 = Total (age)
  // 3095 = Sibiu county
  // 2444 = Municipiul Sibiu locality
  // 4247,4266,... = Years (1992, 1993, ...)
  // 9685 = Number of persons (unit)

  language: 'ro',
  matCode: 'POP107D',
  matMaxDim: 6,      // From matrix.details.matMaxDim
  matRegJ: 0,        // From matrix.details.matRegJ
  matUMSpec: 0       // From matrix.details.matUMSpec
};

const response = await fetch('http://statistici.insse.ro:8077/tempo-ins/pivot', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify(request)
});

const csvData = await response.text();
console.log(csvData);
```

**Response format (CSV):**

```
Sexe, Varste, Judete, Localitati, Ani, UM, Valoare
Total, Total, Sibiu, 143450 MUNICIPIUL SIBIU, Anul 1992, Numar persoane, 169610
Total, Total, Sibiu, 143450 MUNICIPIUL SIBIU, Anul 1993, Numar persoane, 170273
...
```

---

## Complete Examples

### Example 1: Historical Population of Municipiul Sibiu

```javascript
// Step 1: Matrix POP107D has locality-level population data
// Step 2: Find nomItemIds from dimensions:
//   - Sex Total: 1
//   - Age Total: 105
//   - Sibiu County: 3095
//   - Municipiul Sibiu: 2444 (label: "143450 MUNICIPIUL SIBIU")
//   - Years: 4247 (1992) through 4893 (2025)
//   - Unit: 9685

const allYears = '4247,4266,4285,4304,4323,4342,4361,4380,4399,4418,4437,4456,4475,4494,4513,4532,4551,4570,4589,4608,4627,4646,4665,4684,4703,4722,4741,4760,4779,4798,4817,4836,4855,4874,4893';

const response = await fetch('http://statistici.insse.ro:8077/tempo-ins/pivot', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    encQuery: `1:105:3095:2444:${allYears}:9685`,
    language: 'ro',
    matCode: 'POP107D',
    matMaxDim: 6,
    matRegJ: 0,
    matUMSpec: 0
  })
});

const csv = await response.text();
// Parse CSV to extract year and population values
```

### Example 2: Historical Students in Sibiu County

```javascript
// Matrix SCL103B: Pre-university education by counties
// Dimensions:
//   - Education level Total: 8480
//   - Teaching language Total: 8468
//   - Sibiu County: 3095
//   - Years: 4247-4893
//   - Unit: 9685

const years = '4247,4266,4285,4304,4323,4342,4361,4380,4399,4418,4437,4456,4475,4494,4513,4532,4551,4570,4589,4608,4627,4646,4665,4684,4703,4722,4741,4760,4779,4798,4817,4836,4855,4874,4893';

const response = await fetch('http://statistici.insse.ro:8077/tempo-ins/pivot', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    encQuery: `8480:8468:3095:${years}:9685`,
    language: 'ro',
    matCode: 'SCL103B',
    matMaxDim: 5,
    matRegJ: 0,
    matUMSpec: 0
  })
});

const csv = await response.text();
```

---

## API Reference

### Base URL

```
http://statistici.insse.ro:8077/tempo-ins
```

### Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/context/` | GET | List root domains |
| `/context/{code}` | GET | Get context children |
| `/matrix/matrices?lang=ro` | GET | List all matrices |
| `/matrix/{code}` | GET | Get matrix metadata |
| `/pivot` | POST | Query data (returns CSV) |

### Rate Limiting

The API has no official rate limit, but we recommend **750ms between requests** to avoid being blocked.

### Cell Limit

Queries are limited to **30,000 cells**. Calculate: `product of selected option counts per dimension`.

Example: 2 sexes × 20 ages × 42 counties × 30 years = 50,400 cells (exceeds limit)

Solution: Split into smaller queries (e.g., query 10 years at a time).

---

## For AI Agents: Query Building Algorithm

```
1. IDENTIFY the data needed (population, students, unemployment, etc.)

2. SEARCH for relevant matrix:
   GET /matrix/matrices?lang=ro
   Filter by name containing keywords

3. GET matrix structure:
   GET /matrix/{matrixCode}
   Extract: dimensionsMap, details.matMaxDim, details.matRegJ, details.matUMSpec

4. FOR EACH dimension, find nomItemId values:
   - "Total" options usually have low nomItemId (1, 105, 108, etc.)
   - Geographic dimensions contain location names
   - Year dimensions have labels like "Anul 2024"
   - Look for SIRUTA codes in locality labels (6-digit numbers)

5. BUILD encQuery:
   - One segment per dimension, separated by colons
   - Multiple values within a dimension separated by commas
   - Order must match dimensionsMap order

6. EXECUTE query:
   POST /pivot
   Body: { encQuery, language: "ro", matCode, matMaxDim, matRegJ, matUMSpec }

7. PARSE CSV response:
   - First row is header
   - Last column is "Valoare" (the numeric value)
   - Values may be integers or "::" for missing data
```

### Finding Geographic IDs

**Counties (Judete):** Search dimension options for county names

```
"Sibiu" -> nomItemId: 3095
"Cluj" -> nomItemId: 1909
"Bucuresti" -> nomItemId: 370
```

**Localities:** Look for SIRUTA code prefix in label

```
"143450 MUNICIPIUL SIBIU" -> nomItemId: 2444, SIRUTA: 143450
"125150 MUNICIPIUL CLUJ-NAPOCA" -> nomItemId: 1920, SIRUTA: 125150
```

### Common nomItemId Patterns

| Concept | Typical nomItemId | Notes |
|---------|-------------------|-------|
| Total (aggregate) | 1, 105, 108, 112 | First option in dimension |
| Years (1992-2025) | 4247-4893 | Increment by 19 per year |
| Unit: persons | 9685 | "Numar persoane" |
| Unit: thousands | 9686 | "Mii persoane" |

---

## Troubleshooting

### Empty or NaN values

- Check column indices when parsing CSV (value is typically last column)
- Some cells return `::` for missing data

### 400 Bad Request

- Verify encQuery has correct number of segments (one per dimension)
- Check that nomItemId values exist in dimension options

### Exceeds cell limit

- Reduce selections (fewer years, fewer locations)
- Query in batches and combine results

### Rate limited / Connection refused

- Add 750ms+ delay between requests
- Retry with exponential backoff

---

## Data Sync Commands

The CLI includes commands for syncing statistical data from INS to the local PostgreSQL database.

### Sync Workflow

```bash
# 1. Database setup
pnpm db:migrate             # Creates schema + partitions

# 2. Sync metadata
pnpm cli sync all           # Full sync: contexts, matrices, metadata
# OR step by step:
pnpm cli sync contexts      # Domain hierarchy
pnpm cli sync matrices      # Matrix catalog
pnpm cli sync matrices --full --skip-existing  # Detailed metadata

# 3. Sync statistical data
pnpm cli sync data          # Sync all matrices
pnpm cli sync data --matrix POP105A  # Sync specific matrix
```

### Sync Commands Reference

| Command | Description |
|---------|-------------|
| `sync contexts` | Sync context hierarchy (domains) |
| `sync territories` | Bootstrap territory hierarchy |
| `sync matrices` | Sync matrix catalog |
| `sync matrices --full` | Sync full matrix metadata (slow) |
| `sync data` | Sync statistical data |
| `sync data --matrix <code>` | Sync specific matrix |
| `sync partitions` | Verify/create statistics partitions |
| `sync status` | Show sync status for all matrices |
| `sync tasks` | List queued sync tasks |
| `sync worker` | Process queued sync tasks |
| `sync retry --all` | Retry all failed tasks |
| `sync retry --task <id>` | Retry specific failed task |
| `sync history` | Show sync task history |
| `sync plan --matrix <code>` | Preview sync execution plan |
| `sync guide` | Show detailed sync workflow help |

### Sync Data Options

```bash
pnpm cli sync data --matrix POP105A \
  --years 2020-2024 \
  --classifications totals \
  --county AB \
  --continue-on-error
```

| Option | Description |
|--------|-------------|
| `--matrix <code>` | Matrix code(s) to sync (repeatable) |
| `--years <range>` | Year range, e.g., 2020-2024 |
| `--classifications <mode>` | `totals` or `all` (default: totals) |
| `--county <code>` | County code for county-specific sync |
| `--limit <n>` | Limit number of matrices |
| `--refresh` | Only sync matrices with existing data |
| `--stale-only` | Only refresh matrices marked STALE |
| `--continue-on-error` | Continue on individual matrix failures |
| `--force` | Force re-sync ignoring checkpoints |
| `--verbose` | Enable detailed logging |

### Task Queue System

Sync operations use a task queue to prevent duplicate requests and manage rate limiting:

```bash
# View pending tasks
pnpm cli sync tasks

# View task history
pnpm cli sync history
pnpm cli sync history POP105A  # History for specific matrix

# Process tasks (run as background worker)
pnpm cli sync worker
pnpm cli sync worker --once  # Process one task and exit

# Retry failed tasks
pnpm cli sync retry --all
pnpm cli sync retry --task 123
```

### Monitoring Sync Progress

```bash
# Check matrix sync status
pnpm cli sync status
pnpm cli sync status --failed  # Show only failed

# Preview what would be synced
pnpm cli sync plan --matrix POP105A --years 2020-2024
```
