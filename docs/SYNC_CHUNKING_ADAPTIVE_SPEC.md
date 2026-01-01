# Adaptive Sync Chunking Spec

Status: Draft
Purpose: Define an algorithm that estimates CSV size from dimension selections
and auto-splits sync work to maximize chunk size without exceeding limits. The
algorithm must be deterministic, resumable, and idempotent.

## Context

Current flow:

- Build selections per dimension (nomItemId lists).
- Build encQuery and call /pivot (CSV).
- Parse CSV and upsert statistics.
- Track checkpoints per chunk for resume.

Problem:

- /pivot has a hard 30,000 cell limit. We must enforce the limit before querying.
- Chunking has been mostly static (year + county), and ignores CSV size or other
  dimensions that can create oversized requests.

References:

- src/services/sync/chunking.ts
- src/services/sync/data.ts
- src/scraper/client.ts

## Constraints

Hard constraints:

- INS /pivot cell limit: 30,000 cells per request.
- Chunking must be deterministic to support checkpoints/resume.

Soft constraints:

- Avoid too many small chunks (API overhead).
- Avoid duplicate rows across chunks; duplicates are tolerated due to upserts,
  but should be minimized.

## Definitions

Cell count:

- Estimated cells = product of selection counts for all dimensions.

CSV size estimate:

- Estimated bytes = (estimated rows * avgRowBytes) + headerBytes.
- Estimated rows ~= cell count (one row per combination).

Chunk:

- A unit of sync work with a deterministic selection signature.
- Stored in checkpoints for resume.

## CSV Size Estimation

We use a conservative estimate to ensure chunks fit within limits.

Row bytes estimate:

1. For each dimension, compute avg label length (bytes):
   - Prefer labelRo length.
   - Use sample if dimension is large (e.g., first N items).
2. Add a fixed value length estimate (e.g., 12 bytes for numeric + status).
3. Add separators:
   - ", " between columns (2 bytes) for each dimension + value.
4. Add newline (1 byte).

Formula:
avgRowBytes =
  sum(avgLabelBytes[d]) +
  (dimensionCount /*columns without value */) * 2 /* ", " */ +
  valueBytes +
  1 /* newline*/

Estimated CSV bytes:
estimatedBytes = cellCount * avgRowBytes + headerBytes

Budget:

- cellBudget = 30,000
- If we add a CSV byte budget, compute:
  byteBudgetCells = floor((csvByteLimit - headerBytes) / avgRowBytes)
  effectiveBudget = min(cellBudget, byteBudgetCells)

## Adaptive Chunking Algorithm

Goal: create chunks whose estimated cells <= budget, while keeping chunk sizes
as large as possible.

### 1) Build Axis Model

Represent selections as axes that can be split.

Axis:

- axisId: string
- dimIndices: number[] (one or more dimensions)
- options: OptionGroup[]
- count: number (options.length)
- splitPriority: number (lower = earlier split)

OptionGroup:

- selectionByDimIndex: Map<dimIndex, nomItemId[]>
- label: string (for logging/checkpoints)

Notes:

- Some axes group multiple dimensions (e.g., county + locality).
- This prevents invalid cross-products and keeps cell estimates realistic.

### 2) Detect Paired Dimensions

If a matrix has county + locality dimensions:

- Build a paired axis where each option is one county + its localities.
- This keeps selection counts from exploding and reflects how /pivot enforces
  the 30k cell limit (cartesian product of selections).

### 3) Compute Base Selections

Apply user filters and modes:

- Temporal: filter by year range.
- Classification: totals-only vs all.
- Territory: countyCode, expandTerritorial, etc.

The base selections define the initial options for each axis.

### 4) Split Until Under Budget

Pseudo-code:

function buildChunks(axes, budgetCells):
  chunks = cartesianProduct(axes) // each axis has one group initially
  while exists chunk with cells > budgetCells:
    chunk = pickLargestChunk(chunks)
    axis = pickSplitAxis(chunk.axes)
    splitAxisGroups = splitAxis(axis, budgetCells, chunk)
    replace chunk with new chunks using splitAxisGroups
  return chunks

Split axis choice:

- Prefer axes in this order (example):
  1) Temporal
  2) Paired territorial (county+locality)
  3) Territorial
  4) Classification
  5) Unit / other
- Within the same priority, pick the axis with largest count.

Split size calculation:

- fixedProduct = product(count of other axes in the chunk)
- maxOptionsPerChunk = floor(budgetCells / fixedProduct)
- If maxOptionsPerChunk <= 0:
  - Split another axis first (repeat until feasible).
- Partition options into contiguous groups of size maxOptionsPerChunk.
  - Use a stable order to make the output deterministic.
  - Use balanced grouping to minimize the final chunk count.

Balanced grouping:

- chunkCount = ceil(totalOptions / maxOptionsPerChunk)
- groupSize = ceil(totalOptions / chunkCount)
- Create groups of size groupSize in original order.

### 5) Deterministic Ordering

To ensure resumability:

- Sort axes by splitPriority and axisId.
- Sort options by offset_order or nomItemId.
- Always split contiguously in that order.

### 6) Chunk Signature and Hash

Each chunk gets a deterministic signature:

- matrixCode
- chunkAlgorithmVersion
- axis selections (dimIndex + option IDs or ranges)
- filters (yearFrom/yearTo, classificationMode, countyCode)

chunkHash = md5(signature).substring(0, 16)

Include chunkAlgorithmVersion to avoid reusing old checkpoints after changes.

## Resume and Idempotency

Resume:

- Use chunkHash to check checkpoints.
- Skip chunks that have a successful checkpoint.

Idempotency:

- Natural key hash already prevents duplicate rows.
- For statistic_classifications, use unique constraints or upserts to avoid
  duplicate associations.

Failure handling:

- Failed chunk checkpoint stores error and retry count.
- Retrying uses the same chunk signature, so it replays only that chunk.

## Integration Points

Suggested changes (high level):

- src/services/sync/chunking.ts
  - Add Axis model and adaptive chunk generator.
  - Add csv size estimator.
  - Add chunk signature builder (versioned).
- src/services/sync/data.ts
  - Use adaptive chunks and enforce budget before /pivot.
  - Store chunk signature in checkpoints for visibility.
- src/scraper/client.ts
  - Keep cell limit constant; optional CSV byte limit in config.

## Example (UAT Matrix)

Inputs:

- County dim: 42 counties
- Locality dim: ~3,200 localities
- Time: 10 years
- Classification: 3 values

Paired axis:

- 42 options, each option selects:
  - county dim: [countyId]
  - locality dim: [localities in that county]

Other axes:

- Time: 10 options
- Classification: 3 options

Cell estimate per county per year:
1 (county) *~76 (localities)* 1 (year) * 3 (classifications) = ~228

Budget = 30,000:

- Can bundle multiple years into one chunk per county until near budget.
- Example: 1 county *5 years* 3 classifications => ~1,140 cells.
- If we want larger chunks, group multiple counties only if the paired axis is
  removed (not allowed). So county-based chunking remains mandatory here.

## Open Questions

- Do we want a CSV byte limit in addition to the 30k cell limit?
- Should totals (e.g., "Total") be included in every chunk or once in a
  dedicated chunk?
- For quarterly/monthly data, is year-based chunking still desirable?
