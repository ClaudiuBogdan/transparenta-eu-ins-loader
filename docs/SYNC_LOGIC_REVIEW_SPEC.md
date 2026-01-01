# Sync Logic Review Spec

Status: Draft
Purpose: Capture sync logic issues from the initial review so we can add TODOs
and track fixes in a structured way.

## Scope

- Orchestration: src/services/sync/orchestrator.ts
- Chunking: src/services/sync/chunking.ts
- Data sync: src/services/sync/data.ts
- Scraper helpers: src/scraper/client.ts
- Entry points (context only): src/cli/commands/sync.ts, src/server/routes/sync.ts

## Context

The sync pipeline pulls INS Tempo metadata and data, builds selections, queries
the /pivot endpoint (CSV), resolves labels to canonical IDs, and upserts into
statistics with checkpoints to resume.

## Issues

### Issue 1: CSV parsing is not robust (naive comma split and numeric parsing)

Severity: High
Context:

- /pivot returns CSV text that can contain quoted commas and locale-specific
  number formatting.
- Current parser splits on ", " and trims, which breaks when labels contain
  commas or values use comma decimals. This can shift columns and corrupt data.
Evidence:
- src/services/sync/data.ts:1279
- src/scraper/client.ts:411
- src/services/sync/data.ts:1315
TODOs:
- [ ] TODO: Replace the split-based parser with a CSV parser that handles quotes.
- [ ] TODO: Normalize numeric parsing (dot vs comma decimals) before parseFloat.
- [ ] TODO: Add unit tests for quoted labels and comma decimals.

### Issue 2: Nom item resolution uses substring matching

Severity: High
Context:

- Matching uses labelRo equality or substring checks. This can map to the wrong
  option when labels overlap (e.g., "Total" vs "Total urban").
- Incorrect mappings lead to wrong territory/time/classification IDs, and can
  collapse multiple rows into one natural key.
Evidence:
- src/services/sync/data.ts:1334
TODOs:
- [ ] TODO: Switch to exact match using nom_item_id mapping from the CSV header
  when available, or build a normalized label map with disambiguation rules.
- [ ] TODO: Add validation metrics for unmatched or ambiguous labels.

### Issue 3: Cell limit is not enforced during chunked sync

Severity: High
Context:

- Chunk generation estimates cells but does not enforce the 30,000 cell limit.
- Chunks that exceed the limit will still be sent to /pivot and likely fail.
Evidence:
- src/services/sync/data.ts:370
- src/services/sync/chunking.ts:553
- src/services/sync/data.ts:559 (single-call only warns and continues)
TODOs:
- [ ] TODO: Enforce the cell limit before calling /pivot.
- [ ] TODO: Add sub-chunking when a chunk exceeds the limit (split by
  classification or time).
- [ ] TODO: Track the cell count per checkpoint for observability.

### Issue 4: expandTerritorial can create invalid county-locality cross products

Severity: High
Context:

- Some matrices have both county and locality dimensions that require matching
  pairs. Expanding all territorial items can generate invalid combinations and
  oversized queries.
Evidence:
- src/services/sync/data.ts:1246
TODOs:
- [ ] TODO: Guard expandTerritorial for matrices with county+locality dims.
- [ ] TODO: Replace expandTerritorial with county-driven chunking in those
  cases, or block with a clear error.

### Issue 5: countyCode filters are ignored in some paths

Severity: Medium
Context:

- For county-only matrices, chunk generation ignores countyCode and always
  queries all counties.
- In no-chunking mode, full-matrix selections ignore countyCode entirely while
  the checkpoint key still includes it.
Evidence:
- src/services/sync/chunking.ts:212
- src/services/sync/data.ts:731
TODOs:
- [ ] TODO: Honor countyCode for county-only matrices by selecting one county.
- [ ] TODO: Apply county filters in no-chunking mode or reject the option.

### Issue 6: Context parent assignment depends on processing order

Severity: Medium
Context:

- Parent IDs are resolved from a map that is populated as contexts are inserted.
- If a child appears before its parent in the API response, parent_id is set to
  null and not corrected later.
Evidence:
- src/services/sync/orchestrator.ts:148
TODOs:
- [ ] TODO: Sort contexts by level before insert, or add a second pass to
  backfill parent_id after all contexts exist.

### Issue 7: Territorial selection fallback and regex match are fragile

Severity: Medium
Context:

- If a county or locality lookup fails, the code falls back to the first item
  or all items, which can inflate query size or return wrong data.
- The locality lookup uses a regex on territories.path without escaping, which
  can mis-match if the path contains regex characters.
Evidence:
- src/services/sync/chunking.ts:382
- src/services/sync/chunking.ts:544
TODOs:
- [ ] TODO: Replace fallback-to-first with explicit error or safe fallback.
- [ ] TODO: Escape regex input or use a safe prefix match.

### Issue 8: Per-row upsert and classification inserts are N+1 heavy

Severity: Medium
Context:

- Each row does a lookup, update/insert, and optional classification inserts.
- Large syncs will be slow and increase DB load, risking timeouts.
Evidence:
- src/services/sync/data.ts:1381
TODOs:
- [ ] TODO: Batch upserts using insert on conflict or staging tables.
- [ ] TODO: Batch classification inserts per chunk.

## Open Questions

- Should "totals-only" mean only the explicit "Total" item or any top-level
  item (parent_nom_item_id = null) for hierarchical classifications?
- Is the /pivot CSV guaranteed to use dot decimals and no quoted commas?
- For monthly/quarterly data, do we want per-year chunking or full period
  chunking with year range filters?

## References

- src/services/sync/orchestrator.ts
- src/services/sync/chunking.ts
- src/services/sync/data.ts
- src/scraper/client.ts
