# INS Data Sync Issues Specification

**Document Version:** 1.0
**Date:** 2024-12-30
**Status:** Resolved

---

## Executive Summary

During the full matrix metadata sync of 1,898 INS matrices, **92 matrices (4.8%) failed** to sync. Root cause analysis revealed two distinct failure types and multiple data quality issues. All issues have been resolved, but this document serves as a comprehensive reference for future maintenance.

---

## Table of Contents

1. [Sync Failure Overview](#1-sync-failure-overview)
2. [Failure Type A: VARCHAR Truncation](#2-failure-type-a-varchar-truncation)
3. [Failure Type B: Duplicate Key Constraint](#3-failure-type-b-duplicate-key-constraint)
4. [Data Quality Issue: Unmatched Territories](#4-data-quality-issue-unmatched-territories)
5. [Data Quality Issue: Unmatched Time Periods](#5-data-quality-issue-unmatched-time-periods)
6. [Data Quality Issue: Duplicate Classification Types](#6-data-quality-issue-duplicate-classification-types)
7. [Resolution Summary](#7-resolution-summary)
8. [Appendix: Affected Matrices](#appendix-affected-matrices)

---

## 1. Sync Failure Overview

### Metrics

| Metric | Value |
|--------|-------|
| Total matrices | 1,898 |
| Successful syncs | 1,806 |
| Failed syncs | 92 |
| Failure rate | 4.8% |

### Failure Distribution

| Failure Type | Count | Percentage |
|--------------|-------|------------|
| VARCHAR truncation | ~30 | 33% |
| Duplicate key constraint | ~62 | 67% |

### Command to Reproduce

```bash
# Run full matrix metadata sync
LOG_LEVEL=info pnpm cli sync matrices --full 2>&1 | tee logs/sync.log

# Count failures
grep -c "failed" logs/sync.log
```

---

## 2. Failure Type A: VARCHAR Truncation

### Description

Several database columns were defined as `VARCHAR(200)` or `VARCHAR(500)`, but INS classification values and labels can exceed these limits. When a value exceeded the limit, PostgreSQL threw a truncation error and the entire matrix sync failed.

### Error Message

```
error: value too long for type character varying(200)
```

### Root Cause

The INS API returns classification values with very long names, particularly for:
- CAEN economic activity descriptions
- Complex age group ranges
- Detailed territorial descriptions
- Multi-part classification hierarchies

### Evidence

```sql
-- Query showing truncated values at exactly 200 characters
SELECT
    classification_type_id,
    COUNT(*) as count_at_limit
FROM classification_values
WHERE LENGTH(name) = 200
GROUP BY classification_type_id
ORDER BY count_at_limit DESC;

-- Results showed multiple types with values truncated at exactly 200 chars
```

### Affected Columns

| Table | Column | Original Type | Issue |
|-------|--------|---------------|-------|
| `classification_types` | `name` | VARCHAR(200) | Type names truncated |
| `classification_values` | `name` | VARCHAR(200) | Value names truncated |
| `classification_values` | `name_normalized` | VARCHAR(200) | Normalized names truncated |
| `classification_values` | `path` | VARCHAR(500) | Deep hierarchies truncated |
| `territories` | `name` | VARCHAR(200) | Long territory names |
| `territories` | `name_normalized` | VARCHAR(200) | Normalized names truncated |
| `territories` | `path` | VARCHAR(500) | Deep paths truncated |
| `matrix_dimensions` | `label` | VARCHAR(200) | Dimension labels truncated |
| `matrix_dimensions` | `label_en` | VARCHAR(200) | English labels truncated |

### Affected Matrices (30+)

```
CON103J, CON104D, CON104E, CON104F, CON104G, CON104H, CON104I,
CON104J, CON104K, CON104L, CON104M, CON104N, CON104O, CON104P,
CON104Q, CON104R, CON104S, CON104T, CON104U, CON104V, CON104W,
CON106H, CON106I, CON106J, CON106K, CON106L, CON106M, CON106N,
ECC108A, ECC109A, EXP101F, EXP102F, FIN101A, FIN103B, INT102D,
INT105E, PMI114C, PMI116A, PSC102B, TIC100A, TIC200A, TQZ1553,
TRN130B, TRN137C
```

### Resolution

**Fix Applied:** Changed all affected columns from VARCHAR to TEXT.

```sql
-- Migration script
ALTER TABLE classification_types ALTER COLUMN name TYPE TEXT;
ALTER TABLE classification_values ALTER COLUMN name TYPE TEXT;
ALTER TABLE classification_values ALTER COLUMN name_normalized TYPE TEXT;
ALTER TABLE classification_values ALTER COLUMN path TYPE TEXT;
ALTER TABLE territories ALTER COLUMN name TYPE TEXT;
ALTER TABLE territories ALTER COLUMN name_normalized TYPE TEXT;
ALTER TABLE territories ALTER COLUMN path TYPE TEXT;
ALTER TABLE matrix_dimensions ALTER COLUMN label TYPE TEXT;
ALTER TABLE matrix_dimensions ALTER COLUMN label_en TYPE TEXT;
```

**Files Modified:**
- Database via SQL migration
- `src/db/postgres-schema.sql` - Updated column definitions

---

## 3. Failure Type B: Duplicate Key Constraint

### Description

When re-syncing matrices, the `findOrCreateValue` method in `classifications.ts` would attempt to INSERT a classification value with a code that already existed, causing a duplicate key constraint violation.

### Error Message

```
error: duplicate key value violates unique constraint "uq_classification_values"
Detail: Key (classification_type_id, code)=(123, SOME_CODE) already exists.
```

### Root Cause

The bug was in the collision resolution logic. The flow was:

```
1. Generate code from label
2. Check if code exists with same name_normalized → return existing ID ✓
3. Check if code exists with DIFFERENT name_normalized → collision!
4. Call resolveCodeCollision() to find a unique code
5. BUG: resolveCodeCollision() finds an entry with matching name_normalized
        but returns its CODE instead of its ID
6. Back in findOrCreateValue, attempt INSERT with that code → DUPLICATE KEY
```

### Detailed Analysis

**Initial Sync (Day 1):**
```
Label: "Foarte lung text care depaseste 200 caractere si este trunchiat..."
Stored name_normalized: "FOARTE LUNG TEXT CARE DEPASESTE 200 CARACTERE SI EST" (truncated at 200)
Code: "FOARTE_LUNG_TEXT"
```

**Re-sync (Day 2, after data refresh):**
```
Label: "Foarte lung text care depaseste 200 caractere si este trunchiat la limita"
New name_normalized: "FOARTE LUNG TEXT CARE DEPASESTE 200 CARACTERE SI ESTE TRUNCHIAT LA LIMITA" (full, >200 chars)
Generated code: "FOARTE_LUNG_TEXT" (same as before)
```

**What happened:**
1. Code "FOARTE_LUNG_TEXT" exists
2. Compare name_normalized: "...EST" ≠ "...LIMITA" → collision detected!
3. `resolveCodeCollision()` searches for entries with matching full name_normalized
4. Finds none (because DB has truncated version)
5. Generates new code "FOARTE_LUNG_TEXT_2"
6. BUT the original entry's truncated name SHOULD have matched!

**The fundamental issue:** The VARCHAR(200) truncation caused `name_normalized` comparisons to fail, triggering false collision detection.

### Buggy Code (Before)

```typescript
// src/services/sync/classifications.ts - Line 191-256

async findOrCreateValue(
  typeId: number,
  label: string,
  parentId: number | null,
  sortOrder: number
): Promise<number> {
  const code = this.generateValueCode(label);
  const normalized = this.normalize(label);

  // Check if code exists
  const existing = await this.db
    .selectFrom("classification_values")
    .select(["id", "code", "name_normalized"])
    .where("classification_type_id", "=", typeId)
    .where("code", "=", code)
    .executeTakeFirst();

  if (existing) {
    if (existing.name_normalized === normalized) {
      return existing.id;  // ✓ Same entity
    }
    // Different name with same code → collision
    // BUG: resolveCodeCollision might return a code that already exists!
    const newCode = await this.resolveCodeCollision(typeId, code, normalized);
    // Then we INSERT with newCode... which might already exist!
  }

  // INSERT - could fail with duplicate key
  const result = await this.db
    .insertInto("classification_values")
    .values({...})
    .returning("id")
    .executeTakeFirst();

  return result!.id;
}
```

### Affected Matrices (60+)

```
CON110A, CON110C, CON111D, CON111E, CON111H, FOM103A, FOM103B,
FOM103C, FOM104A, FOM105B, FOM105D, FOM106A, FOM107A, FOM111A,
FOM112A, FOM112B, FOM117A, FOM117B, FOM118A, FOM118B, FOM118D,
FOM118E, FOM119A, FOM119B, IND104N, IND104O, IND104P, INT101A,
INT101G, INT102C, INT102D, INT104C, INT104D, INT105B, INT105C,
INT105D, INT105E, INT109A, INT109B, INV101C, INV102A, INV102B,
LMV101A, LMV102A, PNS101A, POP206E, POP206G, POP206K, POP207B,
POP207D, PPI1036, PPI1037, TNA1211, TNR1232
```

### Resolution

**Fix Applied:** Rewrote `findOrCreateValue` with a safer strategy:

```typescript
// src/services/sync/classifications.ts - Fixed version

async findOrCreateValue(
  typeId: number,
  label: string,
  parentId: number | null,
  sortOrder: number
): Promise<number> {
  const code = this.generateValueCode(label);
  const normalized = this.normalize(label);

  // Strategy 1: Look up by normalized name FIRST (most reliable)
  const existingByName = await this.db
    .selectFrom("classification_values")
    .select("id")
    .where("classification_type_id", "=", typeId)
    .where("name_normalized", "=", normalized)
    .executeTakeFirst();

  if (existingByName) {
    return existingByName.id;  // Found by name - definitely the same entity
  }

  // Strategy 2: Check for code collision
  const existingByCode = await this.db
    .selectFrom("classification_values")
    .select(["id", "name_normalized"])
    .where("classification_type_id", "=", typeId)
    .where("code", "=", code)
    .executeTakeFirst();

  let finalCode = code;
  if (existingByCode) {
    // Code exists with different name - generate unique code with suffix
    finalCode = await this.generateUniqueCode(typeId, code);
    logger.warn(
      { typeId, originalCode: code, newCode: finalCode, normalized },
      "Classification code collision - using suffix"
    );
  }

  // Strategy 3: INSERT with ON CONFLICT as safety net
  const result = await this.db
    .insertInto("classification_values")
    .values({
      classification_type_id: typeId,
      code: finalCode,
      name: label.trim(),
      name_normalized: normalized,
      parent_id: parentId,
      level: parentId ? 1 : 0,
      sort_order: sortOrder,
    })
    .onConflict((oc) =>
      oc.columns(["classification_type_id", "code"]).doUpdateSet({
        name: label.trim(),
        name_normalized: normalized,
      })
    )
    .returning("id")
    .executeTakeFirst();

  return result!.id;
}
```

**Key Improvements:**
1. Look up by `name_normalized` first (catches truncated matches)
2. Generate unique code with suffix for actual collisions
3. Use `ON CONFLICT DO UPDATE` as a safety net

**Files Modified:**
- `src/services/sync/classifications.ts`

---

## 4. Data Quality Issue: Unmatched Territories

### Description

After successful sync, 251 territorial options in `matrix_dimension_options` had `territory_id = NULL`, meaning the label could not be matched to a canonical territory.

### Query to Identify

```sql
SELECT
    mdo.label,
    COUNT(*) as occurrence_count
FROM matrix_dimension_options mdo
JOIN matrix_dimensions md ON mdo.matrix_dimension_id = md.id
WHERE md.dimension_type = 'TERRITORIAL'
  AND mdo.territory_id IS NULL
GROUP BY mdo.label
ORDER BY occurrence_count DESC
LIMIT 20;
```

### Unmatched Labels

| Label | Occurrences | Issue |
|-------|-------------|-------|
| `Mun. Bucuresti -incl. SAI` | 137 | Bucharest variant with SAI suffix |
| `Extra-regiuni` | 9 | Not a valid territory (EU concept) |
| `Nivel National` | 8 | National level aggregate |
| `Regiunea Sud -Vest - Oltenia` | 6 | Extra hyphen before "Vest" |
| `Dolj, Mehedinti, Olt` | 4 | Multi-county aggregate |
| `Bihor, Satu Mare` | 4 | Multi-county aggregate |
| `Total` | 3 | Aggregate, not territory |

### Root Cause

The territory matching patterns in `territories.ts` did not handle these edge cases:

1. **Bucharest variants:** "Mun. Bucuresti -incl. SAI" (includes Agricultural Sector)
2. **Extra-regiuni:** EU statistical concept, not a Romanian territory
3. **National level:** "Nivel National" and "TOTAL" represent Romania-wide aggregates
4. **Multi-county aggregates:** Labels like "Dolj, Mehedinti, Olt" represent combined county data
5. **Hyphen variations:** "Sud -Vest" vs "Sud-Vest" (inconsistent spacing)

### Resolution

**Fix Applied:** Added special case handling in `territories.ts`:

```typescript
// src/services/sync/territories.ts

async findOrCreateFromLabel(label: string): Promise<number | null> {
  const trimmed = label.trim();
  const normalizedLower = trimmed.toLowerCase();

  // "Extra-regiuni" - not a valid territory, return null
  if (normalizedLower === "extra-regiuni") {
    return null;
  }

  // Multi-county aggregates (e.g., "Dolj, Mehedinti, Olt") - return null
  if (trimmed.includes(",") && /[A-Z][a-z]+,\s*[A-Z][a-z]+/.test(trimmed)) {
    return null;
  }

  // "Mun. Bucuresti -incl. SAI" and similar Bucharest patterns
  if (
    normalizedLower.includes("bucuresti") &&
    (normalizedLower.includes("sai") || normalizedLower.includes("incl"))
  ) {
    const bucharest = await this.db
      .selectFrom("territories")
      .select("id")
      .where("code", "=", "B")
      .executeTakeFirst();
    return bucharest?.id ?? null;
  }

  // "Nivel National" or "TOTAL" - national level
  if (/^TOTAL$/i.test(trimmed) || normalizedLower === "nivel national") {
    const national = await this.db
      .selectFrom("territories")
      .select("id")
      .where("level", "=", "NATIONAL")
      .executeTakeFirst();
    return national?.id ?? null;
  }

  // Fix hyphen variations in region names
  // "Regiunea Sud -Vest - Oltenia" → "Regiunea Sud-Vest Oltenia"
  const normalizedForRegion = trimmed
    .replace(/\s+-\s*/g, "-")  // " - " or " -" → "-"
    .replace(/-\s+/g, "-");    // "- " → "-"

  // ... continue with normal matching
}
```

**Files Modified:**
- `src/services/sync/territories.ts`

---

## 5. Data Quality Issue: Unmatched Time Periods

### Description

27 time period options in `matrix_dimension_options` had `time_period_id = NULL`, meaning the label could not be parsed into a valid time period.

### Query to Identify

```sql
SELECT
    mdo.label,
    COUNT(*) as occurrence_count
FROM matrix_dimension_options mdo
JOIN matrix_dimensions md ON mdo.matrix_dimension_id = md.id
WHERE md.dimension_type = 'TEMPORAL'
  AND mdo.time_period_id IS NULL
GROUP BY mdo.label
ORDER BY occurrence_count DESC;
```

### Unmatched Labels

| Label | Occurrences | Issue |
|-------|-------------|-------|
| `Ianuarie` | 2 | Month without year |
| `Februarie` | 2 | Month without year |
| `Martie` | 2 | Month without year |
| `Aprilie` | 2 | Month without year |
| `Mai` | 2 | Month without year |
| `Iunie` | 2 | Month without year |
| `Iulie` | 2 | Month without year |
| `August` | 2 | Month without year |
| `Septembrie` | 2 | Month without year |
| `Octombrie` | 2 | Month without year |
| `Noiembrie` | 2 | Month without year |
| `Decembrie` | 2 | Month without year |
| `Total` | 2 | Not a time period |
| `Anii 1901 - 2000` | 1 | Year range |

### Root Cause

The time period parser in `time-periods.ts` only recognized specific patterns:
- "Anul 2023" (annual)
- "Trimestrul I 2024" (quarterly)
- "Luna Ianuarie 2024" (monthly)

It did not handle:
1. **Month-only labels:** "Ianuarie" without a year (used in survey data where year is implicit)
2. **Year ranges:** "Anii 1901 - 2000" (used for historical aggregates)
3. **"Total":** Not a time period, should return null

### Resolution

**Fix Applied:** Added new patterns and handling in `time-periods.ts`:

```typescript
// src/services/sync/time-periods.ts

// New patterns added
const YEAR_RANGE_ANII_PATTERN = /^Ani+i?\s+(\d{4})\s*[-–]\s*(\d{4})$/i;
const MONTH_ONLY_PATTERN = /^(Ianuarie|Februarie|Martie|Aprilie|Mai|Iunie|Iulie|August|Septembrie|Octombrie|Noiembrie|Decembrie)$/i;

parseLabel(label: string): ParsedTimePeriod | null {
  const trimmed = label.trim();

  // Skip labels that are NOT time periods
  if (MONTH_ONLY_PATTERN.test(trimmed)) {
    // Month without year is ambiguous - not a valid time period
    return null;
  }

  if (/^Total$/i.test(trimmed)) {
    // "Total" is not a time period
    return null;
  }

  // Year range with "Anii": "Anii 1901 - 2000" -> use end year
  const yearRangeAniiMatch = YEAR_RANGE_ANII_PATTERN.exec(trimmed);
  if (yearRangeAniiMatch?.[2] !== undefined) {
    return {
      year: Number.parseInt(yearRangeAniiMatch[2], 10),
      periodicity: "ANNUAL",
    };
  }

  // ... existing patterns continue
}
```

**Files Modified:**
- `src/services/sync/time-periods.ts`

---

## 6. Data Quality Issue: Duplicate Classification Types

### Description

Multiple classification types exist for similar concepts, potentially causing confusion and data fragmentation.

### Query to Identify

```sql
SELECT code, name, id
FROM classification_types
WHERE code LIKE 'CAEN%'
ORDER BY code;
```

### Duplicates Found

| Code | Name | ID | Values Count |
|------|------|-----|--------------|
| `CAEN_REV1` | CAEN Rev.1 | 73 | 1,050 |
| `CAEN_REV2` | CAEN Rev.2 | 72 | 1,130 |
| `CAEN__ACTIVITATI_ALE_ECONOMIEI_NATIONALE__SECTIUNI` | Generated from label | 98 | 45 |
| `CAEN_2__ACTIVITATI_ALE_ECONOMIEI_NATIONALE` | Generated from label | 105 | 120 |

### Root Cause

When a dimension label doesn't match any known classification pattern, the system generates a new classification type from the label itself. This leads to:
1. Multiple types for the same underlying classification
2. Inconsistent naming
3. Fragmented classification values

### Current Status

**Not yet resolved.** This is a data quality issue that doesn't cause sync failures but affects data usability. Recommended actions:

1. **Merge duplicate types:** Create migration to consolidate CAEN variants
2. **Improve pattern matching:** Add more patterns to `CLASSIFICATION_PATTERNS`
3. **Add manual mappings:** Create a mapping table for edge cases

### Proposed Fix

```typescript
// Add to CLASSIFICATION_PATTERNS in classifications.ts
{
  code: "CAEN_REV2",
  name: "CAEN Rev.2",
  patterns: [
    /caen\s+rev\.?2/i,
    /caen\s*2/i,
    /activitati\s+ale\s+economiei\s+nationale/i,
    /sectiuni\s+caen/i,
  ],
  isHierarchical: true,
},
```

---

## 7. Resolution Summary

### Fixes Applied

| Issue | Status | Fix Location |
|-------|--------|--------------|
| VARCHAR truncation | ✅ Resolved | Database migration + schema file |
| Duplicate key constraint | ✅ Resolved | `classifications.ts` |
| Unmatched territories | ✅ Resolved | `territories.ts` |
| Unmatched time periods | ✅ Resolved | `time-periods.ts` |
| Duplicate classification types | ⚠️ Documented | Future improvement |

### Verification

```bash
# Re-sync all previously failed matrices
for code in AGR109A CNF102A CON101U ...; do
  LOG_LEVEL=warn pnpm cli sync matrices --full --code "$code"
done

# Result: 92 synced, 0 failed
```

### Final State

```sql
SELECT sync_status, COUNT(*)
FROM matrix_sync_status
GROUP BY sync_status;

-- Result:
-- SYNCED | 1898
```

---

## Appendix: Affected Matrices

### Complete List of 92 Previously Failed Matrices

```
AGR109A, CNF102A, CON101U, CON103J, CON104P, CON104Q, CON104R,
CON104S, CON104T, CON104U, CON104V, CON104W, CON106H, CON106I,
CON106J, CON106K, CON106L, CON106M, CON106N, CON109A, CON110A,
CON110C, CON111D, CON111E, CON111H, ECC108A, ECC109A, EXP101F,
EXP102F, FIN101A, FIN103B, FOM103A, FOM103B, FOM103C, FOM104A,
FOM105B, FOM105D, FOM106A, FOM107A, FOM111A, FOM112A, FOM112B,
FOM117A, FOM117B, FOM118A, FOM118B, FOM118D, FOM118E, FOM119A,
FOM119B, IND104N, IND104O, IND104P, INT101A, INT101G, INT102C,
INT102D, INT104C, INT104D, INT105B, INT105C, INT105D, INT105E,
INT109A, INT109B, INV101C, INV102A, INV102B, LMV101A, LMV102A,
PMI114C, PMI116A, PNS101A, POP206E, POP206G, POP206K, POP207B,
POP207D, PPI1036, PPI1037, PSC102B, TIC100A, TIC111A, TIC111B,
TIC111C, TIC111D, TIC200A, TNA1211, TNR1232, TQZ1553, TRN130B,
TRN137C
```

### Matrices by Domain

| Domain | Failed Count | Examples |
|--------|--------------|----------|
| CON (Construction) | 22 | CON103J, CON104P-W, CON106H-N, CON110A-111H |
| FOM (Labor Force) | 20 | FOM103A-119B |
| INT (Enterprises) | 14 | INT101A-109B |
| TIC (ICT) | 6 | TIC100A, TIC111A-D, TIC200A |
| POP (Population) | 5 | POP206E-207D |
| INV (Investments) | 3 | INV101C, INV102A-B |
| IND (Industry) | 3 | IND104N-P |
| Other | 19 | Various |

---

## Appendix: Diagnostic Queries

### Check for VARCHAR columns still at limit

```sql
SELECT
    table_name,
    column_name,
    data_type,
    character_maximum_length
FROM information_schema.columns
WHERE table_schema = 'public'
  AND data_type = 'character varying'
  AND character_maximum_length IS NOT NULL
ORDER BY table_name, column_name;
```

### Find unresolved dimension options

```sql
-- Territories
SELECT COUNT(*) as unresolved_territories
FROM matrix_dimension_options mdo
JOIN matrix_dimensions md ON mdo.matrix_dimension_id = md.id
WHERE md.dimension_type = 'TERRITORIAL'
  AND mdo.territory_id IS NULL;

-- Time periods
SELECT COUNT(*) as unresolved_time_periods
FROM matrix_dimension_options mdo
JOIN matrix_dimensions md ON mdo.matrix_dimension_id = md.id
WHERE md.dimension_type = 'TEMPORAL'
  AND mdo.time_period_id IS NULL;

-- Classifications
SELECT COUNT(*) as unresolved_classifications
FROM matrix_dimension_options mdo
JOIN matrix_dimensions md ON mdo.matrix_dimension_id = md.id
WHERE md.dimension_type = 'CLASSIFICATION'
  AND mdo.classification_value_id IS NULL;
```

### Check sync status distribution

```sql
SELECT
    sync_status,
    COUNT(*) as count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
FROM matrix_sync_status
GROUP BY sync_status
ORDER BY count DESC;
```

### Find classification type duplicates

```sql
WITH type_stats AS (
    SELECT
        ct.id,
        ct.code,
        ct.name,
        COUNT(cv.id) as value_count
    FROM classification_types ct
    LEFT JOIN classification_values cv ON cv.classification_type_id = ct.id
    GROUP BY ct.id, ct.code, ct.name
)
SELECT * FROM type_stats
WHERE code LIKE 'CAEN%'
   OR code LIKE '%ACTIVITATI%'
   OR code LIKE '%SECTIUNI%'
ORDER BY code;
```

---

*End of Specification*
