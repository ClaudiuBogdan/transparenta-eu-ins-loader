# Seed Data

This folder contains seed data files that must be loaded into the database before running data syncs.

## Files

| File | Description | Records |
|------|-------------|---------|
| `territories.csv` | Romanian administrative territories (NUTS + LAU) | 3,238 |
| `siruta-official.csv` | Official SIRUTA reference data (source) | 16,978 |

## Territory Seed

### Overview

The `territories.csv` file contains the complete Romanian administrative hierarchy:

- **NATIONAL** (1): Romania (RO)
- **NUTS1** (4): Macroregions (RO1-RO4)
- **NUTS2** (8): Development regions
- **NUTS3** (42): Counties (județe)
- **LAU** (3,183): Local Administrative Units (municipalities, cities, communes)

### Why Seed-Based?

Previously, territories were created dynamically during sync using SIRUTA code ranges. This approach had a critical flaw: the numeric ranges didn't correctly map SIRUTA codes to counties, resulting in **~80% of LAUs being assigned to wrong counties**.

The seed-based approach uses the **official SIRUTA data** (from INS) which includes the authoritative `JUD` field that correctly maps each locality to its county.

### CSV Format

```csv
id,code,siruta_code,level,parent_code,name_ro,nuts,type,urban,source
1,RO,,NATIONAL,,TOTAL,,,,STATIC
14,AB,,NUTS3,RO12,Alba,,40,,STATIC
957,54975,54975,LAU,CJ,MUNICIPIUL CLUJ-NAPOCA,RO113,1,1,SIRUTA
```

| Column | Description |
|--------|-------------|
| `id` | Unique identifier (preserved from existing DB where possible) |
| `code` | Territory code (NUTS code or SIRUTA code for LAU) |
| `siruta_code` | Official SIRUTA code (for LAU level) |
| `level` | Hierarchy level: NATIONAL, NUTS1, NUTS2, NUTS3, LAU |
| `parent_code` | Code of parent territory |
| `name_ro` | Romanian name (from official SIRUTA) |
| `nuts` | NUTS code (for LAU: their NUTS3 region code) |
| `type` | SIRUTA TIP field (1=municipality, 2=city, 3=commune, etc.) |
| `urban` | Urban indicator (1=urban, 0=rural) |
| `source` | Data source: STATIC (NUTS hierarchy), SIRUTA (official data), INS (INS-only codes) |

### Hierarchy Structure

```
RO (NATIONAL)
├── RO1 (NUTS1 - Macroregiunea Unu)
│   ├── RO11 (NUTS2 - Nord-Vest)
│   │   ├── BH (NUTS3 - Bihor)
│   │   │   ├── 26011 (LAU - Oradea)
│   │   │   └── ...
│   │   ├── CJ (NUTS3 - Cluj)
│   │   │   ├── 54975 (LAU - Cluj-Napoca)
│   │   │   └── ...
│   │   └── ...
│   └── RO12 (NUTS2 - Centru)
│       └── ...
├── RO2 (NUTS1 - Macroregiunea Doi)
│   └── ...
├── RO3 (NUTS1 - Macroregiunea Trei)
│   └── ...
└── RO4 (NUTS1 - Macroregiunea Patru)
    └── ...
```

### Loading Seed Data

```bash
# Load territories into database (upsert mode - safe for updates)
pnpm cli seed territories

# Dry run - validate without making changes
pnpm cli seed territories --dry-run

# Force mode - clean slate (TRUNCATE + INSERT)
# WARNING: This deletes all linked statistics data!
pnpm cli seed territories --force

# Use custom file path
pnpm cli seed territories --file /path/to/custom.csv
```

### Seed Behavior

**Upsert Mode (default):**
- Inserts new territories (not yet in database)
- Updates existing territories (name, parent, metadata changes)
- Reports territories in DB but not in seed (doesn't delete them)
- Preserves linked statistics data
- Safe for incremental SIRUTA updates

**Force Mode (`--force`):**
- Truncates all territories (CASCADE deletes linked data)
- Inserts all territories fresh
- Use for initial setup or when you want a clean slate
- WARNING: Deletes all linked statistics!

The seed command:
1. Parses the CSV file
2. Validates all records (parent references, county completeness)
3. Either upserts (default) or truncates+inserts (--force)
4. Updates the ID sequence

### Special Territories

#### Extra-regiuni (EXTRA)

**Extra-regiuni** is a special EU statistical category for extra-territorial regions (e.g., embassies, consulates, international waters). It appears in some INS matrices as a territorial dimension option.

This territory is **not in the seed CSV** - it's added to the database as part of the sync verification process:

```sql
INSERT INTO territories (code, name, level, path, parent_id)
VALUES ('EXTRA', 'Extra-regiuni', 'NUTS1', 'RO.EXTRA', 
        (SELECT id FROM territories WHERE code = 'RO'));
```

| Field | Value |
|-------|-------|
| Code | EXTRA |
| Name | Extra-regiuni |
| Level | NUTS1 |
| Path | RO.EXTRA |
| Parent | RO (National) |

The sync script automatically resolves labels like "Extra-regiuni" or "Extra regiuni" to this territory.

### Edge Cases

Two localities exist in INS data but not in official SIRUTA UAT list:

| SIRUTA Code | Name | County | Notes |
|-------------|------|--------|-------|
| 70049 | CERNELE | DJ | Appears in INS population data |
| 167589 | GORANU | VL | Appears in INS population data |

These are included in the seed with `source=INS` to ensure sync doesn't fail on these localities.

## Regenerating Seed Data

If you need to regenerate the seed file (e.g., after SIRUTA updates):

```bash
# Run the generation script
python scripts/generate-territory-seed.py

# The script reads:
# - seed/siruta-official.csv (official SIRUTA)
# - data/territories_export.csv (current DB export for ID preservation)

# And outputs:
# - seed/territories.csv
```

### Handling SIRUTA Updates

When INS releases updated SIRUTA data:

1. Download updated `siruta-official.csv`
2. Regenerate: `python scripts/generate-territory-seed.py`
3. Apply changes: `pnpm cli seed territories`

The upsert mode will:
- Add any new localities
- Update names, parent assignments, or metadata changes
- Report any localities that were removed from SIRUTA (won't delete them)

Use `--force` only if you want to completely reset territories.

## Related Files

- `scripts/generate-territory-seed.py` - Python script to generate territories.csv
- `src/cli/commands/seed.ts` - CLI seed command implementation
- `src/services/sync/canonical/territories.ts` - TerritoryService (lookup-only)
