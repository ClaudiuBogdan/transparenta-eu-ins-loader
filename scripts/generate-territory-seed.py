#!/usr/bin/env python3
"""
Generate territories seed CSV from official SIRUTA data.

This script creates a seed file for the territories table by:
1. Loading existing territories to preserve IDs where possible
2. Creating the static NUTS hierarchy (RO, RO1-RO4, RO11-RO42, counties)
3. Parsing official SIRUTA for UATs (NIV=2)
4. Mapping JUD codes to county codes
5. Handling INS-only edge cases

Usage:
    python3 scripts/generate-territory-seed.py

Output:
    seed/territories.csv
"""

import csv
from pathlib import Path
from collections import defaultdict

# Paths
PROJECT_ROOT = Path(__file__).parent.parent
SEED_DIR = PROJECT_ROOT / "seed"
DATA_DIR = PROJECT_ROOT / "data"

SIRUTA_FILE = SEED_DIR / "siruta-official.csv"
CURRENT_EXPORT = DATA_DIR / "territories_export.csv"
OUTPUT_FILE = SEED_DIR / "territories.csv"

# JUD code to 2-letter county code mapping
JUD_TO_CODE = {
    1: 'AB', 2: 'AR', 3: 'AG', 4: 'BC', 5: 'BH', 6: 'BN', 7: 'BT', 8: 'BV',
    9: 'BR', 10: 'BZ', 11: 'CS', 12: 'CJ', 13: 'CT', 14: 'CV', 15: 'DB',
    16: 'DJ', 17: 'GL', 18: 'GJ', 19: 'HR', 20: 'HD', 21: 'IL', 22: 'IS',
    23: 'IF', 24: 'MM', 25: 'MH', 26: 'MS', 27: 'NT', 28: 'OT', 29: 'PH',
    30: 'SM', 31: 'SJ', 32: 'SB', 33: 'SV', 34: 'TR', 35: 'TM', 36: 'TL',
    37: 'VS', 38: 'VL', 39: 'VN', 40: 'B', 51: 'CL', 52: 'GR'
}

# Static NUTS hierarchy
MACROREGIONS = [
    {'code': 'RO1', 'name': 'MACROREGIUNEA UNU', 'parent': 'RO'},
    {'code': 'RO2', 'name': 'MACROREGIUNEA DOI', 'parent': 'RO'},
    {'code': 'RO3', 'name': 'MACROREGIUNEA TREI', 'parent': 'RO'},
    {'code': 'RO4', 'name': 'MACROREGIUNEA PATRU', 'parent': 'RO'},
]

REGIONS = [
    {'code': 'RO11', 'name': 'Nord-Vest', 'parent': 'RO1', 'counties': ['BH', 'BN', 'CJ', 'MM', 'SJ', 'SM']},
    {'code': 'RO12', 'name': 'Centru', 'parent': 'RO1', 'counties': ['AB', 'BV', 'CV', 'HR', 'MS', 'SB']},
    {'code': 'RO21', 'name': 'Nord-Est', 'parent': 'RO2', 'counties': ['BC', 'BT', 'IS', 'NT', 'SV', 'VS']},
    {'code': 'RO22', 'name': 'Sud-Est', 'parent': 'RO2', 'counties': ['BR', 'BZ', 'CT', 'GL', 'TL', 'VN']},
    {'code': 'RO31', 'name': 'Sud - Muntenia', 'parent': 'RO3', 'counties': ['AG', 'CL', 'DB', 'GR', 'IL', 'PH', 'TR']},
    {'code': 'RO32', 'name': 'București - Ilfov', 'parent': 'RO3', 'counties': ['B', 'IF']},
    {'code': 'RO41', 'name': 'Sud-Vest Oltenia', 'parent': 'RO4', 'counties': ['DJ', 'GJ', 'MH', 'OT', 'VL']},
    {'code': 'RO42', 'name': 'Vest', 'parent': 'RO4', 'counties': ['AR', 'CS', 'HD', 'TM']},
]

COUNTIES = [
    {'code': 'AB', 'name': 'Alba'},
    {'code': 'AR', 'name': 'Arad'},
    {'code': 'AG', 'name': 'Argeș'},
    {'code': 'BC', 'name': 'Bacău'},
    {'code': 'BH', 'name': 'Bihor'},
    {'code': 'BN', 'name': 'Bistrița-Năsăud'},
    {'code': 'BT', 'name': 'Botoșani'},
    {'code': 'BV', 'name': 'Brașov'},
    {'code': 'BR', 'name': 'Brăila'},
    {'code': 'B', 'name': 'București'},
    {'code': 'BZ', 'name': 'Buzău'},
    {'code': 'CS', 'name': 'Caraș-Severin'},
    {'code': 'CL', 'name': 'Călărași'},
    {'code': 'CJ', 'name': 'Cluj'},
    {'code': 'CT', 'name': 'Constanța'},
    {'code': 'CV', 'name': 'Covasna'},
    {'code': 'DB', 'name': 'Dâmbovița'},
    {'code': 'DJ', 'name': 'Dolj'},
    {'code': 'GL', 'name': 'Galați'},
    {'code': 'GR', 'name': 'Giurgiu'},
    {'code': 'GJ', 'name': 'Gorj'},
    {'code': 'HR', 'name': 'Harghita'},
    {'code': 'HD', 'name': 'Hunedoara'},
    {'code': 'IL', 'name': 'Ialomița'},
    {'code': 'IS', 'name': 'Iași'},
    {'code': 'IF', 'name': 'Ilfov'},
    {'code': 'MM', 'name': 'Maramureș'},
    {'code': 'MH', 'name': 'Mehedinți'},
    {'code': 'MS', 'name': 'Mureș'},
    {'code': 'NT', 'name': 'Neamț'},
    {'code': 'OT', 'name': 'Olt'},
    {'code': 'PH', 'name': 'Prahova'},
    {'code': 'SM', 'name': 'Satu Mare'},
    {'code': 'SJ', 'name': 'Sălaj'},
    {'code': 'SB', 'name': 'Sibiu'},
    {'code': 'SV', 'name': 'Suceava'},
    {'code': 'TR', 'name': 'Teleorman'},
    {'code': 'TM', 'name': 'Timiș'},
    {'code': 'TL', 'name': 'Tulcea'},
    {'code': 'VS', 'name': 'Vaslui'},
    {'code': 'VL', 'name': 'Vâlcea'},
    {'code': 'VN', 'name': 'Vrancea'},
]

# INS-only codes that don't exist in official SIRUTA UATs
# These are villages that INS treats as UATs
INS_ONLY_CODES = {
    '70049': {'name': 'CERNELE', 'county': 'DJ'},  # Village in Dolj
    '167589': {'name': 'GORANU', 'county': 'VL'},  # Village in Vâlcea
}


def load_existing_ids():
    """Load existing territory IDs from current export to preserve them."""
    id_map = {}  # code -> id
    siruta_id_map = {}  # siruta_code -> id

    if not CURRENT_EXPORT.exists():
        print(f"Warning: {CURRENT_EXPORT} not found, will generate new IDs")
        return id_map, siruta_id_map

    with open(CURRENT_EXPORT, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            tid = int(row['id'])
            code = row['code']
            siruta = row.get('siruta_code', '').strip()

            id_map[code] = tid
            if siruta:
                siruta_id_map[siruta] = tid

    print(f"Loaded {len(id_map)} existing territory IDs")
    return id_map, siruta_id_map


def get_region_for_county(county_code):
    """Get the NUTS2 region code for a county."""
    for region in REGIONS:
        if county_code in region['counties']:
            return region['code']
    return None


def get_macro_for_region(region_code):
    """Get the NUTS1 macroregion code for a region."""
    for region in REGIONS:
        if region['code'] == region_code:
            return region['parent']
    return None


def build_path(level, code, county_code=None):
    """Build the ltree path for a territory."""
    if level == 'NATIONAL':
        return 'RO'
    elif level == 'NUTS1':
        return f'RO.{code}'
    elif level == 'NUTS2':
        macro = get_macro_for_region(code)
        return f'RO.{macro}.{code}'
    elif level == 'NUTS3':
        region = get_region_for_county(code)
        macro = get_macro_for_region(region)
        return f'RO.{macro}.{region}.{code}'
    elif level == 'LAU':
        region = get_region_for_county(county_code)
        macro = get_macro_for_region(region)
        return f'RO.{macro}.{region}.{county_code}.{code}'
    return code


def parse_siruta():
    """Parse official SIRUTA file and extract UATs (NIV=2)."""
    uats = []

    with open(SIRUTA_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            niv = row.get('NIV', '')
            if niv != '2':  # Only UAT level
                continue

            siruta = row['SIRUTA']
            name = row['DENLOC']
            jud = int(row['JUD'])
            nuts = row.get('NUTS', '')
            tip = row.get('TIP', '')
            med = row.get('MED', '0')

            county_code = JUD_TO_CODE.get(jud)
            if not county_code:
                print(f"Warning: Unknown JUD code {jud} for {name}")
                continue

            uats.append({
                'siruta_code': siruta,
                'name_ro': name,
                'county_code': county_code,
                'nuts': nuts,
                'type': tip,
                'urban': '1' if med == '1' else '0',
                'source': 'SIRUTA'
            })

    print(f"Parsed {len(uats)} UATs from official SIRUTA")
    return uats


def generate_seed():
    """Generate the complete territories seed data."""
    # Load existing IDs
    id_map, siruta_id_map = load_existing_ids()

    # Parse official SIRUTA
    uats = parse_siruta()

    # Build territories list
    territories = []
    next_id = max(list(id_map.values()) + list(siruta_id_map.values()) + [0]) + 1

    def get_or_create_id(code, siruta_code=None):
        nonlocal next_id
        # First try siruta_code
        if siruta_code and siruta_code in siruta_id_map:
            return siruta_id_map[siruta_code]
        # Then try code
        if code in id_map:
            return id_map[code]
        # Generate new ID
        new_id = next_id
        next_id += 1
        return new_id

    # 1. National level
    territories.append({
        'id': get_or_create_id('RO'),
        'code': 'RO',
        'siruta_code': '',
        'level': 'NATIONAL',
        'parent_code': '',
        'name_ro': 'TOTAL',
        'nuts': '',
        'type': '',
        'urban': '',
        'source': 'STATIC'
    })

    # 2. Macroregions (NUTS1)
    for macro in MACROREGIONS:
        territories.append({
            'id': get_or_create_id(macro['code']),
            'code': macro['code'],
            'siruta_code': '',
            'level': 'NUTS1',
            'parent_code': 'RO',
            'name_ro': macro['name'],
            'nuts': '',
            'type': '',
            'urban': '',
            'source': 'STATIC'
        })

    # 3. Regions (NUTS2)
    for region in REGIONS:
        territories.append({
            'id': get_or_create_id(region['code']),
            'code': region['code'],
            'siruta_code': '',
            'level': 'NUTS2',
            'parent_code': region['parent'],
            'name_ro': region['name'],
            'nuts': '',
            'type': '',
            'urban': '',
            'source': 'STATIC'
        })

    # 4. Counties (NUTS3)
    for county in COUNTIES:
        region_code = get_region_for_county(county['code'])
        territories.append({
            'id': get_or_create_id(county['code']),
            'code': county['code'],
            'siruta_code': '',
            'level': 'NUTS3',
            'parent_code': region_code,
            'name_ro': county['name'],
            'nuts': '',
            'type': '40',  # County type
            'urban': '',
            'source': 'STATIC'
        })

    # 5. UATs from official SIRUTA
    for uat in uats:
        territories.append({
            'id': get_or_create_id(uat['siruta_code'], uat['siruta_code']),
            'code': uat['siruta_code'],
            'siruta_code': uat['siruta_code'],
            'level': 'LAU',
            'parent_code': uat['county_code'],
            'name_ro': uat['name_ro'],
            'nuts': uat['nuts'],
            'type': uat['type'],
            'urban': uat['urban'],
            'source': 'SIRUTA'
        })

    # 6. INS-only codes (edge cases)
    for siruta_code, data in INS_ONLY_CODES.items():
        territories.append({
            'id': get_or_create_id(siruta_code, siruta_code),
            'code': siruta_code,
            'siruta_code': siruta_code,
            'level': 'LAU',
            'parent_code': data['county'],
            'name_ro': data['name'],
            'nuts': '',
            'type': '',
            'urban': '0',
            'source': 'INS'
        })

    print(f"Generated {len(territories)} total territories")
    return territories


def validate_seed(territories):
    """Validate the generated seed data."""
    errors = []

    # Build lookup maps
    codes = set()
    siruta_codes = set()
    parent_codes = set(t['parent_code'] for t in territories if t['parent_code'])
    all_codes = set(t['code'] for t in territories)

    # Check for duplicates and missing parents
    for t in territories:
        if t['code'] in codes:
            errors.append(f"Duplicate code: {t['code']}")
        codes.add(t['code'])

        if t['siruta_code']:
            if t['siruta_code'] in siruta_codes:
                errors.append(f"Duplicate siruta_code: {t['siruta_code']}")
            siruta_codes.add(t['siruta_code'])

    # Check all parent_codes resolve
    for parent in parent_codes:
        if parent and parent not in all_codes:
            errors.append(f"Unresolved parent_code: {parent}")

    # Check all counties have UATs
    county_uats = defaultdict(int)
    for t in territories:
        if t['level'] == 'LAU':
            county_uats[t['parent_code']] += 1

    for county in COUNTIES:
        if county['code'] not in county_uats:
            errors.append(f"County {county['code']} has no UATs")
        elif county_uats[county['code']] == 0:
            errors.append(f"County {county['code']} has 0 UATs")

    # Print stats
    level_counts = defaultdict(int)
    for t in territories:
        level_counts[t['level']] += 1

    print("\nTerritory counts by level:")
    for level in ['NATIONAL', 'NUTS1', 'NUTS2', 'NUTS3', 'LAU']:
        print(f"  {level}: {level_counts[level]}")

    print(f"\nUATs per county (sample):")
    for code in sorted(county_uats.keys())[:5]:
        print(f"  {code}: {county_uats[code]}")
    print(f"  ...")

    if errors:
        print(f"\n{len(errors)} validation errors:")
        for e in errors[:10]:
            print(f"  - {e}")
        if len(errors) > 10:
            print(f"  ... and {len(errors) - 10} more")
        return False

    print("\nValidation passed!")
    return True


def write_seed(territories):
    """Write territories to CSV seed file."""
    fieldnames = ['id', 'code', 'siruta_code', 'level', 'parent_code', 'name_ro', 'nuts', 'type', 'urban', 'source']

    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        # Sort by level then by code
        level_order = {'NATIONAL': 0, 'NUTS1': 1, 'NUTS2': 2, 'NUTS3': 3, 'LAU': 4}
        sorted_territories = sorted(territories, key=lambda t: (level_order[t['level']], t['code']))

        for t in sorted_territories:
            writer.writerow(t)

    print(f"\nWrote {len(territories)} territories to {OUTPUT_FILE}")


def main():
    print("=" * 60)
    print("Territory Seed Generator")
    print("=" * 60)

    # Check input files exist
    if not SIRUTA_FILE.exists():
        print(f"Error: {SIRUTA_FILE} not found")
        return 1

    # Generate seed data
    territories = generate_seed()

    # Validate
    if not validate_seed(territories):
        print("\nWarning: Validation failed, but continuing to write output")

    # Write output
    write_seed(territories)

    # Print sample output
    print("\nSample output (first 10 rows):")
    with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i >= 11:  # header + 10 rows
                break
            print(f"  {line.rstrip()}")

    return 0


if __name__ == '__main__':
    exit(main())
