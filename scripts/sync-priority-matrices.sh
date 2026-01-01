#!/bin/bash
# sync-priority-matrices.sh
# Syncs priority matrices with statistical data from INS Tempo API
#
# Usage:
#   ./scripts/sync-priority-matrices.sh              # Default: 2016-current year
#   ./scripts/sync-priority-matrices.sh 2020-2026    # Custom year range

set -e

CURRENT_YEAR=$(date +%Y)
YEARS="${1:-2016-$CURRENT_YEAR}"

MATRICES=(
  # Population & Demographics
  "POP105A"   # Population by counties
  "POP201A"   # Live births by counties
  "POP202A"   # Deaths by counties
  "POP206A"   # Natural increase by counties
  "POP301A"   # Internal migration by counties

  # Labor Market & Salaries (for income tax estimation)
  "SOM101B"   # Registered unemployed by counties
  "SOM103B"   # Unemployment rate by counties
  "AMG110F"   # Employed population by age and sex (AMIGO survey)
  "AMG1010"   # Labor force participation by sex and residence (AMIGO survey)
  "FOM104B"   # Average employees by counties
  "FOM104F"   # Average employees by NACE Rev.2 activities and counties
  "FOM105A"   # Year-end employee count by counties
  "FOM105F"   # Year-end employee count by NACE Rev.2 and counties
  "FOM106D"   # Average net salary by economic activity
  "FOM106E"   # Average net salary by NACE Rev.2 and counties
  "FOM107E"   # Average gross salary by NACE Rev.2 and counties (for tax calc)

  # Economy & GDP (Critical for economic analysis)
  "CON103I"   # PIB pe macroregiuni, regiuni de dezvoltare si judete - Absolute GDP in millions LEI
  "CON103H"   # PIB regional pe locuitor - GDP per capita in LEI
  "CON103J"   # PIB pe ramuri de activitate, judete - GDP by economic activity (NACE)

  # Enterprises (for profit tax estimation)
  "INT101I"   # Active enterprises by counties
  "INT101O"   # Active enterprises by counties (NACE Rev.2)
  "INT101R"   # Active local units by counties (NACE Rev.2)
  "INT102D"   # Personnel in local units by counties (NACE Rev.2)
  "INT104D"   # Turnover of local units by counties (NACE Rev.2)

  # Retail & Commerce (for TVA estimation)
  "COM101B"   # Retail trade turnover by NACE Rev.2
  "COM104B"   # Retail sales value by NACE Rev.2

  # Transport & Vehicles (for fuel excise estimation)
  "TRN102A"   # New passenger vehicle registrations by counties
  "TRN102B"   # New cargo vehicle registrations by counties
  "TRN103B"   # Registered vehicles by categories and counties
  "TRN103D"   # Registered vehicles by fuel type

  # Consumption (for alcohol/tobacco excise estimation)
  "CLV104A"   # Average annual consumption per capita
  "CLV105A"   # Durable goods per 1000 inhabitants

  # Education
  "SCL101A"   # Schools by counties
  "SCL103B"   # Pre-university students by counties
  "SCL104A"   # Teaching staff by counties and sex
  "SCL108A"   # Higher education students

  # Health
  "SAN101A"   # Hospitals by counties
  "SAN103A"   # Hospital beds by counties

  # Construction & Housing
  "LOC103A"   # Completed dwellings by counties
  "LOC104A"   # Building permits by counties

  # Agriculture
  "AGR101A"   # Agricultural land by counties
  "AGR201A"   # Crop production by counties
  "AGR301A"   # Livestock by counties

  # ═══════════════════════════════════════════════════════════════════════════════
  # UAT-LEVEL MATRICES (Locality data - requires county iteration)
  # These matrices have data at the lowest administrative level (UAT/commune/town)
  # ═══════════════════════════════════════════════════════════════════════════════

  # Population by localities (Critical for per-capita calculations)
  "POP107D"   # Population by localities - January 1st
  "POP108D"   # Population by localities - July 1st (mid-year)
  "POP201D"   # Live births by localities
  "POP206D"   # Deaths by localities

  # Employment by localities (for income tax estimation)
  "FOM104D"   # Average number of employees by localities
  "SOM101E"   # Registered unemployed by localities

  # Education by localities (infrastructure & enrollment)
  "SCL101C"   # Schools by localities
  "SCL103D"   # Student enrollment by localities
  "SCL104D"   # Teaching staff by localities
  "SCL105B"   # Classrooms by localities
  "SCL109D"   # Graduates by localities

  # Health by localities (healthcare capacity)
  "SAN101B"   # Healthcare units by localities
  "SAN102C"   # Hospital beds by localities
  "SAN103B"   # Children in nurseries by localities
  "SAN104B"   # Medical staff by localities

  # Housing by localities (housing stock and development)
  "LOC101B"   # Housing stock by localities
  "LOC103B"   # Living area by localities
  "LOC104B"   # Completed dwellings by localities
  "LOC108B"   # Building permits by localities

  # Public Utilities by localities (infrastructure coverage)
  "GOS106B"   # Water distribution network by localities
  "GOS108A"   # Potable water distributed by localities
  "GOS110A"   # Sewage network length by localities
  "GOS116A"   # Gas distribution network by localities

  # Agriculture by localities (primary sector)
  "AGR101B"   # Agricultural land by localities
  "AGR108B"   # Crop area by localities
  "AGR109B"   # Agricultural production by localities
  "AGR201B"   # Livestock by localities

  # Tourism by localities (tourism capacity)
  "TUR101C"   # Tourism structures by localities
  "TUR102C"   # Tourism accommodation capacity by localities
  "TUR104E"   # Tourist arrivals by localities

  # Culture by localities (cultural infrastructure)
  "ART101B"   # Libraries by localities
  "ART104A"   # Museums by localities
)

TOTAL=${#MATRICES[@]}
SUCCESS=0
FAILED=0
FAILED_LIST=()

echo ""
echo "╔══════════════════════════════════════════════════════════════════════════════╗"
echo "║                    INS TEMPO PRIORITY MATRICES SYNC                          ║"
echo "╠══════════════════════════════════════════════════════════════════════════════╣"
echo "║  Matrices: $TOTAL                                                              ║"
echo "║  Years:    $YEARS                                                         ║"
echo "╚══════════════════════════════════════════════════════════════════════════════╝"
echo ""

START_TIME=$(date +%s)

for i in "${!MATRICES[@]}"; do
  code="${MATRICES[$i]}"
  num=$((i + 1))

  echo "────────────────────────────────────────────────────────────────────────────────"
  echo "[$num/$TOTAL] Syncing $code (years: $YEARS)..."
  echo "────────────────────────────────────────────────────────────────────────────────"

  if pnpm cli sync data --matrix "$code" --years "$YEARS"; then
    echo "✓ $code completed"
    ((SUCCESS++))
  else
    echo "✗ $code failed"
    ((FAILED++))
    FAILED_LIST+=("$code")
  fi

  echo ""
done

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
MINUTES=$((DURATION / 60))
SECONDS=$((DURATION % 60))

echo "╔══════════════════════════════════════════════════════════════════════════════╗"
echo "║                              SYNC COMPLETE                                   ║"
echo "╠══════════════════════════════════════════════════════════════════════════════╣"
echo "║  Total:    $TOTAL matrices                                                     ║"
echo "║  Success:  $SUCCESS                                                              ║"
echo "║  Failed:   $FAILED                                                               ║"
echo "║  Duration: ${MINUTES}m ${SECONDS}s                                                         ║"
echo "╚══════════════════════════════════════════════════════════════════════════════╝"

if [ $FAILED -gt 0 ]; then
  echo ""
  echo "Failed matrices:"
  for code in "${FAILED_LIST[@]}"; do
    echo "  - $code"
  done
  echo ""
  echo "Retry failed matrices with:"
  echo "  for code in ${FAILED_LIST[*]}; do pnpm cli sync data --matrix \"\$code\" --years $YEARS; done"
  exit 1
fi
