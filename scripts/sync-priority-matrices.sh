#!/bin/bash
# sync-priority-matrices.sh
# Syncs priority matrices with statistical data from INS Tempo API
#
# Usage:
#   ./scripts/sync-priority-matrices.sh              # Default: 2016-2026
#   ./scripts/sync-priority-matrices.sh 2020-2026    # Custom year range

set -e

YEARS="${1:-2016-2026}"

MATRICES=(
  # Population & Demographics
  "POP105A"   # Population by counties
  "POP107D"   # Population by localities (UAT level)
  "POP108D"   # Mid-year population by localities (UAT level)
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
  "FOM104D"   # Average employees by counties and localities (UAT level)
  "FOM104F"   # Average employees by NACE Rev.2 activities and counties
  "FOM105A"   # Year-end employee count by counties
  "FOM105F"   # Year-end employee count by NACE Rev.2 and counties
  "FOM106D"   # Average net salary by economic activity
  "FOM106E"   # Average net salary by NACE Rev.2 and counties
  "FOM107E"   # Average gross salary by NACE Rev.2 and counties (for tax calc)

  # Economy & Enterprises (for profit tax estimation)
  "CON103I"   # GDP by macroregions, regions and counties (NACE Rev.2)
  "CON103H"   # GDP per capita by regions (NACE Rev.2)
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
  "SAN104B"   # Medical staff by counties

  # Construction & Housing
  "LOC101B"   # Housing stock by counties
  "LOC103A"   # Completed dwellings by counties
  "LOC104A"   # Building permits by counties

  # Agriculture
  "AGR101A"   # Agricultural land by counties
  "AGR201A"   # Crop production by counties
  "AGR301A"   # Livestock by counties
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
