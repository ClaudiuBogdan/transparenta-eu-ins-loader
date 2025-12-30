#!/bin/bash
# sync-priority-matrices.sh
# Syncs priority matrices with statistical data from INS Tempo API
#
# Usage:
#   ./scripts/sync-priority-matrices.sh              # Default: 2016-2024
#   ./scripts/sync-priority-matrices.sh 2020-2024    # Custom year range

set -e

YEARS="${1:-2016-2024}"

MATRICES=(
  # Population & Demographics
  "POP105A"   # Population by counties
  "POP107D"   # Population by localities (UAT level)
  "POP201A"   # Live births by counties
  "POP202A"   # Deaths by counties
  "POP206A"   # Natural increase by counties
  "POP301A"   # Internal migration by counties

  # Labor Market
  "SOM101B"   # Registered unemployed by counties
  "SOM103B"   # Unemployment rate by counties
  "FOR101B"   # Labor force by counties
  "FOR103A"   # Employment by counties
  "FOM104B"   # Average net salary by counties
  "FOM106D"   # Average salary by economic activity

  # Economy
  "CON101C"   # GDP by regions
  "CON103F"   # GDP per capita by regions
  "INT101I"   # Active enterprises by counties
  "INT102I"   # New enterprises by counties

  # Education
  "SCL101A"   # Schools by counties
  "SCL103B"   # Pre-university students by counties
  "SCL104J"   # Teaching staff by counties
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

  if pnpm cli sync data "$code" --years "$YEARS"; then
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
  echo "  for code in ${FAILED_LIST[*]}; do pnpm cli sync data \"\$code\" --years $YEARS; done"
  exit 1
fi
