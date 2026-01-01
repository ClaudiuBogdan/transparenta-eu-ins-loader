#!/bin/bash
# sync-priority-uat.sh
# Syncs the most important UAT-level (locality) matrices for Transparenta.eu
#
# These matrices contain data at the lowest administrative level (UAT/commune/town)
# which is essential for local budget analysis and transparency.
#
# Usage:
#   ./scripts/sync-priority-uat.sh              # Default: 2016-current year
#   ./scripts/sync-priority-uat.sh 2018-2026    # Custom year range
#
# Note: UAT matrices require iterating over counties. Each matrix takes ~3-5 minutes.

set -e

CURRENT_YEAR=$(date +%Y)
YEARS="${1:-2016-$CURRENT_YEAR}"

# Priority UAT matrices organized by category
# These are the most important for local budget transparency
UAT_MATRICES=(
  # ═══════════════════════════════════════════════════════════════════════════
  # POPULATION (Critical for per-capita calculations)
  # ═══════════════════════════════════════════════════════════════════════════
  "POP107D"   # Population by localities - January 1st
  "POP108D"   # Population by localities - July 1st (mid-year)
  "POP201D"   # Live births by localities
  "POP206D"   # Deaths by localities

  # ═══════════════════════════════════════════════════════════════════════════
  # EMPLOYMENT & SALARIES (Critical for income tax estimation)
  # ═══════════════════════════════════════════════════════════════════════════
  "FOM104D"   # Average number of employees by localities
  "SOM101E"   # Registered unemployed by localities

  # ═══════════════════════════════════════════════════════════════════════════
  # EDUCATION (Infrastructure & enrollment by localities)
  # ═══════════════════════════════════════════════════════════════════════════
  "SCL101C"   # Schools by localities
  "SCL103D"   # Student enrollment by localities
  "SCL104D"   # Teaching staff by localities
  "SCL105B"   # Classrooms by localities
  "SCL109D"   # Graduates by localities

  # ═══════════════════════════════════════════════════════════════════════════
  # HEALTH (Healthcare capacity by localities)
  # ═══════════════════════════════════════════════════════════════════════════
  "SAN101B"   # Healthcare units by localities
  "SAN102C"   # Hospital beds by localities
  "SAN103B"   # Children in nurseries by localities
  "SAN104B"   # Medical staff by localities

  # ═══════════════════════════════════════════════════════════════════════════
  # HOUSING & CONSTRUCTION (Housing stock and development)
  # ═══════════════════════════════════════════════════════════════════════════
  "LOC101B"   # Housing stock by localities
  "LOC103B"   # Living area by localities
  "LOC104B"   # Completed dwellings by localities
  "LOC108B"   # Building permits by localities

  # ═══════════════════════════════════════════════════════════════════════════
  # PUBLIC UTILITIES (Infrastructure coverage)
  # ═══════════════════════════════════════════════════════════════════════════
  "GOS106B"   # Water distribution network by localities
  "GOS108A"   # Potable water distributed by localities
  "GOS110A"   # Sewage network length by localities
  "GOS116A"   # Gas distribution network by localities

  # ═══════════════════════════════════════════════════════════════════════════
  # AGRICULTURE (Primary sector by localities)
  # ═══════════════════════════════════════════════════════════════════════════
  "AGR101B"   # Agricultural land by localities
  "AGR108B"   # Crop area by localities
  "AGR109B"   # Agricultural production by localities
  "AGR201B"   # Livestock by localities

  # ═══════════════════════════════════════════════════════════════════════════
  # TOURISM (Tourism capacity by localities)
  # ═══════════════════════════════════════════════════════════════════════════
  "TUR101C"   # Tourism structures by localities
  "TUR102C"   # Tourism accommodation capacity by localities
  "TUR104E"   # Tourist arrivals by localities

  # ═══════════════════════════════════════════════════════════════════════════
  # CULTURE (Cultural infrastructure)
  # ═══════════════════════════════════════════════════════════════════════════
  "ART101B"   # Libraries by localities
  "ART104A"   # Museums by localities
)

# 42 Romanian counties
COUNTIES=(
  "AB" "AR" "AG" "BC" "BH" "BN" "BT" "BV" "BR" "BZ"
  "CS" "CL" "CJ" "CT" "CV" "DB" "DJ" "GL" "GR" "GJ"
  "HR" "HD" "IL" "IS" "IF" "MM" "MH" "MS" "NT" "OT"
  "PH" "SM" "SJ" "SB" "SV" "TR" "TM" "TL" "VS" "VL"
  "VN" "B"
)

TOTAL_MATRICES=${#UAT_MATRICES[@]}
TOTAL_COUNTIES=${#COUNTIES[@]}
MATRIX_SUCCESS=0
MATRIX_FAILED=0
FAILED_MATRICES=()

echo ""
echo "╔══════════════════════════════════════════════════════════════════════════════╗"
echo "║                    PRIORITY UAT-LEVEL MATRICES SYNC                          ║"
echo "╠══════════════════════════════════════════════════════════════════════════════╣"
echo "║  Matrices:  $TOTAL_MATRICES UAT datasets                                               ║"
echo "║  Counties:  $TOTAL_COUNTIES (full locality coverage)                                   ║"
echo "║  Years:     $YEARS                                                         ║"
echo "║  Est. Time: ~$((TOTAL_MATRICES * 5)) minutes (5 min per matrix)                              ║"
echo "╚══════════════════════════════════════════════════════════════════════════════╝"
echo ""

GLOBAL_START=$(date +%s)

for m in "${!UAT_MATRICES[@]}"; do
  MATRIX_CODE="${UAT_MATRICES[$m]}"
  MATRIX_NUM=$((m + 1))

  echo "════════════════════════════════════════════════════════════════════════════════"
  echo "[$MATRIX_NUM/$TOTAL_MATRICES] MATRIX: $MATRIX_CODE (years: $YEARS)"
  echo "════════════════════════════════════════════════════════════════════════════════"

  MATRIX_START=$(date +%s)
  COUNTY_SUCCESS=0
  COUNTY_FAILED=0
  MATRIX_ROWS=0

  for c in "${!COUNTIES[@]}"; do
    COUNTY_CODE="${COUNTIES[$c]}"
    COUNTY_NUM=$((c + 1))

    printf "  [%2d/42] %-3s ... " "$COUNTY_NUM" "$COUNTY_CODE"

    if OUTPUT=$(pnpm cli sync data --matrix "$MATRIX_CODE" --years "$YEARS" --county "$COUNTY_CODE" 2>&1); then
      # Extract rows from output
      ROWS=$(echo "$OUTPUT" | grep -oE '[0-9]+ inserted' | grep -oE '[0-9]+' | head -1 || echo "0")
      UPDATED=$(echo "$OUTPUT" | grep -oE '[0-9]+ updated' | grep -oE '[0-9]+' | head -1 || echo "0")
      MATRIX_ROWS=$((MATRIX_ROWS + ROWS + UPDATED))
      COUNTY_SUCCESS=$((COUNTY_SUCCESS + 1))
      echo "✓ (+$ROWS/$UPDATED)"
    else
      COUNTY_FAILED=$((COUNTY_FAILED + 1))
      echo "✗ FAILED"
    fi

    # Small delay to be nice to the INS API
    sleep 0.3
  done

  MATRIX_END=$(date +%s)
  MATRIX_DURATION=$((MATRIX_END - MATRIX_START))

  if [ $COUNTY_FAILED -eq 0 ]; then
    echo "  ────────────────────────────────────────────────────────────────────────────"
    echo "  ✓ $MATRIX_CODE complete: $COUNTY_SUCCESS/42 counties, $MATRIX_ROWS rows, ${MATRIX_DURATION}s"
    MATRIX_SUCCESS=$((MATRIX_SUCCESS + 1))
  else
    echo "  ────────────────────────────────────────────────────────────────────────────"
    echo "  ⚠ $MATRIX_CODE partial: $COUNTY_SUCCESS/42 ok, $COUNTY_FAILED failed, $MATRIX_ROWS rows, ${MATRIX_DURATION}s"
    MATRIX_FAILED=$((MATRIX_FAILED + 1))
    FAILED_MATRICES+=("$MATRIX_CODE")
  fi
  echo ""
done

GLOBAL_END=$(date +%s)
TOTAL_DURATION=$((GLOBAL_END - GLOBAL_START))
HOURS=$((TOTAL_DURATION / 3600))
MINUTES=$(((TOTAL_DURATION % 3600) / 60))
SECONDS=$((TOTAL_DURATION % 60))

echo "╔══════════════════════════════════════════════════════════════════════════════╗"
echo "║                           SYNC COMPLETE                                      ║"
echo "╠══════════════════════════════════════════════════════════════════════════════╣"
echo "║  Matrices:  $MATRIX_SUCCESS succeeded, $MATRIX_FAILED with partial failures                 ║"
echo "║  Duration:  ${HOURS}h ${MINUTES}m ${SECONDS}s                                                        ║"
echo "╚══════════════════════════════════════════════════════════════════════════════╝"

if [ ${#FAILED_MATRICES[@]} -gt 0 ]; then
  echo ""
  echo "Matrices with partial failures (some counties failed):"
  for code in "${FAILED_MATRICES[@]}"; do
    echo "  - $code"
  done
  echo ""
  echo "Retry failed matrices with:"
  echo "  ./scripts/sync-uat-matrix.sh <MATRIX_CODE> $YEARS"
fi
