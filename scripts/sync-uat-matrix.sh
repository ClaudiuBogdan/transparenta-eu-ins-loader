#!/bin/bash
# Sync a matrix with full UAT (locality) data by iterating over each county
#
# For matrices like POP107D, POP108D, FOM104D that have county+locality dimensions,
# the INS API requires county-locality pairs to match. This script syncs one county
# at a time to get full locality-level data.
#
# Usage: ./scripts/sync-uat-matrix.sh <MATRIX_CODE> [YEAR_RANGE]
# Example: ./scripts/sync-uat-matrix.sh POP107D 2020-2024

set -e

MATRIX_CODE="${1:-POP107D}"
YEAR_RANGE="${2:-2020-2024}"

# 42 counties in Romania (NUTS3 territory codes)
COUNTIES=(
  "AB"  # Alba
  "AR"  # Arad
  "AG"  # Arges
  "BC"  # Bacau
  "BH"  # Bihor
  "BN"  # Bistrita-Nasaud
  "BT"  # Botosani
  "BV"  # Brasov
  "BR"  # Braila
  "BZ"  # Buzau
  "CS"  # Caras-Severin
  "CL"  # Calarasi
  "CJ"  # Cluj
  "CT"  # Constanta
  "CV"  # Covasna
  "DB"  # Dambovita
  "DJ"  # Dolj
  "GL"  # Galati
  "GR"  # Giurgiu
  "GJ"  # Gorj
  "HR"  # Harghita
  "HD"  # Hunedoara
  "IL"  # Ialomita
  "IS"  # Iasi
  "IF"  # Ilfov
  "MM"  # Maramures
  "MH"  # Mehedinti
  "MS"  # Mures
  "NT"  # Neamt
  "OT"  # Olt
  "PH"  # Prahova
  "SM"  # Satu Mare
  "SJ"  # Salaj
  "SB"  # Sibiu
  "SV"  # Suceava
  "TR"  # Teleorman
  "TM"  # Timis
  "TL"  # Tulcea
  "VS"  # Vaslui
  "VL"  # Valcea
  "VN"  # Vrancea
  "B"   # Municipiul Bucuresti
)

echo "================================================================"
echo "Syncing UAT-level data for $MATRIX_CODE ($YEAR_RANGE)"
echo "This will sync locality data for all 42 counties"
echo "================================================================"
echo ""

TOTAL_ROWS=0
SUCCESS_COUNT=0
FAILED_COUNT=0
FAILED_COUNTIES=()
START_TIME=$(date +%s)

for i in "${!COUNTIES[@]}"; do
  COUNTY_INDEX=$((i + 1))
  COUNTY_CODE="${COUNTIES[$i]}"

  echo -n "[$COUNTY_INDEX/42] $COUNTY_CODE... "

  # Sync this county's localities
  if OUTPUT=$(pnpm cli sync data "$MATRIX_CODE" --years "$YEAR_RANGE" --county "$COUNTY_CODE" 2>&1); then
    # Extract row count from output
    ROWS=$(echo "$OUTPUT" | grep -o '[0-9]* inserted' | grep -o '[0-9]*' || echo "0")
    TOTAL_ROWS=$((TOTAL_ROWS + ROWS))
    SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
    echo "done (+$ROWS rows)"
  else
    FAILED_COUNT=$((FAILED_COUNT + 1))
    FAILED_COUNTIES+=("$COUNTY_CODE")
    echo "FAILED"
  fi

  # Small delay to avoid overwhelming the INS API
  sleep 0.5
done

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
MINUTES=$((DURATION / 60))
SECONDS=$((DURATION % 60))

echo ""
echo "================================================================"
echo "Sync complete"
echo "================================================================"
echo "  Matrix:    $MATRIX_CODE"
echo "  Years:     $YEAR_RANGE"
echo "  Counties:  $SUCCESS_COUNT succeeded, $FAILED_COUNT failed"
echo "  Rows:      $TOTAL_ROWS total inserted"
echo "  Duration:  ${MINUTES}m ${SECONDS}s"
echo "================================================================"

if [ ${#FAILED_COUNTIES[@]} -gt 0 ]; then
  echo ""
  echo "Failed counties:"
  for county in "${FAILED_COUNTIES[@]}"; do
    echo "  - $county"
  done
  echo ""
  echo "Retry with:"
  echo "  for c in ${FAILED_COUNTIES[*]}; do pnpm cli sync data $MATRIX_CODE --years $YEAR_RANGE --county \$c; done"
fi
