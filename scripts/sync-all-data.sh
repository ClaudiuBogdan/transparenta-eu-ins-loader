#!/bin/bash
# sync-all-data.sh
# Full data sync for all matrices with metadata
#
# Usage:
#   ./scripts/sync-all-data.sh              # Default: 2020-current
#   ./scripts/sync-all-data.sh 2020-2024    # Custom year range
#   ./scripts/sync-all-data.sh --limit 100  # Limit to first 100 matrices

set -e

YEARS=""
LIMIT=""
YEAR_FLAG=""
LIMIT_FLAG=""

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --limit)
      LIMIT="$2"
      LIMIT_FLAG="--limit $2"
      shift 2
      ;;
    *)
      if [[ -z "$YEARS" ]]; then
        YEARS="$1"
        YEAR_FLAG="--years $1"
      fi
      shift
      ;;
  esac
done

echo ""
echo "╔══════════════════════════════════════════════════════════════════════════════╗"
echo "║                    INS TEMPO FULL DATA SYNC                                  ║"
echo "╠══════════════════════════════════════════════════════════════════════════════╣"
if [ -n "$YEARS" ]; then
echo "║  Years:    $YEARS                                                            ║"
else
echo "║  Years:    Default (2020-current)                                            ║"
fi
if [ -n "$LIMIT" ]; then
echo "║  Limit:    $LIMIT matrices                                                   ║"
fi
echo "╚══════════════════════════════════════════════════════════════════════════════╝"
echo ""
echo "This will sync statistical data for ALL matrices with synced metadata."
echo "Estimated time: Several hours to days depending on data volume."
echo ""
echo "Press Ctrl+C to cancel, or wait 5 seconds to continue..."
sleep 5

START_TIME=$(date +%s)

# Run the CLI command with continue-on-error flag
pnpm cli sync data $YEAR_FLAG $LIMIT_FLAG --continue-on-error

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
HOURS=$((DURATION / 3600))
MINUTES=$(((DURATION % 3600) / 60))
SECONDS=$((DURATION % 60))

echo ""
echo "════════════════════════════════════════════════════════════════════════════════"
echo "  Total duration: ${HOURS}h ${MINUTES}m ${SECONDS}s"
echo "════════════════════════════════════════════════════════════════════════════════"
