#!/usr/bin/env bash
# Check PeerDB mirror health
# Usage: ./scripts/check-connector-health.sh [peerdb-url]

set -euo pipefail

PEERDB_URL="${1:-http://localhost:8085}"

echo "=== PeerDB Mirror Health Check ==="
echo ""

# Check PeerDB API is reachable
if ! curl -sf "${PEERDB_URL}/health" > /dev/null 2>&1; then
    echo "✗ PeerDB is not reachable at ${PEERDB_URL}"
    exit 1
fi
echo "✓ PeerDB API reachable"
echo ""

# List all mirrors and check status
MIRRORS_JSON=$(curl -sf "${PEERDB_URL}/v1/mirrors" 2>/dev/null || echo '{"mirrors": []}')
MIRROR_COUNT=$(echo "$MIRRORS_JSON" | python3 -c "
import sys, json
data = json.load(sys.stdin)
mirrors = data.get('mirrors', [])
print(len(mirrors))
" 2>/dev/null || echo "0")

if [[ "$MIRROR_COUNT" == "0" ]]; then
    echo "No mirrors registered"
    exit 0
fi

UNHEALTHY=0

echo "$MIRRORS_JSON" | python3 -c "
import sys, json

data = json.load(sys.stdin)
mirrors = data.get('mirrors', [])

for m in mirrors:
    name   = m.get('flow_job_name', m.get('name', 'unknown'))
    status = m.get('status', 'UNKNOWN')
    lag    = m.get('cdc_lag_seconds', None)
    rows   = m.get('rows_synced_total', None)

    mark = '✓' if status == 'RUNNING' else '✗'
    detail = f'status={status}'
    if lag is not None:
        detail += f', lag={lag}s'
    if rows is not None:
        detail += f', rows_synced={rows}'
    print(f'{mark} {name}: {detail}')
" 2>/dev/null

echo ""

# Check specifically for cce_analytics_mirror
MIRROR_JSON=$(curl -sf "${PEERDB_URL}/v1/mirrors/cce_analytics_mirror" 2>/dev/null || echo '{}')
STATUS=$(echo "$MIRROR_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status','NOT_FOUND'))" 2>/dev/null || echo "NOT_FOUND")
LAG=$(echo "$MIRROR_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin).get('cdc_lag_seconds','N/A'))" 2>/dev/null || echo "N/A")

if [[ "$STATUS" == "RUNNING" ]]; then
    echo "✓ cce_analytics_mirror: RUNNING (lag: ${LAG}s)"
else
    echo "✗ cce_analytics_mirror: ${STATUS}"
    UNHEALTHY=$((UNHEALTHY + 1))
fi

echo ""
if [[ $UNHEALTHY -eq 0 ]]; then
    echo "=== ALL MIRRORS HEALTHY ==="
    exit 0
else
    echo "=== ${UNHEALTHY} UNHEALTHY MIRROR(S) ==="
    exit 1
fi
