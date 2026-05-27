#!/usr/bin/env bash
# Check Kafka Connect connector health
# Usage: ./scripts/check-connector-health.sh [connect-url]

set -euo pipefail

CONNECT_URL="${1:-http://localhost:8083}"

echo "=== Kafka Connect Health Check ==="
echo ""

CONNECTORS=$(curl -sf "${CONNECT_URL}/connectors" | python3 -c "import sys,json; [print(c) for c in json.load(sys.stdin)]" 2>/dev/null)

if [[ -z "$CONNECTORS" ]]; then
    echo "No connectors registered"
    exit 0
fi

UNHEALTHY=0
while IFS= read -r connector; do
    STATUS_JSON=$(curl -sf "${CONNECT_URL}/connectors/${connector}/status")
    CONN_STATE=$(echo "$STATUS_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin)['connector']['state'])")
    TASK_STATES=$(echo "$STATUS_JSON" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for t in data.get('tasks', []):
    state = t['state']
    tid = t['id']
    trace = t.get('trace', '')[:100]
    if state == 'FAILED':
        print(f'  Task {tid}: {state} — {trace}')
    else:
        print(f'  Task {tid}: {state}')
")

    if [[ "$CONN_STATE" == "RUNNING" ]]; then
        echo "✓ ${connector}: ${CONN_STATE}"
    else
        echo "✗ ${connector}: ${CONN_STATE}"
        UNHEALTHY=$((UNHEALTHY + 1))
    fi
    echo "$TASK_STATES"
    echo ""
done <<< "$CONNECTORS"

if [[ $UNHEALTHY -eq 0 ]]; then
    echo "=== ALL CONNECTORS HEALTHY ==="
    exit 0
else
    echo "=== ${UNHEALTHY} UNHEALTHY CONNECTORS ==="
    exit 1
fi
