#!/usr/bin/env bash
# Register Kafka Connect connectors
# Usage: ./scripts/register-connectors.sh [connect-url]

set -euo pipefail

CONNECT_URL="${1:-http://localhost:8083}"

echo "=== Registering Kafka Connect Connectors ==="
echo "Connect URL: ${CONNECT_URL}"
echo ""

# Wait for Kafka Connect to be ready
echo "Waiting for Kafka Connect..."
for i in $(seq 1 30); do
    if curl -sf "${CONNECT_URL}/connectors" > /dev/null 2>&1; then
        echo "✓ Kafka Connect is ready"
        break
    fi
    if [[ $i -eq 30 ]]; then
        echo "✗ Kafka Connect not ready after 30 attempts"
        exit 1
    fi
    sleep 2
done

echo ""

# Register CDC source connector (POST full connector object for create-or-update)
echo "Registering Debezium CDC source connector..."
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
    -H "Content-Type: application/json" \
    -d @connectors/cce-cdc-source.json \
    "${CONNECT_URL}/connectors")
if [[ "$HTTP_CODE" == "201" || "$HTTP_CODE" == "409" ]]; then
    echo "✓ CDC source connector registered (HTTP ${HTTP_CODE})"
else
    echo "✗ Failed to register CDC source (HTTP ${HTTP_CODE})"
    exit 1
fi

# Register ClickHouse sink connector
echo "Registering ClickHouse sink connector..."
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
    -H "Content-Type: application/json" \
    -d @connectors/cce-clickhouse-sink.json \
    "${CONNECT_URL}/connectors")
if [[ "$HTTP_CODE" == "201" || "$HTTP_CODE" == "409" ]]; then
    echo "✓ ClickHouse sink connector registered (HTTP ${HTTP_CODE})"
else
    echo "✗ Failed to register ClickHouse sink (HTTP ${HTTP_CODE})"
    exit 1
fi

echo ""
echo "--- Connector Status ---"
for connector in "cce-cdc-source" "cce-clickhouse-sink"; do
    STATUS=$(curl -sf "${CONNECT_URL}/connectors/${connector}/status" | python3 -c "
import sys, json
data = json.load(sys.stdin)
state = data['connector']['state']
tasks = [t['state'] for t in data.get('tasks', [])]
print(f'{state} (tasks: {tasks})')
" 2>/dev/null || echo "UNKNOWN")
    echo "  ${connector}: ${STATUS}"
done

echo ""
echo "=== Registration Complete ==="
