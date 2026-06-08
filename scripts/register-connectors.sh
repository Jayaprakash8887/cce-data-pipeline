#!/usr/bin/env bash
# Create the PeerDB CDC mirror for the CCE analytics pipeline
# Usage: ./scripts/register-connectors.sh [peerdb-url]
#
# Prerequisites:
#   1. PeerDB peers configured: 'ccedb_peer' (PostgreSQL) and 'clickhouse_peer' (ClickHouse)
#      via the PeerDB UI at http://<peerdb-host>:3000 or peerdb CLI
#   2. PostgreSQL configured for logical replication (cdc/01-configure-replication.sql)
#   3. ClickHouse database 'cce_analytics' created

set -euo pipefail

PEERDB_URL="${1:-http://localhost:8085}"

echo "=== Creating PeerDB CDC Mirror ==="
echo "PeerDB API: ${PEERDB_URL}"
echo ""

# Wait for PeerDB to be ready
echo "Waiting for PeerDB..."
for i in $(seq 1 30); do
    if curl -sf "${PEERDB_URL}/health" > /dev/null 2>&1; then
        echo "✓ PeerDB is ready"
        break
    fi
    if [[ $i -eq 30 ]]; then
        echo "✗ PeerDB not ready after 30 attempts"
        exit 1
    fi
    sleep 2
done

echo ""

# Create mirror via PeerDB API
echo "Creating mirror cce_analytics_mirror..."
HTTP_CODE=$(curl -s -o /tmp/peerdb_create_response.json -w "%{http_code}" \
    -X POST "${PEERDB_URL}/v1/mirrors/cdc" \
    -H "Content-Type: application/json" \
    -d @- <<'EOF'
{
  "flow_job_name": "cce_analytics_mirror",
  "connection_configs": {
    "source": { "name": "ccedb_peer" },
    "destination": { "name": "clickhouse_peer" },
    "destination_table_identifier": "cce_analytics"
  },
  "table_mappings": [
    { "source_table_identifier": "public.protocol_definition",         "destination_table_identifier": "protocol_definitions" },
    { "source_table_identifier": "public.protocol_instance",           "destination_table_identifier": "protocol_instances" },
    { "source_table_identifier": "public.step_instance",               "destination_table_identifier": "step_instances" },
    { "source_table_identifier": "public.deviation",                   "destination_table_identifier": "deviations" },
    { "source_table_identifier": "public.inbound_event_log",           "destination_table_identifier": "inbound_event_logs" },
    { "source_table_identifier": "public.intelligence_delivery",       "destination_table_identifier": "intelligence_deliveries" },
    { "source_table_identifier": "public.intelligence_event_log",      "destination_table_identifier": "intelligence_event_logs" },
    { "source_table_identifier": "public.action_definition",           "destination_table_identifier": "action_definitions" },
    { "source_table_identifier": "public.receiver_adaptor",            "destination_table_identifier": "receiver_adaptors" },
    { "source_table_identifier": "public.destination_adaptor_mapping", "destination_table_identifier": "destination_adaptor_mappings" },
    { "source_table_identifier": "public.compliance_event_log",        "destination_table_identifier": "compliance_event_logs" }
  ],
  "do_initial_snapshot": true,
  "snapshot_num_rows_per_partition": 500000,
  "snapshot_num_tables_in_parallel": 4,
  "snapshot_max_parallel_workers": 8,
  "cdc_sync_interval_seconds": 10,
  "soft_delete": true,
  "publication_name": "cce_analytics_pub",
  "replication_slot_name": "cce_analytics_slot"
}
EOF
)

if [[ "$HTTP_CODE" == "200" || "$HTTP_CODE" == "201" ]]; then
    echo "✓ Mirror cce_analytics_mirror created (HTTP ${HTTP_CODE})"
elif [[ "$HTTP_CODE" == "409" ]]; then
    echo "  Mirror already exists (HTTP 409) — skipping creation"
else
    echo "✗ Failed to create mirror (HTTP ${HTTP_CODE})"
    cat /tmp/peerdb_create_response.json 2>/dev/null
    exit 1
fi

echo ""
echo "--- Mirror Status ---"
STATUS_JSON=$(curl -sf "${PEERDB_URL}/v1/mirrors/cce_analytics_mirror" 2>/dev/null || echo '{}')
STATUS=$(echo "$STATUS_JSON" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('status', 'UNKNOWN'))" 2>/dev/null || echo "UNKNOWN")
echo "  cce_analytics_mirror: ${STATUS}"

echo ""
echo "=== Mirror Creation Complete ==="
echo "Monitor snapshot progress in the PeerDB UI: ${PEERDB_URL/8085/3000}"
echo "Run schema/01-create-tables.sql AFTER initial snapshot completes."
