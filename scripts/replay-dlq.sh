#!/usr/bin/env bash
# PeerDB Mirror Re-snapshot
# Pauses the mirror, drops and recreates it to force a full re-snapshot.
# Use when: replication slot was dropped, ClickHouse data is corrupted/truncated,
# or schema diverged beyond what ALTER TABLE can fix.
#
# Usage: ./scripts/replay-dlq.sh [peerdb-url]
#
# WARNING: This will truncate all CDC tables in ClickHouse and re-snapshot from
# PostgreSQL. The initial snapshot may take minutes to hours depending on data volume.

set -euo pipefail

PEERDB_URL="${1:-http://localhost:8085}"
MIRROR_NAME="cce_analytics_mirror"

echo "=== PeerDB Mirror Re-snapshot ==="
echo "PeerDB API:  ${PEERDB_URL}"
echo "Mirror:      ${MIRROR_NAME}"
echo ""
echo "WARNING: This will DROP and RECREATE the mirror, wiping all ClickHouse CDC"
echo "         table data and running a full initial snapshot from PostgreSQL."
echo ""
read -p "Continue? (y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 0
fi

echo ""

# Step 1: Pause the mirror
echo "Step 1: Pausing mirror..."
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
    -X POST "${PEERDB_URL}/v1/mirrors/${MIRROR_NAME}/pause" \
    -H "Content-Type: application/json")
if [[ "$HTTP_CODE" == "200" ]]; then
    echo "  ✓ Mirror paused"
else
    echo "  Mirror may not exist or already stopped (HTTP ${HTTP_CODE}) — continuing"
fi

sleep 3

# Step 2: Drop the mirror
echo "Step 2: Dropping mirror..."
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
    -X DELETE "${PEERDB_URL}/v1/mirrors/${MIRROR_NAME}" \
    -H "Content-Type: application/json" \
    -d '{"drop_flow_stats": true}')
if [[ "$HTTP_CODE" == "200" || "$HTTP_CODE" == "204" ]]; then
    echo "  ✓ Mirror dropped"
else
    echo "  ✗ Failed to drop mirror (HTTP ${HTTP_CODE})"
    exit 1
fi

sleep 2

# Step 3: Recreate the mirror (triggers full snapshot)
echo "Step 3: Recreating mirror with do_initial_snapshot=true..."
HTTP_CODE=$(curl -s -o /tmp/peerdb_resnap_response.json -w "%{http_code}" \
    -X POST "${PEERDB_URL}/v1/mirrors/cdc" \
    -H "Content-Type: application/json" \
    -d @connectors/peerdb-mirror.sql 2>/dev/null || echo "000")

# Fall back to inline payload if file not parseable
HTTP_CODE=$(curl -s -o /tmp/peerdb_resnap_response.json -w "%{http_code}" \
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
    echo "  ✓ Mirror recreated — initial snapshot starting"
else
    echo "  ✗ Failed to recreate mirror (HTTP ${HTTP_CODE})"
    cat /tmp/peerdb_resnap_response.json 2>/dev/null
    exit 1
fi

echo ""
echo "=== Re-snapshot initiated ==="
echo "Monitor progress in the PeerDB UI: ${PEERDB_URL/8085/3000}"
echo "Run schema/01-create-tables.sql again AFTER the initial snapshot completes."
