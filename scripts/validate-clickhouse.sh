#!/usr/bin/env bash
# Validate ClickHouse schema deployment
# Usage: ./scripts/validate-clickhouse.sh [host] [port]

set -euo pipefail

CH_HOST="${1:-localhost}"
CH_PORT="${2:-8123}"
CH_URL="http://${CH_HOST}:${CH_PORT}"

echo "=== ClickHouse Schema Validation ==="
echo "Target: ${CH_URL}"
echo ""

# Check connectivity
if ! curl -sf "${CH_URL}/ping" > /dev/null 2>&1; then
    echo "ERROR: Cannot reach ClickHouse at ${CH_URL}"
    exit 1
fi
echo "✓ ClickHouse is reachable"

# Check database exists
DB_EXISTS=$(curl -sf "${CH_URL}/?query=SELECT+count()+FROM+system.databases+WHERE+name='cce_analytics'" | tr -d '[:space:]')
if [[ "$DB_EXISTS" != "1" ]]; then
    echo "ERROR: Database 'cce_analytics' does not exist"
    exit 1
fi
echo "✓ Database 'cce_analytics' exists"

# Expected tables
EXPECTED_TABLES=(
    "events_fact"
    "event_volume_hourly"
    "intelligence_events"
    "step_transitions"
    "protocol_instances"
    "step_instances"
    "deviations"
    "inbound_events"
    "intelligence_deliveries"
    "intelligence_event_logs"
    "action_definitions"
    "protocol_definitions"
    "receiver_adaptors"
    "destination_adaptor_mappings"
)

echo ""
echo "--- Tables ---"
MISSING=0
for table in "${EXPECTED_TABLES[@]}"; do
    EXISTS=$(curl -sf "${CH_URL}/?query=SELECT+count()+FROM+system.tables+WHERE+database='cce_analytics'+AND+name='${table}'" | tr -d '[:space:]')
    if [[ "$EXISTS" == "1" ]]; then
        echo "  ✓ ${table}"
    else
        echo "  ✗ ${table} MISSING"
        MISSING=$((MISSING + 1))
    fi
done

# Expected materialized views
EXPECTED_MVS=(
    "mv_event_volume_daily"
    "mv_compliance_summary"
    "mv_deviation_trends"
    "mv_ingestion_quality"
    "mv_deviation_by_facility"
    "mv_intelligence_summary"
    "mv_delivery_performance_hourly"
    "mv_scheduler_transitions_daily"
)

echo ""
echo "--- Materialized Views ---"
for mv in "${EXPECTED_MVS[@]}"; do
    EXISTS=$(curl -sf "${CH_URL}/?query=SELECT+count()+FROM+system.tables+WHERE+database='cce_analytics'+AND+name='${mv}'" | tr -d '[:space:]')
    if [[ "$EXISTS" == "1" ]]; then
        echo "  ✓ ${mv}"
    else
        echo "  ✗ ${mv} MISSING"
        MISSING=$((MISSING + 1))
    fi
done

# Check dictionary
echo ""
echo "--- Dictionaries ---"
DICT_EXISTS=$(curl -sf "${CH_URL}/?query=SELECT+count()+FROM+system.dictionaries+WHERE+database='cce_analytics'+AND+name='dict_protocol_definitions'" | tr -d '[:space:]')
if [[ "$DICT_EXISTS" == "1" ]]; then
    echo "  ✓ dict_protocol_definitions"
else
    echo "  ✗ dict_protocol_definitions MISSING"
    MISSING=$((MISSING + 1))
fi

echo ""
if [[ $MISSING -eq 0 ]]; then
    echo "=== ALL CHECKS PASSED ==="
    exit 0
else
    echo "=== ${MISSING} CHECKS FAILED ==="
    exit 1
fi
