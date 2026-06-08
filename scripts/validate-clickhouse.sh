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

# Expected CDC base tables (created by PeerDB mirror)
EXPECTED_TABLES=(
    "protocol_instances"
    "step_instances"
    "deviations"
    "inbound_event_logs"
    "intelligence_deliveries"
    "intelligence_event_logs"
    "action_definitions"
    "protocol_definitions"
    "receiver_adaptors"
    "destination_adaptor_mappings"
    "compliance_event_logs"
)

echo ""
echo "--- Tables (${#EXPECTED_TABLES[@]} expected) ---"
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

# All materialized views
EXPECTED_MVS=(
    # Event volume
    "mv_event_volume_hourly"
    # Compliance
    "mv_compliance_summary"
    "mv_compliance_by_patient"
    "mv_compliance_processing_quality"
    # Deviations
    "mv_deviation_trends"
    "mv_deviation_by_protocol"
    "mv_deviation_by_patient"
    # Ingestion quality
    "mv_ingestion_quality"
    # Intelligence
    "mv_intelligence_summary"
    "mv_intelligence_by_patient"
    "mv_intelligence_by_protocol"
    # Deliveries
    "mv_delivery_performance_hourly"
    "mv_delivery_by_patient"
    "mv_delivery_by_protocol"
    # Steps / Scheduler
    "mv_step_states_daily"
    "mv_step_states_by_protocol"
    "mv_step_states_by_patient"
    "mv_step_completion_timeliness"
    # Practitioners / Facilities
    "mv_practitioner_summary"
    "mv_facility_summary"
    # Patient-facility mapping (dict source)
    "mv_patient_facility_latest"
)

echo ""
echo "--- Materialized Views (${#EXPECTED_MVS[@]} expected) ---"
for mv in "${EXPECTED_MVS[@]}"; do
    EXISTS=$(curl -sf "${CH_URL}/?query=SELECT+count()+FROM+system.tables+WHERE+database='cce_analytics'+AND+name='${mv}'" | tr -d '[:space:]')
    if [[ "$EXISTS" == "1" ]]; then
        echo "  ✓ ${mv}"
    else
        echo "  ✗ ${mv} MISSING"
        MISSING=$((MISSING + 1))
    fi
done

# All three dictionaries
EXPECTED_DICTS=(
    "dict_protocol_definitions"
    "dict_patient_facility"
    "dict_action_definitions"
)

echo ""
echo "--- Dictionaries (${#EXPECTED_DICTS[@]} expected) ---"
for dict in "${EXPECTED_DICTS[@]}"; do
    EXISTS=$(curl -sf "${CH_URL}/?query=SELECT+count()+FROM+system.dictionaries+WHERE+database='cce_analytics'+AND+name='${dict}'" | tr -d '[:space:]')
    if [[ "$EXISTS" == "1" ]]; then
        echo "  ✓ ${dict}"
    else
        echo "  ✗ ${dict} MISSING"
        MISSING=$((MISSING + 1))
    fi
done

# Check MATERIALIZED columns were added to inbound_event_logs
echo ""
echo "--- MATERIALIZED columns on inbound_event_logs ---"
MAT_COLS=("subject" "event_type" "facility_id" "event_time" "resource_type" "practitioner_ref" "practitioner_display")
for col in "${MAT_COLS[@]}"; do
    EXISTS=$(curl -sf "${CH_URL}/?query=SELECT+count()+FROM+system.columns+WHERE+database='cce_analytics'+AND+table='inbound_event_logs'+AND+name='${col}'" | tr -d '[:space:]')
    if [[ "$EXISTS" == "1" ]]; then
        echo "  ✓ ${col}"
    else
        echo "  ✗ ${col} MISSING — run schema/01-create-tables.sql"
        MISSING=$((MISSING + 1))
    fi
done

echo ""
if [[ $MISSING -eq 0 ]]; then
    echo "=== ALL CHECKS PASSED ==="
    exit 0
else
    echo "=== ${MISSING} CHECK(S) FAILED ==="
    exit 1
fi
