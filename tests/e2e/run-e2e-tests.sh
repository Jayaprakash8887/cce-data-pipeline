#!/usr/bin/env bash
# End-to-End Integration Test Suite for CCE Data Pipeline (CDC-only architecture)
# Prerequisites: docker compose up -d (all services healthy)
# Usage: ./tests/e2e/run-e2e-tests.sh [clickhouse-host]

set -euo pipefail

CH_HOST="${1:-localhost}"
CH_PORT="${CH_PORT:-8123}"
CH_DB="cce_analytics"
CONNECT_URL="http://localhost:8083"

PASS=0
FAIL=0

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_pass() { echo -e "  ${GREEN}[PASS]${NC} $1"; PASS=$((PASS + 1)); }
log_fail() { echo -e "  ${RED}[FAIL]${NC} $1"; FAIL=$((FAIL + 1)); }
log_info() { echo -e "  ${YELLOW}[INFO]${NC} $1"; }

ch_query() {
    curl -s "http://${CH_HOST}:${CH_PORT}/?database=${CH_DB}" --data-binary "$1"
}

# ============================================================
echo "=== CCE Data Pipeline — E2E Test Suite (CDC-only) ==="
echo "ClickHouse: ${CH_HOST}:${CH_PORT}"
echo ""

# ============================================================
echo "--- 1. Service Health Checks ---"

# ClickHouse
if curl -sf "http://${CH_HOST}:${CH_PORT}/ping" > /dev/null; then
    log_pass "ClickHouse is healthy"
else
    log_fail "ClickHouse is not responding"
fi

# Kafka Connect
if curl -sf "${CONNECT_URL}/connectors" > /dev/null; then
    log_pass "Kafka Connect is healthy"
else
    log_fail "Kafka Connect is not responding"
fi

# ============================================================
echo ""
echo "--- 2. Schema Validation ---"

TABLE_COUNT=$(ch_query "SELECT count() FROM system.tables WHERE database = '${CH_DB}' AND engine NOT IN ('MaterializedView')" | tr -d '[:space:]')
if [[ "$TABLE_COUNT" -ge 11 ]]; then
    log_pass "ClickHouse has ${TABLE_COUNT} tables (expected >= 11)"
else
    log_fail "ClickHouse has ${TABLE_COUNT} tables (expected >= 11)"
fi

MV_COUNT=$(ch_query "SELECT count() FROM system.tables WHERE database = '${CH_DB}' AND engine = 'MaterializedView'" | tr -d '[:space:]')
if [[ "$MV_COUNT" -ge 10 ]]; then
    log_pass "ClickHouse has ${MV_COUNT} materialized views (expected >= 10)"
else
    log_fail "ClickHouse has ${MV_COUNT} materialized views (expected >= 10)"
fi

# ============================================================
echo ""
echo "--- 3. CDC Data Flow ---"

# Verify inbound_event_logs has data from CDC
INBOUND_COUNT=$(ch_query "SELECT count() FROM inbound_event_logs FINAL" | tr -d '[:space:]')
if [[ "$INBOUND_COUNT" -ge 1 ]]; then
    log_pass "inbound_event_logs has ${INBOUND_COUNT} rows (CDC flowing)"
else
    log_fail "inbound_event_logs is empty (CDC not flowing)"
fi

# Verify MATERIALIZED column extraction works
EXTRACTED=$(ch_query "SELECT count() FROM inbound_event_logs WHERE facility_id != '' AND event_type != ''" | tr -d '[:space:]')
if [[ "$EXTRACTED" -ge 1 ]]; then
    log_pass "MATERIALIZED columns extracting correctly (${EXTRACTED} rows with facility_id + event_type)"
else
    log_fail "MATERIALIZED columns not extracting (no rows with facility_id + event_type)"
fi

# Verify protocol_instances CDC
PI_COUNT=$(ch_query "SELECT count() FROM protocol_instances FINAL" | tr -d '[:space:]')
if [[ "$PI_COUNT" -ge 1 ]]; then
    log_pass "protocol_instances has ${PI_COUNT} rows"
else
    log_fail "protocol_instances is empty"
fi

# ============================================================
echo ""
echo "--- 4. Materialized View Population ---"

# Check event volume MV is populated
VOL_COUNT=$(ch_query "SELECT sum(event_count) FROM mv_event_volume_hourly" | tr -d '[:space:]')
if [[ "$VOL_COUNT" -ge 1 ]]; then
    log_pass "mv_event_volume_hourly has aggregated events (count: ${VOL_COUNT})"
else
    log_fail "mv_event_volume_hourly is empty"
fi

# Check daily/hourly consistency
CONSISTENCY=$(ch_query "SELECT if(abs(a - b) <= greatest(a, 1) * 0.01, 1, 0) FROM (SELECT sum(event_count) as a FROM mv_event_volume_daily) x, (SELECT sum(event_count) as b FROM mv_event_volume_hourly) y" | tr -d '[:space:]')
if [[ "$CONSISTENCY" == "1" ]]; then
    log_pass "mv_event_volume_daily consistent with hourly"
else
    log_fail "mv_event_volume_daily/hourly mismatch"
fi

# ============================================================
echo ""
echo "--- 5. Connector Status ---"

# Check source connector
SOURCE_STATUS=$(curl -sf "${CONNECT_URL}/connectors/cce-cdc-source/status" 2>/dev/null | grep -o '"state":"[A-Z]*"' | head -1 | cut -d'"' -f4)
if [[ "$SOURCE_STATUS" == "RUNNING" ]]; then
    log_pass "CDC source connector is RUNNING"
else
    log_fail "CDC source connector status: ${SOURCE_STATUS:-NOT_FOUND}"
fi

# Check sink connector
SINK_STATUS=$(curl -sf "${CONNECT_URL}/connectors/cce-clickhouse-sink/status" 2>/dev/null | grep -o '"state":"[A-Z]*"' | head -1 | cut -d'"' -f4)
if [[ "$SINK_STATUS" == "RUNNING" ]]; then
    log_pass "ClickHouse sink connector is RUNNING"
else
    log_fail "ClickHouse sink connector status: ${SINK_STATUS:-NOT_FOUND}"
fi

# ============================================================
echo ""
echo "--- 6. Data Quality ---"

# No orphaned step_instances
ORPHANS=$(ch_query "SELECT count() FROM step_instances FINAL WHERE protocol_instance_id NOT IN (SELECT id FROM protocol_instances FINAL)" | tr -d '[:space:]')
if [[ "$ORPHANS" == "0" ]]; then
    log_pass "No orphaned step_instances"
else
    log_fail "Found ${ORPHANS} orphaned step_instances"
fi

# ============================================================
echo ""
echo "=== Results ==="
echo -e "  ${GREEN}Passed: ${PASS}${NC}"
echo -e "  ${RED}Failed: ${FAIL}${NC}"
echo ""

if [[ "$FAIL" -eq 0 ]]; then
    echo "All E2E tests passed!"
    exit 0
else
    echo "${FAIL} test(s) failed."
    exit 1
fi
