#!/usr/bin/env bash
# End-to-End Integration Test Suite for CCE Data Pipeline
# Prerequisites: docker compose up -d (all services healthy)
# Usage: ./tests/e2e/run-e2e-tests.sh [clickhouse-host] [kafka-bootstrap]

set -euo pipefail

CH_HOST="${1:-localhost}"
CH_PORT="${CH_PORT:-8123}"
KAFKA_BOOTSTRAP="${2:-localhost:9092}"
CH_DB="cce_analytics"
CONNECT_URL="http://localhost:8083"
FLINK_URL="http://localhost:8081"

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
echo "=== CCE Data Pipeline — E2E Test Suite ==="
echo "ClickHouse: ${CH_HOST}:${CH_PORT}"
echo "Kafka: ${KAFKA_BOOTSTRAP}"
echo ""

# ============================================================
echo "--- 1. Service Health Checks ---"

# ClickHouse
if curl -sf "http://${CH_HOST}:${CH_PORT}/ping" > /dev/null; then
    log_pass "ClickHouse is healthy"
else
    log_fail "ClickHouse is not responding"
fi

# Flink
if curl -sf "${FLINK_URL}/overview" > /dev/null; then
    log_pass "Flink JobManager is healthy"
else
    log_fail "Flink JobManager is not responding"
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

TABLE_COUNT=$(ch_query "SELECT count() FROM system.tables WHERE database = '${CH_DB}'" | tr -d '[:space:]')
if [[ "$TABLE_COUNT" -ge 14 ]]; then
    log_pass "ClickHouse has ${TABLE_COUNT} tables (expected >= 14)"
else
    log_fail "ClickHouse has ${TABLE_COUNT} tables (expected >= 14)"
fi

MV_COUNT=$(ch_query "SELECT count() FROM system.tables WHERE database = '${CH_DB}' AND engine = 'MaterializedView'" | tr -d '[:space:]')
if [[ "$MV_COUNT" -ge 8 ]]; then
    log_pass "ClickHouse has ${MV_COUNT} materialized views (expected >= 8)"
else
    log_fail "ClickHouse has ${MV_COUNT} materialized views (expected >= 8)"
fi

DICT_COUNT=$(ch_query "SELECT count() FROM system.dictionaries WHERE database = '${CH_DB}'" | tr -d '[:space:]')
if [[ "$DICT_COUNT" -ge 1 ]]; then
    log_pass "ClickHouse has ${DICT_COUNT} dictionary (expected >= 1)"
else
    log_fail "ClickHouse has ${DICT_COUNT} dictionaries (expected >= 1)"
fi

# ============================================================
echo ""
echo "--- 3. Event Ingestion E2E ---"

# Produce a test CloudEvent to the inbound topic
TEST_EVENT_ID="e2e-test-$(date +%s)"
TEST_EVENT=$(cat <<EOF
{
  "specversion": "1.0",
  "id": "${TEST_EVENT_ID}",
  "type": "org.cce.fhir.encounter",
  "source": "e2e-test",
  "time": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "data": {
    "resourceType": "Encounter",
    "id": "${TEST_EVENT_ID}",
    "subject": { "reference": "Patient/e2e-patient-001" },
    "participant": [
      { "individual": { "reference": "Practitioner/e2e-doc-001" } }
    ],
    "class": { "code": "AMB" },
    "serviceType": { "coding": [{ "code": "e2e-service" }] },
    "location": [{ "location": { "reference": "Location/e2e-facility-001" } }]
  }
}
EOF
)

echo "$TEST_EVENT" | kafka-console-producer \
    --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --topic cce.events.inbound 2>/dev/null && \
    log_info "Published test event ${TEST_EVENT_ID}" || \
    log_fail "Failed to publish test event"

# Wait for processing
log_info "Waiting 15s for pipeline processing..."
sleep 15

# Verify the event landed in events_fact
FOUND=$(ch_query "SELECT count() FROM events_fact WHERE event_id = '${TEST_EVENT_ID}'" | tr -d '[:space:]')
if [[ "$FOUND" == "1" ]]; then
    log_pass "Test event found in events_fact"
else
    log_fail "Test event NOT found in events_fact (found: ${FOUND})"
fi

# Verify patient_id extraction
PATIENT=$(ch_query "SELECT patient_id FROM events_fact WHERE event_id = '${TEST_EVENT_ID}'" | tr -d '[:space:]')
if [[ "$PATIENT" == "e2e-patient-001" ]]; then
    log_pass "patient_id correctly extracted: ${PATIENT}"
else
    log_fail "patient_id incorrect: '${PATIENT}' (expected 'e2e-patient-001')"
fi

# Verify practitioner extraction
PRACTITIONER=$(ch_query "SELECT practitioner_id FROM events_fact WHERE event_id = '${TEST_EVENT_ID}'" | tr -d '[:space:]')
if [[ "$PRACTITIONER" == "e2e-doc-001" ]]; then
    log_pass "practitioner_id correctly extracted: ${PRACTITIONER}"
else
    log_fail "practitioner_id incorrect: '${PRACTITIONER}' (expected 'e2e-doc-001')"
fi

# ============================================================
echo ""
echo "--- 4. Aggregation Pipeline ---"

# Check event_volume_hourly got the event
HOUR_COUNT=$(ch_query "SELECT sum(event_count) FROM event_volume_hourly WHERE event_type = 'org.cce.fhir.encounter' AND window_start >= now() - INTERVAL 2 HOUR" | tr -d '[:space:]')
if [[ "$HOUR_COUNT" -ge 1 ]]; then
    log_pass "event_volume_hourly has aggregated events (count: ${HOUR_COUNT})"
else
    log_fail "event_volume_hourly has no recent aggregated events"
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
echo "--- 6. Flink Jobs ---"

JOB_COUNT=$(curl -sf "${FLINK_URL}/jobs/overview" 2>/dev/null | grep -o '"state":"RUNNING"' | wc -l)
if [[ "$JOB_COUNT" -ge 5 ]]; then
    log_pass "Flink has ${JOB_COUNT} running jobs (expected >= 5)"
else
    log_fail "Flink has ${JOB_COUNT} running jobs (expected >= 5)"
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
