#!/usr/bin/env bash
# Data quality checks for ClickHouse analytics tables
# Usage: ./scripts/data-quality-checks.sh [clickhouse-host]
#
# Runs a suite of quality checks and exits non-zero if any fail.

set -euo pipefail

CH_HOST="${1:-localhost}"
CH_PORT="${CH_PORT:-8123}"
CH_DB="cce_analytics"
FAILURES=0

query() {
    curl -s "http://${CH_HOST}:${CH_PORT}/?database=${CH_DB}" --data-binary "$1"
}

check() {
    local name="$1"
    local sql="$2"
    local expected="$3"

    local result
    result=$(query "$sql" | tr -d '[:space:]')

    if [[ "$result" == "$expected" ]]; then
        echo "  [PASS] ${name}"
    else
        echo "  [FAIL] ${name} — expected '${expected}', got '${result}'"
        FAILURES=$((FAILURES + 1))
    fi
}

check_gt() {
    local name="$1"
    local sql="$2"
    local threshold="$3"

    local result
    result=$(query "$sql" | tr -d '[:space:]')

    if [[ "$result" -gt "$threshold" ]] 2>/dev/null; then
        echo "  [PASS] ${name} (value: ${result})"
    else
        echo "  [FAIL] ${name} — value '${result}' not > ${threshold}"
        FAILURES=$((FAILURES + 1))
    fi
}

check_eq_zero() {
    local name="$1"
    local sql="$2"

    local result
    result=$(query "$sql" | tr -d '[:space:]')

    if [[ "$result" == "0" ]]; then
        echo "  [PASS] ${name}"
    else
        echo "  [WARN] ${name} — found ${result} issues"
        FAILURES=$((FAILURES + 1))
    fi
}

echo "=== CCE Data Quality Checks ==="
echo "Host: ${CH_HOST}:${CH_PORT} | Database: ${CH_DB}"
echo ""

echo "--- Table Row Counts ---"
check_gt "events_fact has rows" "SELECT count() FROM events_fact" 0
check_gt "event_volume_hourly has rows" "SELECT count() FROM event_volume_hourly" 0
check_gt "intelligence_events has rows" "SELECT count() FROM intelligence_events" 0
check_gt "step_transitions has rows" "SELECT count() FROM step_transitions" 0
check_gt "protocol_definitions has rows" "SELECT count() FROM protocol_definitions" 0

echo ""
echo "--- Referential Integrity ---"
check_eq_zero "events_fact: no NULL patient_id" \
    "SELECT count() FROM events_fact WHERE patient_id = ''"
check_eq_zero "events_fact: no future event_time" \
    "SELECT count() FROM events_fact WHERE event_time > now() + INTERVAL 1 HOUR"
check_eq_zero "step_instances: orphaned protocol_instance_id" \
    "SELECT count() FROM step_instances FINAL WHERE protocol_instance_id NOT IN (SELECT id FROM protocol_instances FINAL)"
check_eq_zero "deviations: orphaned step_instance_id" \
    "SELECT count() FROM deviations FINAL WHERE step_instance_id NOT IN (SELECT id FROM step_instances FINAL)"

echo ""
echo "--- Freshness ---"
check "events_fact fresh (last 10min)" \
    "SELECT if(max(processed_at) >= now() - INTERVAL 10 MINUTE, 'ok', 'stale') FROM events_fact" \
    "ok"
check "event_volume_hourly fresh (last 2h)" \
    "SELECT if(max(hour) >= now() - INTERVAL 2 HOUR, 'ok', 'stale') FROM event_volume_hourly" \
    "ok"

echo ""
echo "--- Materialized View Consistency ---"
check_eq_zero "MV daily vs hourly drift" \
    "SELECT abs(a - b) FROM (SELECT sum(event_count) as a FROM mv_event_volume_daily WHERE day = today()) x, (SELECT sum(event_count) as b FROM event_volume_hourly WHERE toDate(hour) = today()) y WHERE abs(a-b) > a * 0.01"

echo ""
echo "--- Duplicates ---"
check_eq_zero "events_fact: no duplicate event_id (last hour)" \
    "SELECT count() FROM (SELECT event_id, count() as c FROM events_fact WHERE processed_at >= now() - INTERVAL 1 HOUR GROUP BY event_id HAVING c > 1)"

echo ""
echo "=== Results ==="
if [[ "$FAILURES" -eq 0 ]]; then
    echo "All checks passed!"
    exit 0
else
    echo "${FAILURES} check(s) failed."
    exit 1
fi
