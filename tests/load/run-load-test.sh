#!/usr/bin/env bash
# Load Test for CCE Data Pipeline
# Produces a configurable number of CloudEvents to simulate production load.
# Usage: ./tests/load/run-load-test.sh [events-per-second] [duration-seconds] [kafka-bootstrap]
#
# Example:
#   ./tests/load/run-load-test.sh 500 60        # 500 eps for 60 seconds
#   ./tests/load/run-load-test.sh 1000 300      # 1000 eps for 5 minutes

set -euo pipefail

EPS="${1:-100}"
DURATION="${2:-60}"
KAFKA_BOOTSTRAP="${3:-localhost:9092}"
TOPIC="cce.events.inbound"

TOTAL=$((EPS * DURATION))
BATCH_SIZE=100
SLEEP_INTERVAL=$(echo "scale=4; ${BATCH_SIZE} / ${EPS}" | bc)

EVENT_TYPES=("org.cce.fhir.encounter" "org.cce.fhir.observation" "org.cce.fhir.medicationrequest" "org.cce.fhir.procedure" "org.cce.fhir.condition")
FACILITIES=("facility-001" "facility-002" "facility-003" "facility-004" "facility-005")

echo "=== CCE Pipeline Load Test ==="
echo "Rate:       ${EPS} events/second"
echo "Duration:   ${DURATION} seconds"
echo "Total:      ${TOTAL} events"
echo "Batch size: ${BATCH_SIZE}"
echo "Topic:      ${TOPIC}"
echo "Bootstrap:  ${KAFKA_BOOTSTRAP}"
echo ""

generate_event() {
    local seq_num="$1"
    local event_type="${EVENT_TYPES[$((RANDOM % ${#EVENT_TYPES[@]}))]}"
    local facility="${FACILITIES[$((RANDOM % ${#FACILITIES[@]}))]}"
    local patient_id="patient-$(printf '%05d' $((RANDOM % 10000)))"
    local practitioner_id="doc-$(printf '%03d' $((RANDOM % 200)))"

    cat <<EOF
{"specversion":"1.0","id":"load-${seq_num}-$(date +%s%N)","type":"${event_type}","source":"load-test","time":"$(date -u +%Y-%m-%dT%H:%M:%SZ)","data":{"resourceType":"Encounter","id":"enc-${seq_num}","subject":{"reference":"Patient/${patient_id}"},"participant":[{"individual":{"reference":"Practitioner/${practitioner_id}"}}],"class":{"code":"AMB"},"serviceType":{"coding":[{"code":"svc-${seq_num}"}]},"location":[{"location":{"reference":"Location/${facility}"}}]}}
EOF
}

SENT=0
START_TIME=$(date +%s)

echo "Starting load test at $(date -u +%H:%M:%S)..."
echo ""

while [[ $SENT -lt $TOTAL ]]; do
    # Generate a batch
    BATCH=""
    for ((i = 0; i < BATCH_SIZE && SENT < TOTAL; i++)); do
        BATCH+="$(generate_event $SENT)"$'\n'
        SENT=$((SENT + 1))
    done

    # Send batch
    echo -n "$BATCH" | kafka-console-producer \
        --bootstrap-server "$KAFKA_BOOTSTRAP" \
        --topic "$TOPIC" 2>/dev/null

    # Progress
    if ((SENT % (EPS * 10) == 0)); then
        ELAPSED=$(($(date +%s) - START_TIME))
        ACTUAL_RATE=$((SENT / (ELAPSED + 1)))
        echo "  Sent: ${SENT}/${TOTAL} | Elapsed: ${ELAPSED}s | Actual rate: ~${ACTUAL_RATE} eps"
    fi

    sleep "$SLEEP_INTERVAL"
done

END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))
ACTUAL_RATE=$((TOTAL / (ELAPSED + 1)))

echo ""
echo "=== Load Test Complete ==="
echo "Total sent:   ${TOTAL} events"
echo "Duration:     ${ELAPSED}s"
echo "Actual rate:  ~${ACTUAL_RATE} events/second"
echo ""
echo "--- Verification ---"
echo "Wait 30s for pipeline to process, then run:"
echo "  curl 'http://localhost:8123/?database=cce_analytics' --data-binary \\"
echo "    \"SELECT count() FROM events_fact WHERE processed_at >= now() - INTERVAL $((DURATION + 60)) SECOND\""
