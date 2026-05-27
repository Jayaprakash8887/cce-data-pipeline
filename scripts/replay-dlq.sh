#!/usr/bin/env bash
# Replay messages from a DLQ topic back to the original topic
# Usage: ./scripts/replay-dlq.sh <dlq-topic> [kafka-bootstrap] [max-messages]
#
# Examples:
#   ./scripts/replay-dlq.sh cce.events.inbound.dlq
#   ./scripts/replay-dlq.sh cce.intelligence.triggers.dlq localhost:9092 100

set -euo pipefail

DLQ_TOPIC="${1:?Usage: $0 <dlq-topic> [kafka-bootstrap] [max-messages]}"
KAFKA_BOOTSTRAP="${2:-localhost:9092}"
MAX_MESSAGES="${3:-}"

# Derive original topic by removing .dlq suffix
ORIGINAL_TOPIC="${DLQ_TOPIC%.dlq}"

if [[ "$ORIGINAL_TOPIC" == "$DLQ_TOPIC" ]]; then
    echo "ERROR: Topic '${DLQ_TOPIC}' does not end in .dlq"
    exit 1
fi

echo "=== DLQ Replay ==="
echo "Source (DLQ):  ${DLQ_TOPIC}"
echo "Target:        ${ORIGINAL_TOPIC}"
echo "Bootstrap:     ${KAFKA_BOOTSTRAP}"
echo ""

# Count messages in DLQ
MSG_COUNT=$(kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list "$KAFKA_BOOTSTRAP" \
    --topic "$DLQ_TOPIC" \
    --time -1 2>/dev/null | awk -F: '{sum += $3} END {print sum}')

echo "Messages in DLQ: ${MSG_COUNT}"

if [[ "$MSG_COUNT" == "0" ]]; then
    echo "DLQ is empty. Nothing to replay."
    exit 0
fi

LIMIT_ARG=""
if [[ -n "$MAX_MESSAGES" ]]; then
    LIMIT_ARG="--max-messages ${MAX_MESSAGES}"
    echo "Replaying up to ${MAX_MESSAGES} messages..."
else
    echo "Replaying all ${MSG_COUNT} messages..."
fi

echo ""
read -p "Continue? (y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 0
fi

# Replay: consume from DLQ, produce to original topic
kafka-console-consumer \
    --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --topic "$DLQ_TOPIC" \
    --from-beginning \
    $LIMIT_ARG \
    --timeout-ms 10000 | \
kafka-console-producer \
    --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --topic "$ORIGINAL_TOPIC"

echo ""
echo "=== Replay Complete ==="
echo "Messages have been replayed to ${ORIGINAL_TOPIC}"
echo "Monitor the pipeline for processing results."
