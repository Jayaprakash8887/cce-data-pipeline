#!/usr/bin/env bash
# Submit Flink jobs to the cluster
# Usage: ./scripts/submit-flink-jobs.sh [flink-rest-url]

set -euo pipefail

FLINK_URL="${1:-http://localhost:8081}"
JOBS_DIR="flink-jobs"

echo "=== Submitting Flink Jobs ==="
echo "Flink REST: ${FLINK_URL}"
echo ""

JOBS=(
    "event-enrichment"
    "event-volume-aggregator"
    "intelligence-tracker"
    "scheduler-tracker"
    "cdc-enrichment"
)

for job in "${JOBS[@]}"; do
    JAR_PATH="${JOBS_DIR}/${job}/build/libs/${job}-1.0.0-SNAPSHOT-all.jar"
    if [[ ! -f "$JAR_PATH" ]]; then
        echo "✗ ${job}: JAR not found at ${JAR_PATH} (run ./gradlew shadowJar first)"
        continue
    fi

    echo "Uploading ${job}..."
    JAR_ID=$(curl -sf -X POST -H "Expect:" \
        -F "jarfile=@${JAR_PATH}" \
        "${FLINK_URL}/jars/upload" | python3 -c "import sys,json; print(json.load(sys.stdin)['filename'].split('/')[-1])")

    echo "Running ${job} (jar: ${JAR_ID})..."
    RUN_RESPONSE=$(curl -sf -X POST "${FLINK_URL}/jars/${JAR_ID}/run")
    JOB_ID=$(echo "$RUN_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('jobid','unknown'))")
    echo "✓ ${job}: started (jobId: ${JOB_ID})"
    echo ""
done

echo "--- Running Jobs ---"
curl -sf "${FLINK_URL}/jobs/overview" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for job in data.get('jobs', []):
    print(f\"  {job['name']}: {job['state']}\")
"
echo ""
echo "=== Done ==="
