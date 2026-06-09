#!/usr/bin/env bash
# Check the health of the PeerDB CDC stack and mirror.
# Usage: ./scripts/check-connector-health.sh
#
# Verifies the PeerDB containers are up/healthy, probes the nexus (9900) and
# flow-api HTTP gateway (8113), and lists the mirror via the nexus `peers`/`mirrors`
# views. Detailed per-table lag/rows is best viewed in the PeerDB UI (port 3000)
# or Temporal UI (port 8085).

set -euo pipefail

PEERDB_HOST="${PEERDB_HOST:-localhost}"
PEERDB_PORT="${PEERDB_PORT:-9900}"
PEERDB_USER="${PEERDB_USER:-peerdb}"
PEERDB_PASSWORD="${PEERDB_PASSWORD:-peerdb}"
FLOW_API_HTTP="${FLOW_API_HTTP:-http://localhost:8113}"

PEERDB_SERVICES=(catalog temporal temporal-admin-tools flow-api flow-snapshot-worker flow-worker peerdb peerdb-ui minio)

echo "=== PeerDB Stack Health ==="
echo ""

UNHEALTHY=0

# 1. Container state (compose project must be running locally)
echo "--- Containers ---"
if docker compose ps >/dev/null 2>&1; then
    for svc in "${PEERDB_SERVICES[@]}"; do
        STATE=$(docker compose ps --format '{{.State}}' "$svc" 2>/dev/null | head -1)
        STATE="${STATE:-absent}"
        if [[ "$STATE" == "running" ]]; then
            echo "  ✓ ${svc}: running"
        else
            echo "  ✗ ${svc}: ${STATE}"
            UNHEALTHY=$((UNHEALTHY + 1))
        fi
    done
else
    echo "  (docker compose not available here — skipping container check)"
fi
echo ""

# 2. Port probes (TCP)
echo "--- Endpoints ---"
probe() {  # probe <label> <host> <port>
    if timeout 3 bash -c ">/dev/tcp/$2/$3" 2>/dev/null; then
        echo "  ✓ $1 ($2:$3) reachable"
    else
        echo "  ✗ $1 ($2:$3) unreachable"
        UNHEALTHY=$((UNHEALTHY + 1))
    fi
}
probe "nexus SQL" "$PEERDB_HOST" "$PEERDB_PORT"
probe "flow-api HTTP" "$(echo "$FLOW_API_HTTP" | sed -E 's#https?://##; s#:.*##')" "$(echo "$FLOW_API_HTTP" | sed -E 's#.*:##')"
echo ""

# 3. Mirror listing via nexus
echo "--- Mirror (nexus) ---"
if command -v psql >/dev/null 2>&1; then
    if MIRRORS=$(PGPASSWORD="$PEERDB_PASSWORD" psql \
            "host=${PEERDB_HOST} port=${PEERDB_PORT} user=${PEERDB_USER} dbname=peerdb" \
            -tAc "SELECT name FROM mirrors;" 2>/dev/null); then
        if echo "$MIRRORS" | grep -qw "cce_analytics_mirror"; then
            echo "  ✓ cce_analytics_mirror present"
        else
            echo "  ✗ cce_analytics_mirror not found (mirrors: ${MIRRORS:-none})"
            UNHEALTHY=$((UNHEALTHY + 1))
        fi
    else
        echo "  (could not query nexus 'mirrors' view — check PeerDB version / nexus up)"
    fi
else
    echo "  (psql not installed — skipping nexus query)"
fi

echo ""
echo "Detailed lag/rows-synced: PeerDB UI http://localhost:3000  ·  Temporal UI http://localhost:8085"
echo ""
if [[ $UNHEALTHY -eq 0 ]]; then
    echo "=== STACK HEALTHY ==="
    exit 0
else
    echo "=== ${UNHEALTHY} CHECK(S) FAILED ==="
    exit 1
fi
