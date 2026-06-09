#!/usr/bin/env bash
# Create the PeerDB CDC mirror for the CCE analytics pipeline.
#
# Uses the PeerDB nexus SQL interface (port 9900) and applies the CREATE MIRROR
# statement from connectors/peerdb-mirror.sql.
#
# Prerequisites (run in this order):
#   1. ClickHouse tables pre-created:   clickhouse-client ... < schema/01-create-tables.sql
#   2. PostgreSQL logical replication:  psql ... -f cdc/01-configure-replication.sql
#   3. PeerDB peers created:            ./scripts/create-peers.sh
#      (creates 'ccedb_peer' and 'clickhouse_peer')
#
# The ClickHouse database 'cce_analytics' is created automatically by the
# CLICKHOUSE_DB env var in docker-compose.yml (dev) or manually in production.
#
# Usage: ./scripts/register-connectors.sh

set -euo pipefail

PEERDB_HOST="${PEERDB_HOST:-localhost}"
PEERDB_PORT="${PEERDB_PORT:-9900}"
PEERDB_USER="${PEERDB_USER:-peerdb}"
PEERDB_PASSWORD="${PEERDB_PASSWORD:-peerdb}"

MIRROR_SQL="connectors/peerdb-mirror.sql"

echo "=== Creating PeerDB CDC Mirror (via nexus SQL @ ${PEERDB_HOST}:${PEERDB_PORT}) ==="
echo "Applying ${MIRROR_SQL}..."
echo ""

if [[ ! -f "$MIRROR_SQL" ]]; then
    echo "✗ ${MIRROR_SQL} not found — run from the repo root."
    exit 1
fi

OUT=$(PGPASSWORD="$PEERDB_PASSWORD" psql \
        "host=${PEERDB_HOST} port=${PEERDB_PORT} user=${PEERDB_USER} dbname=peerdb" \
        -v ON_ERROR_STOP=1 -f "$MIRROR_SQL" 2>&1) && RC=0 || RC=$?

echo "$OUT" | sed 's/^/  /'

if [[ $RC -eq 0 ]]; then
    echo ""
    echo "✓ Mirror cce_analytics_mirror created — initial snapshot starting"
elif echo "$OUT" | grep -qi "already exists"; then
    echo ""
    echo "  Mirror already exists — skipping creation"
else
    echo ""
    echo "✗ Failed to create mirror (exit ${RC})"
    exit 1
fi

echo ""
echo "=== Mirror Creation Complete ==="
echo "Monitor:  ./scripts/check-connector-health.sh"
echo "          PeerDB UI  http://localhost:3000   |   Temporal UI  http://localhost:8085"
echo "After the initial snapshot completes, create MVs/indexes/dictionaries:"
echo "  clickhouse-client --database cce_analytics --multiquery < schema/02-create-materialized-views.sql"
echo "  clickhouse-client --database cce_analytics --multiquery < schema/03-create-indexes-projections.sql"
echo "  clickhouse-client --database cce_analytics --multiquery < schema/04-create-dictionary.sql"
