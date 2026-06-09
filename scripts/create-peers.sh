#!/usr/bin/env bash
# Create the PeerDB peers required by the CCE analytics mirror.
#
# Uses the PeerDB nexus SQL interface (PostgreSQL wire protocol, port 9900) — the
# stable, documented way to manage peers/mirrors. Creates:
#   - ccedb_peer       (PostgreSQL source, the existing CCE ccedb)
#   - clickhouse_peer  (ClickHouse destination, cce_analytics)
#
# Usage: ./scripts/create-peers.sh
#
# Configuration is read from the environment (see .env.example):
#   Nexus:                      PEERDB_HOST PEERDB_PORT PEERDB_USER PEERDB_PASSWORD
#   Source PostgreSQL (ccedb):  PG_HOST PG_PORT PG_DATABASE CDC_USER CDC_PASSWORD
#   Destination ClickHouse:     CH_HOST CH_NATIVE_PORT CH_DATABASE CH_USER CH_PASSWORD
#
# NOTE: the ClickHouse peer uses the NATIVE protocol port (9000), not HTTP (8123).
# S3/MinIO staging credentials come from the flow services' env (PEERDB_CLICKHOUSE_AWS_*
# in docker-compose.yml), so they are not repeated in the peer definition.

set -euo pipefail

# --- PeerDB nexus ---
PEERDB_HOST="${PEERDB_HOST:-localhost}"
PEERDB_PORT="${PEERDB_PORT:-9900}"
PEERDB_USER="${PEERDB_USER:-peerdb}"
PEERDB_PASSWORD="${PEERDB_PASSWORD:-peerdb}"

# --- Source: PostgreSQL (existing CCE ccedb) ---
PG_HOST="${PG_HOST:-cce-postgres}"
PG_PORT="${PG_PORT:-5432}"
PG_DATABASE="${PG_DATABASE:-ccedb}"
CDC_USER="${CDC_USER:-cce_cdc_user}"
CDC_PASSWORD="${CDC_PASSWORD:-CHANGE_ME_IN_PRODUCTION}"

# --- Destination: ClickHouse (cce_analytics) ---
CH_HOST="${CH_HOST:-clickhouse}"
CH_NATIVE_PORT="${CH_NATIVE_PORT:-9000}"
CH_DATABASE="${CH_DATABASE:-cce_analytics}"
CH_USER="${CH_USER:-cce_pipeline}"
CH_PASSWORD="${CH_PASSWORD:-${CLICKHOUSE_PASSWORD:-cce_analytics_dev}}"

echo "=== Creating PeerDB Peers (via nexus SQL @ ${PEERDB_HOST}:${PEERDB_PORT}) ==="
echo "  ccedb_peer       → postgres://${CDC_USER}@${PG_HOST}:${PG_PORT}/${PG_DATABASE}"
echo "  clickhouse_peer  → clickhouse://${CH_USER}@${CH_HOST}:${CH_NATIVE_PORT}/${CH_DATABASE}"
echo ""

# nexus_sql <sql> — run a statement against the PeerDB nexus, tolerating
# "already exists" so the script is idempotent.
nexus_sql() {
    local sql="$1"
    local out
    if ! out=$(PGPASSWORD="$PEERDB_PASSWORD" psql \
            "host=${PEERDB_HOST} port=${PEERDB_PORT} user=${PEERDB_USER} dbname=peerdb" \
            -v ON_ERROR_STOP=1 -c "$sql" 2>&1); then
        if echo "$out" | grep -qi "already exists"; then
            echo "  (already exists — skipping)"
            return 0
        fi
        echo "  ✗ nexus error:"
        echo "$out" | sed 's/^/    /'
        return 1
    fi
    echo "$out" | sed 's/^/    /'
}

echo "Creating ccedb_peer..."
nexus_sql "CREATE PEER ccedb_peer FROM POSTGRES WITH (
    host = '${PG_HOST}',
    port = '${PG_PORT}',
    user = '${CDC_USER}',
    password = '${CDC_PASSWORD}',
    database = '${PG_DATABASE}'
);"

echo ""
echo "Creating clickhouse_peer..."
# disable_tls = true: ClickHouse native port 9000 is non-TLS in this stack.
nexus_sql "CREATE PEER clickhouse_peer FROM CLICKHOUSE WITH (
    host = '${CH_HOST}',
    port = '${CH_NATIVE_PORT}',
    user = '${CH_USER}',
    password = '${CH_PASSWORD}',
    database = '${CH_DATABASE}',
    disable_tls = true
);"

echo ""
echo "=== Peer Creation Complete ==="
echo "Verify:  PGPASSWORD=\$PEERDB_PASSWORD psql \"host=${PEERDB_HOST} port=${PEERDB_PORT} user=${PEERDB_USER} dbname=peerdb\" -c 'SELECT * FROM peers;'"
echo "Next:    ./scripts/register-connectors.sh"
