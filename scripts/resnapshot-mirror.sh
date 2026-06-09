#!/usr/bin/env bash
# PeerDB Mirror Re-snapshot
# Drops and recreates the mirror via the PeerDB nexus SQL interface (port 9900) to
# force a full re-snapshot.
# Use when: replication slot was dropped, ClickHouse data is corrupted/truncated,
# or schema diverged beyond what ALTER TABLE can fix.
#
# Usage: ./scripts/resnapshot-mirror.sh
#
# WARNING: This re-snapshots all CDC tables from PostgreSQL. The initial snapshot
# may take minutes to hours depending on data volume.

set -euo pipefail

PEERDB_HOST="${PEERDB_HOST:-localhost}"
PEERDB_PORT="${PEERDB_PORT:-9900}"
PEERDB_USER="${PEERDB_USER:-peerdb}"
PEERDB_PASSWORD="${PEERDB_PASSWORD:-peerdb}"
MIRROR_NAME="cce_analytics_mirror"

nexus() {  # nexus <sql>
    PGPASSWORD="$PEERDB_PASSWORD" psql \
        "host=${PEERDB_HOST} port=${PEERDB_PORT} user=${PEERDB_USER} dbname=peerdb" \
        -v ON_ERROR_STOP=1 -c "$1"
}

echo "=== PeerDB Mirror Re-snapshot ==="
echo "Nexus:  ${PEERDB_HOST}:${PEERDB_PORT}"
echo "Mirror: ${MIRROR_NAME}"
echo ""
echo "WARNING: This will DROP and RECREATE the mirror and run a full initial"
echo "         snapshot from PostgreSQL."
echo ""
read -p "Continue? (y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 0
fi
echo ""

# Step 1: Drop the existing mirror (tolerate "does not exist")
echo "Step 1: Dropping mirror..."
if OUT=$(nexus "DROP MIRROR IF EXISTS ${MIRROR_NAME};" 2>&1); then
    echo "  ✓ Mirror dropped (or did not exist)"
else
    echo "$OUT" | sed 's/^/    /'
    echo "  ✗ Failed to drop mirror"
    exit 1
fi
sleep 2

# Step 2: Truncate MV backing tables to avoid double-counting on re-snapshot.
# The re-snapshot re-inserts every row, which re-fires the MV triggers; the mv_*
# backing tables still hold their pre-resync aggregates and would double-count.
echo "Step 2: Truncating MV backing tables (prevents double-count)..."
CH_HOST="${CH_HOST:-localhost}"
CH_USER="${CH_USER:-cce_pipeline}"
CH_PASS="${CH_PASSWORD:-${CLICKHOUSE_PASSWORD:-cce_analytics_dev}}"
if command -v clickhouse-client >/dev/null 2>&1; then
    MVS=$(clickhouse-client --host "$CH_HOST" --user "$CH_USER" --password "$CH_PASS" -q \
        "SELECT name FROM system.tables WHERE database='cce_analytics' AND name LIKE 'mv_%' AND engine NOT LIKE '%View%'" 2>/dev/null || true)
    for t in $MVS; do
        clickhouse-client --host "$CH_HOST" --user "$CH_USER" --password "$CH_PASS" \
            -q "TRUNCATE TABLE cce_analytics.${t}" 2>/dev/null && echo "    ✓ truncated ${t}"
    done
else
    echo "  (clickhouse-client not found — truncate mv_* backing tables manually before the snapshot lands)"
fi

# Step 3: Recreate the mirror (triggers full snapshot)
echo "Step 3: Recreating mirror (do_initial_snapshot=true)..."
./scripts/register-connectors.sh

echo ""
echo "=== Re-snapshot initiated ==="
echo "Monitor: ./scripts/check-connector-health.sh"
echo "See deployment-guide.md → 'Recreating / Backfilling an MV Safely'."
