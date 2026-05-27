#!/usr/bin/env bash
# Validate PostgreSQL CDC configuration
# Usage: ./scripts/validate-cdc-config.sh [host] [port] [user] [dbname]

set -euo pipefail

PG_HOST="${1:-localhost}"
PG_PORT="${2:-5432}"
PG_USER="${3:-postgres}"
PG_DB="${4:-ccedb}"

echo "=== PostgreSQL CDC Configuration Validation ==="
echo "Target: ${PG_HOST}:${PG_PORT}/${PG_DB}"
echo ""

# Check wal_level
WAL_LEVEL=$(psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -tAc "SHOW wal_level;")
if [[ "$WAL_LEVEL" == "logical" ]]; then
    echo "✓ wal_level = logical"
else
    echo "✗ wal_level = ${WAL_LEVEL} (expected: logical) — RESTART REQUIRED after ALTER SYSTEM"
fi

# Check CDC user exists
USER_EXISTS=$(psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -tAc "SELECT 1 FROM pg_roles WHERE rolname = 'cce_cdc_user';")
if [[ "$USER_EXISTS" == "1" ]]; then
    echo "✓ Role cce_cdc_user exists"
else
    echo "✗ Role cce_cdc_user does not exist"
fi

# Check publication exists
PUB_EXISTS=$(psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -tAc "SELECT 1 FROM pg_publication WHERE pubname = 'cce_analytics_pub';")
if [[ "$PUB_EXISTS" == "1" ]]; then
    echo "✓ Publication cce_analytics_pub exists"
else
    echo "✗ Publication cce_analytics_pub does not exist"
fi

# Check publication tables
echo ""
echo "--- Publication Tables ---"
EXPECTED_TABLES=(
    "protocol_definition"
    "protocol_instance"
    "step_instance"
    "deviation"
    "inbound_event"
    "intelligence_delivery"
    "intelligence_event_log"
    "action_definition"
    "receiver_adaptor"
    "destination_adaptor_mapping"
)

PUB_TABLES=$(psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -tAc \
    "SELECT tablename FROM pg_publication_tables WHERE pubname = 'cce_analytics_pub' ORDER BY tablename;")

MISSING=0
for table in "${EXPECTED_TABLES[@]}"; do
    if echo "$PUB_TABLES" | grep -q "^${table}$"; then
        echo "  ✓ ${table}"
    else
        echo "  ✗ ${table} NOT IN PUBLICATION"
        MISSING=$((MISSING + 1))
    fi
done

echo ""
TOTAL_TABLES=$(echo "$PUB_TABLES" | grep -c '.' || true)
echo "Total tables in publication: ${TOTAL_TABLES}/10"

if [[ $MISSING -eq 0 && "$WAL_LEVEL" == "logical" ]]; then
    echo ""
    echo "=== ALL CHECKS PASSED ==="
    exit 0
else
    echo ""
    echo "=== SOME CHECKS FAILED ==="
    exit 1
fi
