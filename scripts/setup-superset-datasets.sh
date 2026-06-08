#!/usr/bin/env bash
# Register all ClickHouse datasets in Superset and configure RLS
# Usage: ./scripts/setup-superset-datasets.sh [superset-url] [admin-user] [admin-password]

set -euo pipefail

SUPERSET_URL="${1:-http://localhost:8088}"
ADMIN_USER="${2:-admin}"
ADMIN_PASS="${3:-admin}"

echo "=== Setting up Superset Datasets ==="

# Get CSRF token and login
LOGIN_RESPONSE=$(curl -sf -X POST "${SUPERSET_URL}/api/v1/security/login" \
    -H "Content-Type: application/json" \
    -d "{\"username\":\"${ADMIN_USER}\",\"password\":\"${ADMIN_PASS}\",\"provider\":\"db\"}")
ACCESS_TOKEN=$(echo "$LOGIN_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")
AUTH_HEADER="Authorization: Bearer ${ACCESS_TOKEN}"

echo "✓ Authenticated"

# Get or create ClickHouse database connection
echo "Creating ClickHouse database connection..."
DB_PAYLOAD='{
  "database_name": "CCE ClickHouse",
  "sqlalchemy_uri": "clickhousedb://cce_pipeline:cce_analytics_dev@clickhouse:8123/cce_analytics",
  "expose_in_sqllab": true,
  "allow_run_async": true,
  "extra": "{\"engine_params\": {\"connect_args\": {\"connect_timeout\": 30}}}"
}'
DB_RESPONSE=$(curl -sf -X POST "${SUPERSET_URL}/api/v1/database/" \
    -H "${AUTH_HEADER}" -H "Content-Type: application/json" \
    -d "$DB_PAYLOAD" 2>/dev/null || echo '{"id": 1}')
DB_ID=$(echo "$DB_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('id', 1))")
echo "  Database ID: ${DB_ID}"

# Register all datasets — base tables + all MVs
DATASETS=(
    # CDC base tables
    "protocol_instances"
    "step_instances"
    "deviations"
    "inbound_event_logs"
    "intelligence_deliveries"
    "intelligence_event_logs"
    "action_definitions"
    "protocol_definitions"
    "receiver_adaptors"
    "destination_adaptor_mappings"
    "compliance_event_logs"
    # Event volume
    "mv_event_volume_hourly"
    # Compliance
    "mv_compliance_summary"
    "mv_compliance_by_patient"
    "mv_compliance_processing_quality"
    # Deviations
    "mv_deviation_trends"
    "mv_deviation_by_protocol"
    "mv_deviation_by_patient"
    # Ingestion quality
    "mv_ingestion_quality"
    # Intelligence
    "mv_intelligence_summary"
    "mv_intelligence_by_patient"
    "mv_intelligence_by_protocol"
    # Denormalized current-state views
    "mv_step_current"
    "mv_delivery_current"
    # Practitioners / Facilities
    "mv_practitioner_summary"
    "mv_facility_summary"
)

echo ""
echo "--- Registering Datasets (${#DATASETS[@]} total) ---"
for dataset in "${DATASETS[@]}"; do
    PAYLOAD="{\"database\": ${DB_ID}, \"schema\": \"\", \"table_name\": \"${dataset}\"}"
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
        "${SUPERSET_URL}/api/v1/dataset/" \
        -H "${AUTH_HEADER}" -H "Content-Type: application/json" \
        -d "$PAYLOAD")
    if [[ "$HTTP_CODE" == "201" || "$HTTP_CODE" == "422" ]]; then
        echo "  ✓ ${dataset}"
    else
        echo "  ✗ ${dataset} (HTTP ${HTTP_CODE})"
    fi
done

# Configure Row-Level Security via Superset Admin UI
# Dataset table IDs are auto-assigned on each deployment — do NOT hardcode them.
# Instead, apply RLS via the Superset Admin UI:
#   Security → Row Level Security → + Rule
#   Name:        Facility-based data isolation
#   Filter Type: Regular
#   Tables:      inbound_event_logs, protocol_instances, deviations,
#                mv_event_volume_hourly, mv_compliance_summary, mv_facility_summary
#   Roles:       Gamma
#   Clause:      facility_id = '{{ current_user().extra_attributes.facility_id }}'
echo ""
echo "--- Row-Level Security ---"
echo "  RLS must be configured via the Superset Admin UI (dataset IDs are dynamic)."
echo "  Security → Row Level Security → + Rule"
echo "  Tables:  inbound_event_logs, protocol_instances, deviations, facility MVs"
echo "  Roles:   Gamma"
echo "  Clause:  facility_id = '{{ current_user().extra_attributes.facility_id }}'"

echo ""
echo "=== Dataset Setup Complete ==="
