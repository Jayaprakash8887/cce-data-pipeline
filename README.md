# CCE Data Pipeline

Open-source analytics pipeline for the CCE (Clinical Care Engine) platform. Replaces the custom `cce-insights-service` and `cce-insights-ui` with a CDC-only architecture: **PostgreSQL → Debezium → ClickHouse → Superset**.

## Architecture

```
PostgreSQL (CCE DBs) → Debezium CDC → Kafka → ClickHouse Sink → ClickHouse (MVs) → Apache Superset
```

**No custom application code.** All enrichment is handled by ClickHouse MATERIALIZED columns and Materialized Views.

## Components

| Component | Version | Purpose |
|-----------|---------|---------|
| ClickHouse | 24.8 | Columnar OLAP analytics store |
| Debezium | 2.6.1 | CDC from PostgreSQL (WAL-based) |
| ClickHouse Kafka Connect Sink | 0.14.0 | CDC topics → ClickHouse tables |
| Apache Superset | 4.0.2 | Interactive dashboards & reports |
| Grafana | 11.0 | Pipeline health monitoring |
| Prometheus | 2.53 | Metrics collection |

## Quick Start (Local Development)

```bash
# Start all services
docker compose up -d

# Wait for Kafka Connect to be ready
until curl -sf http://localhost:8083/connectors; do sleep 5; done

# Register CDC connectors
./scripts/register-connectors.sh

# Validate schema
./scripts/validate-clickhouse.sh
```

## CDC Tables

All data flows via Change Data Capture from committed PostgreSQL records:

| Source Table | ClickHouse Table | Source DB |
|--------------|------------------|-----------|
| `inbound_event_log` | `inbound_event_logs` | collector-service |
| `protocol_definition` | `protocol_definitions` | compliance-service |
| `protocol_instance` | `protocol_instances` | compliance-service |
| `step_instance` | `step_instances` | compliance-service |
| `deviation` | `deviations` | compliance-service |
| `intelligence_event_log` | `intelligence_event_logs` | compliance-service |
| `intelligence_delivery` | `intelligence_deliveries` | compliance-service |
| `action_definition` | `action_definitions` | compliance-service |
| `compliance_event_log` | `compliance_event_logs` | compliance-service |
| `receiver_adaptor` | `receiver_adaptors` | collector-service |
| `destination_adaptor_mapping` | `destination_adaptor_mappings` | collector-service |

## Materialized Views

Pre-aggregated analytics computed at insert time:

| View | Source Table | Purpose |
|------|-------------|---------|
| `mv_event_volume_hourly` | `inbound_event_logs` | Event counts by facility/source/type |
| `mv_event_volume_daily` | `inbound_event_logs` | Daily rollup |
| `mv_facility_summary` | `inbound_event_logs` | Facility-level metrics |
| `mv_practitioner_summary` | `inbound_event_logs` | Practitioner activity |
| `mv_compliance_summary` | `protocol_instances` | Protocol compliance rates |
| `mv_deviation_trends` | `deviations` | Daily deviation counts |
| `mv_deviation_by_protocol` | `deviations` | Deviations per protocol |
| `mv_ingestion_quality` | `inbound_event_logs` | Source quality metrics |
| `mv_intelligence_summary` | `intelligence_event_logs` | Intelligence trigger aggregation |
| `mv_delivery_performance_hourly` | `intelligence_deliveries` | Delivery latency & success |
| `mv_step_states_daily` | `step_instances` | Step state distribution |

## Documentation

| Document | Purpose |
|----------|---------|
| [Architecture Overview](docs/architecture-overview.md) | System context, principles, data domains |
| [Technology Stack](docs/technology-stack.md) | Technology choices with justification |
| [Data Flow & Schema](docs/data-flow.md) | CDC pipelines, ClickHouse DDL, query patterns |
| [Dashboard Design](docs/dashboard-design.md) | 11 dashboard wireframes with SQL |
| [Deployment Guide](docs/deployment-guide.md) | K8s manifests, Docker Compose, setup |
| [Deployment Runbook](docs/deployment-runbook.md) | Step-by-step production deployment |

## Key Design Decisions

1. **CDC-only (no Kafka topic consumption)** — Analytics based solely on committed database records. Avoids discrepancies from in-flight events that may be rejected or reprocessed.
2. **No Flink/stream processing** — ClickHouse MATERIALIZED columns extract fields from `raw_payload` JSON at insert time. Materialized Views pre-aggregate. Zero custom code.
3. **ReplacingMergeTree** — All CDC tables use `_version` column for idempotent upserts via Debezium.
4. **AggregatingMergeTree** — MVs with `uniq()`, `quantile()`, `any()` use `-State`/`-Merge` combinators for correct incremental aggregation.

## Scripts

| Script | Purpose |
|--------|---------|
| `scripts/register-connectors.sh` | Register Debezium source + ClickHouse sink |
| `scripts/validate-clickhouse.sh` | Validate all tables and MVs exist |
| `scripts/data-quality-checks.sh` | Row counts, freshness, integrity checks |
| `scripts/check-connector-health.sh` | Monitor connector status |
| `scripts/setup-superset-datasets.sh` | Register datasets in Superset |
| `scripts/replay-dlq.sh` | Replay dead letter queue messages |
| `scripts/validate-cdc-config.sh` | Validate CDC connector configuration |