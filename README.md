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

All data flows via Change Data Capture from committed PostgreSQL records (shared `ccedb` database). **11 tables** captured from 3 services (Collector, Compliance, Intelligence) → ClickHouse `cce_analytics` database.

For full table listing, CDC topics, and schema details, see [Data Flow & Schema Design](docs/data-flow.md).

## Materialized Views

**19 pre-aggregated views** computed at insert time, covering event volume, compliance, deviations, intelligence, delivery, and step states — with full Entity × Behavior cross-dimensional coverage (patient, facility, practitioner, protocol dimensions).

For the complete MV catalog and coverage matrix, see [Data Flow & Schema Design § 4](docs/data-flow.md).

## Documentation

| Document | Purpose |
|----------|---------|
| [Architecture Overview](docs/architecture-overview.md) | System context, principles, technology decisions, capacity planning, security |
| [Data Flow & Schema](docs/data-flow.md) | CDC pipelines, ClickHouse DDL, MV catalog, query patterns |
| [Dashboard Design](docs/dashboard-design.md) | 11 dashboard wireframes with SQL |
| [Deployment Guide](docs/deployment-guide.md) | Full lifecycle: setup, deploy, validate, operate, troubleshoot |

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