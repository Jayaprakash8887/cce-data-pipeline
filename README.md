# CCE Data Pipeline

Open-source analytics pipeline for the CCE (Clinical Care Engine) platform. Replaces the custom `cce-insights-service` and `cce-insights-ui` with a CDC-only architecture: **PostgreSQL → PeerDB → ClickHouse → Superset**.

## Architecture

```
PostgreSQL (ccedb) → PeerDB (logical replication) → ClickHouse (MVs) → Apache Superset
```

**No Kafka. No custom application code.** PeerDB replicates directly from PostgreSQL WAL. All enrichment is handled by ClickHouse MATERIALIZED columns and Materialized Views.

## Components

| Component | Version | Purpose |
|-----------|---------|---------|
| ClickHouse | 24.8 LTS | Columnar OLAP analytics store |
| PeerDB | latest (OSS) | CDC from PostgreSQL (WAL-based, direct to ClickHouse) |
| Apache Superset | 4.0.2 | Interactive dashboards & reports |
| Grafana | 11.x | Pipeline health monitoring |
| Prometheus | 2.53 | Metrics collection |

## Quick Start (Local Development)

```bash
# Start all services
docker compose up -d

# Wait for PeerDB to be ready
until curl -sf http://localhost:3000; do sleep 5; done

# Create PeerDB mirror (replicates PostgreSQL → ClickHouse)
psql "host=localhost port=9900 dbname=peerdb" < connectors/peerdb-mirror.sql

# Wait for initial snapshot, then apply schema customizations
clickhouse-client --database cce_analytics --multiquery < schema/01-create-tables.sql
clickhouse-client --database cce_analytics --multiquery < schema/02-create-materialized-views.sql
clickhouse-client --database cce_analytics --multiquery < schema/03-create-indexes-projections.sql
clickhouse-client --database cce_analytics --multiquery < schema/04-create-dictionary.sql
```

## CDC Tables

All data flows via Change Data Capture from committed PostgreSQL records (shared `ccedb` database). **11 tables** captured from 3 services (Collector, Compliance, Intelligence) → ClickHouse `cce_analytics` database.

For full table listing and schema details, see [Data Flow & Schema Design](docs/data-flow.md).

## Materialized Views

**22 pre-aggregated views** computed at insert time, covering event volume, compliance, deviations, intelligence, delivery, step states, and processing quality — with full Entity × Behavior cross-dimensional coverage (patient, facility, practitioner, protocol dimensions).

For the complete MV catalog and coverage matrix, see [Data Flow & Schema Design § 4](docs/data-flow.md).

## Documentation

| Document | Purpose |
|----------|---------|
| [Architecture Overview](docs/architecture-overview.md) | System context, principles, technology decisions, capacity planning, security |
| [Data Flow & Schema](docs/data-flow.md) | CDC pipeline, ClickHouse DDL, MV catalog, query patterns |
| [Dashboard Design](docs/dashboard-design.md) | 11 dashboard wireframes with SQL |
| [Deployment Guide](docs/deployment-guide.md) | Full lifecycle: setup, deploy, validate, operate, troubleshoot |

## Key Design Decisions

1. **CDC-only** — Analytics based solely on committed database records. No in-flight event consumption.
2. **PeerDB (not Debezium + Kafka)** — Direct WAL replication to ClickHouse. Simpler operations, fewer components, native TOAST support.
3. **No Flink/stream processing** — ClickHouse MATERIALIZED columns extract fields from `raw_payload` JSON at insert time. Materialized Views pre-aggregate. Zero custom code.
4. **ReplacingMergeTree** — All CDC tables use `_peerdb_version` for idempotent upserts.
5. **AggregatingMergeTree** — MVs with `uniq()`, `quantile()`, `any()` use `-State`/`-Merge` combinators for correct incremental aggregation.

## Scripts

| Script | Purpose |
|--------|---------|
| `scripts/validate-clickhouse.sh` | Validate all tables and MVs exist |
| `scripts/data-quality-checks.sh` | Row counts, freshness, integrity checks |
| `scripts/setup-superset-datasets.sh` | Register datasets in Superset |