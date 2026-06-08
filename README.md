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

# Step 1 — Pre-create ClickHouse tables (MUST run before PeerDB mirror)
# Uses ReplacingMergeTree(_peerdb_version, _peerdb_is_deleted) + clean_deleted_rows='Always'
# Requires ClickHouse 23.2+
clickhouse-client --database cce_analytics --multiquery < schema/01-create-tables.sql

# Step 2 — Create PeerDB mirror (uses existing tables; does not recreate them)
psql "host=localhost port=9900 dbname=peerdb" < connectors/peerdb-mirror.sql

# Step 3 — Wait for initial snapshot, then create MVs, indexes, and dictionaries
clickhouse-client --database cce_analytics --multiquery < schema/02-create-materialized-views.sql
clickhouse-client --database cce_analytics --multiquery < schema/03-create-indexes-projections.sql
clickhouse-client --database cce_analytics --multiquery < schema/04-create-dictionary.sql
```

## CDC Tables

All data flows via Change Data Capture from committed PostgreSQL records (shared `ccedb` database). **11 tables** captured from 3 services (Collector, Compliance, Intelligence) → ClickHouse `cce_analytics` database.

For full table listing and schema details, see [Data Flow & Schema Design](docs/data-flow.md).

## Materialized Views

**14 pre-aggregated views** computed at insert time, covering event volume, compliance, deviations, intelligence, and processing quality — with full Entity × Behavior cross-dimensional coverage (patient, facility, practitioner, protocol dimensions). Step and delivery current-state queries run directly against `step_instances FINAL` and `intelligence_deliveries FINAL` (base tables with `ReplacingMergeTree`).

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
4. **ReplacingMergeTree (two-parameter form)** — All CDC tables are pre-created with `ReplacingMergeTree(_peerdb_version, _peerdb_is_deleted)` + `SETTINGS clean_deleted_rows = 'Always'` (ClickHouse 23.2+). PeerDB writes into existing tables. Deleted rows are physically purged during background merges.
5. **AggregatingMergeTree / SummingMergeTree** — MVs on append-only tables (event logs, deviations) use `-State`/`-Merge` combinators for correct incremental aggregation. Mutable entities (step_instances, intelligence_deliveries, protocol_instances) are queried directly via `FINAL` — not via MVs — to avoid double-counting CDC UPDATE events.
6. **Two ClickHouse user profiles** — `analytics` (readonly, `final=1` applied automatically) for Superset/analysts; `peerdb_writer` (write access, no FINAL overhead) for the PeerDB CDC writer.

## Scripts

| Script | Purpose |
|--------|---------|
| `scripts/validate-clickhouse.sh` | Validate all tables and MVs exist |
| `scripts/data-quality-checks.sh` | Row counts, freshness, integrity checks |
| `scripts/setup-superset-datasets.sh` | Register datasets in Superset |