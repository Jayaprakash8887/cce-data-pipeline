# CCE Data Pipeline

Open-source **data pipeline** for the CCE (Clinical Care Engine) platform: a CDC-only path that lands committed PostgreSQL data into ClickHouse for analytics — **PostgreSQL → PeerDB → ClickHouse**. The `cce-insights-service` + `cce-insights-ui` apps consume ClickHouse to serve dashboards.

## Architecture

```
PostgreSQL (ccedb) → PeerDB (logical replication) → ClickHouse (MVs)
                                                          ↓
                                  cce-insights-service / cce-insights-ui (separate repos)
```

**No Kafka. No custom application code in the pipeline.** PeerDB replicates directly from PostgreSQL WAL. All enrichment is handled by ClickHouse MATERIALIZED columns and Materialized Views. The presentation layer (dashboards/UI) is **not** in this repo — it lives in `cce-insights-service` / `cce-insights-ui`, which query ClickHouse directly.

## Components

| Component | Version | Purpose |
|-----------|---------|---------|
| ClickHouse | 26.3 LTS | Columnar OLAP analytics store (serving layer for insights-service) |
| PeerDB | stable-v0.36.26 (OSS) | CDC from PostgreSQL (WAL-based, direct to ClickHouse). Full stack: nexus + flow-api + workers + temporal + MinIO |
| Grafana | 11.x | Pipeline health monitoring (host port **3001**) |
| Prometheus | 2.53 | Metrics collection |

`docker compose up` runs ClickHouse **and** the full PeerDB OSS stack
(catalog, temporal, flow-api, flow-snapshot-worker, flow-worker, nexus, peerdb-ui, minio),
vendored under `infra/peerdb/` pinned to `stable-v0.36.26`, plus Prometheus/Grafana for
pipeline monitoring. PeerDB UI is on **:3000**, Temporal UI on **:8085**, the nexus SQL
interface on **:9900**. Dashboards/UI run separately (insights-service/ui).

## Quick Start (Local Development)

> **Note:** `ccedb` (the CCE source PostgreSQL) is **not** part of this stack — it is the
> existing operational DB. Set `PG_HOST`/`CDC_USER`/`CDC_PASSWORD` etc. in `.env` to point at it.

```bash
cp .env.example .env   # then edit: PG_*/CDC_* → your ccedb, MINIO_ROOT_PASSWORD, etc.
set -a; source .env; set +a

# Start everything (analytics + full PeerDB stack)
docker compose up -d

# Wait for the PeerDB nexus SQL interface to accept connections (port 9900)
until PGPASSWORD="$PEERDB_PASSWORD" psql "host=localhost port=9900 user=peerdb dbname=peerdb" -c '\q' 2>/dev/null; do sleep 5; done

# Step 1 — Pre-create ClickHouse tables (MUST exist before the mirror starts)
# ReplacingMergeTree(_peerdb_version, _peerdb_is_deleted) + clean_deleted_rows='Always' (ClickHouse 23.2+)
# In dev these auto-run via docker-entrypoint-initdb.d; run manually for prod/re-runs.
clickhouse-client --database cce_analytics --multiquery < schema/01-create-tables.sql

# Step 2 — Configure logical replication on the source ccedb (run against your ccedb host)
psql -h "$PG_HOST" -U postgres -d ccedb -f cdc/01-configure-replication.sql

# Step 3 — Create PeerDB peers, then the mirror (nexus SQL @ :9900; scripts read .env)
./scripts/create-peers.sh
./scripts/register-connectors.sh

# Step 4 — Wait for initial snapshot, then create MVs, indexes, and dictionaries
./scripts/check-connector-health.sh
clickhouse-client --database cce_analytics --multiquery < schema/02-create-materialized-views.sql
clickhouse-client --database cce_analytics --multiquery < schema/03-create-indexes.sql
clickhouse-client --database cce_analytics --multiquery < schema/04-create-dictionary.sql
```

**Web UIs:** PeerDB http://localhost:3000 · Temporal http://localhost:8085 · Grafana http://localhost:3001 · MinIO console http://localhost:9002

Once the snapshot completes and MVs exist, point `cce-insights-service` at ClickHouse
(`http://localhost:8123` or native `9000`, user `cce_pipeline`) to serve dashboards.

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
| [Query Reference](docs/query-reference/) | Per-domain ClickHouse SQL for `cce-insights-service` to reuse |
| [Deployment Guide](docs/deployment-guide.md) | Full lifecycle: setup, deploy, validate, operate, troubleshoot |

## Key Design Decisions

1. **CDC-only** — Analytics based solely on committed database records. No in-flight event consumption.
2. **PeerDB (not Debezium + Kafka)** — Direct WAL replication to ClickHouse. Simpler operations, fewer components, native TOAST support.
3. **No Flink/stream processing** — ClickHouse MATERIALIZED columns extract fields from `raw_payload` JSON at insert time. Materialized Views pre-aggregate. Zero custom code.
4. **ReplacingMergeTree (two-parameter form)** — All CDC tables are pre-created with `ReplacingMergeTree(_peerdb_version, _peerdb_is_deleted)` + `SETTINGS clean_deleted_rows = 'Always'` (ClickHouse 23.2+). PeerDB writes into existing tables. Deleted rows are physically purged during background merges.
5. **AggregatingMergeTree / SummingMergeTree** — MVs on append-only tables (event logs, deviations) use `-State`/`-Merge` combinators for correct incremental aggregation. Mutable entities (step_instances, intelligence_deliveries, protocol_instances) are queried directly via `FINAL` — not via MVs — to avoid double-counting CDC UPDATE events.
6. **Two ClickHouse user profiles** — `analytics` (readonly, `final=1` applied automatically) for `cce-insights-service`/analysts; `peerdb_writer` (write access, no FINAL overhead) for the PeerDB CDC writer.

## Scripts

| Script | Purpose |
|--------|---------|
| `scripts/create-peers.sh` | Create PeerDB peers (`ccedb_peer`, `clickhouse_peer`) via the nexus |
| `scripts/register-connectors.sh` | Create the `cce_analytics_mirror` CDC mirror |
| `scripts/check-connector-health.sh` | PeerDB stack + mirror health |
| `scripts/validate-clickhouse.sh` | Validate all tables and MVs exist |
| `scripts/data-quality-checks.sh` | Row counts, freshness, integrity checks |
| `scripts/validate-cdc-config.sh` | Validate PostgreSQL logical-replication config |
| `scripts/replay-dlq.sh` | Drop + re-snapshot the mirror |