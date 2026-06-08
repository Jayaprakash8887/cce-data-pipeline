# CCE Data Pipeline — Deployment Guide

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Infrastructure Components](#2-infrastructure-components)
3. [Docker Compose (Development)](#3-docker-compose-development)
4. [Kubernetes (Production)](#4-kubernetes-production)
5. [Bootstrap Order](#5-bootstrap-order)
6. [PeerDB Mirror Setup](#6-peerdb-mirror-setup)
7. [Schema Deployment](#7-schema-deployment)
8. [Post-Deployment Validation](#8-post-deployment-validation)
9. [Monitoring & Observability](#9-monitoring--observability)
10. [Backfill & Replay](#10-backfill--replay)
11. [Rollback Procedures](#11-rollback-procedures)
12. [Operational Procedures](#12-operational-procedures)
13. [Troubleshooting](#13-troubleshooting)

---

## 1. Prerequisites

### Pre-deployment Checklist
- [ ] All secrets provisioned in secret store
- [ ] PostgreSQL `wal_level = logical` confirmed
- [ ] PostgreSQL `REPLICA IDENTITY FULL` set on all 11 CDC tables
- [ ] PostgreSQL replication slot created: `cce_analytics_slot`
- [ ] PostgreSQL publication created: `cce_analytics_pub`
- [ ] ClickHouse database `cce_analytics` created
- [ ] ClickHouse user `cce_pipeline` created with appropriate grants
- [ ] PeerDB deployed (OSS Docker or managed)
- [ ] MinIO/S3 configured for PeerDB staging
- [ ] Network connectivity verified between all services
- [ ] DNS entries configured for Superset/Grafana
- [ ] Docker + Docker Compose v2 (development) or Kubernetes 1.28+ (production)

### PostgreSQL Configuration
```sql
-- Enable logical replication (requires restart)
ALTER SYSTEM SET wal_level = 'logical';
ALTER SYSTEM SET max_replication_slots = 4;
ALTER SYSTEM SET max_slot_wal_keep_size = '10GB';

-- Create CDC user with replication permissions
CREATE USER cce_cdc_user WITH REPLICATION PASSWORD '***';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cce_cdc_user;
GRANT CREATE ON DATABASE ccedb TO cce_cdc_user;  -- PeerDB needs to create publication

-- Set REPLICA IDENTITY FULL on all CDC tables (required for TOAST columns)
ALTER TABLE inbound_event_log REPLICA IDENTITY FULL;
ALTER TABLE protocol_definition REPLICA IDENTITY FULL;
ALTER TABLE protocol_instance REPLICA IDENTITY FULL;
ALTER TABLE step_instance REPLICA IDENTITY FULL;
ALTER TABLE deviation REPLICA IDENTITY FULL;
ALTER TABLE intelligence_event_log REPLICA IDENTITY FULL;
ALTER TABLE intelligence_delivery REPLICA IDENTITY FULL;
ALTER TABLE action_definition REPLICA IDENTITY FULL;
ALTER TABLE compliance_event_log REPLICA IDENTITY FULL;
ALTER TABLE receiver_adaptor REPLICA IDENTITY FULL;
ALTER TABLE destination_adaptor_mapping REPLICA IDENTITY FULL;

-- Create publication for all CDC tables
CREATE PUBLICATION cce_analytics_pub FOR TABLE
    inbound_event_log,
    protocol_definition,
    protocol_instance,
    step_instance,
    deviation,
    intelligence_event_log,
    intelligence_delivery,
    action_definition,
    compliance_event_log,
    receiver_adaptor,
    destination_adaptor_mapping;
```

### Secrets

| Secret | Purpose | Required By |
|--------|---------|-------------|
| `CLICKHOUSE_PASSWORD` | ClickHouse pipeline user | peerdb, superset |
| `CDC_PASSWORD` | PostgreSQL replication user | peerdb |
| `SUPERSET_SECRET_KEY` | Session encryption | superset |
| `SUPERSET_DB_PASSWORD` | Metadata DB | superset |
| `GRAFANA_PASSWORD` | Admin password | grafana |
| `KEYCLOAK_CLIENT_SECRET` | Superset OAuth2 | superset |

---

## 2. Infrastructure Components

### 2.1 ClickHouse

Single-node deployment with `ReplacingMergeTree` tables (auto-created by PeerDB).

- **Database:** `cce_analytics`
- **User:** `cce_pipeline`
- **Ports:** 8123 (HTTP), 9000 (Native), 9363 (Prometheus metrics)
- **Storage:** SSD recommended

For resource sizing (dev vs prod), see [Architecture Overview § 8.3](architecture-overview.md#83-resource-requirements).

### 2.2 PeerDB

PeerDB replicates PostgreSQL WAL directly to ClickHouse. Deployed as Docker containers.

- **Components:** PeerDB server + Temporal (workflow engine) + PostgreSQL (metadata)
- **Staging:** MinIO (local) or S3 (production) for intermediary data transfer
- **Port:** 3000 (PeerDB UI)

### 2.3 Apache Superset

- **Port:** 8088
- **Dependencies:** Redis (cache), PostgreSQL (metadata DB)
- **ClickHouse connection:** `clickhousedb://cce_pipeline:***@clickhouse:8123/cce_analytics`
- **OAuth2/OIDC:** Keycloak integration for production

### 2.4 Prometheus + Grafana

```yaml
scrape_configs:
  - job_name: 'clickhouse'
    static_configs:
      - targets: ['clickhouse:9363']
```

---

## 3. Docker Compose (Development)

```bash
# Start all services
docker compose up -d

# Check health
docker compose ps

# View logs
docker compose logs -f peerdb-server
```

**Services started:** `clickhouse`, `peerdb-server`, `peerdb-temporal`, `peerdb-catalog`, `minio`, `redis`, `superset-db`, `superset`, `prometheus`, `grafana`

**Volumes:** `clickhouse-data`, `superset-db-data`, `prometheus-data`, `grafana-data`, `minio-data`

### Build Custom Images
```bash
docker build -t cce-superset:latest -f docker/Dockerfile.superset ./docker
```

---

## 4. Kubernetes (Production)

### Namespace Layout
```
cce-analytics/
├── clickhouse (StatefulSet, 1 replica)
├── peerdb (Deployment, 1 replica + Temporal sidecar)
├── minio (StatefulSet, 1 replica) or S3
├── superset-web (Deployment, 2 replicas)
├── superset-worker (Deployment, 2 replicas)
├── superset-db (StatefulSet or managed RDS)
├── redis (Deployment, 1 replica)
├── prometheus (StatefulSet, 1 replica)
└── grafana (Deployment, 1 replica)
```

### Key K8s Resources
- **PersistentVolumeClaims:** ClickHouse data (500 GB), Prometheus TSDB (50 GB)
- **ConfigMaps:** Prometheus config, Grafana dashboards, ClickHouse server config
- **Secrets:** All passwords, connection strings
- **Services:** ClusterIP for internal; LoadBalancer/Ingress for Superset, Grafana

---

## 5. Bootstrap Order

```mermaid
flowchart TD
    A["1. ClickHouse + MinIO"] --> B["2. PeerDB (create mirror)"]
    B --> C["3. Wait for initial snapshot"]
    C --> D["4. Add MATERIALIZED columns (schema/01)"]
    D --> E["5. Create MVs + Indexes (schema/02-04)"]
    E --> F["6. Backfill MVs with snapshot data"]
    F --> G["7. Redis + Superset DB"]
    G --> H["8. Superset (init + start)"]
    H --> I["9. Prometheus + Grafana"]
```

---

## 6. PeerDB Mirror Setup

### Create PostgreSQL and ClickHouse Peers (via PeerDB UI or CLI)

```bash
# Access PeerDB UI at http://localhost:3000
# Create peers:
#   1. PostgreSQL peer: host=cce-postgres, port=5432, db=ccedb, user=cce_cdc_user
#   2. ClickHouse peer: host=clickhouse, port=9000, db=cce_analytics, user=cce_pipeline
```

### Create Mirror

Apply the mirror configuration:
```bash
# Via PeerDB SQL interface or UI
psql "host=localhost port=9900 dbname=peerdb" < connectors/peerdb-mirror.sql
```

### Verify Mirror Running
```bash
# Check mirror status via PeerDB UI at http://localhost:3000
# Or query PeerDB catalog:
psql "host=localhost port=9900 dbname=peerdb" \
  -c "SELECT mirror_name, mirror_state FROM peerdb.mirrors;"
```

### Monitor Initial Snapshot Progress
```bash
# Check rows synced per table
psql "host=localhost port=9900 dbname=peerdb" \
  -c "SELECT table_name, rows_synced FROM peerdb.mirror_stats WHERE mirror_name='cce_analytics_mirror';"
```

---

## 7. Schema Deployment

Run AFTER PeerDB initial snapshot completes (tables must exist):

```bash
# Apply in order (all scripts are idempotent)
CH_HOST=${CH_HOST:-localhost}
CH_USER=${CH_USER:-cce_pipeline}
CH_PASS=${CLICKHOUSE_PASSWORD:-cce_analytics_dev}

# Step 1: Add MATERIALIZED columns to PeerDB-created tables
clickhouse-client --host "$CH_HOST" --user "$CH_USER" --password "$CH_PASS" \
  --database cce_analytics --multiquery < schema/01-create-tables.sql

# Step 2: Create Materialized Views
clickhouse-client --host "$CH_HOST" --user "$CH_USER" --password "$CH_PASS" \
  --database cce_analytics --multiquery < schema/02-create-materialized-views.sql

# Step 3: Create indexes and projections
clickhouse-client --host "$CH_HOST" --user "$CH_USER" --password "$CH_PASS" \
  --database cce_analytics --multiquery < schema/03-create-indexes-projections.sql

# Step 4: Create dictionaries
clickhouse-client --host "$CH_HOST" --user "$CH_USER" --password "$CH_PASS" \
  --database cce_analytics --multiquery < schema/04-create-dictionary.sql
```

### Validate Schema
```bash
./scripts/validate-clickhouse.sh
```

---

## 9. Post-Deployment Validation

### Automated
```bash
./scripts/validate-clickhouse.sh
./scripts/data-quality-checks.sh
./tests/e2e/run-e2e-tests.sh
```

### Manual Checks

| Check | Command | Expected |
|-------|---------|----------|
| ClickHouse alive | `curl -s http://localhost:8123/ping` | `Ok.` |
| Tables exist | `clickhouse-client -q "SELECT count() FROM system.tables WHERE database='cce_analytics'"` | `>= 11` |
| MVs exist | `clickhouse-client -q "SELECT count() FROM system.tables WHERE database='cce_analytics' AND engine LIKE '%View%'"` | `>= 22` |
| Data flowing | `clickhouse-client -q "SELECT name, total_rows FROM system.tables WHERE database='cce_analytics' AND total_rows > 0"` | Tables with rows |
| PeerDB mirror | `psql "host=localhost port=9900 dbname=peerdb" -c "SELECT mirror_state FROM peerdb.mirrors"` | `active` |
| Superset | `curl -s http://localhost:8088/health` | `OK` |
| Grafana | `curl -s http://localhost:3000/api/health` | `{"database":"ok"}` |

---

## 10. Monitoring & Observability

### Prometheus Metrics

| Metric | Source | Alert Threshold |
|--------|--------|-----------------|
| `clickhouse_insert_rows` | ClickHouse | Rate drop > 50% |
| `clickhouse_merge_tree_parts_count` | ClickHouse | `> 300` per table |
| `clickhouse_query_duration_ms` | ClickHouse | p99 > 5000ms |
| `pg_replication_slots_active` | PostgreSQL | `= 0` (slot inactive) |
| `pg_wal_lsn_diff_bytes` | PostgreSQL | `> 1 GB` |

### PostgreSQL WAL / Replication Slot Monitoring

PeerDB uses a logical replication slot (`cce_analytics_slot`) in PostgreSQL. If the slot becomes inactive or WAL accumulates beyond safe limits, CDC will stall and disk may fill.

**Key queries:**

```sql
-- Check slot is active
SELECT slot_name, active, restart_lsn, confirmed_flush_lsn
FROM pg_replication_slots WHERE slot_name = 'cce_analytics_slot';

-- WAL lag in bytes
SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS lag_bytes
FROM pg_replication_slots WHERE slot_name = 'cce_analytics_slot';
```

**Required PostgreSQL setting:**

```
max_slot_wal_keep_size = 10GB   -- prevents unbounded WAL growth if PeerDB is down
```

> **Recovery**: If the slot is inactive and WAL exceeds the limit, PostgreSQL will invalidate the slot. PeerDB will fail to resume and requires a full re-snapshot via mirror resync. Monitor the `pg_wal_lsn_diff_bytes` metric and alert at 1 GB.

### Alerts

Configured in `infra/grafana/provisioning/alerting/alerts.yaml`:

| Alert | Condition | Severity |
|-------|-----------|----------|
| PeerDB Mirror Stalled | No new rows synced for 5min | Critical |
| CDC Source Slot Inactive | `pg_replication_slots.active = false` for 1min | Critical |
| WAL Lag Excessive | `pg_wal_lsn_diff > 1 GB` for 5min | Warning |
| ClickHouse Insert Stall | Zero inserts for 5min | Warning |
| ClickHouse Disk Usage | > 80% | Warning |

### Alerting Contacts

Configure in Grafana → Alerting → Contact Points:
- **Slack**: `#cce-pipeline-alerts` channel webhook
- **PagerDuty**: Critical alerts (mirror stalled, disk full)
- **Email**: `cce-ops@organization.com`

---

## 10. Backfill & Replay

### Full Re-snapshot

If ClickHouse data is lost or needs full refresh:

```bash
# 1. Drop the mirror
psql "host=localhost port=9900 dbname=peerdb" \
  -c "DROP MIRROR cce_analytics_mirror;"

# 2. Truncate ClickHouse tables
clickhouse-client -q "TRUNCATE TABLE cce_analytics.inbound_event_logs"
# (repeat for other tables or drop/recreate database)

# 3. Recreate mirror (triggers full initial snapshot)
psql "host=localhost port=9900 dbname=peerdb" < connectors/peerdb-mirror.sql

# 4. Wait for snapshot to complete, then re-apply schema customizations
clickhouse-client --database cce_analytics --multiquery < schema/01-create-tables.sql
```

### Partial Table Resync

PeerDB supports resyncing individual tables without dropping the entire mirror:

```bash
# Resync a specific table via PeerDB UI
# Navigate to Mirrors → cce_analytics_mirror → Resync Table → select table
```

---

## 11. Rollback Procedures

### PeerDB Mirror Rollback
```bash
# Pause mirror (stop CDC without dropping state)
# Via PeerDB UI: Mirrors → cce_analytics_mirror → Pause

# Drop and recreate with previous config if needed
psql "host=localhost port=9900 dbname=peerdb" \
  -c "DROP MIRROR cce_analytics_mirror;"
psql "host=localhost port=9900 dbname=peerdb" < connectors/peerdb-mirror.sql
```

### ClickHouse Schema Rollback
```bash
# Non-destructive: ALTER TABLE for column additions
# Destructive: Restore from backup
clickhouse-client --host $CH_HOST --query \
  "RESTORE DATABASE cce_analytics FROM Disk('backups', 'latest/')"
```

### Full Rollback
```bash
docker compose down
git checkout LAST_GOOD_TAG
docker compose up -d
```

---

## 14. Operational Procedures

### ClickHouse Maintenance
```bash
# Check table sizes
clickhouse-client --query "
  SELECT table, formatReadableSize(sum(bytes_on_disk)) as size, sum(rows) as rows
  FROM system.parts
  WHERE database = 'cce_analytics' AND active
  GROUP BY table ORDER BY sum(bytes_on_disk) DESC"

# Optimize tables (merge parts)
clickhouse-client --query "OPTIMIZE TABLE cce_analytics.inbound_event_logs FINAL"

# Check merge health
clickhouse-client --query "
  SELECT table, count() as parts
  FROM system.parts WHERE database='cce_analytics' AND active
  GROUP BY table HAVING parts > 100 ORDER BY parts DESC"
```

### PeerDB Mirror Management
```bash
# Pause mirror (via PeerDB UI or SQL)
# UI: Mirrors → cce_analytics_mirror → Pause

# Resume mirror
# UI: Mirrors → cce_analytics_mirror → Resume

# Restart PeerDB server
docker compose restart peerdb-server
```

### Materialized View Backfill

When a new MV is created, it only captures data inserted **after** creation. To populate with historical data:

```bash
# 1. Identify the target MV and its source table
clickhouse-client --query "SHOW CREATE TABLE cce_analytics.mv_step_current"

# 2. Insert historical data using the MV's SELECT query against the source table
clickhouse-client --query "
  INSERT INTO cce_analytics.mv_step_current
  SELECT
      id,
      _peerdb_version,
      protocol_instance_id,
      action_id,
      state,
      completion_status,
      created_at,
      updated_at,
      completed_at
  FROM cce_analytics.step_instances"
```

**General pattern:**
```sql
-- INSERT INTO <mv_target_table> SELECT <mv_select_query> FROM <source_table>
-- Copy the SELECT from the MV definition and run as an INSERT INTO the MV's target table
INSERT INTO cce_analytics.<mv_target_table>
SELECT <columns_from_mv_definition>
FROM cce_analytics.<source_table>
WHERE <source_conditions>;
```

> **Note**: For `AggregatingMergeTree` MVs using `-State` functions, the backfill SELECT must use the same `-State` aggregate functions (e.g., `countState()`, `uniqState()`) — not their plain counterparts.

### Scaling Guidance

| Component | Scaling Strategy |
|-----------|-----------------|
| ClickHouse | Add replicas (ReplicatedMergeTree), shard for > 1TB/day |
| PeerDB | Increase `snapshot_max_parallel_workers`; use dedicated instance |
| Superset | Add Celery workers for async queries |

---

## 13. Troubleshooting

### PeerDB Mirror Not Starting
```bash
# Check mirror status
psql "host=localhost port=9900 dbname=peerdb" \
  -c "SELECT mirror_name, mirror_state, error FROM peerdb.mirrors;"

# Check PeerDB logs
docker compose logs peerdb-server | grep ERROR

# Common fixes:
# - PostgreSQL: verify wal_level=logical, replication slot exists, REPLICA IDENTITY FULL
# - ClickHouse: verify database/user exists, network connectivity
# - MinIO: verify bucket exists and is accessible
```

### Data Not Appearing in ClickHouse
```bash
# Check PeerDB sync stats
psql "host=localhost port=9900 dbname=peerdb" \
  -c "SELECT table_name, rows_synced, last_synced_at FROM peerdb.mirror_stats WHERE mirror_name='cce_analytics_mirror';"

# Check ClickHouse insert errors
clickhouse-client -q "SELECT * FROM system.query_log WHERE type='ExceptionWhileProcessing' ORDER BY event_time DESC LIMIT 5"

# Verify table has data
clickhouse-client -q "SELECT count() FROM cce_analytics.inbound_event_logs"
```

### Materialized Views Not Populating
```bash
# MVs trigger on INSERT to source table — check source table has data
clickhouse-client -q "SELECT count() FROM cce_analytics.inbound_event_logs"

# Check MV target table
clickhouse-client -q "SELECT count() FROM cce_analytics.mv_event_volume_hourly"

# If source has data but MV doesn't, the MV may have been created AFTER data was inserted
# Solution: recreate MV or backfill manually
```

### High ClickHouse Merge Pressure
```bash
# Check parts count
clickhouse-client -q "
  SELECT table, count() as parts, sum(rows) as total_rows
  FROM system.parts WHERE database='cce_analytics' AND active
  GROUP BY table ORDER BY parts DESC"

# Force optimize if needed
clickhouse-client -q "OPTIMIZE TABLE cce_analytics.inbound_event_logs FINAL"
```
