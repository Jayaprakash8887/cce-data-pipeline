# CCE Data Pipeline — Deployment Guide

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Kafka Topics](#2-kafka-topics)
3. [Infrastructure Components](#3-infrastructure-components)
4. [Docker Compose (Development)](#4-docker-compose-development)
5. [Kubernetes (Production)](#5-kubernetes-production)
6. [Bootstrap Order](#6-bootstrap-order)
7. [Connector Setup](#7-connector-setup)
8. [Schema Deployment](#8-schema-deployment)
9. [Post-Deployment Validation](#9-post-deployment-validation)
10. [Monitoring & Observability](#10-monitoring--observability)
11. [Dead Letter Queue (DLQ)](#11-dead-letter-queue-dlq)
12. [Backfill & Replay](#12-backfill--replay)
13. [Rollback Procedures](#13-rollback-procedures)
14. [Operational Procedures](#14-operational-procedures)
15. [Troubleshooting](#15-troubleshooting)

---

## 1. Prerequisites

### Pre-deployment Checklist
- [ ] All secrets provisioned in secret store
- [ ] PostgreSQL `wal_level = logical` confirmed
- [ ] PostgreSQL replication slot created: `cce_analytics_slot`
- [ ] PostgreSQL publication created: `cce_analytics_pub`
- [ ] Kafka CDC topics pre-created (or auto-create enabled)
- [ ] ClickHouse user `cce_pipeline` created with appropriate grants
- [ ] Network connectivity verified between all services
- [ ] DNS entries configured for Superset/Grafana
- [ ] Docker + Docker Compose v2 (development) or Kubernetes 1.28+ (production)
- [ ] External: Apache Kafka 3.6+ (existing CCE cluster)

### PostgreSQL Configuration
```sql
-- Enable logical replication (requires restart)
ALTER SYSTEM SET wal_level = 'logical';

-- Create CDC user with replication permissions
CREATE USER cce_cdc_user WITH REPLICATION PASSWORD '***';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cce_cdc_user;

-- Create replication slot
SELECT pg_create_logical_replication_slot('cce_analytics_slot', 'pgoutput');

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
| `CLICKHOUSE_PASSWORD` | ClickHouse pipeline user | kafka-connect, superset |
| `CDC_PASSWORD` | PostgreSQL replication user | kafka-connect |
| `KAFKA_SASL_PASSWORD` | Kafka client auth | kafka-connect |
| `SUPERSET_SECRET_KEY` | Session encryption | superset |
| `SUPERSET_DB_PASSWORD` | Metadata DB | superset |
| `GRAFANA_PASSWORD` | Admin password | grafana |
| `KEYCLOAK_CLIENT_SECRET` | Superset OAuth2 | superset |

---

## 2. Kafka Topics

Pre-create CDC topics for partition control (Debezium will auto-create otherwise):

```bash
KAFKA_BOOTSTRAP=${KAFKA_BOOTSTRAP:-localhost:9092}

# High-volume tables (6 partitions)
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP --create --topic cce.cdc.public.inbound_event_log --partitions 6 --replication-factor 3
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP --create --topic cce.cdc.public.protocol_instance --partitions 6 --replication-factor 3
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP --create --topic cce.cdc.public.step_instance --partitions 6 --replication-factor 3
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP --create --topic cce.cdc.public.deviation --partitions 6 --replication-factor 3
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP --create --topic cce.cdc.public.intelligence_event_log --partitions 6 --replication-factor 3
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP --create --topic cce.cdc.public.intelligence_delivery --partitions 6 --replication-factor 3
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP --create --topic cce.cdc.public.compliance_event_log --partitions 6 --replication-factor 3

# Low-volume reference tables (3 partitions)
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP --create --topic cce.cdc.public.protocol_definition --partitions 3 --replication-factor 3
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP --create --topic cce.cdc.public.action_definition --partitions 3 --replication-factor 3
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP --create --topic cce.cdc.public.receiver_adaptor --partitions 3 --replication-factor 3
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP --create --topic cce.cdc.public.destination_adaptor_mapping --partitions 3 --replication-factor 3
```

---

## 3. Infrastructure Components

### 3.1 ClickHouse

Single-node deployment with `ReplacingMergeTree` tables for CDC.

- **Database:** `cce_analytics`
- **User:** `cce_pipeline`
- **Ports:** 8123 (HTTP), 9000 (Native), 9363 (Prometheus metrics)
- **Storage:** SSD recommended

For resource sizing (dev vs prod), see [Architecture Overview § 8.3](architecture-overview.md#83-resource-requirements).

### 3.2 Kafka Connect

Custom image with both connectors:
- `io.debezium:debezium-connector-postgresql:2.6.1` (source)
- `com.clickhouse:clickhouse-kafka-connect:0.14.0` (sink)

**Configuration:**
- Workers: 1 (dev), 2 (prod, HA)
- Port: 8083 (REST API)
- `offset.storage.topic`: `cce-connect-offsets`
- `config.storage.topic`: `cce-connect-config`
- `status.storage.topic`: `cce-connect-status`

### 3.3 Apache Superset

- **Port:** 8088
- **Dependencies:** Redis (cache), PostgreSQL (metadata DB)
- **ClickHouse connection:** `clickhousedb://cce_pipeline:***@clickhouse:8123/cce_analytics`
- **OAuth2/OIDC:** Keycloak integration for production

### 3.4 Prometheus + Grafana

```yaml
scrape_configs:
  - job_name: 'kafka-connect'
    static_configs:
      - targets: ['kafka-connect:8083']
  - job_name: 'clickhouse'
    static_configs:
      - targets: ['clickhouse:9363']
```

---

## 4. Docker Compose (Development)

```bash
# Start all services
docker compose up -d

# Check health
docker compose ps

# View logs
docker compose logs -f kafka-connect
```

**Services started:** `clickhouse`, `kafka-connect`, `redis`, `superset-db`, `superset`, `prometheus`, `grafana`

**Volumes:** `clickhouse-data`, `superset-db-data`, `prometheus-data`, `grafana-data`

### Build Custom Images
```bash
docker build -t cce-kafka-connect:latest -f docker/Dockerfile.kafka-connect ./docker
docker build -t cce-superset:latest -f docker/Dockerfile.superset ./docker
```

---

## 5. Kubernetes (Production)

### Namespace Layout
```
cce-analytics/
├── clickhouse (StatefulSet, 1 replica)
├── kafka-connect (Deployment, 2 replicas)
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

## 6. Bootstrap Order

```mermaid
flowchart TD
    A["1. ClickHouse"] --> B["2. Apply Schema DDL"]
    B --> C["3. Kafka Connect"]
    C --> D["4. Register Connectors"]
    D --> E["5. Verify CDC flowing"]
    E --> F["6. Redis + Superset DB"]
    F --> G["7. Superset (init + start)"]
    G --> H["8. Prometheus + Grafana"]
```

---

## 7. Connector Setup

### Register Source Connector (Debezium)
```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/cce-cdc-source.json
```

### Register Sink Connector (ClickHouse)
```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/cce-clickhouse-sink.json
```

### Verify Both Running
```bash
curl -s http://localhost:8083/connectors/cce-cdc-source/status | jq '.connector.state'
# "RUNNING"

curl -s http://localhost:8083/connectors/cce-clickhouse-sink/status | jq '.connector.state'
# "RUNNING"
```

---

## 8. Schema Deployment

```bash
# Apply in order (all scripts use IF NOT EXISTS — idempotent)
CH_HOST=${CH_HOST:-localhost}
CH_USER=${CH_USER:-cce_pipeline}
CH_PASS=${CLICKHOUSE_PASSWORD:-cce_analytics_dev}

clickhouse-client --host "$CH_HOST" --user "$CH_USER" --password "$CH_PASS" \
  --database cce_analytics --multiquery < schema/01-create-tables.sql

clickhouse-client --host "$CH_HOST" --user "$CH_USER" --password "$CH_PASS" \
  --database cce_analytics --multiquery < schema/02-create-materialized-views.sql

clickhouse-client --host "$CH_HOST" --user "$CH_USER" --password "$CH_PASS" \
  --database cce_analytics --multiquery < schema/03-create-indexes-projections.sql

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
| MVs exist | `clickhouse-client -q "SELECT count() FROM system.tables WHERE database='cce_analytics' AND engine LIKE '%View%'"` | `>= 19` |
| Data flowing | `clickhouse-client -q "SELECT name, total_rows FROM system.tables WHERE database='cce_analytics' AND total_rows > 0"` | Tables with rows |
| Connectors | `curl -s http://localhost:8083/connectors` | `["cce-cdc-source","cce-clickhouse-sink"]` |
| Source status | `curl localhost:8083/connectors/cce-cdc-source/status \| jq '.connector.state'` | `RUNNING` |
| Sink status | `curl localhost:8083/connectors/cce-clickhouse-sink/status \| jq '.connector.state'` | `RUNNING` |
| Superset | `curl -s http://localhost:8088/health` | `OK` |
| Grafana | `curl -s http://localhost:3000/api/health` | `{"database":"ok"}` |

---

## 10. Monitoring & Observability

### Prometheus Metrics

| Metric | Source | Alert Threshold |
|--------|--------|-----------------|
| `kafka_connect_connector_status` | Kafka Connect | `!= RUNNING` |
| `kafka_consumer_lag` | Kafka Connect | `> 10000` |
| `clickhouse_insert_rows` | ClickHouse | Rate drop > 50% |
| `clickhouse_merge_tree_parts_count` | ClickHouse | `> 300` per table |
| `clickhouse_query_duration_ms` | ClickHouse | p99 > 5000ms |

### Alerts

Configured in `infra/grafana/provisioning/alerting/alerts.yaml`:

| Alert | Condition | Severity |
|-------|-----------|----------|
| CDC Sink Connector Down | Status != RUNNING for 2min | Critical |
| ClickHouse Insert Stall | Zero inserts for 5min | Warning |
| High Consumer Lag | Lag > 10000 for 5min | Warning |
| ClickHouse Disk Usage | > 80% | Warning |

### Alerting Contacts

Configure in Grafana → Alerting → Contact Points:
- **Slack**: `#cce-pipeline-alerts` channel webhook
- **PagerDuty**: Critical alerts (connector down, disk full)
- **Email**: `cce-ops@organization.com`

---

## 11. Dead Letter Queue (DLQ)

**DLQ topic:** `cce.clickhouse-sink.dlq`

### Monitor DLQ
```bash
kafka-console-consumer.sh --bootstrap-server $KAFKA_BOOTSTRAP \
  --topic cce.clickhouse-sink.dlq --from-beginning --max-messages 5
```

### Replay DLQ
```bash
./scripts/replay-dlq.sh cce.clickhouse-sink.dlq $KAFKA_BOOTSTRAP
```

### Common DLQ Causes
| Cause | Fix |
|-------|-----|
| Schema mismatch | Update ClickHouse DDL to match new source columns |
| Type conversion error | Check Debezium `transforms` configuration |
| ClickHouse full | Expand storage, run TTL cleanup |

---

## 12. Backfill & Replay

### Full Re-snapshot (CDC)

If ClickHouse data is lost or needs full refresh:

```bash
# 1. Stop sink connector
curl -X PUT http://localhost:8083/connectors/cce-clickhouse-sink/pause

# 2. Truncate ClickHouse tables
clickhouse-client -q "TRUNCATE TABLE cce_analytics.inbound_event_logs"
# (repeat for other tables)

# 3. Delete and recreate source connector (triggers snapshot)
curl -X DELETE http://localhost:8083/connectors/cce-cdc-source
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/cce-cdc-source.json

# 4. Resume sink connector
curl -X PUT http://localhost:8083/connectors/cce-clickhouse-sink/resume

# 5. Monitor progress
watch -n 5 'curl -s http://localhost:8083/connectors/cce-cdc-source/status | jq ".tasks[].state"'
```

### Partial Replay (Single Topic)

Reset the sink consumer offset for a specific table:

```bash
# Stop sink
curl -X PUT http://localhost:8083/connectors/cce-clickhouse-sink/pause

# Reset offset for specific topic
kafka-consumer-groups.sh --bootstrap-server $KAFKA_BOOTSTRAP \
  --group connect-cce-clickhouse-sink \
  --topic cce.cdc.public.protocol_instance \
  --reset-offsets --to-earliest --execute

# Resume sink
curl -X PUT http://localhost:8083/connectors/cce-clickhouse-sink/resume
```

---

## 13. Rollback Procedures

### Kafka Connect Rollback
```bash
# Pause connectors
curl -X PUT http://localhost:8083/connectors/cce-cdc-source/pause
curl -X PUT http://localhost:8083/connectors/cce-clickhouse-sink/pause

# Delete and recreate with previous config
curl -X DELETE http://localhost:8083/connectors/cce-cdc-source
curl -X DELETE http://localhost:8083/connectors/cce-clickhouse-sink

# Restore previous connector configs
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/cce-cdc-source.PREVIOUS.json
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/cce-clickhouse-sink.PREVIOUS.json
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

### Connector Restart
```bash
# Restart a failed task
curl -X POST http://localhost:8083/connectors/cce-clickhouse-sink/tasks/0/restart

# Full connector restart
curl -X POST http://localhost:8083/connectors/cce-clickhouse-sink/restart
```

### Scaling Guidance

| Component | Scaling Strategy |
|-----------|-----------------|
| ClickHouse | Add replicas (ReplicatedMergeTree), shard for > 1TB/day |
| Kafka Connect | Increase `tasks.max` in connector config; add workers |
| Superset | Add Celery workers for async queries |

---

## 15. Troubleshooting

### Connector Not Starting
```bash
# Check connector status
curl -s http://localhost:8083/connectors/cce-cdc-source/status | jq '.'

# Check worker logs
docker compose logs kafka-connect | grep ERROR

# Common fixes:
# - PostgreSQL: verify wal_level=logical, replication slot exists
# - ClickHouse: verify database/user exists, network connectivity
```

### Data Not Appearing in ClickHouse
```bash
# Check connector lag
curl -s http://localhost:8083/connectors/cce-clickhouse-sink/status | jq '.tasks[]'

# Verify data in Kafka topic
kafka-console-consumer.sh --bootstrap-server $KAFKA_BOOTSTRAP \
  --topic cce.cdc.public.protocol_instance \
  --from-beginning --max-messages 1

# Check ClickHouse insert errors
clickhouse-client -q "SELECT * FROM system.query_log WHERE type='ExceptionWhileProcessing' ORDER BY event_time DESC LIMIT 5"
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
