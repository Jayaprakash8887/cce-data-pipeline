# CCE Data Pipeline — Production Deployment Runbook

## Table of Contents
1. [Prerequisites](#prerequisites)
2. [Environment Preparation](#environment-preparation)
3. [Deployment Steps](#deployment-steps)
4. [Post-Deployment Validation](#post-deployment-validation)
5. [Rollback Procedures](#rollback-procedures)
6. [Operational Procedures](#operational-procedures)

---

## Prerequisites

### Infrastructure
- Kubernetes cluster (1.28+) or Docker Compose host with 16GB+ RAM
- PostgreSQL 15+ (source database with logical replication enabled)
- Apache Kafka 3.6+ (6 partitions minimum per topic)
- ClickHouse 24.8+ (dedicated instance, 8GB+ RAM, SSD storage)
- Redis 7+ (for Superset caching)

### Access & Credentials
| Secret | Purpose | Storage |
|--------|---------|---------|
| `CLICKHOUSE_PASSWORD` | ClickHouse pipeline user | Vault / K8s secret |
| `CDC_PASSWORD` | PostgreSQL CDC replication user | Vault / K8s secret |
| `KAFKA_SASL_PASSWORD` | Kafka client auth | Vault / K8s secret |
| `SUPERSET_SECRET_KEY` | Superset session encryption | Vault / K8s secret |
| `SUPERSET_DB_PASSWORD` | Superset metadata DB | Vault / K8s secret |
| `GRAFANA_PASSWORD` | Grafana admin | Vault / K8s secret |
| `KEYCLOAK_CLIENT_SECRET` | Superset OAuth2 | Vault / K8s secret |

### Pre-deployment Checklist
- [ ] All secrets provisioned in secret store
- [ ] PostgreSQL `wal_level = logical` confirmed
- [ ] PostgreSQL replication slot created: `cce_analytics_slot`
- [ ] PostgreSQL publication created: `cce_analytics_pub`
- [ ] Kafka topics created (see Topic List below)
- [ ] ClickHouse user `cce_pipeline` created with appropriate grants
- [ ] Network connectivity verified between all services
- [ ] DNS entries configured for Superset/Grafana

### Kafka Topics
```bash
# Create all required topics (adjust replication factor for production)
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP --create --topic cce.events.inbound --partitions 6 --replication-factor 3
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP --create --topic cce.intelligence.triggers --partitions 6 --replication-factor 3
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP --create --topic cce.scheduler.triggers --partitions 6 --replication-factor 3
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP --create --topic cce.events.inbound.dlq --partitions 3 --replication-factor 3
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP --create --topic cce.intelligence.triggers.dlq --partitions 3 --replication-factor 3

# CDC topics (auto-created by Debezium, but pre-create for partition control)
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP --create --topic cce.cdc.public.protocol_definition --partitions 3 --replication-factor 3
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP --create --topic cce.cdc.public.protocol_instance --partitions 6 --replication-factor 3
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP --create --topic cce.cdc.public.step_instance --partitions 6 --replication-factor 3
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP --create --topic cce.cdc.public.deviation --partitions 6 --replication-factor 3
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP --create --topic cce.cdc.public.inbound_event --partitions 6 --replication-factor 3
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP --create --topic cce.cdc.public.intelligence_delivery --partitions 6 --replication-factor 3
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP --create --topic cce.cdc.public.intelligence_event_log --partitions 6 --replication-factor 3
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP --create --topic cce.cdc.public.action_definition --partitions 3 --replication-factor 3
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP --create --topic cce.cdc.public.receiver_adaptor --partitions 3 --replication-factor 3
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP --create --topic cce.cdc.public.destination_adaptor_mapping --partitions 3 --replication-factor 3
```

---

## Environment Preparation

### 1. Deploy ClickHouse Schema
```bash
# Apply in order (idempotent — uses IF NOT EXISTS)
clickhouse-client --host $CH_HOST --user cce_pipeline --password $CLICKHOUSE_PASSWORD \
  --database cce_analytics --multiquery < schema/01-create-tables.sql

clickhouse-client --host $CH_HOST --user cce_pipeline --password $CLICKHOUSE_PASSWORD \
  --database cce_analytics --multiquery < schema/02-create-materialized-views.sql

clickhouse-client --host $CH_HOST --user cce_pipeline --password $CLICKHOUSE_PASSWORD \
  --database cce_analytics --multiquery < schema/03-create-indexes-projections.sql

clickhouse-client --host $CH_HOST --user cce_pipeline --password $CLICKHOUSE_PASSWORD \
  --database cce_analytics --multiquery < schema/04-create-dictionary.sql
```

### 2. Build Flink Jobs
```bash
cd flink-jobs
./gradlew clean shadowJar

# Artifacts produced:
# event-enrichment/build/libs/event-enrichment-1.0.0-all.jar
# event-volume-aggregator/build/libs/event-volume-aggregator-1.0.0-all.jar
# intelligence-tracker/build/libs/intelligence-tracker-1.0.0-all.jar
# scheduler-tracker/build/libs/scheduler-tracker-1.0.0-all.jar
# cdc-enrichment/build/libs/cdc-enrichment-1.0.0-all.jar
```

### 3. Build Custom Docker Images
```bash
docker build -t cce-kafka-connect:latest -f docker/Dockerfile.kafka-connect ./docker
docker build -t cce-superset:latest -f docker/Dockerfile.superset ./docker
```

---

## Deployment Steps

### Step 1: Start Infrastructure (if not using managed services)
```bash
docker compose up -d clickhouse redis superset-db prometheus
# Wait for healthy
docker compose ps  # All should show "healthy"
```

### Step 2: Deploy Kafka Connect
```bash
docker compose up -d kafka-connect
# Wait for REST API
until curl -sf http://localhost:8083/connectors; do sleep 5; done
```

### Step 3: Register Connectors
```bash
# Source connector (Debezium PostgreSQL CDC)
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/cce-cdc-source.json

# Sink connector (ClickHouse)
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/cce-clickhouse-sink.json

# Verify both RUNNING
curl -s http://localhost:8083/connectors/cce-cdc-source/status | jq '.connector.state'
curl -s http://localhost:8083/connectors/cce-clickhouse-sink/status | jq '.connector.state'
```

### Step 4: Deploy Flink Jobs
```bash
docker compose up -d flink-jobmanager flink-taskmanager

# Submit jobs (order matters: event-enrichment first)
FLINK_HOST=http://localhost:8081

flink run -d -m $FLINK_HOST \
  flink-jobs/event-enrichment/build/libs/event-enrichment-1.0.0-all.jar

flink run -d -m $FLINK_HOST \
  flink-jobs/event-volume-aggregator/build/libs/event-volume-aggregator-1.0.0-all.jar

flink run -d -m $FLINK_HOST \
  flink-jobs/intelligence-tracker/build/libs/intelligence-tracker-1.0.0-all.jar

flink run -d -m $FLINK_HOST \
  flink-jobs/scheduler-tracker/build/libs/scheduler-tracker-1.0.0-all.jar

flink run -d -m $FLINK_HOST \
  flink-jobs/cdc-enrichment/build/libs/cdc-enrichment-1.0.0-all.jar

# Verify all 5 jobs running
curl -s $FLINK_HOST/jobs/overview | jq '.jobs[] | {id, state}'
```

### Step 5: Deploy Visualization & Monitoring
```bash
docker compose up -d superset grafana

# Initialize Superset (first deploy only)
docker exec -it cce-superset superset fab create-admin \
  --username admin --firstname Admin --lastname User \
  --email admin@cce.org --password admin

docker exec -it cce-superset superset db upgrade
docker exec -it cce-superset superset init
```

---

## Post-Deployment Validation

### Automated Validation
```bash
# Run E2E test suite
./tests/e2e/run-e2e-tests.sh

# Run data quality checks
./scripts/data-quality-checks.sh
```

### Manual Checks
| Check | Command | Expected |
|-------|---------|----------|
| ClickHouse tables | `SELECT count() FROM system.tables WHERE database='cce_analytics'` | >= 14 |
| Flink jobs | `curl localhost:8081/jobs/overview \| jq '.jobs \| length'` | 5 |
| Kafka Connect | `curl localhost:8083/connectors` | 2 connectors |
| Consumer lag | Check Grafana dashboard | < 1000 per partition |
| Superset health | `curl localhost:8088/health` | `OK` |
| Grafana health | `curl localhost:3000/api/health` | `{"database":"ok"}` |

---

## Rollback Procedures

### Flink Job Rollback
```bash
# Cancel the problematic job
JOB_ID=$(curl -s $FLINK_HOST/jobs/overview | jq -r '.jobs[] | select(.name=="EventEnrichmentJob") | .jid')
curl -X PATCH "$FLINK_HOST/jobs/$JOB_ID?mode=cancel"

# Redeploy previous version from savepoint
flink run -d -m $FLINK_HOST \
  -s /opt/flink/savepoints/savepoint-$JOB_ID-* \
  flink-jobs/event-enrichment/build/libs/event-enrichment-PREVIOUS-all.jar
```

### Kafka Connect Rollback
```bash
# Pause connector
curl -X PUT http://localhost:8083/connectors/cce-cdc-source/pause

# Delete and recreate with previous config
curl -X DELETE http://localhost:8083/connectors/cce-cdc-source
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/cce-cdc-source.PREVIOUS.json
```

### ClickHouse Schema Rollback
```bash
# ClickHouse supports ALTER TABLE for non-destructive changes
# For destructive changes, restore from backup:
clickhouse-client --host $CH_HOST --query \
  "RESTORE DATABASE cce_analytics FROM Disk('backups', 'latest/')"
```

### Full Rollback
```bash
# Stop all pipeline services
docker compose down

# Restore from last known good state
git checkout LAST_GOOD_TAG
docker compose up -d
```

---

## Operational Procedures

### DLQ Replay
```bash
# View DLQ message count
kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list $KAFKA_BOOTSTRAP \
  --topic cce.events.inbound.dlq --time -1

# Replay messages back to source topic
./scripts/replay-dlq.sh cce.events.inbound.dlq $KAFKA_BOOTSTRAP
```

### Flink Savepoint (before upgrades)
```bash
JOB_ID="<job-id>"
curl -X POST "$FLINK_HOST/jobs/$JOB_ID/savepoints" \
  -H "Content-Type: application/json" \
  -d '{"cancel-job": false, "target-directory": "/opt/flink/savepoints"}'
```

### ClickHouse Maintenance
```bash
# Optimize tables (merge parts)
clickhouse-client --query "OPTIMIZE TABLE cce_analytics.events_fact FINAL"

# Check table sizes
clickhouse-client --query "
  SELECT table, formatReadableSize(sum(bytes_on_disk)) as size, sum(rows) as rows
  FROM system.parts
  WHERE database = 'cce_analytics' AND active
  GROUP BY table ORDER BY sum(bytes_on_disk) DESC"

# TTL / data retention (if configured)
clickhouse-client --query "
  ALTER TABLE cce_analytics.events_fact MODIFY TTL event_time + INTERVAL 2 YEAR"
```

### Scaling Guidance
| Component | Scaling Strategy |
|-----------|-----------------|
| Flink | Add TaskManagers, increase `taskmanager.numberOfTaskSlots` |
| ClickHouse | Add replicas (ReplicatedMergeTree), shard for > 1TB/day |
| Kafka Connect | Increase `tasks.max` in connector config |
| Kafka | Add partitions (careful: rebalancing) |
| Superset | Add Celery workers for async queries |

### Alerting Contacts
Configure in Grafana → Alerting → Contact Points:
- **Slack**: `#cce-pipeline-alerts` channel webhook
- **PagerDuty**: Critical alerts (Flink down, disk full)
- **Email**: `cce-ops@organization.com`
