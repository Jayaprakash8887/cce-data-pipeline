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
- PostgreSQL 15+ (source databases with logical replication enabled)
- Apache Kafka 3.6+ (existing CCE cluster)
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
- [ ] PostgreSQL `wal_level = logical` confirmed on both source DBs
- [ ] PostgreSQL replication slot created: `cce_analytics_slot`
- [ ] PostgreSQL publication created: `cce_analytics_pub`
- [ ] Kafka CDC topics pre-created (or auto-create enabled)
- [ ] ClickHouse user `cce_pipeline` created with appropriate grants
- [ ] Network connectivity verified between all services
- [ ] DNS entries configured for Superset/Grafana

### Kafka Topics (CDC)
```bash
# Pre-create CDC topics for partition control (Debezium will auto-create otherwise)
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

### 2. Build Custom Docker Images
```bash
docker build -t cce-kafka-connect:latest -f docker/Dockerfile.kafka-connect ./docker
docker build -t cce-superset:latest -f docker/Dockerfile.superset ./docker
```

---

## Deployment Steps

### Step 1: Start Infrastructure
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

### Step 4: Verify CDC Flow
```bash
# Wait for initial snapshot to complete (check for data in ClickHouse)
sleep 30
clickhouse-client --host $CH_HOST --user cce_pipeline --password $CLICKHOUSE_PASSWORD \
  -q "SELECT name, total_rows FROM system.tables WHERE database='cce_analytics' AND total_rows > 0"
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
# Run schema validation
./scripts/validate-clickhouse.sh

# Run data quality checks
./scripts/data-quality-checks.sh

# Run E2E test suite
./tests/e2e/run-e2e-tests.sh
```

### Manual Checks
| Check | Command | Expected |
|-------|---------|----------|
| ClickHouse tables | `clickhouse-client -q "SELECT count() FROM system.tables WHERE database='cce_analytics'"` | >= 11 tables |
| MVs exist | `clickhouse-client -q "SELECT count() FROM system.tables WHERE database='cce_analytics' AND engine LIKE '%View%'"` | 11 |
| Kafka Connect | `curl localhost:8083/connectors` | 2 connectors |
| Source status | `curl localhost:8083/connectors/cce-cdc-source/status \| jq '.connector.state'` | `RUNNING` |
| Sink status | `curl localhost:8083/connectors/cce-clickhouse-sink/status \| jq '.connector.state'` | `RUNNING` |
| Superset health | `curl localhost:8088/health` | `OK` |
| Grafana health | `curl localhost:3000/api/health` | `{"database":"ok"}` |

---

## Rollback Procedures

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

## Operational Procedures

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

### Alerting Contacts
Configure in Grafana → Alerting → Contact Points:
- **Slack**: `#cce-pipeline-alerts` channel webhook
- **PagerDuty**: Critical alerts (connector down, disk full)
- **Email**: `cce-ops@organization.com`
