# CCE Data Pipeline — Deployment Guide

## 1. Prerequisites

| Requirement | Details |
|-------------|---------|
| Kubernetes cluster | 1.27+ (recommended) or Docker Compose for dev |
| Existing Kafka cluster | 3.5+ (KRaft mode) — already deployed by CCE |
| Existing PostgreSQL | 16+ with logical replication enabled (`wal_level = logical`) |
| Existing Keycloak | For Superset OAuth2/OIDC integration |
| Container registry | For custom Flink job images |
| Storage class | SSD-backed persistent volumes for ClickHouse |

---

## 2. Deployment Topology

```mermaid
graph TB
    subgraph "Kubernetes Namespace: cce-data-pipeline"
        subgraph "Stream Processing"
            FJM["Flink JobManager<br/>(StatefulSet, 1 replica)"]
            FTM["Flink TaskManagers<br/>(Deployment, 2 replicas)"]
        end

        subgraph "CDC"
            KC["Kafka Connect<br/>(Deployment, 2 replicas)"]
        end

        subgraph "Analytics Store"
            CH["ClickHouse<br/>(StatefulSet, 1 replica)<br/>PVC: 500GB SSD"]
        end

        subgraph "Visualization"
            SS_WEB["Superset Web<br/>(Deployment, 2 replicas)"]
            SS_WORKER["Superset Worker<br/>(Deployment, 2 replicas)"]
            SS_BEAT["Superset Beat<br/>(Deployment, 1 replica)"]
            REDIS["Redis<br/>(StatefulSet, 1 replica)"]
            SS_DB["Superset Metadata DB<br/>(PostgreSQL, 1 replica)"]
        end
    end

    subgraph "External (Existing)"
        KAFKA["Kafka Cluster"]
        PG["CCE PostgreSQL"]
        KEYCLOAK["Keycloak"]
    end

    KC --> KAFKA
    KC --> PG
    FJM --> KAFKA
    FTM --> KAFKA
    FTM --> CH
    KC --> CH
    SS_WEB --> CH
    SS_WEB --> REDIS
    SS_WEB --> SS_DB
    SS_WORKER --> CH
    SS_WORKER --> REDIS
    SS_WEB --> KEYCLOAK
```

---

## 3. Component Deployment

### 3.1 ClickHouse

**Deployment method:** StatefulSet with PersistentVolumeClaim

```yaml
# clickhouse-deployment.yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: clickhouse
  namespace: cce-data-pipeline
spec:
  serviceName: clickhouse
  replicas: 1
  selector:
    matchLabels:
      app: clickhouse
  template:
    metadata:
      labels:
        app: clickhouse
    spec:
      containers:
      - name: clickhouse
        image: clickhouse/clickhouse-server:24.8-alpine
        ports:
        - containerPort: 8123  # HTTP
          name: http
        - containerPort: 9000  # Native
          name: native
        - containerPort: 9009  # Interserver
          name: interserver
        env:
        - name: CLICKHOUSE_DB
          value: cce_analytics
        - name: CLICKHOUSE_USER
          value: cce_pipeline
        - name: CLICKHOUSE_PASSWORD
          valueFrom:
            secretKeyRef:
              name: clickhouse-secrets
              key: password
        resources:
          requests:
            cpu: "4"
            memory: "16Gi"
          limits:
            cpu: "8"
            memory: "32Gi"
        volumeMounts:
        - name: data
          mountPath: /var/lib/clickhouse
        - name: config
          mountPath: /etc/clickhouse-server/config.d/
        livenessProbe:
          httpGet:
            path: /ping
            port: 8123
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ping
            port: 8123
          initialDelaySeconds: 10
          periodSeconds: 5
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: ["ReadWriteOnce"]
      storageClassName: ssd
      resources:
        requests:
          storage: 500Gi
```

**ClickHouse server config (`config.d/custom.xml`):**
```xml
<clickhouse>
    <max_concurrent_queries>100</max_concurrent_queries>
    <max_memory_usage>28000000000</max_memory_usage>
    <max_bytes_before_external_group_by>10000000000</max_bytes_before_external_group_by>

    <merge_tree>
        <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
    </merge_tree>

    <query_log>
        <database>system</database>
        <table>query_log</table>
        <partition_by>toYYYYMM(event_date)</partition_by>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    </query_log>

    <prometheus>
        <endpoint>/metrics</endpoint>
        <port>9363</port>
        <metrics>true</metrics>
        <events>true</events>
        <asynchronous_metrics>true</asynchronous_metrics>
    </prometheus>
</clickhouse>
```

---

### 3.2 Apache Flink

**Deployment method:** Flink Kubernetes Operator (recommended) or standalone StatefulSet

```yaml
# flink-cluster.yaml (Flink Kubernetes Operator CRD)
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: cce-data-pipeline
  namespace: cce-data-pipeline
spec:
  image: registry.internal/cce/flink-pipeline:1.19.1
  flinkVersion: v1_19
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "4"
    state.backend: rocksdb
    state.checkpoints.dir: file:///opt/flink/checkpoints
    execution.checkpointing.interval: "60000"
    execution.checkpointing.min-pause: "30000"
    restart-strategy: fixed-delay
    restart-strategy.fixed-delay.attempts: "3"
    restart-strategy.fixed-delay.delay: "10s"
  serviceAccount: flink-service-account
  jobManager:
    resource:
      memory: "4096m"
      cpu: 2
    replicas: 1
  taskManager:
    resource:
      memory: "8192m"
      cpu: 4
    replicas: 2
  job:
    jarURI: local:///opt/flink/jobs/cce-event-pipeline.jar
    parallelism: 4
    upgradeMode: savepoint
```

**Flink job Docker image:**
```dockerfile
FROM flink:1.19.1-java21
COPY target/cce-event-pipeline-*.jar /opt/flink/jobs/cce-event-pipeline.jar
COPY lib/flink-connector-kafka-*.jar /opt/flink/lib/
COPY lib/flink-connector-jdbc-*.jar /opt/flink/lib/
COPY lib/clickhouse-jdbc-*.jar /opt/flink/lib/
```

---

### 3.3 Kafka Connect (Debezium)

**Deployment method:** Kafka Connect Distributed mode

```yaml
# kafka-connect-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-connect
  namespace: cce-data-pipeline
spec:
  replicas: 2
  selector:
    matchLabels:
      app: kafka-connect
  template:
    metadata:
      labels:
        app: kafka-connect
    spec:
      containers:
      - name: kafka-connect
        image: registry.internal/cce/kafka-connect:3.7.1
        ports:
        - containerPort: 8083
          name: rest
        env:
        - name: CONNECT_BOOTSTRAP_SERVERS
          value: "${KAFKA_BOOTSTRAP_SERVERS}"
        - name: CONNECT_GROUP_ID
          value: "cce-data-pipeline-connect"
        - name: CONNECT_CONFIG_STORAGE_TOPIC
          value: "cce.connect.configs"
        - name: CONNECT_OFFSET_STORAGE_TOPIC
          value: "cce.connect.offsets"
        - name: CONNECT_STATUS_STORAGE_TOPIC
          value: "cce.connect.status"
        - name: CONNECT_KEY_CONVERTER
          value: "org.apache.kafka.connect.json.JsonConverter"
        - name: CONNECT_VALUE_CONVERTER
          value: "org.apache.kafka.connect.json.JsonConverter"
        resources:
          requests:
            cpu: "1"
            memory: "2Gi"
          limits:
            cpu: "2"
            memory: "4Gi"
```

**Custom Connect image with Debezium + ClickHouse Sink:**
```dockerfile
FROM confluentinc/cp-kafka-connect:7.6.0
# Debezium PostgreSQL connector
RUN confluent-hub install --no-prompt debezium/debezium-connector-postgresql:2.6.1
# ClickHouse sink connector
RUN confluent-hub install --no-prompt clickhouse/clickhouse-kafka-connect:0.14.0
```

**Debezium connector registration (POST to Connect REST API):**
```json
{
  "name": "cce-cdc-source",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "${POSTGRES_HOST}",
    "database.port": "5432",
    "database.user": "${CDC_USER}",
    "database.password": "${CDC_PASSWORD}",
    "database.dbname": "ccedb",
    "database.server.name": "cce",
    "topic.prefix": "cce.cdc",
    "plugin.name": "pgoutput",
    "slot.name": "cce_analytics_slot",
    "publication.name": "cce_analytics_pub",
    "table.include.list": "public.protocol_definition,public.protocol_instance,public.step_instance,public.deviation,public.inbound_event,public.intelligence_delivery,public.intelligence_event_log,public.action_definition,public.receiver_adaptor,public.destination_adaptor_mapping",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "true",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "snapshot.mode": "initial",
    "heartbeat.interval.ms": "10000"
  }
}
```

**ClickHouse sink connector registration:**
```json
{
  "name": "cce-clickhouse-sink",
  "config": {
    "connector.class": "com.clickhouse.kafka.connect.ClickHouseSinkConnector",
    "topics.regex": "cce\\.cdc\\.public\\.(protocol_definition|protocol_instance|step_instance|deviation|inbound_event|intelligence_delivery|intelligence_event_log|action_definition|receiver_adaptor|destination_adaptor_mapping)",
    "hostname": "clickhouse.cce-data-pipeline.svc.cluster.local",
    "port": "8123",
    "database": "cce_analytics",
    "username": "cce_pipeline",
    "password": "${CLICKHOUSE_PASSWORD}",
    "schemas.enable": "false",
    "batch.size": "10000",
    "flush.interval.ms": "5000"
  }
}
```

---

### 3.4 Apache Superset

**Deployment method:** Helm chart (official)

```bash
helm repo add superset https://apache.github.io/superset
helm install superset superset/superset \
  --namespace cce-data-pipeline \
  --values superset-values.yaml
```

**superset-values.yaml:**
```yaml
image:
  repository: apache/superset
  tag: 4.0.2

replicaCount: 2

supersetNode:
  connections:
    db_host: superset-postgresql
    db_port: 5432
    db_name: superset
    db_user: superset

configOverrides:
  secret: |
    SECRET_KEY = '${SUPERSET_SECRET_KEY}'
  
  enable_oauth: |
    from flask_appbuilder.security.manager import AUTH_OAUTH
    AUTH_TYPE = AUTH_OAUTH
    OAUTH_PROVIDERS = [
        {
            'name': 'keycloak',
            'icon': 'fa-key',
            'token_key': 'access_token',
            'remote_app': {
                'client_id': '${KEYCLOAK_CLIENT_ID}',
                'client_secret': '${KEYCLOAK_CLIENT_SECRET}',
                'api_base_url': '${KEYCLOAK_URL}/realms/cce/protocol/openid-connect/',
                'access_token_url': '${KEYCLOAK_URL}/realms/cce/protocol/openid-connect/token',
                'authorize_url': '${KEYCLOAK_URL}/realms/cce/protocol/openid-connect/auth',
                'server_metadata_url': '${KEYCLOAK_URL}/realms/cce/.well-known/openid-configuration',
                'client_kwargs': {
                    'scope': 'openid email profile'
                }
            }
        }
    ]
  
  clickhouse_driver: |
    SQLALCHEMY_CUSTOM_PASSWORD_STORE = None
    # pip install clickhouse-connect in init container

extraEnvRaw:
  - name: SUPERSET_LOAD_EXAMPLES
    value: "no"

init:
  initContainers:
    - name: install-clickhouse-driver
      image: apache/superset:4.0.2
      command: ['sh', '-c', 'pip install clickhouse-connect']

redis:
  enabled: true

postgresql:
  enabled: true
  auth:
    postgresPassword: "${SUPERSET_DB_PASSWORD}"
```

**ClickHouse database connection in Superset:**
```
clickhousedb://cce_pipeline:${PASSWORD}@clickhouse.cce-data-pipeline.svc.cluster.local:8123/cce_analytics
```

---

## 4. PostgreSQL Configuration (Source DB)

Enable logical replication for Debezium CDC:

```sql
-- Run on CCE PostgreSQL (requires superuser or replication role)

-- 1. Set wal_level (requires restart)
ALTER SYSTEM SET wal_level = 'logical';

-- 2. Create dedicated replication user
CREATE ROLE cce_cdc_user WITH LOGIN REPLICATION PASSWORD '${CDC_PASSWORD}';
GRANT CONNECT ON DATABASE ccedb TO cce_cdc_user;
GRANT USAGE ON SCHEMA public TO cce_cdc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cce_cdc_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO cce_cdc_user;

-- 3. Create publication for analytics tables
CREATE PUBLICATION cce_analytics_pub FOR TABLE
    protocol_definition,
    protocol_instance,
    step_instance,
    deviation,
    inbound_event,
    intelligence_delivery,
    intelligence_event_log,
    action_definition,
    receiver_adaptor,
    destination_adaptor_mapping;
```

---

## 5. Environment Variables

### 5.1 Shared

| Variable | Description | Example |
|----------|-------------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker addresses | `kafka-0:9092,kafka-1:9092,kafka-2:9092` |
| `CLICKHOUSE_HOST` | ClickHouse hostname | `clickhouse.cce-data-pipeline.svc.cluster.local` |
| `CLICKHOUSE_PORT` | ClickHouse HTTP port | `8123` |
| `CLICKHOUSE_DB` | Analytics database name | `cce_analytics` |
| `CLICKHOUSE_USER` | Pipeline user | `cce_pipeline` |
| `CLICKHOUSE_PASSWORD` | Pipeline password | (from Secret) |

### 5.2 Debezium-specific

| Variable | Description | Example |
|----------|-------------|---------|
| `POSTGRES_HOST` | Source PostgreSQL host | `cce-postgresql.cce.svc.cluster.local` |
| `CDC_USER` | Replication user | `cce_cdc_user` |
| `CDC_PASSWORD` | Replication password | (from Secret) |

### 5.3 Superset-specific

| Variable | Description | Example |
|----------|-------------|---------|
| `SUPERSET_SECRET_KEY` | Flask secret key | (generated, 64+ chars) |
| `KEYCLOAK_URL` | Keycloak base URL | `https://auth.cce.example.org` |
| `KEYCLOAK_CLIENT_ID` | OAuth2 client ID | `cce-superset` |
| `KEYCLOAK_CLIENT_SECRET` | OAuth2 client secret | (from Secret) |

---

## 6. Initialization Steps

### 6.1 Bootstrap Order

```mermaid
flowchart TD
    A["1. Deploy ClickHouse"] --> B["2. Run DDL scripts<br/>(create tables & MVs)"]
    B --> C["3. Deploy Kafka Connect"]
    C --> D["4. Register Debezium connector<br/>(initial snapshot)"]
    D --> E["5. Register ClickHouse sink"]
    E --> F["6. Deploy Flink cluster"]
    F --> G["7. Submit Flink jobs"]
    G --> H["8. Deploy Superset"]
    H --> I["9. Import dashboard templates"]
    I --> J["10. Configure Superset database connection"]
```

### 6.2 ClickHouse Schema Initialization

```bash
# Apply DDL scripts
clickhouse-client --host ${CLICKHOUSE_HOST} \
  --user ${CLICKHOUSE_USER} \
  --password ${CLICKHOUSE_PASSWORD} \
  --database cce_analytics \
  --multiquery < schema/01-create-tables.sql

clickhouse-client --host ${CLICKHOUSE_HOST} \
  --user ${CLICKHOUSE_USER} \
  --password ${CLICKHOUSE_PASSWORD} \
  --database cce_analytics \
  --multiquery < schema/02-create-materialized-views.sql
```

---

## 7. Docker Compose (Development)

```yaml
version: '3.8'
services:
  clickhouse:
    image: clickhouse/clickhouse-server:24.8-alpine
    ports:
      - "8123:8123"
      - "9000:9000"
    environment:
      CLICKHOUSE_DB: cce_analytics
      CLICKHOUSE_USER: cce_pipeline
      CLICKHOUSE_PASSWORD: dev_password
    volumes:
      - clickhouse_data:/var/lib/clickhouse
      - ./schema:/docker-entrypoint-initdb.d

  kafka-connect:
    image: debezium/connect:2.6
    ports:
      - "8083:8083"
    environment:
      BOOTSTRAP_SERVERS: ${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}
      GROUP_ID: cce-data-pipeline-connect
      CONFIG_STORAGE_TOPIC: cce.connect.configs
      OFFSET_STORAGE_TOPIC: cce.connect.offsets
      STATUS_STORAGE_TOPIC: cce.connect.status
    depends_on:
      - clickhouse

  flink-jobmanager:
    image: flink:1.19.1-java21
    command: jobmanager
    ports:
      - "8081:8081"
    environment:
      FLINK_PROPERTIES: |
        jobmanager.rpc.address: flink-jobmanager
        state.backend: rocksdb
        execution.checkpointing.interval: 60000

  flink-taskmanager:
    image: flink:1.19.1-java21
    command: taskmanager
    environment:
      FLINK_PROPERTIES: |
        jobmanager.rpc.address: flink-jobmanager
        taskmanager.numberOfTaskSlots: 4
    depends_on:
      - flink-jobmanager

  superset:
    image: apache/superset:4.0.2
    ports:
      - "8088:8088"
    environment:
      SUPERSET_SECRET_KEY: dev-secret-key-change-in-production
      SQLALCHEMY_DATABASE_URI: sqlite:////app/superset_home/superset.db
    volumes:
      - superset_data:/app/superset_home
    depends_on:
      - clickhouse

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"

volumes:
  clickhouse_data:
  superset_data:
```

---

## 8. Health Checks & Monitoring

### 8.1 Health Endpoints

| Component | Health Check | Port |
|-----------|-------------|------|
| ClickHouse | `GET /ping` → `Ok.\n` | 8123 |
| Flink JobManager | `GET /overview` | 8081 |
| Kafka Connect | `GET /connectors` | 8083 |
| Superset | `GET /health` | 8088 |

### 8.2 Key Metrics to Monitor

| Metric | Source | Alert Threshold |
|--------|--------|-----------------|
| `kafka_consumer_group_lag` | Kafka | > 100,000 (5 min sustained) |
| `flink_jobmanager_job_uptime` | Flink | < 60s (job restarting) |
| `flink_taskmanager_Status_JVM_Memory_Heap_Used` | Flink | > 80% |
| `ClickHouseMetrics_Query` | ClickHouse | > 50 concurrent |
| `ClickHouseMetrics_MergesRunning` | ClickHouse | > 20 sustained |
| `ClickHouseAsyncMetrics_ReplicasMaxAbsoluteDelay` | ClickHouse | > 300s |

### 8.3 Prometheus scrape config

```yaml
scrape_configs:
  - job_name: 'clickhouse'
    static_configs:
      - targets: ['clickhouse:9363']

  - job_name: 'flink'
    static_configs:
      - targets: ['flink-jobmanager:9249']

  - job_name: 'kafka-connect'
    static_configs:
      - targets: ['kafka-connect:9404']
```

---

## 9. Backup & Disaster Recovery

### 9.1 ClickHouse Backup

```bash
# Using clickhouse-backup tool
# Install: https://github.com/Altinity/clickhouse-backup

# Daily backup (cron)
clickhouse-backup create --tables='cce_analytics.*' daily_$(date +%Y%m%d)

# Upload to S3/MinIO
clickhouse-backup upload daily_$(date +%Y%m%d)

# Restore
clickhouse-backup restore_remote daily_20260101
```

### 9.2 Superset Dashboard Export

```bash
# Export all dashboards as JSON (for version control)
superset export-dashboards --dashboard-file /backup/dashboards.json

# Import dashboards
superset import-dashboards --path /backup/dashboards.json
```

---

## 10. Security Hardening

| Component | Measure |
|-----------|---------|
| ClickHouse | Dedicated user with restricted permissions; TLS for client connections; no default user |
| Kafka Connect | SASL/SCRAM authentication to Kafka; TLS; dedicated service account |
| Flink | Pod security context (non-root); network policies restricting egress |
| Superset | OAuth2 only (no local auth); HTTPS via Ingress; CSP headers |
| Secrets | All credentials in Kubernetes Secrets (or external vault); never in ConfigMaps |
| Network | NetworkPolicies: pipeline namespace → Kafka, PostgreSQL only; no public ingress except Superset |
| CDC user | SELECT + REPLICATION only; no write permissions to source DB |

---

## 11. Dead Letter Queue (DLQ) Handling

Each Kafka topic has a corresponding DLQ topic (`.dlq` suffix) for messages that fail processing:

| Source Topic | DLQ Topic | Failure Scenarios |
|--------------|-----------|-------------------|
| `cce.events.inbound` | `cce.events.inbound.dlq` | Malformed CloudEvents, invalid FHIR payload, schema violations |
| `cce.intelligence.triggers` | `cce.intelligence.triggers.dlq` | Missing required fields, deserialization errors |
| `cce.scheduler.triggers` | `cce.scheduler.triggers.dlq` | Invalid UUID, unparseable timestamp |

### 11.1 Monitoring

```yaml
# Prometheus alert rule for DLQ messages
- alert: DLQMessagesDetected
  expr: kafka_consumer_group_lag{topic=~".*\\.dlq"} > 0
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "DLQ has {{ $value }} unprocessed messages on {{ $labels.topic }}"
```

### 11.2 Reprocessing Playbook

```bash
# 1. Inspect DLQ messages
kafka-console-consumer --bootstrap-server ${KAFKA_BOOTSTRAP_SERVERS} \
  --topic cce.events.inbound.dlq --from-beginning --max-messages 10

# 2. Identify root cause (schema change, bug in enrichment logic, etc.)

# 3. After fixing the root cause, replay DLQ back to source topic:
kafka-console-consumer --bootstrap-server ${KAFKA_BOOTSTRAP_SERVERS} \
  --topic cce.events.inbound.dlq --from-beginning | \
kafka-console-producer --bootstrap-server ${KAFKA_BOOTSTRAP_SERVERS} \
  --topic cce.events.inbound

# 4. Verify DLQ is drained
kafka-consumer-groups --bootstrap-server ${KAFKA_BOOTSTRAP_SERVERS} \
  --describe --group cce-data-pipeline-flink | grep dlq
```

### 11.3 DLQ Retention

DLQ topics have 30-day retention (vs 7-day for source topics) to allow investigation time.

---

## 12. Backfill & Replay Runbook

### 12.1 Flink Job State Loss (checkpoint corruption, redeployment without savepoint)

```bash
# 1. Identify affected time range
#    Check last successful checkpoint timestamp in Flink UI

# 2. Determine which ClickHouse tables need backfill
#    - events_fact (Flink: event-enrichment)
#    - event_volume_hourly (Flink: event-volume-aggregator)
#    - intelligence_events (Flink: intelligence-tracker)
#    - step_transitions (Flink: scheduler-tracker)

# 3. Delete affected data in ClickHouse (by time range)
clickhouse-client --query "
  ALTER TABLE events_fact DELETE
  WHERE processed_at >= '2026-05-01 00:00:00'
    AND processed_at <= '2026-05-02 00:00:00'
"

# 4. Reset Flink consumer group to target timestamp
kafka-consumer-groups --bootstrap-server ${KAFKA_BOOTSTRAP_SERVERS} \
  --group cce-data-pipeline-flink \
  --topic cce.events.inbound \
  --reset-offsets --to-datetime 2026-05-01T00:00:00.000 \
  --execute

# 5. Restart Flink job (it will replay from the reset offset)
kubectl -n cce-data-pipeline rollout restart deployment/flink-taskmanager
```

### 12.2 CDC Full Re-sync (replication slot lost, extended outage > 7 days)

```bash
# 1. Drop the old replication slot (if still exists)
psql -h ${POSTGRES_HOST} -U ${CDC_USER} -d ccedb -c \
  "SELECT pg_drop_replication_slot('cce_analytics_slot');"

# 2. Truncate affected ClickHouse tables
clickhouse-client --query "TRUNCATE TABLE protocol_instances"
clickhouse-client --query "TRUNCATE TABLE step_instances"
clickhouse-client --query "TRUNCATE TABLE deviations"
# ... repeat for all CDC tables

# 3. Delete and recreate the Debezium connector (triggers fresh snapshot)
curl -X DELETE http://kafka-connect:8083/connectors/cce-cdc-source
curl -X POST http://kafka-connect:8083/connectors \
  -H 'Content-Type: application/json' \
  -d @connectors/cce-cdc-source.json

# 4. Monitor snapshot progress
curl http://kafka-connect:8083/connectors/cce-cdc-source/status | jq .
```

### 12.3 Partial Table Backfill (single table corruption)

```bash
# 1. Truncate only the affected table
clickhouse-client --query "TRUNCATE TABLE intelligence_deliveries"

# 2. Reset only that table's CDC offset
curl -X PUT http://kafka-connect:8083/connectors/cce-cdc-source/offsets/reset \
  -H 'Content-Type: application/json'

# 3. Alternative: use Debezium signal table for ad-hoc snapshot
psql -h ${POSTGRES_HOST} -U ${CDC_USER} -d ccedb -c \
  "INSERT INTO debezium_signal (id, type, data)
   VALUES ('snapshot-intel-delivery', 'execute-snapshot',
           '{\"data-collections\": [\"public.intelligence_delivery\"]}');"
```

---

## 13. Schema Evolution Strategy

### 13.1 FHIR Payload Changes

The pipeline is resilient to FHIR R4 payload evolution by design:

| Change Type | Impact | Handling |
|-------------|--------|----------|
| **New field added** to FHIR resource | No impact — `raw_payload` preserves full data | Extract new field with `ALTER TABLE ADD COLUMN` + backfill |
| **New resource type** | Auto-captured (events_fact uses `LowCardinality(String)`) | Update dashboards to include new type in filters |
| **Field renamed** | Flink extraction breaks for renamed field | Update Flink job's JSON_VALUE path; redeploy with savepoint |
| **Field removed** | Extracted column gets NULLs going forward | Acceptable; no action unless field was critical |

### 13.2 Adding New Extracted Fields

```sql
-- 1. Add column to ClickHouse (instant, no rewrite)
ALTER TABLE events_fact ADD COLUMN encounter_class LowCardinality(Nullable(String))
  AFTER primary_code_display;

-- 2. Update Flink job to extract the new field (savepoint-based redeploy)
--    Add to INSERT INTO: JSON_VALUE(`data`, '$.class.code') AS encounter_class

-- 3. Backfill historical data (optional, for full coverage)
ALTER TABLE events_fact UPDATE
    encounter_class = JSONExtractString(raw_payload, 'class', 'code')
WHERE encounter_class IS NULL;
```

### 13.3 CDC Schema Changes (PostgreSQL ALTER TABLE)

Debezium handles most DDL changes automatically:

| DDL Change | Debezium Behavior | ClickHouse Action Required |
|------------|-------------------|---------------------------|
| `ADD COLUMN` | New field appears in CDC events | `ALTER TABLE ADD COLUMN` on ClickHouse table |
| `DROP COLUMN` | Field disappears from CDC events | Column gets NULLs; optionally `DROP COLUMN` later |
| `ALTER TYPE` (compatible) | Auto-handled (e.g., VARCHAR length increase) | None |
| `ALTER TYPE` (incompatible) | May require connector restart | `ALTER TABLE MODIFY COLUMN` or migrate |
| `RENAME COLUMN` | Treated as DROP + ADD | Manual mapping in sink connector SMT |

---

## 14. Data Validation & Quality Checks

### 14.1 Flink-Side Validation (Event Enrichment Job)

```java
// Validation rules applied before writing to ClickHouse
// Events failing validation are routed to DLQ

ValidationRules:
  - patient_id (subject) must not be null or empty
  - event_time must not be in the future (> now + 5 minutes)
  - event_time must not be older than 2 years
  - resource_type must be a valid FHIR R4 resource type
  - source must not be null
```

### 14.2 ClickHouse-Side Quality Metrics

```sql
-- Run daily as a scheduled Superset alert or Grafana query
-- Detects anomalies in data completeness

SELECT
    toStartOfHour(processed_at) AS hour,
    count() AS events_received,
    countIf(patient_id = '') AS missing_patient_id,
    countIf(facility_id IS NULL OR facility_id = '') AS missing_facility,
    countIf(resource_type = '') AS missing_resource_type,
    countIf(processed_at - event_time > 300) AS late_events_over_5min,
    round(missing_patient_id / events_received * 100, 2) AS pct_missing_patient
FROM events_fact
WHERE processed_at >= now() - INTERVAL 24 HOUR
GROUP BY hour
HAVING missing_patient_id > 0 OR missing_facility > events_received * 0.1
ORDER BY hour;
```

### 14.3 Cross-Source Consistency Check

```sql
-- Compare CDC count vs Kafka stream count (should be close)
SELECT
    'inbound_events (CDC)' AS source,
    count() AS count_24h
FROM inbound_events
WHERE received_at >= now() - INTERVAL 24 HOUR

UNION ALL

SELECT
    'events_fact (Kafka stream)' AS source,
    count() AS count_24h
FROM events_fact
WHERE event_time >= now() - INTERVAL 24 HOUR;

-- Delta > 5% indicates a pipeline issue
```
