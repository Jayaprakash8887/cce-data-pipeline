# CCE Data Pipeline — Implementation Subtasks

Each task below produces a discrete, reviewable PR. Tasks are ordered by dependency — later tasks build on earlier ones.

---

## Phase 1: Foundation (Infrastructure & Schema)

### Task 1.1 — ClickHouse Deployment & DDL

**PR scope:** Infrastructure manifests + complete schema scripts

**Deliverables:**
- Docker Compose service for ClickHouse (dev) / K8s StatefulSet (prod)
- `schema/01-create-tables.sql` — all 11 CDC tables:
  - `protocol_instances`, `step_instances`, `deviations`, `inbound_event_logs`
  - `intelligence_deliveries`, `intelligence_event_logs`, `action_definitions`
  - `protocol_definitions`, `receiver_adaptors`, `destination_adaptor_mappings`
  - `compliance_event_logs`
- `schema/02-create-materialized-views.sql` — all 11 MVs:
  - `mv_event_volume_hourly`, `mv_event_volume_daily`, `mv_facility_summary`
  - `mv_practitioner_summary`, `mv_compliance_summary`, `mv_deviation_trends`
  - `mv_deviation_by_protocol`, `mv_ingestion_quality`, `mv_intelligence_summary`
  - `mv_delivery_performance_hourly`, `mv_step_states_daily`
- `schema/03-create-indexes-projections.sql` — secondary indexes + projections:
  - bloom_filter indexes on `inbound_event_logs`, `intelligence_event_logs`, `intelligence_deliveries`, `step_instances`
  - Projections: `prj_patient_timeline`, `prj_patient_protocols`, `prj_protocol_deviations`
- `schema/04-create-dictionary.sql` — `dict_protocol_definitions`
- `scripts/validate-clickhouse.sh` — health check validation script

**Acceptance criteria:**
- `clickhouse-client -q "SELECT count() FROM system.tables WHERE database='cce_analytics'"` returns >= 11
- All MVs exist and target correct engines (SummingMergeTree / AggregatingMergeTree)
- Validate script passes

---

### Task 1.2 — Kafka Connect & CDC Connectors

**PR scope:** Kafka Connect deployment + Debezium source + ClickHouse sink

**Deliverables:**
- `docker/Dockerfile.kafka-connect` — Custom image with Debezium + ClickHouse sink plugins
- `connectors/cce-cdc-source.json` — Debezium PostgreSQL source config
- `connectors/cce-clickhouse-sink.json` — ClickHouse Kafka Connect sink config
- `scripts/register-connectors.sh` — Registration script
- Docker Compose service: `kafka-connect`

**Acceptance criteria:**
- Both connectors report `RUNNING` status
- Data flows from PostgreSQL to ClickHouse (verified by row count)
- `ReplacingMergeTree` deduplication works (update same row, query with FINAL returns latest)

---

## Phase 2: Visualization & Dashboards

### Task 2.1 — Superset Deployment & Configuration

**PR scope:** Superset + Redis deployment, ClickHouse connection, initial datasets

**Deliverables:**
- Docker Compose services: `superset`, `redis`, `superset-db`
- Superset ClickHouse datasource configuration
- `scripts/setup-superset-datasets.sh` — Register all datasets
- OAuth2/OIDC integration config (Keycloak)

**Acceptance criteria:**
- Superset accessible at `:8088`
- ClickHouse datasets registered and queryable
- SQL Lab can query all 11 tables + 11 MVs

---

### Task 2.2 — Dashboard Implementation

**PR scope:** All 10 Superset dashboards (SQL-based)

**Deliverables:**
- `superset/dashboards/01-operations-overview.sql`
- `superset/dashboards/02-compliance-monitoring.sql`
- `superset/dashboards/03-deviation-analytics.sql`
- `superset/dashboards/04-event-volume-ingestion.sql` (if exists)
- `superset/dashboards/05-facility-performance.sql`
- `superset/dashboards/06-patient-risk.sql`
- `superset/dashboards/09-intelligence-triggers.sql`
- `superset/dashboards/10-practitioner-activity.sql`
- `superset/dashboards/10.5-scheduler-timeliness.sql`
- `superset/alerts/alert-report-config.yaml`

**Acceptance criteria:**
- All queries execute without error against ClickHouse
- MVs with AggregatingMergeTree use `-Merge` combinators in queries
- Proper JOINs used (no referencing non-existent columns)

---

## Phase 3: Monitoring & Operations

### Task 3.1 — Prometheus & Grafana Setup

**PR scope:** Pipeline health monitoring stack

**Deliverables:**
- Docker Compose services: `prometheus`, `grafana`
- `infra/prometheus/prometheus.yml` — Scrape configs for Kafka Connect + ClickHouse
- `infra/grafana/provisioning/dashboards/json/pipeline-health.json` — CDC pipeline dashboard
- `infra/grafana/provisioning/alerting/alerts.yaml` — Alert rules

**Acceptance criteria:**
- Grafana accessible at `:3000`
- Pipeline health dashboard shows CDC sink throughput, insert rate, connector lag
- Alerts fire when connector goes down

---

### Task 3.2 — Data Quality & E2E Tests

**PR scope:** Automated validation scripts

**Deliverables:**
- `scripts/data-quality-checks.sh` — Row counts, freshness, integrity
- `tests/e2e/run-e2e-tests.sh` — End-to-end CDC flow validation
- `tests/load/run-load-test.sh` — Load test runner

**Acceptance criteria:**
- E2E test verifies full path: PG insert → Debezium → Kafka → ClickHouse
- Data quality checks validate freshness (< 5 min lag)
- All scripts pass on clean deployment

---

## Phase 4: Documentation & Finalization

### Task 4.1 — Documentation

**PR scope:** Complete documentation suite

**Deliverables:**
- `README.md` — Project overview with quick start
- `docs/architecture-overview.md` — System context + principles
- `docs/technology-stack.md` — Technology choices with justification
- `docs/data-flow.md` — CDC pipelines, schema, query patterns
- `docs/dashboard-design.md` — Dashboard wireframes + SQL
- `docs/deployment-guide.md` — Full deployment reference
- `docs/deployment-runbook.md` — Step-by-step production procedures
- `docs/index.md` — Documentation index

**Acceptance criteria:**
- All SQL examples use current table/MV names
- Architecture diagrams reflect CDC-only design

---

## Summary

| Phase | Tasks | Key Outputs |
|-------|-------|-------------|
| 1: Foundation | 1.1, 1.2 | ClickHouse schema, CDC connectors, data flowing |
| 2: Visualization | 2.1, 2.2 | Superset dashboards, SQL queries |
| 3: Monitoring | 3.1, 3.2 | Grafana dashboard, alerts, quality checks |
| 4: Documentation | 4.1 | Complete docs aligned with CDC-only architecture |
