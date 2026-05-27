# CCE Data Pipeline — Documentation Index

## Overview

The CCE Data Pipeline replaces the custom `cce-insights-service` and `cce-insights-ui` with an open-source analytics stack (Flink + ClickHouse + Superset). It consumes events from existing CCE Kafka topics, captures dimension data via CDC from PostgreSQL, and exposes interactive dashboards for clinical and operational analytics.

---

## Document Map

| Document | Purpose | Key Audience |
|----------|---------|--------------|
| [Architecture Overview](architecture-overview.md) | System context, principles, component roles, data domains, security, failure modes | Architects, tech leads, new team members |
| [Technology Stack](technology-stack.md) | Detailed technology choices with justification, version matrix, resource sizing | DevOps, architects, procurement |
| [Data Flow & Schema Design](data-flow.md) | Stream pipelines, ClickHouse DDL, CDC flow, query patterns, data lineage, TTL policy | Data engineers, backend developers |
| [Dashboard Design](dashboard-design.md) | 11 dashboard wireframes, SQL queries, Superset config, RBAC, alerts, scheduled reports | Clinical managers, dashboard creators, product |
| [Deployment Guide](deployment-guide.md) | K8s manifests, Helm configs, Docker Compose, Debezium setup, monitoring, DLQ handling, runbooks | DevOps, SRE, platform engineers |

---

## Quick Reference

### Event Sources (Kafka Topics)

| Topic | Producer | Content |
|-------|----------|---------|
| `cce.events.inbound` | Collector Service | CloudEvents + FHIR R4 clinical events |
| `cce.intelligence.triggers` | Compliance Service | Intelligence action triggers |
| `cce.scheduler.triggers` | Scheduler Service | Step state transitions (PENDING→DUE→OVERDUE→MISSED) |

### Flink Jobs

| Job | Input Topic | Output Table |
|-----|-------------|--------------|
| `event-enrichment` | `cce.events.inbound` | `events_fact` |
| `event-volume-aggregator` | `cce.events.inbound` | `event_volume_hourly` |
| `intelligence-tracker` | `cce.intelligence.triggers` | `intelligence_events` |
| `scheduler-tracker` | `cce.scheduler.triggers` | `step_transitions` |

### CDC Tables (PostgreSQL → ClickHouse)

`protocol_definition`, `protocol_instance`, `step_instance`, `deviation`, `inbound_event`, `intelligence_delivery`, `intelligence_event_log`, `action_definition`, `receiver_adaptor`, `destination_adaptor_mapping`

### Key Infrastructure

| Component | Production Spec | Port |
|-----------|----------------|------|
| ClickHouse | 8 cores, 32 GB RAM, 500 GB SSD | 8123 (HTTP), 9000 (native) |
| Flink (2 TaskManagers) | 4 cores × 8 GB each | 8081 (UI) |
| Kafka Connect (2 workers) | 2 cores × 4 GB each | 8083 (REST) |
| Superset (2 web + 2 workers) | 4 cores × 8 GB each | 8088 |

---

## Reading Order

1. **New to the project?** Start with [Architecture Overview](architecture-overview.md)
2. **Evaluating technology?** Read [Technology Stack](technology-stack.md)
3. **Building/modifying pipelines?** Reference [Data Flow & Schema Design](data-flow.md)
4. **Creating dashboards?** Use [Dashboard Design](dashboard-design.md)
5. **Deploying/operating?** Follow [Deployment Guide](deployment-guide.md)
