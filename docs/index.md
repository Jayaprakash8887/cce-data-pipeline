# CCE Data Pipeline — Documentation Index

## Overview

The CCE Data Pipeline replaces the custom `cce-insights-service` and `cce-insights-ui` with a CDC-only analytics stack (**Debezium + ClickHouse + Superset**). All data flows from committed PostgreSQL records via Change Data Capture — no Kafka topic consumption or stream processing.

**Core principle:** Analytics should be purely on committed data in the database.

---

## Document Map

| Document | Purpose | Key Audience |
|----------|---------|--------------|
| [Architecture Overview](architecture-overview.md) | System context, principles, component roles, data domains, security, failure modes | Architects, tech leads, new team members |
| [Technology Stack](technology-stack.md) | Detailed technology choices with justification, version matrix, resource sizing | DevOps, architects, procurement |
| [Data Flow & Schema Design](data-flow.md) | CDC pipelines, ClickHouse DDL, MATERIALIZED columns, MV patterns, query examples | Data engineers, backend developers |
| [Dashboard Design](dashboard-design.md) | 11 dashboard wireframes, SQL queries, Superset config, RBAC, alerts, scheduled reports | Clinical managers, dashboard creators, product |
| [Deployment Guide](deployment-guide.md) | K8s manifests, Docker Compose, Debezium setup, monitoring, DLQ handling | DevOps, SRE, platform engineers |
| [Deployment Runbook](deployment-runbook.md) | Step-by-step production deployment, validation, rollback | DevOps, SRE |

---

## Quick Reference

### Data Path

```
PostgreSQL (WAL) → Debezium Source Connector → Kafka (CDC topics) → ClickHouse Sink Connector → ClickHouse Tables
```

### CDC Tables (PostgreSQL → ClickHouse)

| Source Table | ClickHouse Table | Source DB |
|--------------|------------------|-----------|
| `inbound_event_log` | `inbound_event_logs` | collector-service |
| `protocol_definition` | `protocol_definitions` | compliance-service |
| `protocol_instance` | `protocol_instances` | compliance-service |
| `step_instance` | `step_instances` | compliance-service |
| `deviation` | `deviations` | compliance-service |
| `intelligence_event_log` | `intelligence_event_logs` | compliance-service |
| `intelligence_delivery` | `intelligence_deliveries` | compliance-service |
| `action_definition` | `action_definitions` | compliance-service |
| `compliance_event_log` | `compliance_event_logs` | compliance-service |
| `receiver_adaptor` | `receiver_adaptors` | collector-service |
| `destination_adaptor_mapping` | `destination_adaptor_mappings` | collector-service |

### Key Infrastructure

| Component | Production Spec | Port |
|-----------|----------------|------|
| ClickHouse | 8 cores, 32 GB RAM, 500 GB SSD | 8123 (HTTP), 9000 (native) |
| Kafka Connect (2 workers) | 2 cores × 4 GB each | 8083 (REST) |
| Superset (2 web + 2 workers) | 4 cores × 8 GB each | 8088 |
| Grafana | 1 core × 1 GB | 3000 |
| Prometheus | 2 cores × 4 GB | 9090 |

---

## Reading Order

1. **New to the project?** Start with [Architecture Overview](architecture-overview.md)
2. **Evaluating technology?** Read [Technology Stack](technology-stack.md)
3. **Working on schema/CDC?** Reference [Data Flow & Schema Design](data-flow.md)
4. **Creating dashboards?** Use [Dashboard Design](dashboard-design.md)
5. **Deploying/operating?** Follow [Deployment Runbook](deployment-runbook.md)
