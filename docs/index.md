# CCE Data Pipeline — Documentation Index

## Overview

The CCE Data Pipeline replaces the custom `cce-insights-service` and `cce-insights-ui` with a CDC-only analytics stack (**PeerDB + ClickHouse + Superset**). All data flows from committed PostgreSQL records via Change Data Capture — no Kafka, no custom stream processing.

**Core principle:** Analytics should be purely on committed data in the database.

---

## Document Map

| Document | Purpose | Key Audience |
|----------|---------|--------------|
| [Architecture Overview](architecture-overview.md) | System context, principles, technology decisions, component roles, capacity planning, security, failure modes | Architects, tech leads, DevOps |
| [Data Flow & Schema Design](data-flow.md) | CDC pipeline config, ClickHouse DDL, MATERIALIZED columns, MV catalog, Entity × Behavior matrix, query patterns | Data engineers, backend developers |
| [Dashboard Design](dashboard-design.md) | Dashboard wireframes, SQL queries, Superset config, RBAC, alerts, scheduled reports | Clinical managers, dashboard creators, product |
| [Deployment Guide](deployment-guide.md) | Prerequisites, Docker/K8s setup, connector registration, schema deployment, monitoring, validation, rollback, operational procedures, troubleshooting | DevOps, SRE, platform engineers |

---

## Quick Reference

### Data Path

```
PostgreSQL (WAL) → PeerDB (direct replication) → ClickHouse (MVs + Dictionaries) → Apache Superset
```

### Reading Order

1. **New to the project?** Start with [Architecture Overview](architecture-overview.md)
2. **Working on schema/CDC?** Reference [Data Flow & Schema Design](data-flow.md)
3. **Creating dashboards?** Use [Dashboard Design](dashboard-design.md)
4. **Deploying/operating?** Follow [Deployment Guide](deployment-guide.md)
