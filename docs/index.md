# CCE Data Pipeline — Documentation Index

## Overview

The CCE Data Pipeline lands committed PostgreSQL data into ClickHouse via Change Data Capture (**Debezium + Kafka + ClickHouse**) — no custom stream processing. The `cce-insights-service` + `cce-insights-ui` apps (separate repos) consume ClickHouse to serve dashboards.

**Core principle:** Analytics should be purely on committed data in the database.

---

## Document Map

| Document | Purpose | Key Audience |
|----------|---------|--------------|
| [Architecture Overview](architecture-overview.md) | System context, principles, technology decisions, component roles, capacity planning, security, failure modes | Architects, tech leads, DevOps |
| [Data Flow & Schema Design](data-flow.md) | CDC pipeline config, ClickHouse DDL, MATERIALIZED columns, MV catalog, Entity × Behavior matrix, query patterns | Data engineers, backend developers |
| [Query Reference](query-reference/) | Per-domain ClickHouse SQL for `cce-insights-service` to reuse | Backend developers (insights-service) |
| [Deployment Guide](deployment-guide.md) | Prerequisites, Docker/K8s setup, connector registration, schema deployment, monitoring, validation, rollback, operational procedures, troubleshooting | DevOps, SRE, platform engineers |

---

## Quick Reference

### Data Path

```
PostgreSQL (WAL) → Debezium → Kafka → ClickHouse (Kafka engine + MVs) → cce-insights-service / cce-insights-ui
```

### Reading Order

1. **New to the project?** Start with [Architecture Overview](architecture-overview.md)
2. **Working on schema/CDC?** Reference [Data Flow & Schema Design](data-flow.md)
3. **Building insights-service queries?** Use [Query Reference](query-reference/)
4. **Deploying/operating?** Follow [Deployment Guide](deployment-guide.md)
