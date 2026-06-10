# ClickHouse Query Reference

These `.sql` files are **reference queries** for the CCE analytics domains, written against
the ClickHouse `cce_analytics` schema (base tables, materialized views, and dictionaries
defined in [`schema/`](../../schema/)).

The presentation layer is **`cce-insights-service` + `cce-insights-ui`**,
which query ClickHouse directly. These files are kept as the canonical, copy-pasteable
definition of how each metric is computed, for the insights-service to reuse.

## Conventions

- **Current-state counts** (status breakdowns, step states, delivery outcomes) — prefer the
  always-fresh `argMaxState` **current-state rollups** in [`schema/06`](../../schema/06-current-state-rollups.sql):
  `rollup_protocol_instance_current`, `rollup_step_current`, `rollup_delivery_current`. Resolve
  the current value per entity with a nested `GROUP BY ... argMaxMerge(col)` and **always filter
  `WHERE is_deleted = 0`** (templates in the schema/06 header). These avoid `FINAL` and never
  double-count. The `.sql` files below still show the equivalent `... FINAL` base-table form,
  which is correct too (just heavier) — never use the removed count-based MVs.
- **Pre-aggregated trends/volumes** read the materialized-view backing tables
  (`mv_event_volume_hourly`, `mv_deviation_trends`, `mv_ingestion_quality`, etc.).
- `AggregatingMergeTree` MVs require `-Merge` combinators at query time
  (`uniqMerge`, `countMerge`, `argMaxMerge`, …).
- The `cce_pipeline` ClickHouse user runs with `final = 1`, so ad-hoc reads of the base tables
  dedupe automatically; explicit `FINAL` in these files documents intent under any profile.

## Files

| File | Domain |
|------|--------|
| `01-operations-overview.sql`     | KPIs, event volume, compliance donut, facility deviations |
| `02-compliance-monitoring.sql`   | Protocol adherence, step states, adherence trend |
| `03-deviation-analytics.sql`     | Deviation trends, resolution rate, step breakdown |
| `04-event-volume-ingestion.sql`  | Event volume, resource types, ingestion quality |
| `05-facility-performance.sql`    | Facility ranking and comparison |
| `06-patient-risk.sql`            | At-risk patients, facility hotspots |
| `07-protocol-analytics.sql`      | Step completion funnel, timeliness, enrollment |
| `08-adaptor-performance.sql`     | Delivery success/latency per receiver-adaptor |
| `09-intelligence-triggers.sql`   | Intelligence trigger volume, action types, destinations |
| `10-practitioner-activity.sql`   | Practitioner workload and deviation correlation |
| `10.5-scheduler-timeliness.sql`  | Step state transitions, escalation rates |
