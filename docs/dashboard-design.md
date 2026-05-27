# CCE Data Pipeline — Dashboard Design

## 1. Dashboard Overview

The following dashboards replace the custom React insights-ui application. All dashboards are built in **Apache Superset** using SQL queries against ClickHouse.

| # | Dashboard | Primary Audience | Refresh Rate |
|---|-----------|-----------------|--------------|
| 1 | **Operations Overview** | System admins, program managers | 5 min |
| 2 | **Compliance Monitoring** | Clinical managers, M&E officers | 15 min |
| 3 | **Deviation Analytics** | Clinical supervisors | 15 min |
| 4 | **Event Volume & Ingestion** | System admins, integration engineers | 5 min |
| 5 | **Facility Performance** | District health officers | 30 min |
| 6 | **Patient Risk** | Clinical supervisors, CHW managers | 15 min |
| 7 | **Protocol Analytics** | Program managers, clinical leads | 30 min |
| 8 | **Receiver-Adaptor Performance** | Integration engineers, system admins | 5 min |
| 9 | **Intelligence & Triggers** | Clinical supervisors, program managers | 15 min |
| 10 | **Practitioner Activity** | Clinical managers, facility leads | 30 min |
| 10.5 | **Scheduler Timeliness** | Program managers, clinical leads | 15 min |
| 11 | **Pipeline Health** | DevOps / SRE team | 1 min (Grafana) |

---

## 2. Dashboard 1: Operations Overview

**Purpose:** Executive summary of the entire CCE platform health and clinical operations.

### Layout

```
┌──────────────────────────────────────────────────────────────────────────┐
│  FILTERS: [Date Range] [Facility] [Protocol]                             │
├────────────────┬────────────────┬────────────────┬───────────────────────┤
│  KPI Card      │  KPI Card      │  KPI Card      │  KPI Card             │
│  Total Events  │  Active        │  Overall       │  Active               │
│  Today         │  Patients      │  Adherence %   │  Deviations           │
├────────────────┴────────────────┴────────────────┴───────────────────────┤
│                                                                          │
│  [Line Chart] Event Volume — Last 30 Days (by resource type)             │
│                                                                          │
├─────────────────────────────────┬────────────────────────────────────────┤
│                                 │                                        │
│  [Donut Chart]                  │  [Bar Chart]                           │
│  Patient Compliance             │  Top 5 Facilities                      │
│  Distribution                   │  by Deviation Count                    │
│  (on_track/at_risk/             │                                        │
│   non_compliant)                │                                        │
│                                 │                                        │
├─────────────────────────────────┴────────────────────────────────────────┤
│                                                                          │
│  [Table] Recent Deviations (last 24h) — patient, protocol, type, time   │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

### KPI Queries

**Total Events Today:**
```sql
SELECT count() AS total_events_today
FROM events_fact
WHERE event_time >= today();
```

**Active Patients:**
```sql
SELECT count(DISTINCT patient_id) AS active_patients
FROM protocol_instances FINAL
WHERE status = 'ACTIVE';
```

**Overall Adherence Rate:**
```sql
SELECT round(
    countIf(state = 'COMPLETED') / nullIf(count(), 0) * 100, 1
) AS adherence_rate_pct
FROM step_instances FINAL
WHERE protocol_instance_id IN (
    SELECT id FROM protocol_instances FINAL WHERE status IN ('ACTIVE', 'COMPLETED')
);
```

---

## 3. Dashboard 2: Compliance Monitoring

**Purpose:** Track protocol adherence across facilities and programs.

### Layout

```
┌──────────────────────────────────────────────────────────────────────────┐
│  FILTERS: [Date Range] [Facility] [Protocol] [Status]                    │
├────────────────┬────────────────┬────────────────┬───────────────────────┤
│  KPI: Total    │  KPI: On Track │  KPI: At Risk  │  KPI: Non-Compliant   │
│  Enrollments   │  (≥80%)        │  (50-80%)      │  (<50%)               │
├────────────────┴────────────────┴────────────────┴───────────────────────┤
│                                                                          │
│  [Stacked Bar] Compliance by Protocol                                    │
│  (COMPLETED | OVERDUE | MISSED | PENDING per protocol)                   │
│                                                                          │
├─────────────────────────────────┬────────────────────────────────────────┤
│  [Line Chart]                   │  [Heatmap]                             │
│  Adherence Rate Trend           │  Facility × Protocol                   │
│  (weekly, by protocol)          │  Adherence Matrix                      │
│                                 │  (color: green→yellow→red)             │
├─────────────────────────────────┴────────────────────────────────────────┤
│                                                                          │
│  [Table] Protocol Instance Details                                       │
│  patient_id | protocol | status | enrolled_at | adherence_rate           │
│  (sortable, paginated, exportable to CSV)                                │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

### Key Queries

**Compliance by Protocol (stacked bar):**
```sql
SELECT
    pd.name AS protocol_name,
    si.state,
    count() AS step_count
FROM step_instances si FINAL
JOIN protocol_instances pi FINAL ON si.protocol_instance_id = pi.id
JOIN protocol_definitions pd FINAL ON pi.protocol_definition_id = pd.id
WHERE pi.status IN ('ACTIVE', 'COMPLETED')
GROUP BY pd.name, si.state
ORDER BY pd.name, si.state;
```

**Adherence Trend (weekly):**
```sql
SELECT
    toStartOfWeek(si.created_at) AS week,
    pd.name AS protocol_name,
    round(countIf(si.state = 'COMPLETED') / nullIf(count(), 0) * 100, 1) AS adherence_pct
FROM step_instances si FINAL
JOIN protocol_instances pi FINAL ON si.protocol_instance_id = pi.id
JOIN protocol_definitions pd FINAL ON pi.protocol_definition_id = pd.id
WHERE si.created_at >= today() - INTERVAL 90 DAY
GROUP BY week, protocol_name
ORDER BY week;
```

---

## 4. Dashboard 3: Deviation Analytics

**Purpose:** Analyze compliance deviations — overdue and missed steps.

### Layout

```
┌──────────────────────────────────────────────────────────────────────────┐
│  FILTERS: [Date Range] [Facility] [Protocol] [Deviation Type]            │
├────────────────┬────────────────┬────────────────┬───────────────────────┤
│  KPI: Total    │  KPI: Overdue  │  KPI: Missed   │  KPI: Resolution      │
│  Deviations    │  Count         │  Count         │  Rate %               │
├────────────────┴────────────────┴────────────────┴───────────────────────┤
│                                                                          │
│  [Area Chart] Deviation Trends (daily/weekly, stacked by type)           │
│                                                                          │
├─────────────────────────────────┬────────────────────────────────────────┤
│  [Bar Chart]                    │  [Funnel Chart]                        │
│  Deviations by Protocol Step    │  Deviation Resolution Funnel           │
│  (action_id breakdown)          │  Overdue → Resolved | Missed           │
│                                 │                                        │
├─────────────────────────────────┼────────────────────────────────────────┤
│  [Pie Chart]                    │  [Box Plot]                            │
│  Deviations by Facility         │  Days to Resolution                   │
│  (top 10)                       │  (by protocol)                        │
│                                 │                                        │
├─────────────────────────────────┴────────────────────────────────────────┤
│                                                                          │
│  [Table] Deviation Details (paginated, sortable)                         │
│  patient_id | protocol | step | type | detected_at | current_state       │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

### Key Queries

**Deviation Trends:**
```sql
SELECT
    toStartOfDay(detected_at) AS day,
    deviation_type,
    count() AS deviation_count
FROM deviations
WHERE detected_at BETWEEN '{{ start_date }}' AND '{{ end_date }}'
GROUP BY day, deviation_type
ORDER BY day;
```

**Resolution Rate:**
```sql
SELECT
    countIf(si.state = 'COMPLETED') AS resolved,
    countIf(si.state = 'MISSED') AS escalated,
    count() AS total_overdue,
    round(countIf(si.state = 'COMPLETED') / nullIf(count(), 0) * 100, 1) AS resolution_rate_pct,
    round(avg(
        if(si.state = 'COMPLETED',
           dateDiff('day', d.detected_at, si.completed_at), NULL)
    ), 1) AS avg_days_to_resolve
FROM deviations d
JOIN step_instances si FINAL ON d.step_instance_id = si.id
WHERE d.deviation_type = 'OVERDUE';
```

---

## 5. Dashboard 4: Event Volume & Ingestion

**Purpose:** Monitor clinical event flow, source system health, and ingestion quality.

### Layout

```
┌──────────────────────────────────────────────────────────────────────────┐
│  FILTERS: [Date Range] [Facility] [Source] [Resource Type]               │
├────────────────┬────────────────┬────────────────┬───────────────────────┤
│  KPI: Events   │  KPI: Accept   │  KPI: Reject   │  KPI: Sources        │
│  Today         │  Rate %        │  Rate %        │  Active              │
├────────────────┴────────────────┴────────────────┴───────────────────────┤
│                                                                          │
│  [Line Chart] Event Volume Trend (daily, by resource type)               │
│                                                                          │
├─────────────────────────────────┬────────────────────────────────────────┤
│  [Treemap]                      │  [Stacked Bar]                         │
│  Events by Resource Type        │  Ingestion Funnel by Source            │
│  (proportional area)            │  (ACCEPTED | REJECTED)                 │
│                                 │                                        │
├─────────────────────────────────┼────────────────────────────────────────┤
│  [Bar Chart]                    │  [Bar Chart]                           │
│  Events by Facility             │  Rejection Reasons                     │
│  (top 15)                       │  (INVALID_FHIR, INVALID_JSON, etc.)    │
│                                 │                                        │
├─────────────────────────────────┴────────────────────────────────────────┤
│  [Table] Source Data Quality Scorecard                                   │
│  source | total | accepted | rejected | quality_score                    │
├──────────────────────────────────────────────────────────────────────────┤
│  [Line Chart] Source Comparison (overlay by source)                       │
│  (multi-line: event volume per source over time, side-by-side)            │
└──────────────────────────────────────────────────────────────────────────┘
```

### Key Queries

**Event Volume Trend (from pre-aggregated table):**
```sql
SELECT
    toStartOfDay(hour) AS day,
    resource_type,
    sum(event_count) AS events
FROM event_volume_hourly
WHERE hour BETWEEN '{{ start_date }}' AND '{{ end_date }}'
GROUP BY day, resource_type
ORDER BY day;
```

**Source Quality Scorecard:**
```sql
SELECT
    source,
    count() AS total,
    countIf(status = 'ACCEPTED') AS accepted,
    countIf(status = 'REJECTED') AS rejected,
    round(countIf(status = 'ACCEPTED') / nullIf(count(), 0) * 100, 1) AS quality_score
FROM inbound_events
WHERE received_at >= today() - INTERVAL 7 DAY
GROUP BY source
ORDER BY quality_score ASC;
```

**Source Comparison (side-by-side):**
```sql
-- Compare event volumes across sources
SELECT
    source,
    toStartOfDay(hour) AS day,
    sum(event_count) AS events
FROM event_volume_hourly
WHERE hour BETWEEN '{{ start_date }}' AND '{{ end_date }}'
GROUP BY source, day
ORDER BY day, source;
```

---

## 6. Dashboard 5: Facility Performance

**Purpose:** Rank and compare facilities by compliance and event metrics.

### Layout

```
┌──────────────────────────────────────────────────────────────────────────┐
│  FILTERS: [Date Range] [Protocol] [Rank By: compliance/deviations/events]│
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  [Horizontal Bar Chart] Facility Ranking                                 │
│  (sorted by selected metric, color-coded: green/yellow/red)              │
│                                                                          │
├─────────────────────────────────┬────────────────────────────────────────┤
│  [Scatter Plot]                 │  [Radar Chart]                         │
│  Compliance Rate vs             │  Top 5 Facilities                      │
│  Deviation Count                │  (multi-metric comparison)             │
│  (bubble = event volume)        │                                        │
├─────────────────────────────────┴────────────────────────────────────────┤
│                                                                          │
│  [Table] Facility Details                                                │
│  facility | enrollments | compliance_rate | deviations | events | rank   │
│  (sortable, exportable)                                                  │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 7. Dashboard 6: Patient Risk

**Purpose:** Identify at-risk patients and facilities with high non-compliance.

### Layout

```
┌──────────────────────────────────────────────────────────────────────────┐
│  FILTERS: [Facility] [Protocol] [Min Deviations]                         │
├────────────────┬────────────────┬────────────────┬───────────────────────┤
│  KPI: On Track │  KPI: At Risk  │  KPI: Non-     │  KPI: Repeat          │
│  Patients      │  Patients      │  Compliant     │  Deviators            │
├────────────────┴────────────────┴────────────────┴───────────────────────┤
│                                                                          │
│  [Stacked Bar] Patient Risk Distribution by Facility                     │
│  (on_track | at_risk | non_compliant per facility)                       │
│                                                                          │
├─────────────────────────────────┬────────────────────────────────────────┤
│  [Bubble Chart]                 │  [Table]                               │
│  Facility Hotspots              │  Repeat Deviation Patients             │
│  (x=total, y=non_compliant%,   │  patient | deviations | protocols |    │
│   size=deviation_count)         │  last_deviation_date                   │
│                                 │                                        │
└─────────────────────────────────┴────────────────────────────────────────┘
```

---

## 8. Dashboard 7: Protocol Analytics

**Purpose:** Deep dive into individual protocol performance — step completion, timeliness, enrollment.

### Layout

```
┌──────────────────────────────────────────────────────────────────────────┐
│  FILTERS: [Protocol (required)] [Facility] [Date Range]                  │
├────────────────┬────────────────┬────────────────┬───────────────────────┤
│  KPI: Total    │  KPI: Active   │  KPI: Avg      │  KPI: Completion      │
│  Enrollments   │  Instances     │  Adherence %   │  Rate %               │
├────────────────┴────────────────┴────────────────┴───────────────────────┤
│                                                                          │
│  [Funnel Chart] Step Completion Funnel                                   │
│  (step1 reached → completed, step2 reached → completed, ...)            │
│                                                                          │
├─────────────────────────────────┬────────────────────────────────────────┤
│  [Grouped Bar]                  │  [Pie Chart]                           │
│  Step State Distribution        │  Outcome Distribution                  │
│  (per action_id: COMPLETED,     │  (ACTIVE | COMPLETED |                 │
│   OVERDUE, MISSED, SKIPPED)     │   WITHDRAWN | EXPIRED)                 │
│                                 │                                        │
├─────────────────────────────────┼────────────────────────────────────────┤
│  [Stacked Bar]                  │  [Line Chart]                          │
│  Timeliness Distribution        │  Enrollment Trends                     │
│  (EARLY | ON_TIME | LATE)       │  (weekly/monthly)                      │
│  per step                       │                                        │
│                                 │                                        │
└─────────────────────────────────┴────────────────────────────────────────┘
```

### Key Queries

**Completion Funnel:**
```sql
SELECT
    si.action_id,
    count(DISTINCT pi.patient_id) AS reached,
    countDistinctIf(pi.patient_id, si.state = 'COMPLETED') AS completed,
    round(countDistinctIf(pi.patient_id, si.state = 'COMPLETED') / 
          nullIf(count(DISTINCT pi.patient_id), 0) * 100, 1) AS completion_rate_pct
FROM step_instances si FINAL
JOIN protocol_instances pi FINAL ON si.protocol_instance_id = pi.id
WHERE pi.protocol_definition_id = '{{ protocol_id }}'
GROUP BY si.action_id
ORDER BY si.action_id;
```

**Timeliness Distribution:**
```sql
SELECT
    si.action_id,
    si.completion_status,
    count() AS count
FROM step_instances si FINAL
JOIN protocol_instances pi FINAL ON si.protocol_instance_id = pi.id
WHERE pi.protocol_definition_id = '{{ protocol_id }}'
    AND si.state = 'COMPLETED'
    AND si.completion_status IS NOT NULL
GROUP BY si.action_id, si.completion_status;
```

---

## 8.5 Dashboard 8: Receiver-Adaptor Performance

**Purpose:** Detailed analysis of each receiver-adaptor's delivery performance — success rates, failures, latency, and error patterns per adaptor and destination.

### Layout

```
┌──────────────────────────────────────────────────────────────────────────┐
│  FILTERS: [Date Range] [Adaptor Name] [Destination] [Severity]           │
├────────────────┬────────────────┬────────────────┬───────────────────────┤
│  KPI: Total    │  KPI: Success  │  KPI: Failed   │  KPI: Avg Latency     │
│  Deliveries    │  Rate %        │  Deliveries    │  (ms)                 │
├────────────────┴────────────────┴────────────────┴───────────────────────┤
│                                                                          │
│  [Multi-Line Chart] Delivery Success Rate Over Time (per adaptor)        │
│  (one line per adaptor_name, y-axis = success_rate_pct)                  │
│                                                                          │
├─────────────────────────────────┬────────────────────────────────────────┤
│  [Stacked Bar]                  │  [Heatmap]                             │
│  Delivery Outcomes per Adaptor  │  Adaptor × Destination Matrix          │
│  (DELIVERED | FAILED |          │  (color = success rate)                │
│   CANCELLED)                    │                                        │
│                                 │                                        │
├─────────────────────────────────┼────────────────────────────────────────┤
│  [Line Chart]                   │  [Bar Chart]                           │
│  P95 Latency Trend              │  Top Error Messages                    │
│  (per adaptor, over time)       │  (by frequency, last 7 days)          │
│                                 │                                        │
├─────────────────────────────────┴────────────────────────────────────────┤
│                                                                          │
│  [Table] Adaptor Performance Scorecard                                   │
│  adaptor_name | endpoint | destination | delivered | failed |            │
│  success_rate | avg_latency | p95_latency | last_error                   │
│                                                                          │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  [Table] Recent Failures (last 24h, drill-down)                          │
│  id | adaptor | destination | status | http_code | error | created_at    │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

### KPI Queries

**Total Deliveries & Success Rate:**
```sql
SELECT
    sum(total_deliveries) AS total_deliveries,
    sum(delivered) AS total_delivered,
    sum(failed + cancelled) AS total_failed,
    round(sum(delivered) / nullIf(sum(total_deliveries), 0) * 100, 1) AS success_rate_pct,
    round(avg(avg_latency_ms), 0) AS avg_latency_ms
FROM mv_delivery_performance_hourly
WHERE hour BETWEEN '{{ start_date }}' AND '{{ end_date }}'
    {% if adaptor_name %} AND adaptor_name = '{{ adaptor_name }}' {% endif %};
```

**Success Rate Trend (per adaptor):**
```sql
SELECT
    toStartOfHour(hour) AS ts,
    adaptor_name,
    destination,
    sum(total_deliveries) AS total,
    sum(delivered) AS delivered,
    round(sum(delivered) / nullIf(sum(total_deliveries), 0) * 100, 1) AS success_rate_pct,
    round(avg(avg_latency_ms), 0) AS avg_latency_ms,
    max(p95_latency_ms) AS p95_latency_ms
FROM mv_delivery_performance_hourly
WHERE hour BETWEEN '{{ start_date }}' AND '{{ end_date }}'
GROUP BY ts, adaptor_name, destination
ORDER BY ts;
```

**Adaptor × Destination Matrix (heatmap):**
```sql
SELECT
    adaptor_name,
    destination,
    sum(total_deliveries) AS total,
    sum(delivered) AS delivered,
    round(sum(delivered) / nullIf(sum(total_deliveries), 0) * 100, 1) AS success_rate_pct
FROM mv_delivery_performance_hourly
WHERE hour >= today() - INTERVAL 7 DAY
GROUP BY adaptor_name, destination
ORDER BY success_rate_pct ASC;
```

**Failure Drill-Down (recent errors):**
```sql
SELECT
    id,
    adaptor_name,
    destination,
    action_type,
    severity,
    status,
    http_status_code,
    error_message,
    attempt_count,
    created_at,
    latency_ms
FROM intelligence_deliveries FINAL
WHERE status IN ('FAILED', 'CANCELLED')
    AND created_at >= now() - INTERVAL 24 HOUR
    {% if adaptor_name %} AND adaptor_name = '{{ adaptor_name }}' {% endif %}
ORDER BY created_at DESC
LIMIT 100;
```

**Retry Analysis (per adaptor):**
```sql
SELECT
    adaptor_name,
    destination,
    count() AS total_deliveries,
    countIf(attempt_count > 1) AS retried,
    round(countIf(attempt_count > 1) / nullIf(count(), 0) * 100, 1) AS retry_rate_pct,
    max(attempt_count) AS max_attempts_seen
FROM intelligence_deliveries FINAL
WHERE created_at BETWEEN '{{ start_date }}' AND '{{ end_date }}'
GROUP BY adaptor_name, destination
ORDER BY retry_rate_pct DESC;
```

---

## 8.6 Dashboard 9: Intelligence & Triggers

**Purpose:** Analyze intelligence events — what triggers are firing, severity distribution, action types, and which destinations receive the most triggers.

### Layout

```
┌──────────────────────────────────────────────────────────────────────────┐
│  FILTERS: [Date Range] [Severity] [Action Type] [Destination] [Protocol] │
├────────────────┬────────────────┬────────────────┬───────────────────────┤
│  KPI: Total    │  KPI: Critical │  KPI: Unique   │  KPI: Top             │
│  Triggers      │  + High        │  Patients      │  Destination          │
├────────────────┴────────────────┴────────────────┴───────────────────────┤
│                                                                          │
│  [Stacked Area] Trigger Volume Over Time (by severity)                   │
│                                                                          │
├─────────────────────────────────┬────────────────────────────────────────┤
│  [Donut Chart]                  │  [Bar Chart]                           │
│  Triggers by Action Type        │  Triggers by Destination              │
│  (CommunicationRequest,         │  (grouped by step_state)              │
│   Task, ServiceRequest)         │                                        │
│                                 │                                        │
├─────────────────────────────────┼────────────────────────────────────────┤
│  [Heatmap]                      │  [Bar Chart]                           │
│  Severity × Step State          │  Top Protocols by Trigger Count        │
│  Matrix                         │  (which protocols fire most triggers)  │
│  (count per cell)               │                                        │
│                                 │                                        │
├─────────────────────────────────┴────────────────────────────────────────┤
│                                                                          │
│  [Table] Recent Triggers (drill-down)                                    │
│  patient | protocol | action_type | severity | destination | detected_at │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

### Key Queries

**Trigger Volume by Severity (trend):**
```sql
SELECT
    toStartOfDay(detected_at) AS day,
    severity,
    count() AS triggers
FROM intelligence_events
WHERE detected_at BETWEEN '{{ start_date }}' AND '{{ end_date }}'
GROUP BY day, severity
ORDER BY day;
```

**Action Type × Destination breakdown:**
```sql
SELECT
    action_type,
    intelligence_destination,
    step_state,
    count() AS count,
    uniq(subject) AS unique_patients
FROM intelligence_events
WHERE detected_at BETWEEN '{{ start_date }}' AND '{{ end_date }}'
GROUP BY action_type, intelligence_destination, step_state
ORDER BY count DESC;
```

---

## 8.7 Dashboard 10: Practitioner Activity

**Purpose:** Analyze individual practitioner workload, event volumes, and correlation with patient outcomes.

### Layout

```
┌──────────────────────────────────────────────────────────────────────────┐
│  FILTERS: [Date Range] [Facility] [Practitioner] [Resource Type]         │
├────────────────┬────────────────┬────────────────┬───────────────────────┤
│  KPI: Active   │  KPI: Total    │  KPI: Avg      │  KPI: Deviation       │
│  Practitioners │  Events        │  Events/       │  Correlation %        │
│                │                │  Practitioner  │                       │
├────────────────┴────────────────┴────────────────┴───────────────────────┤
│                                                                          │
│  [Bar Chart] Top 20 Practitioners by Event Volume                        │
│  (horizontal bars, color-coded by facility)                              │
│                                                                          │
├─────────────────────────────────┬────────────────────────────────────────┤
│  [Treemap]                      │  [Line Chart]                          │
│  Practitioner × Resource Type   │  Practitioner Activity Over Time       │
│  (what types of events each     │  (selected practitioners)             │
│   practitioner handles)         │                                        │
│                                 │                                        │
├─────────────────────────────────┼────────────────────────────────────────┤
│  [Scatter Plot]                 │  [Table]                               │
│  Events vs Deviation Rate       │  Practitioner Scorecard                │
│  per practitioner               │  practitioner | events | patients |    │
│  (identify outliers)            │  resource_types | deviation_pct |      │
│                                 │  last_active                           │
├─────────────────────────────────┴────────────────────────────────────────┤
│                                                                          │
│  [Table] Patient List per Practitioner (drill-down)                      │
│  patient_id | events | last_event | protocols | compliance_status        │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

### Key Queries

**Practitioner Scorecard:**
```sql
SELECT
    practitioner_ref,
    facility_id,
    count() AS total_events,
    uniq(patient_id) AS unique_patients,
    count(DISTINCT resource_type) AS resource_types,
    max(event_time) AS last_active
FROM events_fact
WHERE practitioner_ref IS NOT NULL
    AND event_time BETWEEN '{{ start_date }}' AND '{{ end_date }}'
    {% if facility_id %} AND facility_id = '{{ facility_id }}' {% endif %}
GROUP BY practitioner_ref, facility_id
ORDER BY total_events DESC
LIMIT 50;
```

**Practitioner × Deviation Correlation:**
```sql
SELECT
    ef.practitioner_ref,
    ef.facility_id,
    uniq(ef.patient_id) AS total_patients,
    uniqIf(ef.patient_id, d.id IS NOT NULL) AS patients_with_deviations,
    round(
        uniqIf(ef.patient_id, d.id IS NOT NULL) /
        nullIf(uniq(ef.patient_id), 0) * 100, 1
    ) AS deviation_patient_pct
FROM events_fact ef
LEFT JOIN protocol_instances pi FINAL ON ef.patient_id = pi.patient_id
LEFT JOIN deviations d ON d.protocol_instance_id = pi.id
    AND d.detected_at >= today() - INTERVAL 30 DAY
WHERE ef.practitioner_ref IS NOT NULL
    AND ef.event_time >= today() - INTERVAL 30 DAY
GROUP BY ef.practitioner_ref, ef.facility_id
ORDER BY deviation_patient_pct DESC;
```

---

## 8.8 Dashboard 10.5: Scheduler Timeliness

**Purpose:** Analyze scheduler effectiveness — how quickly steps transition between states, escalation rates, and scheduler health.

### Layout

```
┌──────────────────────────────────────────────────────────────────────────┐
│  FILTERS: [Date Range] [Transition Type] [Protocol]                      │
├────────────────┬────────────────┬────────────────┬───────────────────────┤
│  KPI: Total    │  KPI: PENDING  │  KPI: DUE →    │  KPI: Escalation      │
│  Transitions   │  → DUE         │  OVERDUE       │  Rate %               │
├────────────────┴────────────────┴────────────────┴───────────────────────┤
│                                                                          │
│  [Stacked Area] Transition Volume Over Time (by type)                    │
│  (PENDING_TO_DUE | DUE_TO_OVERDUE | OVERDUE_TO_MISSED)                   │
│                                                                          │
├─────────────────────────────────┬────────────────────────────────────────┤
│  [Donut Chart]                  │  [Bar Chart]                           │
│  Transition Type Distribution   │  Avg Time Between Transitions          │
│  (what % of steps escalate      │  (how long before DUE→OVERDUE,        │
│   beyond DUE?)                  │   OVERDUE→MISSED)                     │
│                                 │                                        │
├─────────────────────────────────┼────────────────────────────────────────┤
│  [Line Chart]                   │  [Table]                               │
│  Escalation Rate Trend          │  Steps with Multiple Transitions       │
│  (% of DUE that become OVERDUE, │  step_id | protocol | transitions |   │
│   % of OVERDUE that become      │  current_state | days_in_state         │
│   MISSED — weekly)              │                                        │
│                                 │                                        │
└─────────────────────────────────┴────────────────────────────────────────┘
```

### Key Queries

**Transition Volume Trend:**
```sql
SELECT
    toStartOfDay(triggered_at) AS day,
    transition_type,
    count() AS transitions
FROM step_transitions
WHERE triggered_at BETWEEN '{{ start_date }}' AND '{{ end_date }}'
GROUP BY day, transition_type
ORDER BY day;
```

**Escalation Rate (DUE → OVERDUE and OVERDUE → MISSED):**
```sql
SELECT
    toStartOfWeek(triggered_at) AS week,
    countIf(transition_type = 'PENDING_TO_DUE') AS became_due,
    countIf(transition_type = 'DUE_TO_OVERDUE') AS became_overdue,
    countIf(transition_type = 'OVERDUE_TO_MISSED') AS became_missed,
    round(countIf(transition_type = 'DUE_TO_OVERDUE') /
          nullIf(countIf(transition_type = 'PENDING_TO_DUE'), 0) * 100, 1) AS due_to_overdue_pct,
    round(countIf(transition_type = 'OVERDUE_TO_MISSED') /
          nullIf(countIf(transition_type = 'DUE_TO_OVERDUE'), 0) * 100, 1) AS overdue_to_missed_pct
FROM step_transitions
WHERE triggered_at BETWEEN '{{ start_date }}' AND '{{ end_date }}'
GROUP BY week
ORDER BY week;
```

**Avg Time Between Escalations (per step):**
```sql
SELECT
    si.action_id,
    pd.name AS protocol_name,
    round(avg(dateDiff('hour', st_due.triggered_at, st_overdue.triggered_at)), 1) AS avg_hours_due_to_overdue,
    round(avg(dateDiff('hour', st_overdue.triggered_at, st_missed.triggered_at)), 1) AS avg_hours_overdue_to_missed,
    count(DISTINCT st_due.step_instance_id) AS sample_size
FROM step_transitions st_due
LEFT JOIN step_transitions st_overdue
    ON st_due.step_instance_id = st_overdue.step_instance_id
    AND st_overdue.transition_type = 'DUE_TO_OVERDUE'
LEFT JOIN step_transitions st_missed
    ON st_due.step_instance_id = st_missed.step_instance_id
    AND st_missed.transition_type = 'OVERDUE_TO_MISSED'
JOIN step_instances si FINAL ON st_due.step_instance_id = si.id
JOIN protocol_instances pi FINAL ON si.protocol_instance_id = pi.id
JOIN protocol_definitions pd FINAL ON pi.protocol_definition_id = pd.id
WHERE st_due.transition_type = 'PENDING_TO_DUE'
    AND st_due.triggered_at BETWEEN '{{ start_date }}' AND '{{ end_date }}'
GROUP BY si.action_id, pd.name
ORDER BY avg_hours_due_to_overdue;
```

---

## 9. Superset Configuration

### 9.1 Datasets (Virtual Tables)

Register these as Superset datasets for drag-and-drop chart building:

| Dataset Name | Source | Description |
|-------------|--------|-------------|
| `Events Fact` | `events_fact` | All clinical events (enriched) |
| `Event Volume (Hourly)` | `event_volume_hourly` | Pre-aggregated event counts |
| `Protocol Instances` | `protocol_instances FINAL` | Patient enrollments |
| `Step Instances` | `step_instances FINAL` | Protocol step tracking |
| `Deviations` | `deviations` | Compliance deviations |
| `Inbound Events` | `inbound_events` | Ingestion audit |
| `Intelligence Events` | `intelligence_events` | Triggers and actions |
| `Protocol Definitions` | `protocol_definitions FINAL` | Protocol metadata |
| `Compliance Summary (MV)` | `mv_compliance_summary` | Pre-aggregated compliance |
| `Deviation Trends (MV)` | `mv_deviation_trends` | Pre-aggregated deviations |
| `Intelligence Deliveries` | `intelligence_deliveries FINAL` | Delivery outcomes per adaptor |
| `Intelligence Event Logs` | `intelligence_event_logs FINAL` | Intelligence trigger audit trail |
| `Action Definitions` | `action_definitions FINAL` | Action template metadata |
| `Delivery Performance (MV)` | `mv_delivery_performance_hourly` | Pre-aggregated delivery metrics |
| `Step Transitions` | `step_transitions` | Scheduler state transitions |
| `Scheduler Transitions (MV)` | `mv_scheduler_transitions_daily` | Pre-aggregated scheduler metrics |

### 9.2 Roles & Permissions

| Superset Role | CCE Equivalent | Access |
|---------------|---------------|--------|
| Admin | Platform admin | Full access; create/edit dashboards; manage users |
| Alpha | Dashboard creators | Create charts/dashboards; SQL Lab access |
| Gamma + RLS | Clinical managers | View dashboards; filtered by facility |
| Public | — | Disabled (no anonymous access) |

### 9.3 Row-Level Security (Facility Filtering)

```python
# Superset RLS rule for facility-based access
# Applied to datasets: protocol_instances, step_instances, deviations, events_fact

# Rule: facility_id = {{ current_user.facility_id }}
# Clause: facility_id IN ('FAC-001', 'FAC-002')  -- per user/role assignment
```

### 9.4 Scheduled Reports

| Report | Schedule | Recipients | Format |
|--------|----------|------------|--------|
| Weekly Compliance Summary | Monday 8:00 AM | Clinical managers | PDF |
| Daily Deviation Alert | Daily 7:00 AM | Supervisors | Email (inline) |
| Monthly Facility Ranking | 1st of month | District health officers | CSV + PDF |
| Weekly Ingestion Quality | Friday 5:00 PM | Integration team | Email |

---

## 10. Alert Rules

Configure in Superset Alerts:

| Alert | Condition | Action | Severity |
|-------|-----------|--------|----------|
| High Deviation Rate | Deviations > 50 in last hour | Email to supervisors | Warning |
| Source Quality Drop | Acceptance rate < 70% for any source | Email + Slack | Critical |
| Zero Events | No events received in 30 minutes | PagerDuty / Slack | Critical |
| Pipeline Lag | Flink consumer lag > 100k messages | Slack #ops channel | Warning |
| Adaptor Failure Spike | Any adaptor success rate < 80% in last hour | Email + Slack | Critical |
| Adaptor Timeout | Adaptor P95 latency > 10s for 15 min | Slack #ops channel | Warning |
| Adaptor Down | Zero deliveries from adaptor in 30 min | PagerDuty / Slack | Critical |

---

## 11. Export Capabilities

Superset provides built-in export capabilities replacing the custom `ExportService`:

| Format | Method | Use Case |
|--------|--------|----------|
| CSV | Chart → "Download as CSV" | Ad-hoc data export |
| Excel | Chart → "Download as Excel" | Formatted reports |
| PDF | Dashboard → "Download as PDF" | Management reporting |
| Image (PNG) | Chart → "Download as image" | Presentations |
| Scheduled email | Alerts & Reports | Automated delivery |
| API | Superset REST API | Programmatic access |

---

## 12. Migration from insights-ui

### Feature Mapping

| insights-ui Page | Superset Dashboard | Notes |
|-----------------|-------------------|-------|
| Compliance Summary | Dashboard 2: Compliance Monitoring | Enhanced with trends |
| Patient Timeline | Embedded in Dashboard 6 (drill-down) | ClickHouse FINAL for latest state |
| Deviation Trends | Dashboard 3: Deviation Analytics | Richer visualizations |
| Facility Overview | Dashboard 5: Facility Performance | Added ranking + comparison |
| Event Volume | Dashboard 4: Event Volume & Ingestion | Combined with ingestion quality |
| Protocol Details | Dashboard 7: Protocol Analytics | Added funnel + timeliness |
| Ingestion Pipeline | Dashboard 4 (lower section) | Unified with event volume |
| Source Comparison | Dashboard 4 → drill-down | Interactive cross-filter |

### User Training Plan

| Session | Audience | Duration | Content |
|---------|----------|----------|---------|
| 1 | All dashboard users | 1 hour | Navigating Superset; using filters; exporting data |
| 2 | Clinical managers | 1 hour | Compliance & deviation dashboards; interpreting metrics |
| 3 | Dashboard creators | 2 hours | SQL Lab; creating charts; building dashboards |
| 4 | Admins | 1 hour | User management; RLS; scheduled reports |

---

## 13. Dashboard 11: Pipeline Health (Grafana)

**Purpose:** Monitor the data pipeline infrastructure — Flink jobs, Kafka consumer lag, ClickHouse performance, and end-to-end latency. Deployed in **Grafana** (not Superset) to leverage native Prometheus integration.

### Layout

```
┌──────────────────────────────────────────────────────────────────────────┐
│  FILTERS: [Time Range] [Job Name] [Topic]                                │
├────────────────┬────────────────┬────────────────┬───────────────────────┤
│  KPI: Flink    │  KPI: Max      │  KPI: CH       │  KPI: End-to-End      │
│  Jobs Running  │  Consumer Lag  │  Insert Rate   │  Latency (P95)        │
├────────────────┴────────────────┴────────────────┴───────────────────────┤
│                                                                          │
│  [Multi-Line] Kafka Consumer Lag by Topic/Partition (messages)            │
│  (one line per topic: cce.events.inbound, cce.intelligence.triggers,     │
│   cce.scheduler.triggers)                                                │
│                                                                          │
├─────────────────────────────────┬────────────────────────────────────────┤
│  [Line Chart]                   │  [Line Chart]                          │
│  Flink Checkpoint Duration      │  Flink Throughput                      │
│  (ms, per job)                  │  (records/sec per job)                 │
│                                 │                                        │
├─────────────────────────────────┼────────────────────────────────────────┤
│  [Gauge]                        │  [Line Chart]                          │
│  Flink Backpressure             │  ClickHouse Queries in Flight          │
│  (per task slot)                │  + Merge Operations                    │
│                                 │                                        │
├─────────────────────────────────┼────────────────────────────────────────┤
│  [Line Chart]                   │  [Line Chart]                          │
│  End-to-End Latency             │  ClickHouse Disk Usage                 │
│  (event_time → processed_at)    │  (GB used / total, with threshold)    │
│  P50, P95, P99                  │                                        │
│                                 │                                        │
├─────────────────────────────────┼────────────────────────────────────────┤
│  [Status Panel]                 │  [Table]                               │
│  Kafka Connect Connectors       │  CDC Replication Slot Status           │
│  (green=RUNNING, red=FAILED)    │  slot_name | lag_bytes | active        │
│                                 │                                        │
└─────────────────────────────────┴────────────────────────────────────────┘
```

### Key Queries (Prometheus / ClickHouse)

**End-to-End Latency (ClickHouse query for Grafana):**
```sql
SELECT
    toStartOfMinute(processed_at) AS minute,
    quantile(0.50)(dateDiff('second', event_time, processed_at)) AS p50_latency_sec,
    quantile(0.95)(dateDiff('second', event_time, processed_at)) AS p95_latency_sec,
    quantile(0.99)(dateDiff('second', event_time, processed_at)) AS p99_latency_sec
FROM events_fact
WHERE processed_at >= now() - INTERVAL 1 HOUR
GROUP BY minute
ORDER BY minute;
```

**ClickHouse Insert Rate (Prometheus):**
```promql
rate(ClickHouseProfileEvents_InsertedRows[5m])
```

**Flink Checkpoint Duration (Prometheus):**
```promql
flink_jobmanager_job_lastCheckpointDuration{job_name=~"event-enrichment|intelligence-tracker|scheduler-tracker"}
```

**Kafka Consumer Lag (Prometheus):**
```promql
kafka_consumer_group_lag{group="cce-data-pipeline-flink",topic=~"cce\\..*"}
```

**CDC Replication Slot Lag (ClickHouse query via pg_stat_replication view):**
```sql
-- Run against source PostgreSQL (Grafana PostgreSQL datasource)
SELECT
    slot_name,
    pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS lag_bytes,
    active
FROM pg_replication_slots
WHERE slot_name = 'cce_analytics_slot';
```

### Alerting Rules (Grafana)

| Alert | PromQL / Query | Threshold | Severity |
|-------|---------------|-----------|----------|
| Flink job down | `flink_jobmanager_job_status != 1` | Any job not RUNNING for 2min | Critical |
| Consumer lag high | `kafka_consumer_group_lag > 100000` | Sustained 5 min | Warning |
| E2E latency high | P95 latency > 300s | Sustained 5 min | Warning |
| ClickHouse disk full | `ClickHouseAsyncMetrics_DiskUsed / DiskTotal > 0.8` | — | Warning |
| CDC slot lag | `lag_bytes > 100MB` | Sustained 5 min | Critical |
| Flink checkpoint failing | `increase(flink_jobmanager_job_numberOfFailedCheckpoints[10m]) > 3` | — | Warning |
