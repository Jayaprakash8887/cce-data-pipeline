-- CCE Analytics ClickHouse Schema
-- Task 1.1: Materialized Views (pre-aggregated)
-- Run: clickhouse-client --database cce_analytics < schema/02-create-materialized-views.sql

USE cce_analytics;

-- Daily rollup from hourly event volumes
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_event_volume_daily
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (facility_id, source, event_type, resource_type, day)
AS SELECT
    toStartOfDay(hour) AS day,
    facility_id,
    source,
    event_type,
    resource_type,
    sum(event_count) AS event_count
FROM event_volume_hourly
GROUP BY day, facility_id, source, event_type, resource_type;

-- Protocol compliance rates (AggregatingMergeTree for -State functions)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_compliance_summary
ENGINE = AggregatingMergeTree()
ORDER BY (protocol_definition_id)
AS SELECT
    protocol_definition_id,
    countState() AS total_enrollments,
    countIfState(status = 'COMPLETED') AS completed_count,
    countIfState(status = 'ACTIVE') AS active_count
FROM protocol_instances
GROUP BY protocol_definition_id;

-- Daily deviation counts by type
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_deviation_trends
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (deviation_type, day)
AS SELECT
    toStartOfDay(detected_at) AS day,
    deviation_type,
    count() AS deviation_count
FROM deviations
GROUP BY day, deviation_type;

-- Ingestion source quality metrics (uses materialized columns from raw_payload)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_ingestion_quality
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (source, status, rejection_reason, day)
AS SELECT
    toStartOfDay(received_at) AS day,
    source,
    status,
    rejection_reason,
    count() AS event_count
FROM inbound_event_logs
GROUP BY day, source, status, rejection_reason;

-- Deviation aggregation by protocol instance (facility resolved via JOINs at query time)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_deviation_by_protocol
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (protocol_instance_id, deviation_type, day)
AS SELECT
    toStartOfDay(detected_at) AS day,
    protocol_instance_id,
    deviation_type,
    count() AS deviation_count
FROM deviations
GROUP BY day, protocol_instance_id, deviation_type;

-- Intelligence trigger aggregation
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_intelligence_summary
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (severity, action_type, intelligence_destination, day)
AS SELECT
    toStartOfDay(detected_at) AS day,
    severity,
    action_type,
    intelligence_destination,
    step_state,
    count() AS trigger_count,
    uniq(subject) AS unique_patients
FROM intelligence_events
GROUP BY day, severity, action_type, intelligence_destination, step_state;

-- Delivery performance hourly (from intelligence_deliveries)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_delivery_performance_hourly
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (adaptor_name, destination, action_type, hour)
AS SELECT
    toStartOfHour(created_at)         AS hour,
    adaptor_name,
    endpoint_url,
    destination,
    action_type,
    severity,
    count()                           AS total_deliveries,
    countIf(status = 'DELIVERED')     AS delivered,
    countIf(status = 'FAILED')        AS failed,
    countIf(status = 'CANCELLED')     AS cancelled,
    avg(latency_ms)                   AS avg_latency_ms,
    quantile(0.95)(latency_ms)        AS p95_latency_ms,
    quantile(0.99)(latency_ms)        AS p99_latency_ms,
    max(latency_ms)                   AS max_latency_ms
FROM intelligence_deliveries
WHERE status IN ('DELIVERED', 'FAILED', 'CANCELLED')
GROUP BY hour, adaptor_name, endpoint_url, destination, action_type, severity;

-- Daily scheduler transition aggregation
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_scheduler_transitions_daily
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (transition_type, day)
AS SELECT
    toStartOfDay(triggered_at) AS day,
    transition_type,
    count() AS transition_count,
    uniq(step_instance_id) AS unique_steps
FROM step_transitions
GROUP BY day, transition_type;

-- Practitioner activity summary (for PractitionerRankingController)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_practitioner_summary
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (facility_id, practitioner_ref, day)
AS SELECT
    toStartOfDay(event_time)    AS day,
    facility_id,
    practitioner_ref,
    any(practitioner_display)   AS practitioner_display,
    count()                     AS event_count,
    uniq(patient_id)            AS unique_patients,
    uniq(resource_type)         AS resource_type_count
FROM events_fact
WHERE practitioner_ref IS NOT NULL AND practitioner_ref != ''
GROUP BY day, facility_id, practitioner_ref;
