-- CCE Analytics ClickHouse Schema
-- Materialized Views (pre-aggregated from CDC tables)
-- Run: clickhouse-client --database cce_analytics < schema/02-create-materialized-views.sql

USE cce_analytics;

-- ============================================================
-- Event Volume (from inbound_event_logs CDC, replaces Flink tumbling window)
-- ============================================================

-- Hourly event volume by facility/source/type (replaces event_volume_hourly table + Flink job)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_event_volume_hourly
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (facility_id, source, event_type, resource_type, hour)
AS SELECT
    toStartOfHour(received_at) AS hour,
    facility_id,
    source,
    event_type,
    resource_type,
    count() AS event_count
FROM inbound_event_logs
WHERE status = 'ACCEPTED'
GROUP BY hour, facility_id, source, event_type, resource_type;

-- Daily rollup from inbound_event_logs
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_event_volume_daily
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (facility_id, source, event_type, resource_type, day)
AS SELECT
    toStartOfDay(received_at) AS day,
    facility_id,
    source,
    event_type,
    resource_type,
    count() AS event_count
FROM inbound_event_logs
WHERE status = 'ACCEPTED'
GROUP BY day, facility_id, source, event_type, resource_type;

-- ============================================================
-- Protocol Compliance
-- ============================================================

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

-- Deviation aggregation by protocol instance
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

-- ============================================================
-- Ingestion Quality
-- ============================================================

-- Ingestion source quality metrics
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

-- ============================================================
-- Intelligence (from intelligence_event_logs CDC)
-- ============================================================

-- Intelligence trigger aggregation (from CDC table, replaces Kafka-sourced intelligence_events)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_intelligence_summary
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (action_type, intelligence_destination, trigger_reason, step_state, day)
AS SELECT
    toStartOfDay(created_at) AS day,
    action_type,
    intelligence_destination,
    step_state,
    trigger_reason,
    countState() AS trigger_count,
    uniqState(subject) AS unique_patients
FROM intelligence_event_logs
GROUP BY day, action_type, intelligence_destination, step_state, trigger_reason;

-- Delivery performance hourly (from intelligence_deliveries CDC)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_delivery_performance_hourly
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (adaptor_name, destination, action_type, severity, hour)
AS SELECT
    toStartOfHour(created_at)         AS hour,
    adaptor_name,
    endpoint_url,
    destination,
    action_type,
    severity,
    countState()                       AS total_deliveries,
    countIfState(status = 'DELIVERED') AS delivered,
    countIfState(status = 'FAILED')   AS failed,
    countIfState(status = 'CANCELLED') AS cancelled,
    avgState(latency_ms)              AS avg_latency_ms,
    quantileState(0.95)(latency_ms)   AS p95_latency_ms,
    quantileState(0.99)(latency_ms)   AS p99_latency_ms,
    maxState(latency_ms)              AS max_latency_ms
FROM intelligence_deliveries
WHERE status IN ('DELIVERED', 'FAILED', 'CANCELLED')
GROUP BY hour, adaptor_name, endpoint_url, destination, action_type, severity;

-- ============================================================
-- Step/Scheduler (from step_instances CDC, replaces Kafka step_transitions)
-- ============================================================

-- Daily step state transitions (from step_instances CDC — state changes captured via ReplacingMergeTree)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_step_states_daily
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (state, completion_status, day)
AS SELECT
    toStartOfDay(updated_at) AS day,
    state,
    completion_status,
    countState() AS step_count,
    uniqState(protocol_instance_id) AS unique_protocols
FROM step_instances
GROUP BY day, state, completion_status;

-- ============================================================
-- Practitioner Metrics (from inbound_event_logs MATERIALIZED columns)
-- ============================================================

-- Practitioner activity summary (uses MATERIALIZED practitioner_ref/facility_id columns)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_practitioner_summary
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (facility_id, practitioner_ref, day)
AS SELECT
    toStartOfDay(received_at)   AS day,
    facility_id,
    practitioner_ref,
    anyState(practitioner_display) AS practitioner_display,
    countState()                AS event_count,
    uniqState(subject)          AS unique_patients,
    uniqState(resource_type)    AS resource_type_count
FROM inbound_event_logs
WHERE practitioner_ref != '' AND status = 'ACCEPTED'
GROUP BY day, facility_id, practitioner_ref;

-- ============================================================
-- Facility Metrics (from inbound_event_logs MATERIALIZED columns)
-- ============================================================

-- Facility activity summary
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_facility_summary
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (facility_id, resource_type, day)
AS SELECT
    toStartOfDay(received_at)   AS day,
    facility_id,
    resource_type,
    countState()                AS event_count,
    uniqState(subject)          AS unique_patients,
    uniqState(practitioner_ref) AS unique_practitioners
FROM inbound_event_logs
WHERE facility_id != '' AND status = 'ACCEPTED'
GROUP BY day, facility_id, resource_type;
