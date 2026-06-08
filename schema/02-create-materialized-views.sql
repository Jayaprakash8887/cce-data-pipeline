-- CCE Analytics ClickHouse Schema
-- Materialized Views (pre-aggregated from CDC tables)
-- Run: clickhouse-client --database cce_analytics < schema/02-create-materialized-views.sql

USE cce_analytics;

-- ============================================================
-- Event Volume (from inbound_event_logs CDC)
-- ============================================================

-- Hourly event volume by facility/source/type
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

-- ============================================================
-- Protocol Compliance
-- ============================================================

-- Protocol compliance: earliest enrollment and most recent update per protocol.
-- NOTE: total_enrollments / active_count / completed_count are intentionally excluded.
-- Those require current state, not accumulated state. Each PeerDB UPDATE arrives as a
-- new INSERT into the base table, so countIfState(status='ACTIVE') would double-count
-- every row that was ever updated. Query protocol_instances FINAL for live status counts.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_compliance_summary
ENGINE = AggregatingMergeTree()
ORDER BY (protocol_definition_id)
AS SELECT
    protocol_definition_id,
    minState(enrolled_at)  AS first_enrolled,
    maxState(updated_at)   AS last_updated
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

-- Intelligence trigger aggregation (from intelligence_event_logs CDC table)
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

-- ============================================================
-- Entity × Behavior Cross-Dimensional Views
-- ============================================================

-- Patient-level compliance: earliest enrollment and most recent update per patient/protocol.
-- NOTE: total_enrollments / active_count / completed_count are intentionally excluded.
-- countIfState(status='X') accumulates across every CDC event (INSERT + each UPDATE), so
-- a single row updated ACTIVE→COMPLETED would produce active_count=1 AND completed_count=1.
-- For current status breakdown, query protocol_instances FINAL directly.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_compliance_by_patient
ENGINE = AggregatingMergeTree()
ORDER BY (patient_id, protocol_definition_id)
AS SELECT
    patient_id,
    protocol_definition_id,
    protocol_canonical,
    minState(enrolled_at) AS first_enrolled,
    maxState(updated_at)  AS last_updated
FROM protocol_instances
GROUP BY patient_id, protocol_definition_id, protocol_canonical;

-- Patient-level deviations (JOIN deviations → protocol_instances for patient_id)
-- LEFT JOIN: if protocol_instances row hasn't arrived yet via CDC, the deviation is
-- still captured with a NULL patient_id rather than silently dropped.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_deviation_by_patient
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (patient_id, deviation_type, day)
AS SELECT
    toStartOfDay(d.detected_at) AS day,
    coalesce(pi.patient_id, '') AS patient_id,
    d.deviation_type,
    countState() AS deviation_count,
    uniqState(d.protocol_instance_id) AS unique_protocols
FROM deviations d
LEFT JOIN protocol_instances pi ON d.protocol_instance_id = pi.id
GROUP BY day, patient_id, d.deviation_type;

-- Patient-level intelligence actions (enables: intelligence actions per patient per type)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_intelligence_by_patient
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (subject, action_type, day)
AS SELECT
    toStartOfDay(created_at) AS day,
    subject,
    action_type,
    intelligence_destination,
    trigger_reason,
    countState() AS trigger_count
FROM intelligence_event_logs
GROUP BY day, subject, action_type, intelligence_destination, trigger_reason;




-- Intelligence triggers per protocol (enables: "which protocols generate the most alerts?")
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_intelligence_by_protocol
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (protocol_instance_id, action_type, day)
AS SELECT
    toStartOfDay(created_at) AS day,
    protocol_instance_id,
    action_type,
    intelligence_destination,
    trigger_reason,
    countState() AS trigger_count,
    uniqState(subject) AS unique_patients
FROM intelligence_event_logs
GROUP BY day, protocol_instance_id, action_type, intelligence_destination, trigger_reason;



-- ============================================================
-- Compliance Event Processing Quality
-- ============================================================

-- Compliance event processing quality per source/day
-- Enables: "How many compliance events processed successfully vs failed, by source?"
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_compliance_processing_quality
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (source, processing_status, day)
AS SELECT
    toStartOfDay(received_at) AS day,
    source,
    processing_status,
    count() AS event_count
FROM compliance_event_logs
GROUP BY day, source, processing_status;

-- ============================================================
-- Patient → Facility Latest Mapping (dict source)
-- ============================================================

-- Bounded MV that tracks patient's latest facility assignment.
-- Used as the SOURCE for dict_patient_facility instead of a full-table argMax scan.
-- ReplacingMergeTree(last_seen) keeps only the latest row per patient after merges.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_patient_facility_latest
ENGINE = ReplacingMergeTree(last_seen)
ORDER BY (patient_id)
AS SELECT
    subject AS patient_id,
    facility_id,
    received_at AS last_seen
FROM inbound_event_logs
WHERE subject != '' AND facility_id != '';
