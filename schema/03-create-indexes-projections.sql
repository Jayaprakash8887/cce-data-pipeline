-- CCE Analytics ClickHouse Schema
-- Secondary indexes, projections, and TTL policies
-- Run: clickhouse-client --database cce_analytics < schema/03-create-indexes-projections.sql

USE cce_analytics;

-- ============================================================
-- SECONDARY INDEXES (bloom_filter for point lookups)
-- ============================================================

-- inbound_event_logs
ALTER TABLE inbound_event_logs ADD INDEX IF NOT EXISTS idx_cloudevents_id     cloudevents_id    TYPE bloom_filter GRANULARITY 4;
ALTER TABLE inbound_event_logs ADD INDEX IF NOT EXISTS idx_correlation         correlation_id    TYPE bloom_filter GRANULARITY 4;
ALTER TABLE inbound_event_logs ADD INDEX IF NOT EXISTS idx_source              source            TYPE bloom_filter GRANULARITY 4;

-- protocol_instances
ALTER TABLE protocol_instances ADD INDEX IF NOT EXISTS idx_patient_id          patient_id                TYPE bloom_filter GRANULARITY 4;
ALTER TABLE protocol_instances ADD INDEX IF NOT EXISTS idx_protocol_definition  protocol_definition_id    TYPE bloom_filter GRANULARITY 4;

-- step_instances
ALTER TABLE step_instances ADD INDEX IF NOT EXISTS idx_protocol_instance        protocol_instance_id  TYPE bloom_filter GRANULARITY 4;
ALTER TABLE step_instances ADD INDEX IF NOT EXISTS idx_state                    state                 TYPE bloom_filter GRANULARITY 4;
ALTER TABLE step_instances ADD INDEX IF NOT EXISTS idx_action_id                action_id             TYPE bloom_filter GRANULARITY 4;

-- deviations
ALTER TABLE deviations ADD INDEX IF NOT EXISTS idx_protocol_instance            protocol_instance_id  TYPE bloom_filter GRANULARITY 4;
ALTER TABLE deviations ADD INDEX IF NOT EXISTS idx_step_instance                step_instance_id      TYPE bloom_filter GRANULARITY 4;

-- intelligence_event_logs
ALTER TABLE intelligence_event_logs ADD INDEX IF NOT EXISTS idx_subject          subject               TYPE bloom_filter GRANULARITY 4;
ALTER TABLE intelligence_event_logs ADD INDEX IF NOT EXISTS idx_protocol_instance protocol_instance_id  TYPE bloom_filter GRANULARITY 4;

-- intelligence_deliveries
ALTER TABLE intelligence_deliveries ADD INDEX IF NOT EXISTS idx_intelligence_event intelligence_event_id TYPE bloom_filter GRANULARITY 4;
ALTER TABLE intelligence_deliveries ADD INDEX IF NOT EXISTS idx_status             status                TYPE bloom_filter GRANULARITY 4;
ALTER TABLE intelligence_deliveries ADD INDEX IF NOT EXISTS idx_subject            subject               TYPE bloom_filter GRANULARITY 4;

-- ============================================================
-- MATERIALIZE INDEXES (backfill for initial snapshot data)
-- ============================================================

ALTER TABLE inbound_event_logs MATERIALIZE INDEX idx_cloudevents_id;
ALTER TABLE inbound_event_logs MATERIALIZE INDEX idx_correlation;
ALTER TABLE inbound_event_logs MATERIALIZE INDEX idx_source;

ALTER TABLE protocol_instances MATERIALIZE INDEX idx_patient_id;
ALTER TABLE protocol_instances MATERIALIZE INDEX idx_protocol_definition;

ALTER TABLE step_instances MATERIALIZE INDEX idx_protocol_instance;
ALTER TABLE step_instances MATERIALIZE INDEX idx_state;
ALTER TABLE step_instances MATERIALIZE INDEX idx_action_id;

ALTER TABLE deviations MATERIALIZE INDEX idx_protocol_instance;
ALTER TABLE deviations MATERIALIZE INDEX idx_step_instance;

ALTER TABLE intelligence_event_logs MATERIALIZE INDEX idx_subject;
ALTER TABLE intelligence_event_logs MATERIALIZE INDEX idx_protocol_instance;

ALTER TABLE intelligence_deliveries MATERIALIZE INDEX idx_intelligence_event;
ALTER TABLE intelligence_deliveries MATERIALIZE INDEX idx_status;
ALTER TABLE intelligence_deliveries MATERIALIZE INDEX idx_subject;

-- ============================================================
-- PROJECTIONS (alternative sort orders for common access patterns)
-- ============================================================

-- Patient timeline: sorted by patient for fast single-patient event queries
ALTER TABLE inbound_event_logs ADD PROJECTION IF NOT EXISTS prj_patient_timeline (
    SELECT *
    ORDER BY (subject, received_at)
);

-- Facility timeline: sorted by facility for facility-scoped event queries
ALTER TABLE inbound_event_logs ADD PROJECTION IF NOT EXISTS prj_facility_timeline (
    SELECT *
    ORDER BY (facility_id, received_at, subject)
);

-- Protocol enrollment lookup: optimized for protocol-level compliance rollups
ALTER TABLE protocol_instances ADD PROJECTION IF NOT EXISTS prj_protocol_lookup (
    SELECT *
    ORDER BY (protocol_definition_id, status)
);

-- Patient enrollment lookup: optimized for patient-centric compliance queries
-- (protocol_instances ORDER BY is (id); patient_id lookups need this projection)
ALTER TABLE protocol_instances ADD PROJECTION IF NOT EXISTS prj_patient_lookup (
    SELECT *
    ORDER BY (patient_id, status)
);

-- Step instances by protocol: most common JOIN/filter access pattern
-- (step_instances ORDER BY is (id); all compliance/scheduler queries filter by protocol_instance_id)
ALTER TABLE step_instances ADD PROJECTION IF NOT EXISTS prj_steps_by_protocol (
    SELECT *
    ORDER BY (protocol_instance_id, state, updated_at)
);

-- Protocol deviations: optimized for deviation drill-downs by protocol
ALTER TABLE deviations ADD PROJECTION IF NOT EXISTS prj_protocol_deviations (
    SELECT *
    ORDER BY (protocol_instance_id, deviation_type, detected_at)
);

-- Deviations by step: optimized for dashboard 03's step-level deviation join
ALTER TABLE deviations ADD PROJECTION IF NOT EXISTS prj_step_deviations (
    SELECT *
    ORDER BY (step_instance_id, detected_at)
);

-- ============================================================
-- MATERIALIZE PROJECTIONS (backfill for initial snapshot data)
-- ============================================================

ALTER TABLE inbound_event_logs  MATERIALIZE PROJECTION prj_patient_timeline;
ALTER TABLE inbound_event_logs  MATERIALIZE PROJECTION prj_facility_timeline;
ALTER TABLE protocol_instances  MATERIALIZE PROJECTION prj_protocol_lookup;
ALTER TABLE protocol_instances  MATERIALIZE PROJECTION prj_patient_lookup;
ALTER TABLE step_instances      MATERIALIZE PROJECTION prj_steps_by_protocol;
ALTER TABLE deviations          MATERIALIZE PROJECTION prj_protocol_deviations;
ALTER TABLE deviations          MATERIALIZE PROJECTION prj_step_deviations;

-- ============================================================
-- TTL POLICIES (storage retention for high-volume log tables)
-- ============================================================
-- Adjust intervals to match your regulatory retention requirements.
-- Healthcare regulations (e.g., HIPAA) typically require 7 years;
-- set hot-tier TTL to 90 days and cold-tier to 7 years accordingly.

-- Inbound events: 90 days hot retention
ALTER TABLE inbound_event_logs
    MODIFY TTL received_at + INTERVAL 90 DAY;

-- Intelligence event triggers: 90 days hot retention
ALTER TABLE intelligence_event_logs
    MODIFY TTL created_at + INTERVAL 90 DAY;

-- Delivery records: 90 days hot retention
ALTER TABLE intelligence_deliveries
    MODIFY TTL created_at + INTERVAL 90 DAY;

-- Compliance processing log: 90 days hot retention
ALTER TABLE compliance_event_logs
    MODIFY TTL received_at + INTERVAL 90 DAY;
