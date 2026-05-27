-- CCE Analytics ClickHouse Schema
-- Task 1.1: Secondary indexes and projections
-- Run: clickhouse-client --database cce_analytics < schema/03-create-indexes-projections.sql

USE cce_analytics;

-- ============================================================
-- SECONDARY INDEXES (bloom_filter for point lookups)
-- ============================================================

-- events_fact indexes
ALTER TABLE events_fact ADD INDEX IF NOT EXISTS idx_patient patient_id TYPE bloom_filter GRANULARITY 4;
ALTER TABLE events_fact ADD INDEX IF NOT EXISTS idx_practitioner practitioner_ref TYPE bloom_filter GRANULARITY 4;
ALTER TABLE events_fact ADD INDEX IF NOT EXISTS idx_correlation correlation_id TYPE bloom_filter GRANULARITY 4;

-- intelligence_events indexes
ALTER TABLE intelligence_events ADD INDEX IF NOT EXISTS idx_subject subject TYPE bloom_filter GRANULARITY 4;

-- step_transitions indexes
ALTER TABLE step_transitions ADD INDEX IF NOT EXISTS idx_step_instance step_instance_id TYPE bloom_filter GRANULARITY 4;

-- ============================================================
-- PROJECTIONS (alternative sort orders for common access patterns)
-- ============================================================

-- Patient timeline: sorted by patient for fast single-patient queries
ALTER TABLE events_fact ADD PROJECTION IF NOT EXISTS prj_patient_timeline (
    SELECT *
    ORDER BY (patient_id, event_time)
);

-- Protocol lookup: optimized for protocol-based queries
ALTER TABLE protocol_instances ADD PROJECTION IF NOT EXISTS prj_protocol_lookup (
    SELECT *
    ORDER BY (protocol_definition_id, status, facility_id)
);

-- Protocol deviations: optimized for deviation drill-downs by protocol
ALTER TABLE deviations ADD PROJECTION IF NOT EXISTS prj_protocol_deviations (
    SELECT *
    ORDER BY (protocol_definition_id, deviation_type, detected_at)
);

-- Materialize existing projections for data already inserted
ALTER TABLE events_fact MATERIALIZE PROJECTION prj_patient_timeline;
ALTER TABLE protocol_instances MATERIALIZE PROJECTION prj_protocol_lookup;
ALTER TABLE deviations MATERIALIZE PROJECTION prj_protocol_deviations;
