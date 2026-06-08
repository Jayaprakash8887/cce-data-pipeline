-- CCE Analytics ClickHouse Schema
-- Secondary indexes and projections for CDC tables
-- Run: clickhouse-client --database cce_analytics < schema/03-create-indexes-projections.sql

USE cce_analytics;

-- ============================================================
-- SECONDARY INDEXES (bloom_filter for point lookups)
-- ============================================================

-- inbound_event_logs indexes
ALTER TABLE inbound_event_logs ADD INDEX IF NOT EXISTS idx_cloudevents_id cloudevents_id TYPE bloom_filter GRANULARITY 4;
ALTER TABLE inbound_event_logs ADD INDEX IF NOT EXISTS idx_correlation correlation_id TYPE bloom_filter GRANULARITY 4;

-- intelligence_event_logs indexes
ALTER TABLE intelligence_event_logs ADD INDEX IF NOT EXISTS idx_subject subject TYPE bloom_filter GRANULARITY 4;
ALTER TABLE intelligence_event_logs ADD INDEX IF NOT EXISTS idx_protocol_instance protocol_instance_id TYPE bloom_filter GRANULARITY 4;

-- intelligence_deliveries indexes
ALTER TABLE intelligence_deliveries ADD INDEX IF NOT EXISTS idx_intelligence_event intelligence_event_id TYPE bloom_filter GRANULARITY 4;

-- step_instances indexes
ALTER TABLE step_instances ADD INDEX IF NOT EXISTS idx_protocol_instance protocol_instance_id TYPE bloom_filter GRANULARITY 4;

-- ============================================================
-- PROJECTIONS (alternative sort orders for common access patterns)
-- ============================================================

-- Patient timeline: sorted by patient (subject) for fast single-patient queries
ALTER TABLE inbound_event_logs ADD PROJECTION IF NOT EXISTS prj_patient_timeline (
    SELECT *
    ORDER BY (subject, received_at)
);

-- Protocol lookup: optimized for protocol-based queries
ALTER TABLE protocol_instances ADD PROJECTION IF NOT EXISTS prj_protocol_lookup (
    SELECT *
    ORDER BY (protocol_definition_id, status)
);

-- Protocol deviations: optimized for deviation drill-downs by protocol
ALTER TABLE deviations ADD PROJECTION IF NOT EXISTS prj_protocol_deviations (
    SELECT *
    ORDER BY (protocol_instance_id, deviation_type, detected_at)
);

-- Facility timeline: sorted by facility for fast facility-scoped event queries
ALTER TABLE inbound_event_logs ADD PROJECTION IF NOT EXISTS prj_facility_timeline (
    SELECT *
    ORDER BY (facility_id, received_at, subject)
);

-- Materialize existing projections for data already inserted
ALTER TABLE inbound_event_logs MATERIALIZE PROJECTION prj_patient_timeline;
ALTER TABLE inbound_event_logs MATERIALIZE PROJECTION prj_facility_timeline;
ALTER TABLE protocol_instances MATERIALIZE PROJECTION prj_protocol_lookup;
ALTER TABLE deviations MATERIALIZE PROJECTION prj_protocol_deviations;
