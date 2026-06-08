-- CCE Analytics ClickHouse Schema
-- PeerDB CDC architecture: base tables auto-created by PeerDB mirror from PostgreSQL
--
-- PeerDB creates ReplacingMergeTree(_peerdb_version) tables with:
--   - All PostgreSQL columns (matching types)
--   - _peerdb_synced_at DateTime64(6) — sync timestamp
--   - _peerdb_is_deleted Boolean — soft-delete marker
--   - _peerdb_version Int64 — version for ReplacingMergeTree dedup
--   - ORDER BY (primary_key_columns)
--
-- This script adds MATERIALIZED columns for zero-cost JSON extraction AFTER
-- PeerDB creates the tables. Run AFTER initial snapshot completes.
--
-- Execution order:
--   1. Create PeerDB mirror (see connectors/peerdb-mirror.sql)
--   2. Wait for initial snapshot to complete
--   3. Pause mirror:  SELECT pg_terminate_backend(...) or PeerDB UI
--   4. Run this script: clickhouse-client --database cce_analytics < schema/01-create-tables.sql
--   5. Materialize columns for snapshot data (see below)
--   6. Resume mirror via PeerDB UI
--   7. Run schema/02-create-materialized-views.sql
--   8. Run schema/03-create-indexes-projections.sql
--   9. Run schema/04-create-dictionary.sql
--  10. Backfill MVs with historical data (see deployment-guide.md §14)

CREATE DATABASE IF NOT EXISTS cce_analytics;

USE cce_analytics;

-- ============================================================
-- MATERIALIZED COLUMNS (added to PeerDB-created tables)
-- ============================================================

-- inbound_event_logs: extract key fields from raw_payload JSONB at insert time
-- These columns power projections, indexes, and all inbound_event_logs MVs
ALTER TABLE inbound_event_logs
    ADD COLUMN IF NOT EXISTS subject String
        MATERIALIZED JSONExtractString(raw_payload, 'subject');
ALTER TABLE inbound_event_logs
    ADD COLUMN IF NOT EXISTS event_type String
        MATERIALIZED JSONExtractString(raw_payload, 'type');
ALTER TABLE inbound_event_logs
    ADD COLUMN IF NOT EXISTS facility_id String
        MATERIALIZED JSONExtractString(raw_payload, 'facilityid');
ALTER TABLE inbound_event_logs
    ADD COLUMN IF NOT EXISTS event_time Nullable(DateTime64(3))
        MATERIALIZED toDateTime64OrNull(JSONExtractString(raw_payload, 'time'), 3);
ALTER TABLE inbound_event_logs
    ADD COLUMN IF NOT EXISTS resource_type String
        MATERIALIZED JSONExtractString(JSONExtractRaw(raw_payload, 'data'), 'resourceType');
ALTER TABLE inbound_event_logs
    ADD COLUMN IF NOT EXISTS patient_id String ALIAS subject;
ALTER TABLE inbound_event_logs
    ADD COLUMN IF NOT EXISTS practitioner_ref String
        MATERIALIZED JSONExtractString(JSONExtractRaw(raw_payload, 'data'), 'practitionerRef');
ALTER TABLE inbound_event_logs
    ADD COLUMN IF NOT EXISTS practitioner_display String
        MATERIALIZED JSONExtractString(JSONExtractRaw(raw_payload, 'data'), 'practitionerDisplay');

-- ============================================================
-- MATERIALIZE COLUMNS (backfill for initial snapshot data)
-- ============================================================
-- These populate MATERIALIZED columns for rows inserted BEFORE the ALTER TABLE.
-- Only needed once after initial snapshot. May take time on large tables.

ALTER TABLE inbound_event_logs MATERIALIZE COLUMN subject;
ALTER TABLE inbound_event_logs MATERIALIZE COLUMN event_type;
ALTER TABLE inbound_event_logs MATERIALIZE COLUMN facility_id;
ALTER TABLE inbound_event_logs MATERIALIZE COLUMN event_time;
ALTER TABLE inbound_event_logs MATERIALIZE COLUMN resource_type;
ALTER TABLE inbound_event_logs MATERIALIZE COLUMN practitioner_ref;
ALTER TABLE inbound_event_logs MATERIALIZE COLUMN practitioner_display;
