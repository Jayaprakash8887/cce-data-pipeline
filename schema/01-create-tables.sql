-- CCE Analytics ClickHouse Schema
-- CDC-only architecture: all tables populated via Debezium CDC from PostgreSQL
-- Run: clickhouse-client --database cce_analytics < schema/01-create-tables.sql

CREATE DATABASE IF NOT EXISTS cce_analytics;

USE cce_analytics;

-- ============================================================
-- CDC TABLES (from Debezium/ClickHouse Sink)
-- ============================================================

-- Patient protocol enrollments
CREATE TABLE IF NOT EXISTS protocol_instances (
    id                      UUID,
    patient_id              String,
    protocol_definition_id  UUID,
    protocol_canonical      String,
    status                  LowCardinality(String),
    enrolled_at             DateTime64(3),
    created_at              DateTime64(3),
    updated_at              DateTime64(3),
    _version                UInt64
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY (id);

-- Protocol step instances
CREATE TABLE IF NOT EXISTS step_instances (
    id                      UUID,
    protocol_instance_id    UUID,
    action_id               String,
    repeat_index            UInt16,
    state                   LowCardinality(String),
    completion_status       LowCardinality(Nullable(String)),
    required_behavior       LowCardinality(Nullable(String)),
    due_date                Nullable(DateTime64(3)),
    overdue_date            Nullable(DateTime64(3)),
    missed_date             Nullable(DateTime64(3)),
    completed_at            Nullable(DateTime64(3)),
    completed_by_source     Nullable(String),
    completed_by_event_id   Nullable(UUID),
    created_at              DateTime64(3),
    updated_at              DateTime64(3),
    _version                UInt64
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY (id);

-- Compliance deviations (OVERDUE, MISSED, ORDER_VIOLATION)
CREATE TABLE IF NOT EXISTS deviations (
    id                      UUID,
    protocol_instance_id    UUID,
    step_instance_id        UUID,
    deviation_type          LowCardinality(String),
    detected_at             DateTime64(3),
    intelligence_event_id   Nullable(UUID),
    metadata                Nullable(String),
    _version                UInt64
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY toYYYYMM(detected_at)
ORDER BY (deviation_type, detected_at, id);

-- Ingestion audit trail (CDC from collector's inbound_event_log)
-- All CloudEvents fields are in raw_payload JSONB; materialized columns extract key fields
CREATE TABLE IF NOT EXISTS inbound_event_logs (
    id               UUID,
    cloudevents_id   String,
    source           LowCardinality(String),
    correlation_id   Nullable(String),
    raw_payload      String CODEC(ZSTD(3)),
    status           LowCardinality(String),
    rejection_reason LowCardinality(Nullable(String)),
    error_details    Nullable(String),
    received_at      DateTime64(3),
    _version         UInt64,
    -- Materialized columns (auto-extracted from raw_payload at insert time)
    subject          String MATERIALIZED JSONExtractString(raw_payload, 'subject'),
    event_type       String MATERIALIZED JSONExtractString(raw_payload, 'type'),
    facility_id      String MATERIALIZED JSONExtractString(raw_payload, 'facilityid'),
    event_time       Nullable(DateTime64(3)) MATERIALIZED
        toDateTime64OrNull(JSONExtractString(raw_payload, 'time'), 3),
    resource_type    String MATERIALIZED
        JSONExtractString(JSONExtractRaw(raw_payload, 'data'), 'resourceType'),
    patient_id       String ALIAS subject,
    practitioner_ref String MATERIALIZED
        JSONExtractString(JSONExtractRaw(raw_payload, 'data'), 'practitionerRef'),
    practitioner_display String MATERIALIZED
        JSONExtractString(JSONExtractRaw(raw_payload, 'data'), 'practitionerDisplay')
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY toYYYYMM(received_at)
ORDER BY (source, received_at, id);

-- Intelligence delivery outcomes
CREATE TABLE IF NOT EXISTS intelligence_deliveries (
    id                          UUID,
    intelligence_event_id       UUID,
    action_definition_id        UUID,
    destination_adaptor_mapping_id Nullable(UUID),
    adaptor_name                LowCardinality(String),
    endpoint_url                LowCardinality(String),
    destination                 LowCardinality(String),
    action_type                 LowCardinality(String),
    severity                    LowCardinality(String),
    status                      LowCardinality(String),
    subject                     String,
    protocol_canonical          String,
    action_id                   String,
    http_status_code            Nullable(UInt16),
    error_message               Nullable(String),
    attempt_count               UInt8,
    created_at                  DateTime64(3),
    delivered_at                Nullable(DateTime64(3)),
    latency_ms                  Nullable(UInt32),
    _version                    UInt64
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY toYYYYMM(created_at)
ORDER BY (destination, adaptor_name, created_at, id)
TTL created_at + INTERVAL 1 YEAR;

-- Intelligence trigger audit trail (from CDC)
CREATE TABLE IF NOT EXISTS intelligence_event_logs (
    id                       UUID,
    event_payload            String CODEC(ZSTD(3)),
    action_definition_id     UUID,
    protocol_instance_id     UUID,
    step_instance_id         Nullable(UUID),
    deviation_id             Nullable(UUID),
    subject                  String,
    action_type              LowCardinality(String),
    intelligence_destination LowCardinality(String),
    step_state               LowCardinality(Nullable(String)),
    trigger_reason           LowCardinality(String),
    step_action_id           Nullable(String),
    evaluation_expression    Nullable(String),
    evaluation_context       Nullable(String),
    published                UInt8,
    published_at             Nullable(DateTime64(3)),
    created_at               DateTime64(3),
    _version                 UInt64
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY toYYYYMM(created_at)
ORDER BY (action_type, intelligence_destination, created_at, id);

-- Intelligence action template definitions (ActivityDefinition resources)
CREATE TABLE IF NOT EXISTS action_definitions (
    id            UUID,
    canonical_url String,
    version       String,
    name          Nullable(String),
    title         Nullable(String),
    status        LowCardinality(String),
    action_type   LowCardinality(String),
    definition    String CODEC(ZSTD(3)),
    created_at    DateTime64(3),
    updated_at    DateTime64(3),
    _version      UInt64
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY (id);

-- Protocol metadata (definition contains full FHIR R4 PlanDefinition JSON)
CREATE TABLE IF NOT EXISTS protocol_definitions (
    id         UUID,
    name       String,
    version    String,
    url        String,
    canonical  String,
    status     LowCardinality(String),
    definition String CODEC(ZSTD(3)),
    loaded_at  Nullable(DateTime64(3)),
    _version   UInt64
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY (id);

-- Adaptor registry
CREATE TABLE IF NOT EXISTS receiver_adaptors (
    id           UUID,
    name         String,
    endpoint_url String,
    status       LowCardinality(String),
    _version     UInt64
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY (id);

-- Destination routing
CREATE TABLE IF NOT EXISTS destination_adaptor_mappings (
    id                  UUID,
    destination         String,
    receiver_adaptor_id UUID,
    status              LowCardinality(String),
    _version            UInt64
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY (id);

-- Compliance event log (CDC from compliance_event_log): lean idempotency + processing outcome
-- Patient/facility/action details accessed via JOINs or from data JSONB
CREATE TABLE IF NOT EXISTS compliance_event_logs (
    id                 UUID,
    cloudevents_id     String,
    source             LowCardinality(String),
    correlation_id     Nullable(String),
    processing_status  LowCardinality(String),
    data               Nullable(String) CODEC(ZSTD(3)),
    received_at        DateTime64(3),
    _version           UInt64
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY toYYYYMM(received_at)
ORDER BY (source, received_at, id);
