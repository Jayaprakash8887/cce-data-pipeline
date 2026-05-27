-- CCE Analytics ClickHouse Schema
-- Task 1.1: All fact + dimension tables
-- Run: clickhouse-client --database cce_analytics < schema/01-create-tables.sql

CREATE DATABASE IF NOT EXISTS cce_analytics;

USE cce_analytics;

-- ============================================================
-- FACT TABLES (Event-Sourced from Kafka Streams)
-- ============================================================

-- Primary analytics table: all clinical events from cce.events.inbound
CREATE TABLE IF NOT EXISTS events_fact (
    event_id           String,
    source             LowCardinality(String),
    event_type         LowCardinality(String),
    patient_id         String,
    event_time         DateTime64(3),
    facility_id        LowCardinality(String),
    correlation_id     String,
    content_type       LowCardinality(String),
    resource_type      LowCardinality(String),
    resource_status    LowCardinality(String),
    primary_code_system String,
    primary_code       String,
    primary_code_display String,
    practitioner_ref   Nullable(String),
    processed_at       DateTime64(3) DEFAULT now64(3),
    raw_payload        String CODEC(ZSTD(3))
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (facility_id, resource_type, event_time, patient_id)
TTL event_time + INTERVAL 2 YEAR
SETTINGS index_granularity = 8192;

-- Pre-aggregated hourly event counts (populated by Flink tumbling window)
CREATE TABLE IF NOT EXISTS event_volume_hourly (
    hour           DateTime,
    facility_id    LowCardinality(String),
    source         LowCardinality(String),
    resource_type  LowCardinality(String),
    event_count    UInt64
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (facility_id, source, resource_type, hour);

-- Intelligence triggers from cce.intelligence.triggers Kafka topic
CREATE TABLE IF NOT EXISTS intelligence_events (
    id                      UUID,
    subject                 String,
    intelligence_event_id   UUID,
    action_definition_id    UUID,
    protocol_definition_id  UUID,
    action_type             LowCardinality(String),
    severity                LowCardinality(String),
    intelligence_destination LowCardinality(String),
    step_state              LowCardinality(String),
    action_id               String,
    protocol_canonical      String,
    detected_at             DateTime64(3),
    processed_at            DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(detected_at)
ORDER BY (severity, step_state, detected_at)
TTL detected_at + INTERVAL 2 YEAR;

-- Scheduler step transitions from cce.scheduler.triggers
CREATE TABLE IF NOT EXISTS step_transitions (
    step_instance_id    UUID,
    transition_type     LowCardinality(String),
    triggered_at        DateTime64(3),
    correlation_id      String,
    processed_at        DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(triggered_at)
ORDER BY (transition_type, triggered_at, step_instance_id)
TTL triggered_at + INTERVAL 2 YEAR;

-- ============================================================
-- DIMENSION TABLES (from CDC via Debezium/ClickHouse Sink)
-- ============================================================

-- Patient protocol enrollments
CREATE TABLE IF NOT EXISTS protocol_instances (
    id                      UUID,
    patient_id              String,
    protocol_definition_id  UUID,
    protocol_canonical      String,
    status                  LowCardinality(String),
    enrolled_at             DateTime64(3),
    completed_at            Nullable(DateTime64(3)),
    facility_id             LowCardinality(Nullable(String)),
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
    matched_event_id        Nullable(UUID),
    created_at              DateTime64(3),
    updated_at              DateTime64(3),
    _version                UInt64
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY (id);

-- Compliance deviations
CREATE TABLE IF NOT EXISTS deviations (
    id                      UUID,
    protocol_instance_id    UUID,
    step_instance_id        UUID,
    patient_id              String,
    facility_id             LowCardinality(Nullable(String)),
    protocol_definition_id  UUID,
    action_id               Nullable(String),
    deviation_type          LowCardinality(String),
    detected_at             DateTime64(3),
    intelligence_event_id   Nullable(UUID),
    _version                UInt64
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY toYYYYMM(detected_at)
ORDER BY (facility_id, deviation_type, detected_at, id);

-- Ingestion audit trail
CREATE TABLE IF NOT EXISTS inbound_events (
    id               UUID,
    cloudevents_id   String,
    source           LowCardinality(String),
    event_type       LowCardinality(String),
    subject          Nullable(String),
    facility_id      LowCardinality(Nullable(String)),
    correlation_id   Nullable(String),
    source_event_id  Nullable(String),
    status           LowCardinality(String),
    rejection_reason LowCardinality(Nullable(String)),
    error_details    Nullable(String),
    received_at      DateTime64(3),
    _version         UInt64
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY toYYYYMM(received_at)
ORDER BY (source, received_at, id);

-- Intelligence delivery outcomes (enriched by Flink CDC job)
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
    action_definition_id     UUID,
    protocol_instance_id     UUID,
    step_instance_id         Nullable(UUID),
    deviation_id             Nullable(UUID),
    subject                  String,
    action_type              LowCardinality(String),
    intelligence_destination LowCardinality(String),
    step_state               LowCardinality(String),
    trigger_reason           LowCardinality(String),
    step_action_id           Nullable(String),
    published                UInt8,
    published_at             Nullable(DateTime64(3)),
    error_message            Nullable(String),
    created_at               DateTime64(3),
    _version                 UInt64
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY toYYYYMM(created_at)
ORDER BY (action_type, intelligence_destination, created_at, id);

-- Intelligence action template definitions
CREATE TABLE IF NOT EXISTS action_definitions (
    id            UUID,
    canonical_url String,
    version       String,
    name          Nullable(String),
    title         Nullable(String),
    status        LowCardinality(String),
    action_type   LowCardinality(String),
    _version      UInt64
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY (id);

-- Protocol metadata
CREATE TABLE IF NOT EXISTS protocol_definitions (
    id        UUID,
    name      String,
    version   String,
    url       String,
    canonical String,
    status    LowCardinality(String),
    _version  UInt64
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
