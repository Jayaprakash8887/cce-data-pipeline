-- CCE Analytics ClickHouse Schema
-- Dictionaries for fast lookups (replaces JOINs on dimension tables)
-- Run: clickhouse-client --database cce_analytics < schema/04-create-dictionary.sql

USE cce_analytics;

-- Protocol definitions lookup (protocol_definition_id → name, canonical, etc.)
CREATE DICTIONARY IF NOT EXISTS dict_protocol_definitions (
    id UUID,
    name String,
    version String,
    url String,
    canonical String,
    status String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(
    TABLE 'protocol_definitions'
    DB 'cce_analytics'
))
LIFETIME(MIN 60 MAX 300)
LAYOUT(HASHED());

-- Patient → most recent facility mapping
-- Enables facility-level behavioral metrics without JOINs at query time
-- Source: subquery that picks the latest facility per patient from inbound_event_logs
CREATE DICTIONARY IF NOT EXISTS dict_patient_facility (
    patient_id String,
    facility_id String,
    last_seen DateTime64(3)
)
PRIMARY KEY patient_id
SOURCE(CLICKHOUSE(
    QUERY 'SELECT
        subject AS patient_id,
        argMax(facility_id, received_at) AS facility_id,
        max(received_at) AS last_seen
    FROM cce_analytics.inbound_event_logs
    WHERE subject != '''' AND facility_id != ''''
    GROUP BY subject'
))
LIFETIME(MIN 300 MAX 600)
LAYOUT(COMPLEX_KEY_HASHED());

-- Action definitions lookup (action_definition_id → name, action_type, canonical_url)
CREATE DICTIONARY IF NOT EXISTS dict_action_definitions (
    id UUID,
    canonical_url String,
    name String DEFAULT '',
    title String DEFAULT '',
    action_type String,
    status String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(
    TABLE 'action_definitions'
    DB 'cce_analytics'
))
LIFETIME(MIN 60 MAX 300)
LAYOUT(HASHED());
