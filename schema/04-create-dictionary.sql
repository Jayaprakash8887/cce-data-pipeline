-- CCE Analytics ClickHouse Schema
-- Task 1.1: Dictionary for fast lookups (replaces JOINs on protocol_definitions)
-- Run: clickhouse-client --database cce_analytics < schema/04-create-dictionary.sql

USE cce_analytics;

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
