-- PeerDB Mirror Configuration (PeerDB SQL-interface syntax)
-- Creates a CDC mirror from PostgreSQL (ccedb) to ClickHouse (cce_analytics)
--
-- This is the canonical CREATE MIRROR definition. scripts/register-connectors.sh
-- applies it to the PeerDB nexus SQL interface (PostgreSQL wire protocol, port 9900):
--   PGPASSWORD=$PEERDB_PASSWORD psql "host=localhost port=9900 user=peerdb dbname=peerdb" \
--     -f connectors/peerdb-mirror.sql
-- You can also run it by hand the same way.
--
-- Prerequisites (run in this order):
--   1. Run schema/01-create-tables.sql FIRST — tables are pre-created with
--      ReplacingMergeTree(_peerdb_version, _peerdb_is_deleted) + clean_deleted_rows = 'Always'.
--      PeerDB will use existing tables; it will NOT recreate them.
--   2. PostgreSQL: wal_level=logical, REPLICA IDENTITY FULL on all tables (cdc/01-configure-replication.sql)
--   3. PeerDB peers created via ./scripts/create-peers.sh: 'ccedb_peer' and 'clickhouse_peer'

-- Create the CDC mirror
CREATE MIRROR cce_analytics_mirror
FROM ccedb_peer TO clickhouse_peer
-- 9 tables. The JSON-like form is used where columns are excluded from the sync
-- (large JSONB blobs unused by analytics). receiver_adaptor / destination_adaptor_mapping
-- are intentionally NOT mirrored (unused; adaptor info is denormalized in intelligence_delivery).
WITH TABLE MAPPING (
    public.protocol_definition:protocol_definitions,
    public.protocol_instance:protocol_instances,
    public.step_instance:step_instances,
    public.deviation:deviations,
    public.inbound_event_log:inbound_event_logs,
    { from: public.intelligence_delivery, to: intelligence_deliveries, exclude: [fhir_payload] },
    { from: public.intelligence_event_log, to: intelligence_event_logs, exclude: [event_payload] },
    public.action_definition:action_definitions,
    public.compliance_event_log:compliance_event_logs
)
WITH (
    do_initial_snapshot = true,
    snapshot_num_rows_per_partition = 500000,
    snapshot_num_tables_in_parallel = 4,
    snapshot_max_parallel_workers = 8,
    sync_interval = 10,                     -- seconds between CDC flushes
    soft_delete = true,                     -- mark deletes with _peerdb_is_deleted column
    publication_name = 'cce_analytics_pub',
    replication_slot_name = 'cce_analytics_slot'
);
