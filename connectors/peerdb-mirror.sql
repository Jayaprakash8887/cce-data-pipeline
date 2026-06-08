-- PeerDB Mirror Configuration
-- Creates a CDC mirror from PostgreSQL (ccedb) to ClickHouse (cce_analytics)
-- Run via PeerDB UI or CLI: peerdb mirror create --from-file connectors/peerdb-mirror.sql
--
-- Prerequisites:
--   1. Run schema/01-create-tables.sql FIRST — tables are pre-created with
--      ReplacingMergeTree(_peerdb_version, _peerdb_is_deleted) + clean_deleted_rows = 'Always'.
--      PeerDB will use existing tables; it will NOT recreate them.
--   2. PostgreSQL: wal_level=logical, REPLICA IDENTITY FULL on all tables (cdc/01-configure-replication.sql)
--   3. PeerDB peers configured: 'ccedb_peer' (PostgreSQL) and 'clickhouse_peer' (ClickHouse)

-- Create the CDC mirror
CREATE MIRROR cce_analytics_mirror
FROM ccedb_peer TO clickhouse_peer
WITH TABLE MAPPING (
    public.protocol_definition:protocol_definitions,
    public.protocol_instance:protocol_instances,
    public.step_instance:step_instances,
    public.deviation:deviations,
    public.inbound_event_log:inbound_event_logs,
    public.intelligence_delivery:intelligence_deliveries,
    public.intelligence_event_log:intelligence_event_logs,
    public.action_definition:action_definitions,
    public.receiver_adaptor:receiver_adaptors,
    public.destination_adaptor_mapping:destination_adaptor_mappings,
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
