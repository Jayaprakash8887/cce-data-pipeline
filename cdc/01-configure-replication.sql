-- CCE Data Pipeline — PostgreSQL CDC Configuration
-- Task 1.2: Configure logical replication for Debezium CDC
--
-- Prerequisites:
--   1. PostgreSQL must have wal_level = 'logical' (requires restart if changing)
--   2. Run this script as a superuser or user with CREATEROLE + REPLICATION privileges
--
-- Usage: psql -h <host> -U postgres -d ccedb -f cdc/01-configure-replication.sql

-- Step 1: Ensure wal_level is logical (requires restart if not already set)
DO $$
BEGIN
    IF current_setting('wal_level') != 'logical' THEN
        RAISE NOTICE 'wal_level is currently "%". Changing to "logical"...', current_setting('wal_level');
        EXECUTE 'ALTER SYSTEM SET wal_level = ''logical''';
        RAISE NOTICE 'wal_level changed. PostgreSQL RESTART REQUIRED for this to take effect.';
    ELSE
        RAISE NOTICE 'wal_level is already "logical". No restart needed.';
    END IF;
END $$;

-- Step 2: Set max_replication_slots (ensure enough for Debezium)
ALTER SYSTEM SET max_replication_slots = 4;
ALTER SYSTEM SET max_wal_senders = 4;

-- Step 3: Create CDC user with minimal privileges
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'cce_cdc_user') THEN
        CREATE ROLE cce_cdc_user WITH LOGIN PASSWORD 'CHANGE_ME_IN_PRODUCTION' REPLICATION;
        RAISE NOTICE 'Created role cce_cdc_user';
    ELSE
        RAISE NOTICE 'Role cce_cdc_user already exists';
    END IF;
END $$;

-- Step 4: Grant SELECT on all CDC source tables
GRANT USAGE ON SCHEMA public TO cce_cdc_user;
GRANT SELECT ON TABLE
    protocol_definition,
    protocol_instance,
    step_instance,
    deviation,
    inbound_event,
    intelligence_delivery,
    intelligence_event_log,
    action_definition,
    receiver_adaptor,
    destination_adaptor_mapping
TO cce_cdc_user;

-- Step 5: Create publication for all 10 CDC tables
DROP PUBLICATION IF EXISTS cce_analytics_pub;
CREATE PUBLICATION cce_analytics_pub FOR TABLE
    protocol_definition,
    protocol_instance,
    step_instance,
    deviation,
    inbound_event,
    intelligence_delivery,
    intelligence_event_log,
    action_definition,
    receiver_adaptor,
    destination_adaptor_mapping;

-- Step 6: Grant replication privileges
ALTER ROLE cce_cdc_user WITH REPLICATION;
