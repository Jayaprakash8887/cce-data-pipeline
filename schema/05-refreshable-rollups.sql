-- CCE Analytics ClickHouse Schema — Refreshable Rollups (OPTIONAL, performance)
-- Run: clickhouse-client --database cce_analytics < schema/05-refreshable-rollups.sql
--
-- WHY THIS EXISTS
-- The hot compliance endpoints (dashboard/overview, compliance-summary [all/protocol/
-- facility], protocols/{id}/patients, facilities|practitioners/ranking,
-- patients/at-risk-hotspots, patients/repeat-deviations) all need the same expensive
-- primitive: per-enrollment step counts (completed/total) from the MUTABLE step_instances
-- table, which must be read with FINAL. That cannot be accelerated by:
--   - projections — ClickHouse skips projections when FINAL is applied (and the
--     cce_pipeline profile sets final=1, so every insights-service query uses FINAL);
--   - count-based MVs — they double-count CDC state transitions on mutable tables;
--   - argMax MVs — double-count-safe but no row reduction vs FINAL.
--
-- A REFRESHABLE materialized view sidesteps all three: it fully recomputes the rollup
-- on a schedule WITH FINAL (correct, no double-count) into a plain MergeTree table that
-- is queried WITHOUT FINAL (fast). One row per protocol_instance.
--
-- TRADE-OFFS (read before enabling)
--   - Staleness: data is at most REFRESH-interval old (5 min below; tune as needed).
--   - Each refresh is a full recompute (FINAL join over step_instances) — cheap at
--     ~600k events/day; revisit the interval if step_instances grows very large.
--
-- Refreshable MVs are GA as of ClickHouse 24.10 (this stack pins 26.3 LTS), so no
-- experimental flag is needed. On ClickHouse < 24.10 you would first need:
--   SET allow_experimental_refreshable_materialized_view = 1;
--
-- This file is NOT part of the core bootstrap (schema/01-04). Apply it only if/when the
-- compliance endpoints need pre-aggregation. Safe to DROP without affecting the pipeline.
--
-- Inspect schedule/last run:  SELECT * FROM system.view_refreshes WHERE view = 'rollup_protocol_instance_compliance_mv';

USE cce_analytics;

-- Target table: one row per protocol enrollment. Queried directly (no FINAL).
CREATE TABLE IF NOT EXISTS rollup_protocol_instance_compliance
(
    protocol_instance_id   UUID,
    patient_id             String,
    protocol_definition_id UUID,
    protocol_canonical     String,
    enrollment_status      String,        -- ACTIVE | COMPLETED | WITHDRAWN | EXPIRED
    enrolled_at            DateTime64(6),
    total_steps            UInt64,
    completed_steps        UInt64,         -- state IN (COMPLETED, SKIPPED)
    overdue_steps          UInt64,
    missed_steps           UInt64,
    compliance_rate        Float64,        -- completed_steps / total_steps * 100
    deviation_count        UInt64,
    refreshed_at           DateTime        -- when this rollup row was last recomputed
)
ENGINE = MergeTree
ORDER BY (protocol_definition_id, protocol_instance_id);

-- Refreshable MV: full recompute every 5 minutes, atomically replacing the target.
-- Explicit FINAL on the mutable sources makes it correct regardless of the executing
-- profile (do not rely on final=1 here — the refresh runs in its own context).
CREATE MATERIALIZED VIEW IF NOT EXISTS rollup_protocol_instance_compliance_mv
REFRESH EVERY 5 MINUTE
TO rollup_protocol_instance_compliance
AS
SELECT
    pi.id                                                       AS protocol_instance_id,
    pi.patient_id                                               AS patient_id,
    pi.protocol_definition_id                                   AS protocol_definition_id,
    pi.protocol_canonical                                       AS protocol_canonical,
    pi.status                                                   AS enrollment_status,
    pi.enrolled_at                                              AS enrolled_at,
    ifNull(s.total_steps, 0)                                    AS total_steps,
    ifNull(s.completed_steps, 0)                                AS completed_steps,
    ifNull(s.overdue_steps, 0)                                  AS overdue_steps,
    ifNull(s.missed_steps, 0)                                   AS missed_steps,
    round(ifNull(s.completed_steps, 0) / nullIf(s.total_steps, 0) * 100, 1) AS compliance_rate,
    ifNull(d.deviation_count, 0)                                AS deviation_count,
    now()                                                       AS refreshed_at
FROM protocol_instances AS pi FINAL
LEFT JOIN
(
    SELECT
        protocol_instance_id,
        count()                                    AS total_steps,
        countIf(state IN ('COMPLETED', 'SKIPPED')) AS completed_steps,
        countIf(state = 'OVERDUE')                 AS overdue_steps,
        countIf(state = 'MISSED')                  AS missed_steps
    FROM step_instances FINAL
    GROUP BY protocol_instance_id
) AS s ON s.protocol_instance_id = pi.id
LEFT JOIN
(
    SELECT protocol_instance_id, count() AS deviation_count
    FROM deviations FINAL
    GROUP BY protocol_instance_id
) AS d ON d.protocol_instance_id = pi.id;

-- ============================================================
-- HOW insights-service queries this (examples — no FINAL needed)
-- ============================================================
-- Per-protocol compliance summary:
--   SELECT protocol_definition_id, protocol_canonical,
--          count()                       AS enrollments,
--          round(avg(compliance_rate),1) AS avg_compliance_rate,
--          sum(deviation_count)          AS deviations
--   FROM rollup_protocol_instance_compliance
--   GROUP BY protocol_definition_id, protocol_canonical;
--
-- Per-facility (resolve facility via dictionary, no JOIN):
--   SELECT dictGet('dict_patient_facility','facility_id', patient_id) AS facility_id,
--          count() AS enrollments, round(avg(compliance_rate),1) AS avg_compliance_rate
--   FROM rollup_protocol_instance_compliance GROUP BY facility_id;
--
-- Patient list with compliance category:
--   SELECT patient_id, compliance_rate, completed_steps, total_steps, deviation_count,
--          multiIf(compliance_rate >= 80,'on_track', compliance_rate >= 50,'at_risk','non_compliant') AS category
--   FROM rollup_protocol_instance_compliance
--   WHERE protocol_definition_id = {id:UUID};
--
-- Check freshness:  SELECT max(refreshed_at) FROM rollup_protocol_instance_compliance;
