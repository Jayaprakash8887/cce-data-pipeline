-- Dashboard 1: Operations Overview
-- KPI cards, event volume, compliance donut, facility deviations

-- KPI: Total Events (Last 24h)
SELECT sum(event_count) AS total_events
FROM mv_event_volume_hourly
WHERE hour >= toStartOfHour(now() - INTERVAL 24 HOUR);

-- KPI: Active Enrollments
SELECT count() AS active_enrollments
FROM protocol_instances FINAL
WHERE status = 'ACTIVE';

-- KPI: Compliance Rate
SELECT round(
    countIf(state IN ('COMPLETED', 'SKIPPED')) / nullIf(count(), 0) * 100, 1
) AS compliance_rate_pct
FROM step_instances FINAL
WHERE protocol_instance_id IN (
    SELECT id FROM protocol_instances FINAL WHERE status = 'ACTIVE'
);

-- KPI: Active Deviations (Last 30 days)
SELECT count() AS active_deviations
FROM deviations
WHERE detected_at >= now() - INTERVAL 30 DAY;

-- Chart: Event Volume Trend (hourly, last 7 days)
SELECT
    hour,
    sum(event_count) AS events
FROM mv_event_volume_hourly
WHERE hour >= now() - INTERVAL 7 DAY
GROUP BY hour
ORDER BY hour;

-- Chart: Compliance by Protocol (donut)
SELECT
    pd.name AS protocol_name,
    countIf(pi.status = 'COMPLETED') AS completed,
    countIf(pi.status = 'ACTIVE') AS active,
    countIf(pi.status IN ('WITHDRAWN', 'EXPIRED')) AS other
FROM protocol_instances pi FINAL
JOIN protocol_definitions pd FINAL ON pd.id = pi.protocol_definition_id
GROUP BY protocol_name;

-- Chart: Facility Deviation Bar (top 10)
-- Deviations joined to protocol_instances for patient context
SELECT
    pi.patient_id AS facility_id,
    count() AS deviations
FROM deviations d
JOIN protocol_instances pi FINAL ON pi.id = d.protocol_instance_id
WHERE d.detected_at >= now() - INTERVAL 30 DAY
GROUP BY facility_id
ORDER BY deviations DESC
LIMIT 10;

-- Table: Recent Deviations
SELECT
    d.detected_at,
    d.deviation_type,
    pi.patient_id,
    dictGet('dict_protocol_definitions', 'name', pi.protocol_definition_id) AS protocol_name
FROM deviations d FINAL
JOIN protocol_instances pi FINAL ON pi.id = d.protocol_instance_id
ORDER BY d.detected_at DESC
LIMIT 20;
