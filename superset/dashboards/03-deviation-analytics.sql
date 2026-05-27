-- Dashboard 3: Deviation Analytics
-- Trends, resolution rates, by step, by facility, details

-- Chart: Deviation Trends (area, daily by type)
SELECT
    day,
    deviation_type,
    deviation_count
FROM mv_deviation_trends
WHERE day >= today() - 90
ORDER BY day;

-- KPI: Resolution Rate (deviations that led to completion)
SELECT round(
    countIf(si.state = 'COMPLETED') / nullIf(count(), 0) * 100, 1
) AS resolution_rate
FROM deviations d
JOIN step_instances si FINAL ON d.step_instance_id = si.id
WHERE d.detected_at >= now() - INTERVAL 30 DAY;

-- Chart: Deviations by Step (action_id grouped bar)
SELECT
    d.action_id,
    d.deviation_type,
    count() AS count
FROM deviations d
WHERE d.detected_at >= now() - INTERVAL 30 DAY
    AND d.action_id IS NOT NULL
GROUP BY d.action_id, d.deviation_type
ORDER BY count DESC
LIMIT 20;

-- Chart: Deviations by Facility
SELECT
    facility_id,
    sum(deviation_count) AS total_deviations,
    sum(affected_patients) AS affected_patients
FROM mv_deviation_by_facility
WHERE day >= today() - 30
GROUP BY facility_id
ORDER BY total_deviations DESC;

-- Table: Deviation Details
SELECT
    d.detected_at,
    d.patient_id,
    d.facility_id,
    d.deviation_type,
    d.action_id,
    dictGet('dict_protocol_definitions', 'name', d.protocol_definition_id) AS protocol_name,
    si.state AS current_step_state
FROM deviations d
LEFT JOIN step_instances si FINAL ON d.step_instance_id = si.id
ORDER BY d.detected_at DESC
LIMIT 50;
