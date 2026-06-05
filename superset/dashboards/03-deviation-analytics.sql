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
    si.action_id,
    d.deviation_type,
    count() AS count
FROM deviations d FINAL
JOIN step_instances si FINAL ON d.step_instance_id = si.id
WHERE d.detected_at >= now() - INTERVAL 30 DAY
GROUP BY si.action_id, d.deviation_type
ORDER BY count DESC
LIMIT 20;

-- Chart: Deviations by Protocol
SELECT
    protocol_instance_id,
    deviation_type,
    sum(deviation_count) AS total_deviations
FROM mv_deviation_by_protocol
WHERE day >= today() - 30
GROUP BY protocol_instance_id, deviation_type
ORDER BY total_deviations DESC
LIMIT 20;

-- Table: Deviation Details
SELECT
    d.detected_at,
    pi.patient_id,
    d.deviation_type,
    dictGet('dict_protocol_definitions', 'name', pi.protocol_definition_id) AS protocol_name,
    si.state AS current_step_state
FROM deviations d FINAL
LEFT JOIN step_instances si FINAL ON d.step_instance_id = si.id
LEFT JOIN protocol_instances pi FINAL ON d.protocol_instance_id = pi.id
ORDER BY d.detected_at DESC
LIMIT 50;
