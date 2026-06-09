-- Dashboard 6: Patient Risk
-- Risk distribution, facility hotspots, repeat deviators

-- Chart: Risk Distribution (pie/donut)
SELECT
    CASE
        WHEN missed_count > 0 THEN 'Non-Compliant'
        WHEN overdue_count > 0 THEN 'At-Risk'
        ELSE 'On-Track'
    END AS risk_level,
    count() AS patient_count
FROM (
    SELECT
        pi.patient_id,
        countIf(si.state = 'MISSED') AS missed_count,
        countIf(si.state = 'OVERDUE') AS overdue_count
    FROM protocol_instances pi FINAL
    LEFT JOIN step_instances si FINAL ON si.protocol_instance_id = pi.id
    WHERE pi.status = 'ACTIVE'
    GROUP BY pi.patient_id
)
GROUP BY risk_level;

-- Chart: Facility Hotspots (bubble chart — facility from inbound_event_logs)
-- Note: protocol_instances doesn't have facility_id directly;
-- use patient_id correlation through inbound_event_logs for facility association
SELECT
    pi.patient_id,
    count(DISTINCT pi.id) AS total_enrollments,
    countIf(si.state = 'MISSED') AS missed_steps,
    countIf(si.state = 'OVERDUE') AS overdue_steps
FROM protocol_instances pi FINAL
JOIN step_instances si FINAL ON si.protocol_instance_id = pi.id
WHERE pi.status = 'ACTIVE'
GROUP BY pi.patient_id
HAVING missed_steps > 0
ORDER BY missed_steps DESC
LIMIT 50;

-- Table: Repeat Deviators (patients with multiple deviations)
SELECT
    pi.patient_id,
    count() AS deviation_count,
    groupArray(DISTINCT d.deviation_type) AS deviation_types,
    min(d.detected_at) AS first_deviation,
    max(d.detected_at) AS last_deviation
FROM deviations d FINAL
JOIN protocol_instances pi FINAL ON pi.id = d.protocol_instance_id
WHERE d.detected_at >= now() - INTERVAL 90 DAY
GROUP BY pi.patient_id
HAVING deviation_count > 1
ORDER BY deviation_count DESC
LIMIT 50;
