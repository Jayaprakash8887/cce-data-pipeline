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

-- Chart: Facility Hotspots (bubble chart)
SELECT
    pi.facility_id,
    count(DISTINCT pi.patient_id) AS total_patients,
    countDistinctIf(pi.patient_id,
        EXISTS (SELECT 1 FROM step_instances si FINAL WHERE si.protocol_instance_id = pi.id AND si.state = 'MISSED')
    ) AS non_compliant_patients
FROM protocol_instances pi FINAL
WHERE pi.status = 'ACTIVE' AND pi.facility_id IS NOT NULL
GROUP BY pi.facility_id
HAVING non_compliant_patients > 0
ORDER BY non_compliant_patients DESC;

-- Table: Repeat Deviators (patients with multiple deviations)
SELECT
    d.patient_id,
    d.facility_id,
    count() AS deviation_count,
    groupArray(DISTINCT d.deviation_type) AS deviation_types,
    min(d.detected_at) AS first_deviation,
    max(d.detected_at) AS last_deviation
FROM deviations d
WHERE d.detected_at >= now() - INTERVAL 90 DAY
GROUP BY d.patient_id, d.facility_id
HAVING deviation_count > 1
ORDER BY deviation_count DESC
LIMIT 50;
