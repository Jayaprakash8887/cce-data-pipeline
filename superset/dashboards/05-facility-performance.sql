-- Dashboard 5: Facility Performance
-- Facility ranking, scatter plot, details

-- Chart: Facility Ranking Bar
SELECT
    pi.facility_id,
    count(DISTINCT pi.id) AS total_enrollments,
    round(
        countIf(si.state IN ('COMPLETED', 'SKIPPED')) /
        nullIf(count(si.id), 0) * 100, 1
    ) AS compliance_rate_pct
FROM protocol_instances pi FINAL
LEFT JOIN step_instances si FINAL ON si.protocol_instance_id = pi.id
WHERE pi.facility_id IS NOT NULL
GROUP BY pi.facility_id
ORDER BY compliance_rate_pct DESC;

-- Chart: Facility Scatter (enrollments vs deviations)
SELECT
    pi.facility_id,
    count(DISTINCT pi.id) AS enrollments,
    countIf(d.id IS NOT NULL) AS deviations
FROM protocol_instances pi FINAL
LEFT JOIN deviations d ON d.protocol_instance_id = pi.id
    AND d.detected_at >= now() - INTERVAL 30 DAY
WHERE pi.facility_id IS NOT NULL
GROUP BY pi.facility_id;

-- Table: Facility Details
SELECT
    pi.facility_id,
    count(DISTINCT pi.id) AS total_enrollments,
    countIf(pi.status = 'ACTIVE') AS active,
    countIf(pi.status = 'COMPLETED') AS completed,
    round(
        countIf(si.state IN ('COMPLETED', 'SKIPPED')) /
        nullIf(count(si.id), 0) * 100, 1
    ) AS compliance_rate_pct,
    sum(CASE WHEN d.id IS NOT NULL THEN 1 ELSE 0 END) AS deviations_30d
FROM protocol_instances pi FINAL
LEFT JOIN step_instances si FINAL ON si.protocol_instance_id = pi.id
LEFT JOIN deviations d ON d.protocol_instance_id = pi.id
    AND d.detected_at >= now() - INTERVAL 30 DAY
WHERE pi.facility_id IS NOT NULL
GROUP BY pi.facility_id
ORDER BY compliance_rate_pct DESC;
