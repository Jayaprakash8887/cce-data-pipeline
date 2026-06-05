-- Dashboard 2: Compliance Monitoring
-- Enrollment KPIs, compliance by protocol, adherence trend, facility heatmap

-- KPI: Total Enrollments
SELECT count() AS total_enrollments FROM protocol_instances FINAL;

-- KPI: Completion Rate
SELECT round(countIf(status = 'COMPLETED') / nullIf(count(), 0) * 100, 1) AS completion_rate
FROM protocol_instances FINAL;

-- Chart: Compliance by Protocol (stacked bar)
SELECT
    dictGet('dict_protocol_definitions', 'name', pi.protocol_definition_id) AS protocol_name,
    pi.status,
    count() AS count
FROM protocol_instances pi FINAL
GROUP BY protocol_name, pi.status
ORDER BY count DESC;

-- Chart: Adherence Trend (line, daily)
SELECT
    toStartOfDay(si.updated_at) AS day,
    round(countIf(si.state = 'COMPLETED') / nullIf(count(), 0) * 100, 1) AS adherence_rate
FROM step_instances si FINAL
WHERE si.updated_at >= now() - INTERVAL 90 DAY
GROUP BY day
ORDER BY day;

-- Chart: Facility × Protocol Heatmap
-- Note: protocol_instances doesn't have facility_id; show by protocol only
SELECT
    dictGet('dict_protocol_definitions', 'name', pi.protocol_definition_id) AS protocol_name,
    pi.status,
    round(countIf(si.state = 'COMPLETED') / nullIf(count(si.id), 0) * 100, 1) AS adherence_pct
FROM protocol_instances pi FINAL
LEFT JOIN step_instances si FINAL ON si.protocol_instance_id = pi.id
GROUP BY protocol_name, pi.status;

-- Table: Protocol Instance Details
SELECT
    pi.id,
    pi.patient_id,
    dictGet('dict_protocol_definitions', 'name', pi.protocol_definition_id) AS protocol_name,
    pi.status,
    pi.enrolled_at
FROM protocol_instances pi FINAL
ORDER BY pi.enrolled_at DESC
LIMIT 50;
