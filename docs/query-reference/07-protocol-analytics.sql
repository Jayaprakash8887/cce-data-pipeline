-- Dashboard 7: Protocol Analytics
-- Completion funnel, step state, timeliness, enrollment trends

-- Chart: Completion Funnel (by step state)
SELECT
    si.state,
    count() AS count
FROM step_instances si FINAL
GROUP BY si.state
ORDER BY
    CASE si.state
        WHEN 'PENDING' THEN 1
        WHEN 'DUE' THEN 2
        WHEN 'OVERDUE' THEN 3
        WHEN 'COMPLETED' THEN 4
        WHEN 'MISSED' THEN 5
        WHEN 'SKIPPED' THEN 6
    END;

-- Chart: Step State by Protocol (grouped bar)
SELECT
    dictGet('dict_protocol_definitions', 'name', pi.protocol_definition_id) AS protocol_name,
    si.state,
    count() AS count
FROM step_instances si FINAL
JOIN protocol_instances pi FINAL ON si.protocol_instance_id = pi.id
GROUP BY protocol_name, si.state;

-- Chart: Completion Timeliness Distribution
SELECT
    si.completion_status,
    count() AS count
FROM step_instances si FINAL
WHERE si.state = 'COMPLETED' AND si.completion_status IS NOT NULL
GROUP BY si.completion_status;

-- Chart: Enrollment Trends (daily)
SELECT
    toStartOfDay(pi.enrolled_at) AS day,
    dictGet('dict_protocol_definitions', 'name', pi.protocol_definition_id) AS protocol_name,
    count() AS enrollments
FROM protocol_instances pi FINAL
WHERE pi.enrolled_at >= now() - INTERVAL 90 DAY
GROUP BY day, protocol_name
ORDER BY day;
