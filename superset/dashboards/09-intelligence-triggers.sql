-- Dashboard 9: Intelligence & Triggers
-- Trigger volume, action type distribution, severity heatmap

-- Chart: Trigger Volume by Severity (line, daily)
SELECT
    day,
    severity,
    sum(trigger_count) AS triggers
FROM mv_intelligence_summary
WHERE day >= today() - 30
GROUP BY day, severity
ORDER BY day;

-- Chart: Action Type Distribution (donut)
SELECT
    action_type,
    sum(trigger_count) AS count
FROM mv_intelligence_summary
WHERE day >= today() - 7
GROUP BY action_type;

-- Chart: Destination Bar
SELECT
    intelligence_destination,
    sum(trigger_count) AS count
FROM mv_intelligence_summary
WHERE day >= today() - 7
GROUP BY intelligence_destination
ORDER BY count DESC;

-- Chart: Severity × Step State Heatmap
SELECT
    severity,
    step_state,
    sum(trigger_count) AS count
FROM mv_intelligence_summary
WHERE day >= today() - 7
GROUP BY severity, step_state;

-- Table: Recent Intelligence Triggers
SELECT
    ie.detected_at,
    ie.subject,
    ie.severity,
    ie.action_type,
    ie.intelligence_destination,
    ie.step_state,
    ie.protocol_canonical
FROM intelligence_events ie
ORDER BY ie.detected_at DESC
LIMIT 30;
