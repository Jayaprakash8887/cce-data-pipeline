-- Dashboard 9: Intelligence & Triggers
-- Trigger volume, action type distribution, destination heatmap
-- Uses mv_intelligence_summary (AggregatingMergeTree — requires -Merge combinators)

-- Chart: Trigger Volume by Trigger Reason (line, daily)
SELECT
    day,
    trigger_reason,
    countMerge(trigger_count) AS triggers
FROM mv_intelligence_summary
WHERE day >= today() - 30
GROUP BY day, trigger_reason
ORDER BY day;

-- Chart: Action Type Distribution (donut)
SELECT
    action_type,
    countMerge(trigger_count) AS count
FROM mv_intelligence_summary
WHERE day >= today() - 7
GROUP BY action_type;

-- Chart: Destination Bar
SELECT
    intelligence_destination,
    countMerge(trigger_count) AS count
FROM mv_intelligence_summary
WHERE day >= today() - 7
GROUP BY intelligence_destination
ORDER BY count DESC;

-- Chart: Action Type × Step State Heatmap
SELECT
    action_type,
    step_state,
    countMerge(trigger_count) AS count
FROM mv_intelligence_summary
WHERE day >= today() - 7
GROUP BY action_type, step_state;

-- Table: Recent Intelligence Triggers
SELECT
    iel.created_at,
    iel.subject,
    iel.action_type,
    iel.intelligence_destination,
    iel.step_state,
    iel.trigger_reason
FROM intelligence_event_logs iel FINAL
ORDER BY iel.created_at DESC
LIMIT 30;
