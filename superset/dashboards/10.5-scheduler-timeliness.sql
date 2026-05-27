-- Dashboard 10.5: Scheduler Timeliness
-- Transition volume, escalation rate, avg time between transitions

-- Chart: Transition Volume Trend (line, daily)
SELECT
    day,
    transition_type,
    transition_count
FROM mv_scheduler_transitions_daily
WHERE day >= today() - 30
ORDER BY day;

-- KPI: Escalation Rate (transitions beyond DUE)
SELECT round(
    (sumIf(transition_count, transition_type IN ('DUE_TO_OVERDUE', 'OVERDUE_TO_MISSED'))
     / nullIf(sum(transition_count), 0)) * 100, 1
) AS escalation_rate_pct
FROM mv_scheduler_transitions_daily
WHERE day >= today() - 7;

-- Chart: Transition Type Distribution (donut)
SELECT
    transition_type,
    sum(transition_count) AS count
FROM mv_scheduler_transitions_daily
WHERE day >= today() - 7
GROUP BY transition_type;

-- Chart: Avg Time Between Transitions (by step)
SELECT
    st1.step_instance_id,
    st1.transition_type AS from_state,
    st2.transition_type AS to_state,
    dateDiff('hour', st1.triggered_at, st2.triggered_at) AS hours_between
FROM step_transitions st1
JOIN step_transitions st2
    ON st1.step_instance_id = st2.step_instance_id
    AND st2.triggered_at > st1.triggered_at
WHERE st1.triggered_at >= now() - INTERVAL 7 DAY
ORDER BY st1.triggered_at DESC
LIMIT 100;
