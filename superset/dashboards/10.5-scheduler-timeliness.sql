-- Dashboard 10.5: Scheduler Timeliness
-- Step state distribution, overdue/missed rates, time-to-completion
-- All queries use step_instances CDC table

-- Chart: Step States Trend (line, daily via MV)
SELECT
    day,
    state,
    countMerge(step_count) AS step_count
FROM mv_step_states_daily
WHERE day >= today() - 30
GROUP BY day, state
ORDER BY day;

-- KPI: Overdue Rate
SELECT round(
    countIf(state = 'OVERDUE') / nullIf(count(), 0) * 100, 1
) AS overdue_rate_pct
FROM step_instances FINAL
WHERE created_at >= now() - INTERVAL 7 DAY;

-- Chart: Completion Status Distribution (donut)
SELECT
    completion_status,
    count() AS count
FROM step_instances FINAL
WHERE state = 'COMPLETED'
    AND completed_at >= now() - INTERVAL 7 DAY
GROUP BY completion_status;

-- Chart: Avg Time to Completion (by action)
SELECT
    action_id,
    avg(dateDiff('hour', created_at, completed_at)) AS avg_hours_to_complete,
    count() AS sample_size
FROM step_instances FINAL
WHERE state = 'COMPLETED'
    AND completed_at >= now() - INTERVAL 30 DAY
GROUP BY action_id
HAVING sample_size >= 5
ORDER BY avg_hours_to_complete DESC
LIMIT 20;
