-- Dashboard 5: Facility Performance
-- Uses mv_facility_summary for event-based facility metrics
-- and inbound_event_logs MATERIALIZED columns for facility identification

-- Chart: Facility Ranking by Event Volume (bar)
SELECT
    facility_id,
    countMerge(event_count) AS total_events,
    uniqMerge(unique_patients) AS unique_patients,
    uniqMerge(unique_practitioners) AS unique_practitioners
FROM mv_facility_summary
WHERE day >= today() - 30 AND facility_id != ''
GROUP BY facility_id
ORDER BY total_events DESC
LIMIT 20;

-- Chart: Facility Activity Trend (line)
SELECT
    day,
    facility_id,
    countMerge(event_count) AS events
FROM mv_facility_summary
WHERE day >= today() - 30 AND facility_id != ''
GROUP BY day, facility_id
ORDER BY day;

-- Chart: Facility Resource Type Breakdown (stacked bar)
SELECT
    facility_id,
    resource_type,
    countMerge(event_count) AS event_count
FROM mv_facility_summary
WHERE day >= today() - 7 AND facility_id != ''
GROUP BY facility_id, resource_type
ORDER BY event_count DESC
LIMIT 50;
