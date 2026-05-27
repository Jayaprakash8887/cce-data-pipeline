-- Dashboard 4: Event Volume & Ingestion
-- Volume trend, resource type treemap, ingestion funnel, source quality

-- Chart: Event Volume Trend (line, daily)
SELECT
    day,
    sum(event_count) AS total_events
FROM mv_event_volume_daily
WHERE day >= today() - 30
GROUP BY day
ORDER BY day;

-- Chart: Resource Type Treemap
SELECT
    resource_type,
    sum(event_count) AS event_count
FROM mv_event_volume_daily
WHERE day >= today() - 7
GROUP BY resource_type
ORDER BY event_count DESC;

-- Chart: Ingestion Funnel (RECEIVED → ACCEPTED → REJECTED)
SELECT
    status,
    sum(event_count) AS count
FROM mv_ingestion_quality
WHERE day >= today() - 7
GROUP BY status
ORDER BY count DESC;

-- Chart: Source Quality Scorecard
SELECT
    source,
    sum(event_count) AS total,
    sumIf(event_count, status = 'ACCEPTED') AS accepted,
    sumIf(event_count, status = 'REJECTED') AS rejected,
    round(sumIf(event_count, status = 'ACCEPTED') / nullIf(sum(event_count), 0) * 100, 1) AS acceptance_rate
FROM mv_ingestion_quality
WHERE day >= today() - 7
GROUP BY source
ORDER BY acceptance_rate ASC;

-- Chart: Source Comparison (by resource_type)
SELECT
    source,
    resource_type,
    sum(event_count) AS event_count
FROM mv_event_volume_daily
WHERE day >= today() - 7
GROUP BY source, resource_type
ORDER BY event_count DESC;
