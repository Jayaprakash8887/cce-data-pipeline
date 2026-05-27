-- Dashboard 8: Receiver-Adaptor Performance
-- Success rate, outcome distribution, latency, errors

-- Chart: Success Rate Trend (line, daily)
SELECT
    toStartOfDay(hour) AS day,
    adaptor_name,
    round(sum(delivered) / nullIf(sum(total_deliveries), 0) * 100, 1) AS success_rate_pct
FROM mv_delivery_performance_hourly
WHERE hour >= now() - INTERVAL 30 DAY
GROUP BY day, adaptor_name
ORDER BY day;

-- Chart: Outcome Stacked Bar (by adaptor)
SELECT
    adaptor_name,
    sum(delivered) AS delivered,
    sum(failed) AS failed,
    sum(cancelled) AS cancelled
FROM mv_delivery_performance_hourly
WHERE hour >= now() - INTERVAL 7 DAY
GROUP BY adaptor_name
ORDER BY sum(total_deliveries) DESC;

-- Chart: Adaptor × Destination Heatmap
SELECT
    adaptor_name,
    destination,
    sum(total_deliveries) AS total,
    round(sum(delivered) / nullIf(sum(total_deliveries), 0) * 100, 1) AS success_rate
FROM mv_delivery_performance_hourly
WHERE hour >= now() - INTERVAL 7 DAY
GROUP BY adaptor_name, destination;

-- Chart: P95 Latency by Adaptor
SELECT
    adaptor_name,
    round(avg(p95_latency_ms), 0) AS p95_latency_ms
FROM mv_delivery_performance_hourly
WHERE hour >= now() - INTERVAL 7 DAY
GROUP BY adaptor_name
ORDER BY p95_latency_ms DESC;

-- Table: Error Breakdown
SELECT
    adaptor_name,
    destination,
    http_status_code,
    error_message,
    count() AS occurrences,
    max(created_at) AS last_seen
FROM intelligence_deliveries FINAL
WHERE status IN ('FAILED', 'CANCELLED')
    AND created_at >= now() - INTERVAL 7 DAY
GROUP BY adaptor_name, destination, http_status_code, error_message
ORDER BY occurrences DESC
LIMIT 30;
