-- Dashboard 8: Receiver-Adaptor Performance
-- Success rate, outcome distribution, latency, errors

-- Chart: Success Rate Trend (line, daily)
SELECT
    toStartOfDay(created_at) AS day,
    adaptor_name,
    round(
        countIf(status = 'DELIVERED') / nullIf(count(), 0) * 100, 1
    ) AS success_rate_pct
FROM mv_delivery_current FINAL
WHERE created_at >= now() - INTERVAL 30 DAY
    AND status IN ('DELIVERED', 'FAILED', 'CANCELLED')
GROUP BY day, adaptor_name
ORDER BY day;

-- Chart: Outcome Stacked Bar (by adaptor)
SELECT
    adaptor_name,
    countIf(status = 'DELIVERED') AS delivered,
    countIf(status = 'FAILED')    AS failed,
    countIf(status = 'CANCELLED') AS cancelled
FROM mv_delivery_current FINAL
WHERE created_at >= now() - INTERVAL 7 DAY
    AND status IN ('DELIVERED', 'FAILED', 'CANCELLED')
GROUP BY adaptor_name
ORDER BY count() DESC;

-- Chart: Adaptor × Destination Heatmap
SELECT
    adaptor_name,
    destination,
    count() AS total,
    round(countIf(status = 'DELIVERED') / nullIf(count(), 0) * 100, 1) AS success_rate
FROM mv_delivery_current FINAL
WHERE created_at >= now() - INTERVAL 7 DAY
    AND status IN ('DELIVERED', 'FAILED', 'CANCELLED')
GROUP BY adaptor_name, destination;

-- Chart: P95 Latency by Adaptor
SELECT
    adaptor_name,
    round(quantile(0.95)(latency_ms), 0) AS p95_latency_ms
FROM mv_delivery_current FINAL
WHERE created_at >= now() - INTERVAL 7 DAY
    AND status = 'DELIVERED'
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
