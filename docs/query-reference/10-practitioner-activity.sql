-- Dashboard 10: Practitioner Activity
-- Top 20, treemap, scatter, scorecard
-- All queries use inbound_event_logs MATERIALIZED columns

-- Chart: Top 20 Practitioners by Activity (bar)
SELECT
    practitioner_ref,
    count() AS total_events,
    uniq(subject) AS unique_patients
FROM inbound_event_logs
WHERE practitioner_ref != ''
    AND status = 'ACCEPTED'
    AND received_at >= now() - INTERVAL 30 DAY
GROUP BY practitioner_ref
ORDER BY total_events DESC
LIMIT 20;

-- Chart: Resource Type Treemap by Practitioner
SELECT
    practitioner_ref,
    resource_type,
    count() AS event_count
FROM inbound_event_logs
WHERE practitioner_ref != ''
    AND status = 'ACCEPTED'
    AND received_at >= now() - INTERVAL 7 DAY
GROUP BY practitioner_ref, resource_type
ORDER BY event_count DESC
LIMIT 50;

-- Chart: Practitioner Scatter (events vs unique patients)
SELECT
    practitioner_ref,
    count() AS total_events,
    uniq(subject) AS unique_patients
FROM inbound_event_logs
WHERE practitioner_ref != ''
    AND status = 'ACCEPTED'
    AND received_at >= now() - INTERVAL 30 DAY
GROUP BY practitioner_ref
HAVING total_events > 10;

-- Table: Practitioner Scorecard
SELECT
    practitioner_ref,
    facility_id,
    count() AS total_events,
    uniq(subject) AS unique_patients,
    uniq(resource_type) AS resource_types_handled,
    min(received_at) AS first_activity,
    max(received_at) AS last_activity
FROM inbound_event_logs
WHERE practitioner_ref != ''
    AND status = 'ACCEPTED'
    AND received_at >= now() - INTERVAL 30 DAY
GROUP BY practitioner_ref, facility_id
ORDER BY total_events DESC
LIMIT 50;
