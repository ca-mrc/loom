BEGIN READ ONLY;
SET LOCAL statement_timeout='60s';
-- Each read at age a of a point (seconds after its timestamp) shows whether that point was readable.
-- Newest seen point pt: readable at age (observed_at - pt). Next point pt+15: unreadable at age (observed_at - pt - 15) when > 0.
WITH w AS (
  SELECT o.target_id, o.observed_at, (n->'io_stall_window'->>'sampled_at')::timestamptz AS pt
  FROM execution_capacity_observations o
  CROSS JOIN LATERAL jsonb_array_elements(o.observation_json->'placement'->'nodes') n
  WHERE o.observed_at >= '2026-11-01 19:00:00+00' AND o.observed_at < '2026-11-02 19:00:00+00'
    AND n->'io_stall_window'->>'source' = 'nebius_io_stalled_counter'
    AND extract(epoch FROM (n->'io_stall_window'->>'sampled_at')::timestamptz - (n->'io_stall_window'->>'window_started_at')::timestamptz) BETWEEN 14.5 AND 15.5
), obs AS (
  SELECT target_id, floor(extract(epoch FROM observed_at - pt))::int AS age, true AS readable FROM w
  UNION ALL
  SELECT target_id, floor(extract(epoch FROM observed_at - pt) - 15)::int, false FROM w
  WHERE extract(epoch FROM observed_at - pt) > 15
)
SELECT target_id, age, count(*) AS n, round(avg(readable::int)::numeric, 2) AS share_readable
FROM obs WHERE age BETWEEN 0 AND 20 GROUP BY 1, 2 ORDER BY 1, 2;
ROLLBACK;
