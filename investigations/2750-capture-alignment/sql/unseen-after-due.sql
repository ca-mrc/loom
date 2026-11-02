BEGIN READ ONLY;
SET LOCAL statement_timeout='60s';
-- Per capture: newest point seen (sampled_at). Ingestion of the NEXT point (sampled_at + step)
-- exceeded (observed_at - next_point) when that point was due but not seen: a lower bound.
-- When a capture sees a point, ingestion of it was at most (observed_at - sampled_at): an upper bound.
WITH w AS (
  SELECT o.target_id, n->>'uid' AS node, o.observed_at,
         (n->'io_stall_window'->>'sampled_at')::timestamptz AS pt,
         extract(epoch FROM (n->'io_stall_window'->>'sampled_at')::timestamptz - (n->'io_stall_window'->>'window_started_at')::timestamptz) AS step
  FROM execution_capacity_observations o
  CROSS JOIN LATERAL jsonb_array_elements(o.observation_json->'placement'->'nodes') n
  WHERE o.observed_at >= '2026-11-01 19:00:00+00' AND o.observed_at < '2026-11-02 19:00:00+00'
    AND n->'io_stall_window'->>'source' = 'nebius_io_stalled_counter'
), b AS (
  SELECT target_id, step,
         extract(epoch FROM observed_at - pt) AS seen_lag,
         extract(epoch FROM observed_at - (pt + make_interval(secs => step))) AS missed_by
  FROM w
)
SELECT target_id, round(step::numeric) AS step, count(*) AS reads,
       round(min(seen_lag)::numeric,1) AS min_seen_lag_s,
       round(max(missed_by) FILTER (WHERE missed_by > 0)::numeric,1) AS max_unseen_after_due_s,
       count(*) FILTER (WHERE missed_by > 3) AS unseen_over_3s,
       count(*) FILTER (WHERE missed_by > 4) AS unseen_over_4s,
       count(*) FILTER (WHERE missed_by > 6) AS unseen_over_6s,
       count(*) FILTER (WHERE missed_by > 0) AS unseen_after_due
FROM b GROUP BY target_id, round(step::numeric) ORDER BY 1, 2;
ROLLBACK;
