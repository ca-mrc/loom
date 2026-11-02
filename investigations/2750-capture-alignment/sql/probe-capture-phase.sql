BEGIN READ ONLY;
SET LOCAL statement_timeout='60s';
SELECT o.target_id,
       count(*) AS windows,
       round(min(extract(epoch FROM o.observed_at-(n->'io_stall_window'->>'sampled_at')::timestamptz))::numeric,1) AS min_lag_s,
       round(percentile_cont(0.1) WITHIN GROUP (ORDER BY extract(epoch FROM o.observed_at-(n->'io_stall_window'->>'sampled_at')::timestamptz))::numeric,1) AS p10_lag_s,
       round(percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM o.observed_at-(n->'io_stall_window'->>'sampled_at')::timestamptz))::numeric,1) AS p50_lag_s,
       round(max(extract(epoch FROM o.observed_at-(n->'io_stall_window'->>'sampled_at')::timestamptz))::numeric,1) AS max_lag_s,
       round(percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM (n->'io_stall_window'->>'sampled_at')::timestamptz-(n->'io_stall_window'->>'window_started_at')::timestamptz))::numeric,1) AS p50_window_s,
       round(percentile_cont(0.5) WITHIN GROUP (ORDER BY mod(extract(epoch FROM (n->'io_stall_window'->>'sampled_at')::timestamptz)::numeric, 15)))::numeric AS p50_point_phase_mod15,
       round(percentile_cont(0.5) WITHIN GROUP (ORDER BY mod(extract(epoch FROM o.observed_at)::numeric, 15)))::numeric AS p50_capture_phase_mod15
FROM execution_capacity_observations o
CROSS JOIN LATERAL jsonb_array_elements(o.observation_json->'placement'->'nodes') n
WHERE o.observed_at BETWEEN '2026-11-02 14:55:00+00' AND '2026-11-02 15:12:00+00'
  AND n->'io_stall_window'->>'source'='nebius_io_stalled_counter'
GROUP BY o.target_id;
ROLLBACK;
