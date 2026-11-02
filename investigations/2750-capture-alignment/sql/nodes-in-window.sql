BEGIN READ ONLY;
SET LOCAL statement_timeout='60s';
-- How many nodes, and how many 15s-step reads each, the availability curve draws on.
WITH r AS (
  SELECT o.target_id, n->>'uid' AS node
  FROM execution_capacity_observations o
  CROSS JOIN LATERAL jsonb_array_elements(o.observation_json->'placement'->'nodes') n
  WHERE o.observed_at >= '2026-11-01 19:00:00+00' AND o.observed_at < '2026-11-02 19:00:00+00'
    AND n->'io_stall_window'->>'source' = 'nebius_io_stalled_counter'
), per AS (SELECT target_id, node, count(*) AS reads FROM r GROUP BY 1, 2)
SELECT target_id, count(*) AS nodes, sum(reads) AS reads, max(reads) AS max_reads_one_node
FROM per GROUP BY 1 ORDER BY 1;
ROLLBACK;
