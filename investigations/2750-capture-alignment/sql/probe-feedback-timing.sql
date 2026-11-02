-- Read-only diagnostic for #2750, probe f565d2e0-2927-4dcf-a2f0-9d262740dc66.
-- observation.created_at is a persistence proxy, not an exact commit timestamp.
-- A complete post-build window is not alone proof of admissibility: pressure,
-- placement, resources and confirmed cleanup can independently affect a claim.
BEGIN READ ONLY;
SET LOCAL statement_timeout='60s';
SET LOCAL lock_timeout='2s';
WITH attempts AS (
 SELECT DISTINCT a.id,a.claimed_at,a.native_build,
        own.target_id AS owner_target,
        a.native_build->>'node_name' AS node,
        a.native_build->'io_binding'->>'node_uid' AS binding_node_uid,
        a.native_build->'io_binding'->>'provider_id' AS binding_provider_id,
        a.native_build->'io_binding'->>'mode' AS binding_mode,
        (a.native_build->>'capacity_released_at')::timestamptz AS released_at,
        own.id AS claim_observation_id,
        (SELECT coalesce(p->'state'->'running'->>'startedAt',p->'state'->'terminated'->>'startedAt')::timestamptz FROM jsonb_array_elements(a.native_build->'phases') p WHERE p->>'name'='prepare') AS prepare_started,
        (SELECT (p->'state'->'terminated'->>'finishedAt')::timestamptz FROM jsonb_array_elements(a.native_build->'phases') p WHERE p->>'name'='prepare') AS prepare_finished,
        (SELECT coalesce(p->'state'->'running'->>'startedAt',p->'state'->'terminated'->>'startedAt')::timestamptz FROM jsonb_array_elements(a.native_build->'phases') p WHERE p->>'name'='build') AS build_started,
        (SELECT (p->'state'->'terminated'->>'finishedAt')::timestamptz FROM jsonb_array_elements(a.native_build->'phases') p WHERE p->>'name'='build') AS build_finished
 FROM trials t
 JOIN trial_task_image_materializations l ON l.trial_id=t.id
 JOIN task_image_materialization_attempts a ON a.materialization_id=l.materialization_id
 LEFT JOIN execution_capacity_observations own ON own.id=(a.native_build->>'capacity_observation_id')::uuid
 WHERE t.batch_id='f565d2e0-2927-4dcf-a2f0-9d262740dc66'
), ordered AS (
 SELECT *,lead(claimed_at) OVER w AS next_claimed_at,
        lead(claim_observation_id) OVER w AS next_claim_observation_id,
        row_number() OVER w AS node_claim_number
 FROM attempts WINDOW w AS (PARTITION BY owner_target,node ORDER BY claimed_at,id)
)
SELECT a.id AS attempt_id,a.owner_target,a.node,a.binding_mode,a.binding_node_uid,
       a.node_claim_number,a.claimed_at,a.prepare_started,a.prepare_finished,
       a.build_started,a.build_finished,a.released_at,a.next_claimed_at,
       f.id AS first_window_observation_id,f.node_uid AS feedback_node_uid,
       f.window_started,f.window_ended,f.observed_at AS feedback_captured_at,
       f.created_at AS feedback_persisted_proxy_at,
       f.pressure_percent,(a.native_build->>'io_full_avg10_max')::numeric AS pressure_limit,
       a.claim_observation_id,a.next_claim_observation_id,
       nx.observed_at AS next_claim_observation_captured_at,
       round(extract(epoch FROM a.prepare_started-a.claimed_at)::numeric,3) AS claim_to_prepare_s,
       round(extract(epoch FROM a.prepare_finished-a.prepare_started)::numeric,3) AS prepare_s,
       round(extract(epoch FROM a.build_started-a.prepare_finished)::numeric,3) AS prepare_to_build_s,
       round(extract(epoch FROM a.build_finished-a.build_started)::numeric,3) AS build_s,
       round(extract(epoch FROM f.window_started-a.build_started)::numeric,3) AS build_to_window_start_s,
       round(extract(epoch FROM f.window_ended-f.window_started)::numeric,3) AS window_s,
       round(extract(epoch FROM f.observed_at-f.window_ended)::numeric,3) AS window_end_to_capture_s,
       round(extract(epoch FROM f.created_at-f.observed_at)::numeric,3) AS capture_to_persist_proxy_s,
       round(extract(epoch FROM f.created_at-a.build_started)::numeric,3) AS build_to_feedback_proxy_s,
       round(extract(epoch FROM a.next_claimed_at-f.created_at)::numeric,3) AS feedback_proxy_to_next_claim_s,
       round(extract(epoch FROM a.released_at-a.build_finished)::numeric,3) AS build_end_to_release_s,
       round(extract(epoch FROM a.next_claimed_at-a.claimed_at)::numeric,3) AS claim_gap_s,
       a.released_at<=a.next_claimed_at AS cleanup_before_next_claim,
       a.next_claimed_at<f.created_at AS next_claim_before_first_complete_window
FROM ordered a
LEFT JOIN LATERAL (
 SELECT o.id,o.observed_at,o.created_at,n->>'uid' AS node_uid,
        (n->'io_stall_window'->>'window_started_at')::timestamptz AS window_started,
        (n->'io_stall_window'->>'sampled_at')::timestamptz AS window_ended,
        (n->'io_stall_window'->>'full_window_percent')::numeric AS pressure_percent
 FROM execution_capacity_observations o
 CROSS JOIN LATERAL jsonb_array_elements(o.observation_json->'placement'->'nodes') n
 WHERE o.target_id=a.owner_target
   AND o.observed_at BETWEEN a.build_started AND a.build_started+interval '5 minutes'
   AND n->>'name'=a.node
   AND (a.binding_node_uid IS NULL OR n->>'uid'=a.binding_node_uid)
   AND (a.binding_provider_id IS NULL OR n->>'provider_id'=a.binding_provider_id)
   AND n->'io_stall_window'->>'source'='nebius_io_stalled_counter'
   AND (n->'io_stall_window'->>'window_started_at')::timestamptz>=a.build_started
   AND (n->'io_stall_window'->>'sampled_at')::timestamptz-(n->'io_stall_window'->>'window_started_at')::timestamptz>=interval '10 seconds'
 ORDER BY o.created_at,o.observed_at,o.id LIMIT 1
) f ON true
LEFT JOIN execution_capacity_observations nx ON nx.id=a.next_claim_observation_id
ORDER BY a.owner_target,a.node,a.claimed_at
;
ROLLBACK;
