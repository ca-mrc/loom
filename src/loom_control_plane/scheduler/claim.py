"""DRF SQL claim query (spec §2.6).

One ordered query with `FOR UPDATE SKIP LOCKED` that atomically transitions
the chosen trial from `queued` to `claimed`.

Ordering (most → least important):
1. Lowest `in_flight_count / fair_share_weight` (Dominant Resource Fairness)
2. Highest `submit_priority`
3. Oldest `submitted_at`

`mounted_fs` is intentionally omitted from the eligibility predicate — per
spec §3.1.1 v1's derive_requires_caps does not emit a mounted_fs requirement.
If we add it later, the matching predicate goes here and the helper signature
grows the corresponding parameter.

#672 family-runs: a trial whose `family_key` is set is only claimable
when the matching `batch_family_state` row is `pending` and its
``task_sequence[current_index]`` equals the trial's task_id. The claim
also flips the family state from `pending` to `running` in the same
transaction so a second concurrent claim can't race the family gate.
"""

from __future__ import annotations

import hmac
import json
import secrets
from dataclasses import dataclass
from hashlib import sha256
from typing import Any
from uuid import UUID, uuid4

from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncSession

from loom.models.worker_capabilities import WorkerCapabilitySnapshotV1
from loom.pipeline.keys import canonical_digest, canonical_document

# This is the explicit local-development worker path. Hosted Nebius admission
# uses service-execution leases and never derives authority from fleet policies.
# The family predicate uses ``task_sequence[current_index + 1]`` because
# Postgres arrays are 1-indexed while ``current_index`` counts from 0.
_CLAIM_SQL = text("""
WITH worker_scope AS (
  SELECT w.id, w.pool_name, 'development'::text AS environment,
         NULL::text AS region
    FROM workers w
   WHERE w.id = (:worker_id)::uuid
), next AS (
  SELECT t.id, t.family_key, t.batch_id, t.team_id,
         t.attempt_count + 1 AS admission_attempt,
         worker_scope.pool_name AS admission_pool_id,
         worker_scope.environment AS admission_environment,
         worker_scope.region AS admission_region,
         t.execution_route_json->>'selected_execution_class_id'
           AS admission_execution_class_id
    FROM trials t
    JOIN tasks task_definition ON task_definition.id = t.task_id
    JOIN team_quotas q ON q.team_id = t.team_id
    CROSS JOIN worker_scope
   WHERE t.state = 'queued'
     AND t.attempt_count < q.max_attempts_ceiling
     AND (t.next_attempt_at IS NULL OR t.next_attempt_at <= NOW())
     AND t.requires_caps->>'os' = ANY(:worker_os)
     AND (
       COALESCE(t.requires_caps->>'cpu_arch', 'x86_64') = 'any'
       OR COALESCE(t.requires_caps->>'cpu_arch', 'x86_64') = ANY(:worker_cpu_arches)
     )
     AND loom_execution_admission_available(
           t.team_id,
           t.batch_id,
           worker_scope.environment,
           worker_scope.region,
           t.execution_route_json->>'selected_execution_class_id',
           worker_scope.pool_name
         )
     AND (
       EXISTS (
         SELECT 1
           FROM trial_task_image_materializations task_image_link
           JOIN task_image_materializations task_image
             ON task_image.id = task_image_link.materialization_id
          WHERE task_image_link.trial_id = t.id
            AND task_image.cpu_arch = ANY(:worker_cpu_arches)
            AND task_image.state = 'ready'
            AND task_image.ready_publication_operation_id IS NULL
       )
       OR (
         NOT EXISTS (
           SELECT 1
             FROM trial_task_image_materializations task_image_link
            WHERE task_image_link.trial_id = t.id
         )
         AND task_definition.config #>> '{environment,dockerfile}' IS NULL
         AND NOT EXISTS (
           SELECT 1
             FROM jsonb_array_elements(
               CASE
                 WHEN jsonb_typeof(
                   task_definition.config #> '{environment,sidecars}'
                 ) = 'array'
                 THEN task_definition.config #> '{environment,sidecars}'
                 ELSE '[]'::jsonb
               END
             ) AS task_sidecar
            WHERE task_sidecar->>'dockerfile' IS NOT NULL
         )
       )
     )
     AND t.requires_caps->>'gpu_vendor' = ANY(:worker_gpu_vendors)
     AND COALESCE(t.requires_caps->>'backend', 'docker') = ANY(:worker_backends)
     AND (t.requires_caps->'network_policies') <@ (:worker_network_policies)::jsonb
     AND (
       t.family_key IS NULL
       OR EXISTS (
         SELECT 1
           FROM batch_family_state bfs
          WHERE bfs.batch_id = t.batch_id
            AND bfs.family_key = t.family_key
            AND bfs.state = 'pending'
            AND bfs.task_sequence[bfs.current_index + 1] = t.task_id
       )
     )
     AND EXISTS (
       SELECT 1
         FROM workers w
        WHERE w.id = (:worker_id)::uuid
          AND w.status = 'active'
          AND w.drain_state = 'active'
          AND (
            NULLIF(t.requires_caps->>'worker_pool', '') IS NULL
            OR w.pool_name = t.requires_caps->>'worker_pool'
          )
          AND (
            NOT :enforce_shared_slot
            OR w.max_concurrent > (
              (SELECT count(*) FROM trials active_trial
                WHERE active_trial.worker_id = w.id
                  AND active_trial.state IN ('claimed','running'))
              +
              (SELECT count(*) FROM execution_attempts active_attempt
                WHERE active_attempt.worker_id = w.id
                  AND active_attempt.state IN ('claimed','running'))
            )
          )
          AND (
            COALESCE((t.requires_caps->>'mainland_proxy')::boolean, false) IS NOT TRUE
            OR EXISTS (
              SELECT 1 FROM jsonb_array_elements(w.capabilities) cap
              WHERE COALESCE((cap->>'mainland_proxy')::boolean, false) IS TRUE
            )
          )
          AND (
            COALESCE((t.requires_caps->>'terminus2_model_switch')::boolean, false) IS NOT TRUE
            OR EXISTS (
              SELECT 1 FROM jsonb_array_elements(w.capabilities) cap
              WHERE COALESCE((cap->>'terminus2_model_switch')::boolean, false) IS TRUE
            )
          )
     )
   ORDER BY
       (q.in_flight_count * 1.0) / NULLIF(q.fair_share_weight, 0) ASC,
       t.submit_priority DESC,
       t.submitted_at ASC
   LIMIT 1
   FOR UPDATE OF t SKIP LOCKED
), admitted AS (
  SELECT next.*
    FROM next
   WHERE loom_execution_admission_reserve(
           next.id, next.admission_attempt, 'attempt', next.team_id,
           next.batch_id, next.admission_environment, next.admission_region,
           next.admission_execution_class_id, next.admission_pool_id,
           'legacy_worker_claim', (:worker_id)::uuid, NOW()
         ) IS NOT NULL
),
-- Same-transaction family gate: flip the picked family to 'running' so a
-- concurrent claim on a sibling task blocks on the predicate above.
family_lock AS (
  UPDATE batch_family_state bfs
     SET state = 'running',
         updated_at = NOW()
    FROM admitted n
   WHERE n.family_key IS NOT NULL
     AND bfs.batch_id = n.batch_id
     AND bfs.family_key = n.family_key
     AND bfs.state = 'pending'
  RETURNING bfs.batch_id, bfs.family_key, bfs.state_uri
)
UPDATE trials t
   SET state = 'claimed',
       worker_id = :worker_id,
       claimed_at = NOW(),
       legacy_claim_id = (:claim_id)::uuid,
       pre_start_heartbeat_at = NULL,
       failure_reason = NULL,
       failure_message = NULL,
       attempt_count = attempt_count + 1
  FROM admitted
 WHERE t.id = admitted.id
 RETURNING t.id, t.team_id, t.task_id, t.config, t.requires_caps,
           t.attempt_count, t.provider_connection_id, t.legacy_claim_id AS claim_id,
           t.family_key, t.batch_id,
           (SELECT state_uri FROM family_lock) AS family_state_uri,
           (SELECT b.family_run_spec
              FROM batches b
             WHERE b.id = t.batch_id) AS family_run_spec;
""")


async def claim_one(
    session: AsyncSession,
    *,
    worker_id: UUID,
    worker_os: list[str],
    worker_cpu_arches: list[str],
    worker_gpu_vendors: list[str],
    worker_network_policies: list[str],
    worker_backends: list[str] | None = None,
    enforce_shared_slot: bool = False,
) -> RowMapping | None:
    """Claim the next eligible trial. Returns the claimed trial row (RowMapping)
    or None if the queue is empty / nothing matches caps.
    """
    params: dict[str, Any] = {
        "worker_id": worker_id,
        "worker_os": worker_os,
        "worker_cpu_arches": worker_cpu_arches,
        "worker_gpu_vendors": worker_gpu_vendors,
        "worker_network_policies": json.dumps(worker_network_policies),
        "worker_backends": worker_backends or ["docker"],
        "enforce_shared_slot": enforce_shared_slot,
        "claim_id": uuid4(),
    }
    result = await session.execute(_CLAIM_SQL, params)
    return result.mappings().one_or_none()


@dataclass(frozen=True)
class WorkClaimConflictError(Exception):
    reason: str


_WORKER_CLAIM_GUARD_SQL = text("""
SELECT w.id,
       w.max_concurrent,
       w.status,
       w.drain_state,
       w.supported_work_kinds,
       w.capability_snapshot_digest,
       w.capability_snapshot_json,
       w.auth_token_hash,
       w.lease_epoch,
       (
         (SELECT count(*) FROM trials t
           WHERE t.worker_id = w.id AND t.state IN ('claimed','running'))
         +
         (SELECT count(*) FROM execution_attempts a
           WHERE a.worker_id = w.id AND a.state IN ('claimed','running'))
       ) AS active_count
  FROM workers w
 WHERE w.id = (:worker_id)::uuid
 FOR UPDATE
""")


_WORK_CLAIM_SQL = text("""
WITH candidates AS (
  SELECT 'trial'::text AS work_kind,
         t.id,
         t.team_id,
         t.submit_priority,
         t.submitted_at AS queued_at,
         q.in_flight_count,
         q.fair_share_weight,
         t.family_key,
         t.batch_id
    FROM trials t
    JOIN tasks task_definition ON task_definition.id = t.task_id
    JOIN team_quotas q ON q.team_id = t.team_id
    JOIN workers w ON w.id = (:worker_id)::uuid
   WHERE 'trial' = ANY((:supported_work_kinds)::text[])
     AND t.state = 'queued'
     AND t.attempt_count < q.max_attempts_ceiling
     AND (t.next_attempt_at IS NULL OR t.next_attempt_at <= NOW())
     AND t.requires_caps->>'os' = ANY(:worker_os)
     AND (COALESCE(t.requires_caps->>'cpu_arch', 'x86_64') = 'any'
          OR COALESCE(t.requires_caps->>'cpu_arch', 'x86_64') = ANY(:worker_cpu_arches))
     AND (
       EXISTS (
         SELECT 1
           FROM trial_task_image_materializations task_image_link
           JOIN task_image_materializations task_image
             ON task_image.id = task_image_link.materialization_id
          WHERE task_image_link.trial_id = t.id
            AND task_image.cpu_arch = ANY(:worker_cpu_arches)
            AND task_image.state = 'ready'
            AND (task_image.ready_publication_operation_id IS NULL
                 OR (:allow_signed_task_images AND task_image.cpu_arch = :signed_cpu_arch))
       )
       OR (
         NOT EXISTS (
           SELECT 1
             FROM trial_task_image_materializations task_image_link
            WHERE task_image_link.trial_id = t.id
         )
         AND task_definition.config #>> '{environment,dockerfile}' IS NULL
         AND NOT EXISTS (
           SELECT 1
             FROM jsonb_array_elements(
               CASE
                 WHEN jsonb_typeof(
                   task_definition.config #> '{environment,sidecars}'
                 ) = 'array'
                 THEN task_definition.config #> '{environment,sidecars}'
                 ELSE '[]'::jsonb
               END
             ) AS task_sidecar
            WHERE task_sidecar->>'dockerfile' IS NOT NULL
         )
       )
     )
     AND t.requires_caps->>'gpu_vendor' = ANY(:worker_gpu_vendors)
     AND COALESCE(t.requires_caps->>'backend', 'docker') = ANY(:worker_backends)
     AND (t.requires_caps->'network_policies') <@ (:worker_network_policies)::jsonb
     AND (
       COALESCE((t.requires_caps->>'mainland_proxy')::boolean, false) IS NOT TRUE
       OR EXISTS (
         SELECT 1 FROM jsonb_array_elements(w.capabilities) cap
         WHERE COALESCE((cap->>'mainland_proxy')::boolean, false) IS TRUE
       )
     )
     AND (
       COALESCE((t.requires_caps->>'terminus2_model_switch')::boolean, false) IS NOT TRUE
       OR EXISTS (
         SELECT 1 FROM jsonb_array_elements(w.capabilities) cap
         WHERE COALESCE((cap->>'terminus2_model_switch')::boolean, false) IS TRUE
       )
     )
     AND (
       t.family_key IS NULL
       OR EXISTS (
         SELECT 1
           FROM batch_family_state bfs
          WHERE bfs.batch_id = t.batch_id
            AND bfs.family_key = t.family_key
            AND bfs.state = 'pending'
           AND bfs.task_sequence[bfs.current_index + 1] = t.task_id
       )
     )
     AND loom_execution_admission_available(
           t.team_id,
           t.batch_id,
           'development',
           NULL,
           t.execution_route_json->>'selected_execution_class_id',
           w.pool_name
         )
     AND (
       NULLIF(t.requires_caps->>'worker_pool', '') IS NULL
       OR w.pool_name = t.requires_caps->>'worker_pool'
     )
  UNION ALL
  SELECT 'execution_attempt'::text AS work_kind,
         a.id,
         r.team_id,
         0 AS submit_priority,
         a.queued_at,
         q.in_flight_count,
         q.fair_share_weight,
         NULL::text AS family_key,
         NULL::uuid AS batch_id
    FROM execution_attempts a
    JOIN pipeline_stage_runs s ON s.id = a.stage_run_id
    JOIN pipeline_runs r ON r.id = s.pipeline_run_id
    JOIN pipeline_budget_ledgers ledger ON ledger.pipeline_run_id = r.id
    JOIN team_quotas q ON q.team_id = r.team_id
    JOIN workers w ON w.id = (:worker_id)::uuid
   WHERE 'execution_attempt' = ANY((:supported_work_kinds)::text[])
     AND a.state = 'queued'
     AND s.state = 'queued'
     AND s.resolved_execution_spec_json IS NOT NULL
     AND s.execution_spec_digest IS NOT NULL
     AND s.resolved_input_bindings_json IS NOT NULL
     AND s.resource_profile_json IS NOT NULL
     AND s.resource_profile_digest IS NOT NULL
     AND s.image_runtime_contract_json IS NOT NULL
     AND s.image_runtime_contract_digest IS NOT NULL
     AND (
       (
         jsonb_array_length(
           COALESCE(s.resolved_execution_spec_json->'control_binding_snapshots', '[]'::jsonb)
         ) = 0
         AND s.provider_connection_ref IS NULL
         AND NOT EXISTS (
           SELECT 1 FROM pipeline_run_control_bindings unexpected
            WHERE unexpected.pipeline_run_id = r.id
              AND unexpected.node_key = s.node_key
         )
       )
       OR EXISTS (
         SELECT 1
           FROM pipeline_run_control_bindings frozen
           JOIN provider_connections connection
             ON connection.id = frozen.provider_connection_id
           JOIN LATERAL jsonb_array_elements(
             s.resolved_execution_spec_json->'control_binding_snapshots'
           ) control_ref ON true
          WHERE frozen.pipeline_run_id = r.id
            AND frozen.node_key = s.node_key
            AND jsonb_array_length(
                  s.resolved_execution_spec_json->'control_binding_snapshots'
                ) = 1
            AND control_ref->>'logical_name' = frozen.logical_name
            AND control_ref->>'kind' = frozen.kind
            AND (control_ref->>'object_id')::uuid = frozen.source_object_id
            AND (control_ref->>'version')::integer = frozen.source_version
            AND control_ref->>'snapshot_sha256' = frozen.snapshot_sha256
            AND s.provider_connection_ref = frozen.provider_connection_id
            AND frozen.snapshot_json->>'recipe_digest' = r.recipe_digest
            AND frozen.snapshot_json->>'node_key' = s.node_key
            AND frozen.snapshot_json->>'status' = 'active'
            AND frozen.snapshot_json->>'provider_connection_id' =
                frozen.provider_connection_id::text
            AND (
              frozen.snapshot_json->'allowed_team_ids' = '[]'::jsonb
              OR frozen.snapshot_json->'allowed_team_ids' ? r.team_id::text
            )
            AND convert_from(
                  substring(
                    frozen.snapshot_bytes FROM 1 FOR octet_length(frozen.snapshot_bytes) - 1
                  ), 'UTF8'
                )::jsonb = frozen.snapshot_json
            AND (
              (
                frozen.kind = 'judge_profile'
                AND EXISTS (
                  SELECT 1 FROM judge_execution_profiles source_profile
                   WHERE source_profile.profile_id = frozen.source_object_id
                     AND source_profile.version = frozen.source_version
                     AND source_profile.snapshot_bytes = frozen.snapshot_bytes
                     AND source_profile.snapshot_sha256 = frozen.snapshot_sha256
                )
                AND EXISTS (
                  SELECT 1 FROM judge_execution_profiles live_profile
                   WHERE live_profile.profile_id = frozen.source_object_id
                     AND live_profile.is_current
                     AND live_profile.status = 'active'
                )
              )
              OR (
                frozen.kind = 'provider'
                AND EXISTS (
                  SELECT 1 FROM recipe_provider_bindings source_binding
                   WHERE source_binding.binding_id = frozen.source_object_id
                     AND source_binding.version = frozen.source_version
                     AND source_binding.snapshot_bytes = frozen.snapshot_bytes
                     AND source_binding.snapshot_sha256 = frozen.snapshot_sha256
                )
                AND EXISTS (
                  SELECT 1 FROM recipe_provider_bindings live_binding
                   WHERE live_binding.binding_id = frozen.source_object_id
                     AND live_binding.is_current
                     AND live_binding.status = 'active'
                )
              )
            )
            AND connection.status = 'valid'
            AND connection.deleted_at IS NULL
            AND (
              connection.allowed_models IS NULL
              OR frozen.snapshot_json->>'model' = ANY(connection.allowed_models)
            )
            AND NOT EXISTS (
              SELECT 1
                FROM jsonb_array_elements(frozen.snapshot_json->'provider_asset_locks') asset_lock
               WHERE NOT EXISTS (
                 SELECT 1
                   FROM jsonb_array_elements(
                     s.image_runtime_contract_json->'provider_assets'
                   ) image_asset
                  WHERE image_asset->>'logical_name' = frozen.logical_name
                    AND image_asset->>'role' = asset_lock->>'role'
                    AND image_asset->>'image_path' = asset_lock->>'image_path'
                    AND image_asset->>'sha256' = asset_lock->>'sha256'
               )
            )
            AND EXISTS (
              SELECT 1
                FROM execution_attempt_provider_budgets attempt_budget
               WHERE attempt_budget.attempt_id = a.id
                 AND attempt_budget.binding_snapshot_sha256 = frozen.snapshot_sha256
                 AND attempt_budget.request_limit = frozen.provider_request_limit
                 AND attempt_budget.cost_limit_microusd = frozen.provider_cost_limit_microusd
                 AND attempt_budget.per_call_timeout_seconds =
                     frozen.per_call_timeout_seconds
            )
       )
     )
     AND w.capability_snapshot_json IS NOT NULL
     AND w.capability_snapshot_json->>'schema_version' = 'loom.worker-capabilities.v1'
     AND (w.capability_snapshot_json->>'cpu_cores')::bigint >=
         (s.resource_profile_json->>'cpu_cores')::bigint
     AND (w.capability_snapshot_json->>'scratch_bytes')::bigint >=
         (s.resource_profile_json->>'scratch_bytes')::bigint
     AND (w.capability_snapshot_json->>'input_cache_capacity_bytes')::bigint >=
         (s.resource_profile_json->>'input_cache_capacity_bytes_min')::bigint
     AND (w.capability_snapshot_json->'network_profiles') ?
         (s.resource_profile_json->>'network_profile')
     AND (s.resource_profile_json->'required_host_runtime_features') <@
         (w.capability_snapshot_json->'container_runtime_features')
     AND r.official_submission_kind IS DISTINCT FROM 'behavior_stage1_smoke_v1'
     AND (s.resource_profile_json->'required_image_features') <@
         (s.image_runtime_contract_json->'application_features')
     AND EXISTS (
       SELECT 1
         FROM jsonb_array_elements(
           s.resource_profile_json->'execution_variants'
         ) variant
        WHERE (
          variant->>'variant_id' =
             s.resolved_execution_spec_json->>'execution_variant_id'
        )
          AND variant->>'cpu_arch' = w.capability_snapshot_json->>'cpu_arch'
          AND variant->>'cpu_arch' = s.image_runtime_contract_json->>'cpu_arch'
          AND w.pool_name = variant->>'pool_class'
          -- GPU Pipeline execution is retired; retained snapshots cannot be claimed.
          AND (variant->>'gpu_count_exact')::integer = 0
          AND (w.capability_snapshot_json->>'memory_bytes')::bigint >=
              COALESCE(
                (variant->>'container_memory_bytes_override')::bigint,
                (s.resource_profile_json->>'memory_bytes')::bigint
              )
          AND jsonb_array_length(w.capability_snapshot_json->'gpu_devices') =
              (variant->>'gpu_count_exact')::integer
          AND s.image_runtime_contract_json->>'gpu_vendor' = 'none'
     )
     AND EXISTS (
       SELECT 1
         FROM jsonb_array_elements(w.capabilities) capability
        WHERE capability->>'os' = 'linux'
          AND COALESCE(capability->>'cpu_arch', 'x86_64') =
              s.image_runtime_contract_json->>'cpu_arch'
          AND capability->>'gpu_vendor' =
              s.image_runtime_contract_json->>'gpu_vendor'
          AND (
            (s.resource_profile_json->>'network_profile' = 'none'
             AND capability->'network_policies' ? 'no-network')
            OR
            (s.resource_profile_json->>'network_profile' = 'gateway'
             AND capability->'network_policies' ? 'allowlist')
          )
     )
     AND r.state IN ('submitted','running')
     AND ledger.terminal_cause IS NULL
     AND ledger.wall_deadline_at > NOW()
     AND r.acceptance_authorization_id IS NULL
), picked AS (
  SELECT work_kind, id, family_key, batch_id
    FROM candidates
   ORDER BY (in_flight_count * 1.0) / NULLIF(fair_share_weight, 0) ASC,
            submit_priority DESC,
            queued_at ASC,
            work_kind ASC,
            id ASC
   LIMIT 1
), admitted AS (
  SELECT picked.*
    FROM picked
   WHERE picked.work_kind <> 'trial'
  UNION ALL
  SELECT picked.*
    FROM picked
    JOIN trials admission_trial ON admission_trial.id = picked.id
    JOIN workers admission_worker ON admission_worker.id = (:worker_id)::uuid
   WHERE picked.work_kind = 'trial'
     AND loom_execution_admission_reserve(
           admission_trial.id,
           admission_trial.attempt_count + 1,
           'attempt',
           admission_trial.team_id,
           admission_trial.batch_id,
           'development',
           NULL,
           admission_trial.execution_route_json->>'selected_execution_class_id',
           admission_worker.pool_name,
           'legacy_worker_claim',
           (:worker_id)::uuid,
           NOW()
         ) IS NOT NULL
), family_lock AS (
  UPDATE batch_family_state bfs
     SET state = 'running', updated_at = NOW()
    FROM admitted p
   WHERE p.work_kind = 'trial'
     AND p.family_key IS NOT NULL
     AND bfs.batch_id = p.batch_id
     AND bfs.family_key = p.family_key
     AND bfs.state = 'pending'
  RETURNING bfs.batch_id, bfs.family_key
), claimed_trial AS (
  UPDATE trials t
     SET state = 'claimed', worker_id = (:worker_id)::uuid, claimed_at = NOW(),
         legacy_claim_id = (:claim_id)::uuid,
         pre_start_heartbeat_at = NULL, failure_reason = NULL, failure_message = NULL,
         attempt_count = attempt_count + 1
    FROM admitted p
   WHERE p.work_kind = 'trial' AND t.id = p.id AND t.state = 'queued'
     AND (
       p.family_key IS NULL
       OR EXISTS (
         SELECT 1 FROM family_lock locked
          WHERE locked.batch_id = p.batch_id
            AND locked.family_key = p.family_key
       )
     )
  RETURNING 'trial'::text AS work_kind, t.id, t.team_id, NULL::uuid AS stage_run_id,
            NULL::uuid AS pipeline_run_id, t.legacy_claim_id AS claim_id,
            NULL::bigint AS lease_epoch, NULL::timestamptz AS lease_expires_at,
            t.task_id, t.config, t.requires_caps, t.attempt_count,
            t.provider_connection_id, t.family_key, t.batch_id,
            (:signed_cpu_arch)::text AS task_image_reader_arch,
            (:worker_lease_epoch)::bigint AS worker_lease_epoch
), claimed_attempt AS (
  UPDATE execution_attempts a
     SET state = 'claimed', worker_id = (:worker_id)::uuid,
         claim_id = (:claim_id)::uuid, lease_epoch = a.lease_epoch + 1,
         lease_token_digest = :lease_token_digest,
         lease_expires_at = NOW() + INTERVAL '60 seconds', claimed_at = NOW(),
         version = a.version + 1
    FROM admitted p, pipeline_stage_runs s, pipeline_runs r
   WHERE p.work_kind = 'execution_attempt' AND a.id = p.id AND a.state = 'queued'
     AND s.id = a.stage_run_id AND s.state = 'queued'
     AND r.id = s.pipeline_run_id AND r.state IN ('submitted','running')
  RETURNING 'execution_attempt'::text AS work_kind, a.id,
            r.team_id, a.stage_run_id, r.id AS pipeline_run_id, a.claim_id,
            a.lease_epoch, a.lease_expires_at, NULL::text AS task_id,
            NULL::jsonb AS config, NULL::jsonb AS requires_caps,
            a.attempt_number, NULL::uuid AS provider_connection_id,
            NULL::text AS family_key, NULL::uuid AS batch_id,
            NULL::text AS task_image_reader_arch, NULL::bigint AS worker_lease_epoch
), stage_claim AS (
  UPDATE pipeline_stage_runs s
     SET state = 'claimed', claimed_at = NOW(), version = s.version + 1
   FROM claimed_attempt a
   WHERE s.id = a.stage_run_id AND s.state = 'queued'
  RETURNING s.id
)
SELECT * FROM claimed_trial
UNION ALL
SELECT a.* FROM claimed_attempt a JOIN stage_claim s ON s.id = a.stage_run_id
""")


async def claim_work(
    session: AsyncSession,
    *,
    worker_id: UUID,
    capability_snapshot_digest: str,
    worker_token_hash: bytes,
    supported_work_kinds: list[str],
    free_slots: int,
    worker_os: list[str],
    worker_cpu_arches: list[str],
    worker_gpu_vendors: list[str],
    worker_network_policies: list[str],
    worker_backends: list[str] | None = None,
    allow_signed_task_images: bool = False,
) -> tuple[RowMapping, str | None] | None:
    """Atomically select one Trial or ExecutionAttempt from the shared queue."""

    if type(allow_signed_task_images) is not bool:
        raise WorkClaimConflictError("invalid_execution_reader_selection")
    # Selection is provisional, not publication/execution authority. Commit
    # these worker/Trial locks before the separate state-first issuance phase;
    # neither signer I/O nor immutable grant preparation runs in this transaction.
    guard = (
        (await session.execute(_WORKER_CLAIM_GUARD_SQL, {"worker_id": worker_id}))
        .mappings()
        .one_or_none()
    )
    if guard is None:
        raise WorkClaimConflictError("worker_unknown")
    if guard["status"] != "active" or guard["drain_state"] != "active":
        raise WorkClaimConflictError("worker_not_active")
    if guard["auth_token_hash"] is None or not hmac.compare_digest(
        bytes(guard["auth_token_hash"]), worker_token_hash
    ):
        raise WorkClaimConflictError("worker_identity_mismatch")
    if guard["capability_snapshot_digest"] != capability_snapshot_digest:
        raise WorkClaimConflictError("capability_snapshot_drift")
    registered_kinds = list(guard["supported_work_kinds"] or ["trial"])
    if supported_work_kinds != registered_kinds:
        raise WorkClaimConflictError("supported_work_kinds_drift")
    if free_slots < 1 or free_slots > int(guard["max_concurrent"]):
        raise WorkClaimConflictError("invalid_free_slots")
    if int(guard["active_count"]) >= int(guard["max_concurrent"]):
        raise WorkClaimConflictError("worker_capacity_exhausted")

    signed_cpu_arch = ""
    if allow_signed_task_images:
        raw = guard["capability_snapshot_json"] or {}
        if "task-image-execution-v2" in raw.get("container_runtime_features", []):
            try:
                snapshot = WorkerCapabilitySnapshotV1.model_validate_json(canonical_document(raw))
            except (ValueError, ValidationError):
                raise WorkClaimConflictError("execution_reader_capability_invalid") from None
            if (
                canonical_digest(snapshot.model_dump(mode="json")) != guard["capability_snapshot_digest"]
                or registered_kinds not in (["trial"], ["trial", "execution_attempt"])
                or snapshot.cpu_arch not in worker_cpu_arches
            ):
                raise WorkClaimConflictError("execution_reader_capability_drift")
            signed_cpu_arch = snapshot.cpu_arch

    raw_lease_token = secrets.token_urlsafe(32)
    params: dict[str, Any] = {
        "worker_id": worker_id,
        "supported_work_kinds": registered_kinds,
        "worker_os": worker_os,
        "worker_cpu_arches": worker_cpu_arches,
        "worker_gpu_vendors": worker_gpu_vendors,
        "worker_network_policies": json.dumps(worker_network_policies),
        "worker_backends": worker_backends or ["docker"],
        "claim_id": uuid4(),
        "lease_token_digest": sha256(raw_lease_token.encode()).hexdigest(),
        "allow_signed_task_images": bool(signed_cpu_arch),
        "signed_cpu_arch": signed_cpu_arch,
        "worker_lease_epoch": guard["lease_epoch"],
    }
    result = (await session.execute(_WORK_CLAIM_SQL, params)).mappings().one_or_none()
    if result is None:
        return None
    if result["work_kind"] == "execution_attempt":
        event = (
            (
                await session.execute(
                    text("""
                    UPDATE pipeline_runs
                       SET state = CASE WHEN state = 'submitted' THEN 'running' ELSE state END,
                           started_at = COALESCE(started_at, NOW()),
                           next_event_seq = next_event_seq + 1,
                           version = version + 1
                     WHERE id = (:run_id)::uuid
                 RETURNING next_event_seq - 1 AS seq
                """),
                    {"run_id": result["pipeline_run_id"]},
                )
            )
            .mappings()
            .one()
        )
        await session.execute(
            text("""
                INSERT INTO pipeline_events (
                    pipeline_run_id, seq, stage_run_id, execution_attempt_id,
                    event_type, actor_kind, actor_id, payload_json
                ) VALUES (
                    (:run_id)::uuid, :seq, (:stage_id)::uuid, (:attempt_id)::uuid,
                    'execution_attempt_claimed', 'worker', (:worker_id)::text,
                    jsonb_build_object('claim_id', (:claim_id)::text,
                                       'lease_epoch', :lease_epoch)
                )
            """),
            {
                "run_id": result["pipeline_run_id"],
                "seq": event["seq"],
                "stage_id": result["stage_run_id"],
                "attempt_id": result["id"],
                "worker_id": worker_id,
                "claim_id": result["claim_id"],
                "lease_epoch": result["lease_epoch"],
            },
        )
    return result, raw_lease_token if result["work_kind"] == "execution_attempt" else None
