"""Normal queued-Trial admission into the durable service-execution path."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, Protocol
from uuid import UUID

from pydantic import ValidationError
from sqlalchemy import Integer, Select, Text, exists, func, or_, select, text, update
from sqlalchemy import cast as sql_cast
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from loom.db.schema import (
    ServiceExecutionLease,
    ServiceExecutionTarget,
    TaskImageMaterialization,
    Trial,
    TrialTaskImageMaterialization,
)
from loom.execution_contract import (
    ExecutionPlacementPolicy,
    ExecutionRoutingReason,
    WorkloadRequirementsV1,
    workload_requirements_from_task,
)
from loom.execution_image_admission import ImageAdmissionKeyring
from loom.execution_runtime_contract import ExecutionRuntimePlanV1
from loom.models.task import TaskConfig, bind_service_execution_runtime_plan
from loom.models.trial import TrialConfig
from loom.pipeline.keys import canonical_digest, canonical_uuid5
from loom.service_execution_materialization import (
    ServiceExecutionRuntimeProfileV1,
    build_verifier_handoff_manifest,
    compile_deferred_verifier_plan,
    compile_service_execution_plan,
    resolve_runner_task_image,
    verifier_handoff_input,
)
from loom.task_image_failure import image_preparation_failure
from loom.task_image_materialization import (
    get_trial_task_image_execution_grant,
    required_task_image_components,
    resolve_prepared_task,
)
from loom.verifier_runtime import apply_legacy_verifier_default
from loom_control_plane.execution_capacity import ExecutionProvisioningBlockedError
from loom_control_plane.execution_resource_allocation import allocate_target_resources
from loom_control_plane.metrics import SERVICE_EXECUTION_RESERVATION_SECONDS
from loom_control_plane.service_execution import (
    ServiceExecutionConflict,
    ServiceExecutionParentTerminalError,
    ServiceExecutionPolicyBlockedError,
    committed_handoff_files,
    mark_verifier_unavailable,
    reserve_trial_execution,
    verifier_retries,
)
from loom_control_plane.service_execution_task_snapshot import (
    ServiceExecutionTaskSnapshotError,
    resolve_service_execution_task_snapshot,
)

_LOG = logging.getLogger(__name__)
_RESERVATION_REQUEST_NAMESPACE = UUID("aaf78d09-4268-4dc5-81ee-4c2408ce2611")
# A handed-off attempt waits for capacity like any queued work, but not forever:
# its committed workspace is graded or the Trial fails as verifier_unavailable.
VERIFIER_HANDOFF_TIMEOUT = timedelta(minutes=30)


class ServiceExecutionConfigurationError(ValueError):
    """A known per-Trial configuration cannot run under this scheduler's bounds."""


class GlobalExecutionSelector(Protocol):
    async def select_next(self) -> object | None: ...


@dataclass(frozen=True)
class CompiledServiceExecution:
    """A proposed workload, not a lease, capacity grant or attempt reservation.

    Consumers must freeze their selected target/allocated plan durably before
    external admission, then recheck local authority when committing the claim.
    """

    runtime_plan: ExecutionRuntimePlanV1
    requirements: WorkloadRequirementsV1
    deadline_at: datetime
    targets: tuple[ServiceExecutionTarget, ...]
    allocate_resources: bool
    image_ready_at: datetime | None
    image_mode: Literal["reused", "built", "unknown", "prebuilt"]


# Mirror required_task_image_components for the persisted TaskConfig shape.
# Missing bindings are not evidence that a Dockerfile/sidecar/Compose build is
# unnecessary. Keep both task and independent-verifier environments covered.
_TASK_IMAGE_BUILD_REQUIRED_SQL = " OR ".join(
    f"jsonb_path_exists(task_definition.config, '$.{environment}.{declaration}')"
    for environment in ("environment", "verifier.environment")
    for declaration in (
        "dockerfile ? (@ != null)",
        "sidecars[*].dockerfile ? (@ != null)",
        'native_compose.services[*] ? (@.name != "main" && @.build != null)',
    )
)

_SERVICE_TRIAL_SQL = f"""
SELECT t.id,
       t.submitted_at,
       t.task_id,
       t.attempt_count,
       t.execution_route_generation,
       t.execution_route_pool_name,
       t.requires_caps,
       task_definition.checksum AS task_checksum,
       task_definition.config AS task_config,
       task_definition.source_provenance AS task_source_provenance,
       task_definition.legacy_separate_verifier_checksum,
       t.config AS trial_config,
       b.service_execution_runtime_profile AS batch_runtime_profile
  FROM trials t
  JOIN batches b ON b.id = t.batch_id
  JOIN tasks task_definition ON task_definition.id = t.task_id
  JOIN team_quotas q ON q.team_id = t.team_id
 WHERE t.state = 'queued'
   AND t.cancellation_requested_at IS NULL
   AND t.attempt_count < q.max_attempts_ceiling
   AND (t.next_attempt_at IS NULL OR t.next_attempt_at <= :now)
   AND t.family_key IS NULL
   AND b.backend = 'nebius'
   AND b.state IN ('submitted', 'running')
   AND t.requires_caps->>'worker_pool' = :pool_id
   AND (
         t.execution_route_pool_name IS NULL
         OR t.execution_route_pool_name = :pool_id
       )
   AND (
         EXISTS (
           SELECT 1
             FROM trial_task_image_materializations prerequisite
             JOIN task_image_materializations image
               ON image.id = prerequisite.materialization_id
            WHERE prerequisite.trial_id = t.id
              AND image.task_id = t.task_id
              AND image.cpu_arch = 'x86_64'
              AND image.state = 'ready'
         )
         OR (
           EXISTS (
             SELECT 1
               FROM trial_task_image_materializations prerequisite
               JOIN task_image_materializations image
                 ON image.id = prerequisite.materialization_id
              WHERE prerequisite.trial_id = t.id
                AND image.task_id = t.task_id
                AND image.cpu_arch = 'x86_64'
           )
           AND NOT EXISTS (
             SELECT 1
               FROM trial_task_image_materializations prerequisite
               JOIN task_image_materializations image
                 ON image.id = prerequisite.materialization_id
              WHERE prerequisite.trial_id = t.id
                AND image.task_id = t.task_id
                AND image.cpu_arch = 'x86_64'
                AND image.state != 'failed'
           )
         )
         OR (
           NOT ({_TASK_IMAGE_BUILD_REQUIRED_SQL})
           AND NOT EXISTS (
             SELECT 1
               FROM trial_task_image_materializations prerequisite
               JOIN task_image_materializations image
                 ON image.id = prerequisite.materialization_id
              WHERE prerequisite.trial_id = t.id
                AND (
                     image.cpu_arch = 'x86_64'
                     OR image.task_id != t.task_id
                )
           )
         )
       )
   AND NOT EXISTS (
         SELECT 1 FROM execution_leases lease
          WHERE lease.trial_id = t.id
            AND lease.execution_role = 'attempt'
            AND (lease.revoked_at IS NULL OR lease.cleanup_state != 'complete')
       )
"""

# Shared by local selection, global proposals and their locked by-id rechecks.
# Ready x86_64 alternatives can run; truly prebuilt tasks need no prerequisite;
# all-failed prerequisites still reach compilation to settle without an attempt.
# Keep the authoritative image/source/grant checks in compilation and claim.
_NEXT_SERVICE_TRIAL = text(_SERVICE_TRIAL_SQL + """
 ORDER BY (q.in_flight_count::double precision / q.fair_share_weight) ASC,
          t.submit_priority DESC,
          t.submitted_at ASC,
          t.id ASC
 FOR UPDATE OF t SKIP LOCKED
 LIMIT 1
""")

# The exact preselected candidate is reloaded under the same eligibility rules.
# Hold source/profile rows too: compilation or post-grant recheck cannot race a
# concurrent change to the task definition or batch runtime configuration.
_SERVICE_TRIAL_BY_ID = text(_SERVICE_TRIAL_SQL + """
   AND t.id = :trial_id
 FOR UPDATE OF t
 FOR SHARE OF task_definition, b
""")


def _task_revision(checksum: str) -> str:
    value = str(checksum).lower()
    if value.startswith("sha256:"):
        value = value.removeprefix("sha256:")
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError("service-execution task checksum is not sha256")
    return "sha256:" + value


def _deadline(
    plan: ExecutionRuntimePlanV1,
    *,
    now: datetime,
    maximum_seconds: int,
) -> datetime:
    phase_seconds = sum(item.timeout_seconds for item in plan.setup) + plan.main.timeout_seconds + plan.main.finalization_timeout_seconds
    if plan.verifier is not None:
        phase_seconds += plan.verifier.timeout_seconds
    requested_seconds = phase_seconds + plan.termination_grace_seconds + 600
    if requested_seconds > maximum_seconds:
        raise ServiceExecutionConfigurationError(
            "service-execution runtime exceeds the scheduler deadline bound: "
            f"requested_seconds={requested_seconds}, maximum_seconds={maximum_seconds}"
        )
    return now + timedelta(seconds=requested_seconds)


async def _ready_targets(
    session: AsyncSession,
    *,
    environment: str,
    pool_id: str,
    execution_class_id: str,
    now: datetime,
    placement_policy: ExecutionPlacementPolicy = "environment-local-health-first",
    region: str | None = None,
) -> list[ServiceExecutionTarget]:
    targets = (
        (
            await session.execute(
                select(ServiceExecutionTarget)
                .where(
                    ServiceExecutionTarget.environment == environment,
                    ServiceExecutionTarget.logical_pool_id == pool_id,
                    ServiceExecutionTarget.execution_class_id == execution_class_id,
                    ServiceExecutionTarget.desired_state == "active",
                    ServiceExecutionTarget.observed_state == "ready",
                    ServiceExecutionTarget.health_status == "healthy",
                )
                .order_by(ServiceExecutionTarget.region, ServiceExecutionTarget.id)
            )
        )
        .scalars()
        .all()
    )
    ready = []
    for target in targets:
        if region is not None and target.region != region:
            continue
        if target.health_observed_at is None:
            continue
        stale_after = int(target.spec_json["health_stale_after_seconds"])
        if target.health_observed_at + timedelta(seconds=stale_after) > now:
            ready.append(target)
    if placement_policy == "environment-local-capacity-balanced":
        from loom_control_plane.execution_capacity_ordering import order_targets_by_capacity

        return await order_targets_by_capacity(session, ready, now=now)
    if placement_policy != "environment-local-health-first":
        raise ValueError("unsupported service execution placement policy")
    return sorted(ready, key=lambda target: target.spec_json.get("health_role") != "primary")


async def reserve_next_service_execution(
    session: AsyncSession,
    *,
    environment: str,
    pool_id: str,
    image_admission_keyring: ImageAdmissionKeyring,
    maximum_deadline_seconds: int = 7200,
    placement_policy: ExecutionPlacementPolicy = "environment-local-health-first",
    now: datetime | None = None,
) -> ServiceExecutionLease | None:
    """Reserve one queued task within a caller-managed transaction.

    This bounded convenience API does not commit its caller's work. The live
    dispatcher uses the single-candidate operation with a fresh transaction for
    each denial, so remembered capacity waits cannot lock the remaining wave.
    """
    current_time = (now or datetime.now(UTC)).astimezone(UTC)
    for _ in range(32):
        lease, continue_scan = await _reserve_service_execution_candidate(
            session, environment=environment, pool_id=pool_id,
            image_admission_keyring=image_admission_keyring,
            maximum_deadline_seconds=maximum_deadline_seconds,
            placement_policy=placement_policy, now=current_time,
        )
        if lease is not None or not continue_scan:
            return lease
    return None


async def _reserve_service_execution_candidate(
    session: AsyncSession,
    *,
    environment: str,
    pool_id: str,
    image_admission_keyring: ImageAdmissionKeyring,
    maximum_deadline_seconds: int = 7200,
    placement_policy: ExecutionPlacementPolicy = "environment-local-health-first",
    now: datetime | None = None,
) -> tuple[ServiceExecutionLease | None, bool]:
    """Examine one candidate; report whether a denial permits further scanning.

    The caller owns the transaction. A continuous dispatcher must commit this
    candidate before compiling another: remembered waits take capacity authority.
    """
    current_time = (now or datetime.now(UTC)).astimezone(UTC)
    row = (
        (await session.execute(_NEXT_SERVICE_TRIAL, {"now": current_time, "pool_id": pool_id}))
        .mappings()
        .one_or_none()
    )
    if row is None:
        return None, False
    # The fair scan holds only Trial, so parent cancellation can commit before
    # compilation. Reload the exact eligible source under shared Task/Batch
    # locks: an edit either wins this check or waits for this claim to commit.
    # Pin outside the write savepoint so a classified compilation failure cannot
    # release its source locks before the terminal Trial update below.
    row = (await session.execute(_SERVICE_TRIAL_BY_ID, {
        "trial_id": row["id"], "pool_id": pool_id, "now": current_time,
    })).mappings().one_or_none()
    if row is None:
        return None, True
    try:
        async with session.begin_nested():
            lease = await _reserve_service_candidate(
                session,
                row=row,
                environment=environment,
                pool_id=pool_id,
                image_admission_keyring=image_admission_keyring,
                maximum_deadline_seconds=maximum_deadline_seconds,
                current_time=current_time,
                placement_policy=placement_policy,
            )
            return lease, False
    except ServiceExecutionParentTerminalError:
        # The parent changed after selection. Its cancellation/settlement
        # owns the child state; rollback this candidate and continue scanning.
        return None, True
    except ServiceExecutionConfigurationError as exc:
        # The candidate savepoint has rolled back. This queued failure owns
        # no attempt, lease, admission slot or spend; leave transient and
        # unexpected errors on their existing retry paths.
        await session.execute(update(Trial).where(
            Trial.id == row["id"], Trial.state == "queued",
            Trial.cancellation_requested_at.is_(None),
        ).values(
            state="failed", failure_reason="service_execution_configuration_invalid",
            failure_message=str(exc), finished_at=current_time, next_attempt_at=None,
            scheduling_observation=None,
        ))
        _LOG.warning("service_execution_configuration_invalid", extra={
            "trial_id": str(row["id"]), "reason": str(exc),
        })
    except ServiceExecutionPolicyBlockedError as exc:
        # Only policy denials are candidate-local waits. The savepoint has
        # released their route, admission seat and spend; identity and
        # invariant conflicts must still escape to the scheduler's error path.
        retry_at = current_time + timedelta(seconds=max(1, min(300, exc.retry_after_seconds)))
        await session.execute(update(Trial).where(
            Trial.id == row["id"], Trial.state == "queued",
            Trial.cancellation_requested_at.is_(None),
        ).values(
            next_attempt_at=retry_at,
            scheduling_observation={"reason": exc.reason, "observed_at": current_time.isoformat()},
        ))
        _LOG.info("service_execution_policy_wait", extra={
            "trial_id": str(row["id"]), "reason": exc.reason,
        })
    except ExecutionProvisioningBlockedError as exc:
        from loom_control_plane.execution_capacity_wait import remember_execution_capacity_wait
        from loom_control_plane.task_image_capacity_wait import WAIT_LEASE_SECONDS

        delay = max(1, min(300, exc.retry_after_seconds))
        if exc.capacity_wait is not None:
            delay = min(delay, WAIT_LEASE_SECONDS // 2)
        retry_at = current_time + timedelta(seconds=delay)
        observation = await remember_execution_capacity_wait(
            session, row=row, context=exc.capacity_wait, reason=exc.reason,
            now=current_time, retry_at=retry_at,
        )
        await session.execute(
            update(Trial)
            .where(Trial.id == row["id"], Trial.state == "queued")
            .values(
                next_attempt_at=retry_at,
                scheduling_observation=observation,
            )
        )
        _LOG.info("service_execution_capacity_wait", extra={"reason": exc.reason})
    return None, True


async def _compile_service_candidate(
    session: AsyncSession,
    *,
    row: Any,
    environment: str,
    pool_id: str,
    maximum_deadline_seconds: int,
    current_time: datetime,
    placement_policy: ExecutionPlacementPolicy = "environment-local-health-first",
) -> CompiledServiceExecution | None:
    try:
        return await _compile_service_candidate_plan(session, row=row, environment=environment,
            pool_id=pool_id, maximum_deadline_seconds=maximum_deadline_seconds, current_time=current_time,
            placement_policy=placement_policy)
    except ValidationError:
        # Pydantic messages include input values. Both local and shared-pool
        # callers receive the same safe error before any attempt or reservation.
        raise ServiceExecutionConfigurationError(
            "Native execution configuration violates the runtime contract; review task and trial "
            "settings and the selected runtime profile. No execution attempt was started."
        ) from None


async def _compile_service_candidate_plan(
    session: AsyncSession,
    *,
    row: Any,
    environment: str,
    pool_id: str,
    maximum_deadline_seconds: int,
    current_time: datetime,
    placement_policy: ExecutionPlacementPolicy = "environment-local-health-first",
) -> CompiledServiceExecution | None:
    # Architecture records are alternatives. Nebius's current execution class
    # uses x86_64; an unused arm64 build must not hold up admission.
    prerequisites = list((await session.execute(
        select(TaskImageMaterialization)
        .join(TrialTaskImageMaterialization,
              TrialTaskImageMaterialization.materialization_id == TaskImageMaterialization.id)
        .join(Trial, Trial.id == TrialTaskImageMaterialization.trial_id)
        .where(Trial.id == row["id"], TaskImageMaterialization.task_id == Trial.task_id,
               TaskImageMaterialization.cpu_arch == "x86_64")
        .with_for_update(of=TaskImageMaterialization)
    )).scalars())
    if prerequisites and not any(item.state == "ready" for item in prerequisites):
        if all(item.state == "failed" for item in prerequisites):
            # Nothing was admitted: no attempt, lease, quota or model accounting
            # exists to release. Use the same queued terminal transition as the
            # retry-exhaustion sweeper; batch status derives from Trial state.
            typed_failure = image_preparation_failure(prerequisites[0].failure_reason)
            result = ({"task_image_preparation_failure": typed_failure} if typed_failure is not None else None)
            await session.execute(update(Trial).where(
                Trial.id == row["id"], Trial.state == "queued",
                Trial.cancellation_requested_at.is_(None),
            ).values(state="failed", failure_reason="task_image_build_failed",
                     failure_message="Task image preparation failed; inspect the build result.",
                     finished_at=current_time, next_attempt_at=None,
                     **({"result": result} if result is not None else {})))
            return None
        raise ExecutionProvisioningBlockedError("task_image_preparation_pending", retry_after_seconds=15)
    try:
        # The selection predicate is advisory. Recheck missing bindings under
        # the usual source locks rather than treating a build task as prebuilt.
        if not prerequisites and required_task_image_components(TaskConfig.model_validate(row["task_config"])):
            raise RuntimeError("required task-image prerequisite is unavailable")
        grant = (await get_trial_task_image_execution_grant(
            session, trial_id=row["id"], cpu_arches=["x86_64"],
        ) if prerequisites else None)
    except RuntimeError as exc:
        raise ExecutionProvisioningBlockedError(
            "task_image_preparation_pending", retry_after_seconds=15,
        ) from exc
    task = TaskConfig.model_validate(grant.task_config if grant else row["task_config"])
    task_revision = _task_revision(grant.task_checksum if grant else row["task_checksum"])
    source_provenance = (grant.task_source_provenance if grant
                         else dict(row["task_source_provenance"] or {}))
    allocate_resources = False
    binding = task.service_execution
    trial_config = None
    if binding is not None:
        if (row["batch_runtime_profile"] or {}).get("task_resource_requests", {}).get(row["task_id"]):
            raise ValueError("task resource requests require automatic native execution")
        if binding.logical_pool_id != pool_id:
            raise ValueError("queued service-execution task binding drift")
        runtime_plan = bind_service_execution_runtime_plan(
            binding.runtime_template,
            task_revision_sha256=task_revision,
        )
    else:
        raw_profile = row["batch_runtime_profile"]
        if raw_profile is None:
            return None
        runtime_profile = ServiceExecutionRuntimeProfileV1.model_validate(raw_profile)
        allocate_resources = runtime_profile.resource_allocation_policy == "node-share-v1"
        if runtime_profile.logical_pool_id != pool_id:
            raise ValueError("queued service-execution runtime profile pool drift")
        trial_config = TrialConfig.model_validate(row["trial_config"])
        effective_trial = apply_legacy_verifier_default(
            task, trial_config, task_checksum=task_revision,
            legacy_separate_verifier_checksum=row.get("legacy_separate_verifier_checksum"),
            source_provenance=source_provenance,
        )
        if effective_trial is not trial_config:
            # An old CP can still submit while the schema-first rollout is
            # paused. Freeze its missing default under the Trial lock before
            # compiling/reserving, using the pinned image revision above.
            await session.execute(update(Trial).where(Trial.id == row["id"]).values(
                config={**row["trial_config"],
                        "verifier_env_mode": effective_trial.verifier_env_mode},
            ))
            trial_config = effective_trial
        runtime_plan = compile_service_execution_plan(
            task_id=row["task_id"],
            task=task,
            trial=trial_config,
            task_revision_sha256=task_revision,
            source_provenance=source_provenance,
            task_image_grant=grant,
            profile=runtime_profile,
        )
    deadline_at = _deadline(
        runtime_plan, now=current_time, maximum_seconds=maximum_deadline_seconds,
    )
    targets = await _ready_targets(
        session,
        environment=environment,
        pool_id=pool_id,
        execution_class_id=runtime_plan.execution_class_id,
        now=current_time,
        placement_policy=placement_policy,
    )
    requirements = workload_requirements_from_task(
        resolve_prepared_task(task, grant) if grant
        else resolve_runner_task_image(task, runtime_plan.task_image_ref),
        trial_config if binding is None else None,
    )
    ready_times = [image.ready_at for image in prerequisites if image.ready_at]
    image_ready = max(ready_times) if ready_times else row["submitted_at"]
    return CompiledServiceExecution(runtime_plan=runtime_plan, requirements=requirements,
        deadline_at=deadline_at, targets=tuple(targets), allocate_resources=allocate_resources,
        image_ready_at=max(row["submitted_at"], image_ready) if ready_times or not prerequisites else None,
        image_mode=("reused" if ready_times and image_ready <= row["submitted_at"]
                    else "built" if ready_times else "unknown" if prerequisites else "prebuilt"))


async def _reserve_service_candidate(
    session: AsyncSession,
    *,
    row: Any,
    environment: str,
    pool_id: str,
    image_admission_keyring: ImageAdmissionKeyring,
    maximum_deadline_seconds: int,
    current_time: datetime,
    placement_policy: ExecutionPlacementPolicy = "environment-local-health-first",
) -> ServiceExecutionLease | None:
    compiled = await _compile_service_candidate(session, row=row, environment=environment,
        pool_id=pool_id, maximum_deadline_seconds=maximum_deadline_seconds, current_time=current_time,
        placement_policy=placement_policy)
    if compiled is None:
        return None
    runtime_plan, requirements = compiled.runtime_plan, compiled.requirements
    blocked: ExecutionProvisioningBlockedError | None = None
    policy_blocked: ServiceExecutionPolicyBlockedError | None = None
    for target in compiled.targets:
        if requirements.data_residency and target.data_residency != requirements.data_residency:
            continue
        # A target's failed admission must not keep a route, cost reservation or
        # attempt increment when the next eligible region is tried.
        target_id = target.id
        try:
            async with session.begin_nested():
                allocated_plan = (await allocate_target_resources(
                    session, runtime_plan, target_id=target_id, now=current_time,
                ) if compiled.allocate_resources else runtime_plan)
                lease = await reserve_trial_execution(
                    session,
                    request_id=canonical_uuid5(
                        _RESERVATION_REQUEST_NAMESPACE,
                        {
                            "schema_version": "loom.service-execution-reservation-request.v1",
                            "trial_id": str(row["id"]),
                            "attempt": int(row["attempt_count"]) + 1,
                            "target_id": target_id,
                            "task_revision_sha256": runtime_plan.task_revision_sha256,
                            "runtime_contract_sha256": canonical_digest(
                                allocated_plan.canonical_payload()
                            ),
                        },
                    ),
                    trial_id=row["id"],
                    execution_class_id=runtime_plan.execution_class_id,
                    target_id=target_id,
                    requirements=requirements,
                    runtime_contract=allocated_plan,
                    image_admission_keyring=image_admission_keyring,
                    routing_reason=ExecutionRoutingReason.PREEXISTING_ASSIGNMENT,
                    deadline_at=compiled.deadline_at,
                    now=current_time,
                    capacity_wait_inputs={key: row[key] for key in (
                        "execution_route_generation", "execution_route_pool_name",
                    )},
                )
                await session.execute(update(Trial).where(Trial.id == row["id"]).values(
                    scheduling_observation={
                        "observed_at": current_time.isoformat(), "lease_id": str(lease.id),
                        "image_ready_at": compiled.image_ready_at.isoformat() if compiled.image_ready_at else None,
                        "image_mode": compiled.image_mode,
                    },
                ))
                return lease
        except ServiceExecutionPolicyBlockedError as exc:
            if exc.scope_kind not in {"target", "region"}:
                raise
            # A local policy denial does not rule out another target/region.
            # Its savepoint has released all reservation side effects.
            if policy_blocked is None:
                policy_blocked = exc
        except ExecutionProvisioningBlockedError as exc:
            # One realizable target is one waiting demand, never a reservation
            # for every alternative region. Preserve its compiled envelope.
            if blocked is None or (blocked.capacity_wait is None and exc.capacity_wait is not None):
                blocked = exc
    if blocked is not None:
        raise blocked
    if policy_blocked is not None:
        raise policy_blocked
    raise ExecutionProvisioningBlockedError("execution_target_unavailable", retry_after_seconds=15)


def verifier_candidates(pool_id: str) -> Select[tuple[ServiceExecutionLease, Trial]]:
    """One eligibility predicate for local reservation and global proposal."""
    child = aliased(ServiceExecutionLease)
    retries = sql_cast(func.coalesce(
        Trial.result["verifier_execution"]["retries"].astext, "0"), Integer)
    return (
        select(ServiceExecutionLease, Trial)
        .join(Trial, Trial.id == ServiceExecutionLease.trial_id)
        .where(
            ServiceExecutionLease.execution_role == "attempt",
            ServiceExecutionLease.selected_pool_id == pool_id,
            ServiceExecutionLease.output_commit_state == "committed",
            ServiceExecutionLease.finalized_at.is_not(None),
            ServiceExecutionLease.cleanup_state == "complete",
            ServiceExecutionLease.deleted_at.is_not(None),
            ServiceExecutionLease.runtime_contract_json["verifier_execution"].astext
            == "separate_execution",
            Trial.state.in_(("claimed", "running")),
            Trial.attempt_count == ServiceExecutionLease.attempt,
            Trial.cancellation_requested_at.is_(None),
            Trial.result["verifier_execution"]["state"].astext == "pending",
            Trial.result["verifier_execution"]["parent_lease_id"].astext
            == sql_cast(ServiceExecutionLease.id, Text),
            ~exists().where(
                child.parent_lease_id == ServiceExecutionLease.id,
                child.execution_role == "verifier",
                or_(child.verifier_retry >= retries, child.deleted_at.is_(None)),
            ),
        )
        .order_by(ServiceExecutionLease.deleted_at, ServiceExecutionLease.id)
        .with_for_update(of=Trial, skip_locked=True)
    )


async def reserve_next_verifier_executions(
    session: AsyncSession,
    *,
    pool_id: str,
    image_admission_keyring: ImageAdmissionKeyring,
    maximum_deadline_seconds: int = 7200,
    placement_policy: ExecutionPlacementPolicy = "environment-local-health-first",
    now: datetime | None = None,
    limit: int = 32,
    parent_lease_id: UUID | None = None,
) -> list[ServiceExecutionLease]:
    """Reserve the deferred verifier of each attempt whose pod has released capacity.

    Stateless and idempotent: the request id derives from the parent lease, the
    retry number and the child plan, and one verifier per attempt and retry is a
    database constraint. A retry waits until the failed verifier's pod is gone.
    """

    current_time = (now or datetime.now(UTC)).astimezone(UTC)
    query = verifier_candidates(pool_id)
    if parent_lease_id is not None:
        query = query.where(ServiceExecutionLease.id == parent_lease_id)
    candidates = (await session.execute(query.limit(limit))).all()
    reserved: list[ServiceExecutionLease] = []
    for parent, trial in candidates:
        try:
            async with session.begin_nested():
                reserved.append(await _reserve_verifier_candidate(
                    session, parent=parent, trial=trial,
                    image_admission_keyring=image_admission_keyring,
                    maximum_deadline_seconds=maximum_deadline_seconds, current_time=current_time,
                    placement_policy=placement_policy,
                ))
        except (ExecutionProvisioningBlockedError, ServiceExecutionConflict) as exc:
            # Capacity, rollout and target health are waits, bounded per handoff.
            waiting_since = _handoff_pending_since(trial) or parent.deleted_at
            if waiting_since is not None and current_time - waiting_since >= VERIFIER_HANDOFF_TIMEOUT:
                _fail_verifier_handoff(trial, reason=getattr(exc, "reason", None) or str(exc),
                                       now=current_time)
            else:
                _LOG.info("service_execution_verifier_wait", extra={
                    "trial_id": str(trial.id), "reason": getattr(exc, "reason", None) or str(exc),
                })
        except (ServiceExecutionTaskSnapshotError, ServiceExecutionConfigurationError,
                ValueError) as exc:
            _fail_verifier_handoff(trial, reason=str(exc), now=current_time)
    return reserved


def _handoff_pending_since(trial: Trial) -> datetime | None:
    handoff = (trial.result or {}).get("verifier_execution")
    raw = handoff.get("pending_since") if isinstance(handoff, dict) else None
    try:
        return datetime.fromisoformat(raw) if isinstance(raw, str) else None
    except ValueError:
        return None


def _fail_verifier_handoff(trial: Trial, *, reason: str, now: datetime) -> None:
    mark_verifier_unavailable(trial, None, reason=reason)
    trial.state = "failed"
    trial.finished_at = now
    trial.failure_message = f"deferred verifier could not be reserved: {reason}"[:2000]
    _LOG.warning("service_execution_verifier_unavailable", extra={
        "trial_id": str(trial.id), "reason": reason,
    })


async def compile_verifier_candidate(
    session: AsyncSession, *, parent: ServiceExecutionLease, trial: Trial,
    maximum_deadline_seconds: int, current_time: datetime,
) -> tuple[ExecutionRuntimePlanV1, datetime]:
    """Compile the retained parent workspace without claiming physical capacity."""
    if parent.target_id is None:
        raise ServiceExecutionConflict("verifier parent has no execution target")
    agent_plan = ExecutionRuntimePlanV1.model_validate(parent.runtime_contract_json)
    snapshot = await resolve_service_execution_task_snapshot(session, lease=parent, trial=trial)
    task = TaskConfig.model_validate(snapshot.config)
    trial_config = TrialConfig.model_validate(trial.config)
    files, _ = await committed_handoff_files(session, parent=parent)
    handoff = verifier_handoff_input(build_verifier_handoff_manifest(
        task_revision_sha256=agent_plan.task_revision_sha256, committed_files=files,
    ))
    verifier_timeout = ((trial_config.override_verifier_timeout_sec or task.verifier.timeout_sec)
                        * trial_config.verifier_timeout_multiplier)
    plan = compile_deferred_verifier_plan(
        agent_plan, task, verifier_timeout_seconds=round(verifier_timeout), handoff_input=handoff,
    )
    # The verifier's own budget starts at this reservation, not at the agent's.
    deadline_at = _deadline(plan, now=current_time, maximum_seconds=maximum_deadline_seconds)
    return plan, deadline_at


async def _reserve_verifier_candidate(
    session: AsyncSession,
    *,
    parent: ServiceExecutionLease,
    trial: Trial,
    image_admission_keyring: ImageAdmissionKeyring,
    maximum_deadline_seconds: int,
    current_time: datetime,
    placement_policy: ExecutionPlacementPolicy = "environment-local-health-first",
) -> ServiceExecutionLease:
    plan, deadline_at = await compile_verifier_candidate(session, parent=parent, trial=trial,
        maximum_deadline_seconds=maximum_deadline_seconds, current_time=current_time)
    assert parent.target_id is not None
    from loom.task_environment_handoff import (
        RESOLVED_VERIFIER_ENVIRONMENT,
        decode_resolved_task_environment,
    )
    from loom.verifier_runtime import verifier_execution_task
    requirements = WorkloadRequirementsV1.model_validate(parent.workload_requirements_json)
    if RESOLVED_VERIFIER_ENVIRONMENT in plan.main.environment:
        snapshot = await resolve_service_execution_task_snapshot(session, lease=parent, trial=trial)
        task = TaskConfig.model_validate(snapshot.config)
        task = task.model_copy(update={"verifier": task.verifier.model_copy(update={"environment":
            decode_resolved_task_environment(plan.main.environment[RESOLVED_VERIFIER_ENVIRONMENT])})})
        requirements = workload_requirements_from_task(verifier_execution_task(task), TrialConfig.model_validate(trial.config))
    retry = verifier_retries(trial)
    request: dict[str, object] = {
        "schema_version": "loom.service-execution-verifier-request.v1",
        "parent_lease_id": str(parent.id),
        "runtime_contract_sha256": canonical_digest(plan.canonical_payload()),
    }
    if retry:
        request["verifier_retry"] = retry
    target_ids = [parent.target_id]
    independent = RESOLVED_VERIFIER_ENVIRONMENT in plan.main.environment
    if independent:
        parent_target = await session.get(ServiceExecutionTarget, parent.target_id)
        if parent_target is None:
            raise ServiceExecutionConflict("verifier parent target is unavailable")
        targets = await _ready_targets(session, environment=parent_target.environment,
            pool_id=parent_target.logical_pool_id, execution_class_id=plan.execution_class_id, now=current_time,
            placement_policy=placement_policy, region=parent_target.region)
        target_ids = [target.id for target in targets
                      if requirements.data_residency is None or target.data_residency == requirements.data_residency]
    blocked = None
    for target_id in target_ids:
        try:
            async with session.begin_nested():
                return await reserve_trial_execution(
                    session,
                    request_id=canonical_uuid5(_RESERVATION_REQUEST_NAMESPACE, {**request, **({"target_id": target_id} if independent else {})}),
                    trial_id=trial.id,
                    execution_class_id=plan.execution_class_id,
                    target_id=target_id,
                    requirements=requirements,
                    runtime_contract=plan,
                    image_admission_keyring=image_admission_keyring,
                    routing_reason=ExecutionRoutingReason.PREEXISTING_ASSIGNMENT,
                    parent_lease_id=parent.id,
                    verifier_retry=retry,
                    deadline_at=deadline_at,
                    now=current_time,
                )
        except (ExecutionProvisioningBlockedError, ServiceExecutionConflict) as exc:
            blocked = exc
    if blocked is not None:
        raise blocked
    raise ServiceExecutionConflict("independent verifier has no ready execution target")



async def run_service_execution_scheduler_loop(
    *,
    session_factory: Any,
    environment: str,
    pool_id: str,
    image_admission_keyring: ImageAdmissionKeyring,
    interval_seconds: float,
    maximum_deadline_seconds: int,
    placement_policy: ExecutionPlacementPolicy = "environment-local-health-first",
    global_selector: GlobalExecutionSelector | None = None,
) -> None:
    """Continuously reserve converted service tasks; cancellation stops the loop."""

    task = asyncio.current_task()

    def cancellation_requested() -> bool:
        # Python 3.11 dependency waits can return a completed result while
        # consuming CancelledError. Preserve shutdown at each work boundary.
        return task is not None and task.cancelling() > 0

    async def reserve_attempt() -> bool:
        # Keep both the fair scan bound and the existing per-wave metric.
        # Commit each denied candidate before compiling the next: remembered
        # waits own capacity authority until that candidate commits.
        with SERVICE_EXECUTION_RESERVATION_SECONDS.labels(role="attempt").time():
            for index in range(32):
                if index:
                    # Metadata-only rechecks run after the prior commit, so a
                    # ready future head need not wait for the remaining wave.
                    await wake_rate_heads()
                    if cancellation_requested():
                        return False
                async with session_factory() as session:
                    lease, continue_scan = await _reserve_service_execution_candidate(
                        session, environment=environment, pool_id=pool_id,
                        image_admission_keyring=image_admission_keyring,
                        maximum_deadline_seconds=maximum_deadline_seconds,
                        placement_policy=placement_policy,
                    )
                    await session.commit()
                if lease is not None:
                    _LOG.info("service_execution_reserved", extra={
                        "target_id": lease.target_id, "pool_id": lease.selected_pool_id,
                    })
                    return True
                if cancellation_requested() or not continue_scan:
                    return False
        return False

    async def wake_rate_heads() -> None:
        from loom_control_plane.execution_capacity_wait import wake_ready_rate_heads

        # Reservation/remember-wait transactions have released their capacity
        # locks. This transaction changes only retry eligibility of valid heads.
        async with session_factory() as session:
            woken = await wake_ready_rate_heads(
                session, environment=environment, pool_id=pool_id, now=datetime.now(UTC),
            )
            await session.commit()
        if woken:
            _LOG.info("service_execution_rate_heads_woken", extra={"heads": woken, "pool_id": pool_id})

    while not cancellation_requested():
        try:
            if global_selector is not None:
                # A selection is not a lease. Empty/failed global selection never
                # falls through to the environment-local capacity writer.
                selected = await global_selector.select_next()
                if cancellation_requested():
                    return
                if selected is not None:
                    continue
                await wake_rate_heads()
                if cancellation_requested():
                    return
                await asyncio.sleep(interval_seconds)
                continue
            async with session_factory() as session:
                # Select a bounded fair handoff wave, then release its row
                # locks. Recheck each parent in its own reservation transaction.
                # Never hold capacity authority while compiling the next child.
                parents = (await session.scalars(verifier_candidates(pool_id)
                    .with_only_columns(ServiceExecutionLease.id).limit(32))).all()
                await session.commit()
            if cancellation_requested():
                return
            processed = checked_attempt = False
            for parent_id in parents:
                with SERVICE_EXECUTION_RESERVATION_SECONDS.labels(role="verifier").time():
                    async with session_factory() as session:
                        verifiers = await reserve_next_verifier_executions(
                            session, pool_id=pool_id, image_admission_keyring=image_admission_keyring,
                            maximum_deadline_seconds=maximum_deadline_seconds,
                            placement_policy=placement_policy, limit=1, parent_lease_id=parent_id,
                        )
                        await session.commit()
                if cancellation_requested():
                    return
                for verifier in verifiers:
                    _LOG.info("service_execution_verifier_reserved", extra={
                        "trial_id": str(verifier.trial_id), "parent_lease_id": str(verifier.parent_lease_id),
                    })
                if verifiers:
                    processed = True
                    # A busy verifier backlog cannot monopolize the scheduler
                    # before new attempts get their admission opportunity.
                    processed = await reserve_attempt() or processed
                    checked_attempt = True
                    if cancellation_requested():
                        return
            if not checked_attempt:
                processed = await reserve_attempt() or processed
            if processed:
                continue
            if cancellation_requested():
                return
            await wake_rate_heads()
        except asyncio.CancelledError:
            return
        except Exception as exc:
            _LOG.warning("service_execution_scheduler_error: %s", exc, exc_info=True)
        if cancellation_requested():
            return
        await asyncio.sleep(interval_seconds)


__all__ = [
    "reserve_next_service_execution",
    "reserve_next_verifier_executions",
    "run_service_execution_scheduler_loop",
]
