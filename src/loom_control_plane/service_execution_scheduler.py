"""Normal queued-Trial admission into the durable service-execution path."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    ServiceExecutionLease,
    ServiceExecutionTarget,
    TaskImageMaterialization,
    Trial,
    TrialTaskImageMaterialization,
)
from loom.execution_contract import ExecutionRoutingReason, workload_requirements_from_task
from loom.execution_image_admission import ImageAdmissionKeyring
from loom.execution_runtime_contract import ExecutionRuntimePlanV1
from loom.models.task import TaskConfig, bind_service_execution_runtime_plan
from loom.models.trial import TrialConfig
from loom.pipeline.keys import canonical_digest, canonical_uuid5
from loom.service_execution_materialization import (
    ServiceExecutionRuntimeProfileV1,
    compile_service_execution_plan,
)
from loom.task_image_materialization import (
    get_trial_task_image_execution_grant,
    resolve_prepared_task,
)
from loom.verifier_runtime import apply_legacy_verifier_default
from loom_control_plane.execution_capacity import ExecutionProvisioningBlockedError
from loom_control_plane.execution_resource_allocation import allocate_target_resources
from loom_control_plane.service_execution import reserve_trial_execution

_LOG = logging.getLogger(__name__)
_RESERVATION_REQUEST_NAMESPACE = UUID("aaf78d09-4268-4dc5-81ee-4c2408ce2611")


class ServiceExecutionConfigurationError(ValueError):
    """A known per-Trial configuration cannot run under this scheduler's bounds."""


_NEXT_SERVICE_TRIAL = text("""
SELECT t.id,
       t.submitted_at,
       t.task_id,
       t.attempt_count,
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
   AND t.requires_caps->>'worker_pool' = :pool_id
   AND (
         t.execution_route_pool_name IS NULL
         OR t.execution_route_pool_name = :pool_id
       )
   AND NOT EXISTS (
         SELECT 1 FROM execution_leases lease
          WHERE lease.trial_id = t.id
            AND lease.execution_role = 'attempt'
            AND (lease.revoked_at IS NULL OR lease.cleanup_state != 'complete')
       )
 ORDER BY (q.in_flight_count::double precision / q.fair_share_weight) ASC,
          t.submit_priority DESC,
          t.submitted_at ASC,
          t.id ASC
 FOR UPDATE OF t SKIP LOCKED
 LIMIT 1
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
    phase_seconds = sum(item.timeout_seconds for item in plan.setup) + plan.main.timeout_seconds
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
        if target.health_observed_at is None:
            continue
        stale_after = int(target.spec_json["health_stale_after_seconds"])
        if target.health_observed_at + timedelta(seconds=stale_after) > now:
            ready.append(target)
    return sorted(ready, key=lambda target: target.spec_json.get("health_role") != "primary")


async def reserve_next_service_execution(
    session: AsyncSession,
    *,
    environment: str,
    pool_id: str,
    image_admission_keyring: ImageAdmissionKeyring,
    maximum_deadline_seconds: int = 7200,
    now: datetime | None = None,
) -> ServiceExecutionLease | None:
    """Reserve one normally queued, explicitly converted service task."""

    current_time = (now or datetime.now(UTC)).astimezone(UTC)
    # Inspect a bounded number in the existing fair-share order. A failed
    # savepoint releases admission/budget writes but preserves the Trial row lock.
    for _ in range(32):
        row = (
            (await session.execute(_NEXT_SERVICE_TRIAL, {"now": current_time, "pool_id": pool_id}))
            .mappings()
            .one_or_none()
        )
        if row is None:
            return None
        try:
            async with session.begin_nested():
                return await _reserve_service_candidate(
                    session,
                    row=row,
                    environment=environment,
                    pool_id=pool_id,
                    image_admission_keyring=image_admission_keyring,
                    maximum_deadline_seconds=maximum_deadline_seconds,
                    current_time=current_time,
                )
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
            ))
            _LOG.warning("service_execution_configuration_invalid", extra={
                "trial_id": str(row["id"]), "reason": str(exc),
            })
        except ExecutionProvisioningBlockedError as exc:
            delay = max(1, min(300, exc.retry_after_seconds))
            await session.execute(
                update(Trial)
                .where(Trial.id == row["id"], Trial.state == "queued")
                .values(
                    next_attempt_at=current_time + timedelta(seconds=delay),
                    scheduling_observation={"reason": exc.reason, "observed_at": current_time.isoformat()},
                )
            )
            _LOG.info("service_execution_capacity_wait", extra={"reason": exc.reason})
    return None


async def _reserve_service_candidate(
    session: AsyncSession,
    *,
    row: Any,
    environment: str,
    pool_id: str,
    image_admission_keyring: ImageAdmissionKeyring,
    maximum_deadline_seconds: int,
    current_time: datetime,
) -> ServiceExecutionLease | None:
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
            await session.execute(update(Trial).where(
                Trial.id == row["id"], Trial.state == "queued",
                Trial.cancellation_requested_at.is_(None),
            ).values(state="failed", failure_reason="task_image_build_failed",
                     failure_message="Task image preparation failed; inspect the build result.",
                     finished_at=current_time, next_attempt_at=None))
            return None
        raise ExecutionProvisioningBlockedError("task_image_preparation_pending", retry_after_seconds=15)
    try:
        grant = await get_trial_task_image_execution_grant(
            session, trial_id=row["id"], cpu_arches=["x86_64"],
        )
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
    )
    requirements = workload_requirements_from_task(
        resolve_prepared_task(task, grant) if grant else task,
        trial_config if binding is None else None,
    )
    blocked: ExecutionProvisioningBlockedError | None = None
    for target in targets:
        if requirements.data_residency and target.data_residency != requirements.data_residency:
            continue
        # A target's failed admission must not keep a route, cost reservation or
        # attempt increment when the next eligible region is tried.
        target_id = target.id
        try:
            async with session.begin_nested():
                allocated_plan = (await allocate_target_resources(
                    session, runtime_plan, target_id=target_id, now=current_time,
                ) if allocate_resources else runtime_plan)
                lease = await reserve_trial_execution(
                    session,
                    request_id=canonical_uuid5(
                        _RESERVATION_REQUEST_NAMESPACE,
                        {
                            "schema_version": "loom.service-execution-reservation-request.v1",
                            "trial_id": str(row["id"]),
                            "attempt": int(row["attempt_count"]) + 1,
                            "target_id": target_id,
                            "task_revision_sha256": task_revision,
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
                    deadline_at=deadline_at,
                    now=current_time,
                )
                ready_times = [image.ready_at for image in prerequisites if image.ready_at]
                image_ready = max(ready_times) if ready_times else row["submitted_at"]
                await session.execute(update(Trial).where(Trial.id == row["id"]).values(
                    scheduling_observation={
                        "observed_at": current_time.isoformat(), "lease_id": str(lease.id),
                        "image_ready_at": (max(row["submitted_at"], image_ready).isoformat()
                                           if ready_times or not prerequisites else None),
                        "image_mode": ("reused" if ready_times and image_ready <= row["submitted_at"]
                                       else "built" if ready_times else "unknown" if prerequisites else "prebuilt"),
                    },
                ))
                return lease
        except ExecutionProvisioningBlockedError as exc:
            blocked = exc
    if blocked is not None:
        raise blocked
    raise ExecutionProvisioningBlockedError("execution_target_unavailable", retry_after_seconds=15)


async def run_service_execution_scheduler_loop(
    *,
    session_factory: Any,
    environment: str,
    pool_id: str,
    image_admission_keyring: ImageAdmissionKeyring,
    interval_seconds: float,
    maximum_deadline_seconds: int,
) -> None:
    """Continuously reserve converted service tasks; cancellation stops the loop."""

    while True:
        try:
            async with session_factory() as session:
                lease = await reserve_next_service_execution(
                    session,
                    environment=environment,
                    pool_id=pool_id,
                    image_admission_keyring=image_admission_keyring,
                    maximum_deadline_seconds=maximum_deadline_seconds,
                )
                await session.commit()
            if lease is not None:
                _LOG.info(
                    "service_execution_reserved",
                    extra={"target_id": lease.target_id, "pool_id": lease.selected_pool_id},
                )
                continue
        except asyncio.CancelledError:
            return
        except Exception as exc:
            _LOG.warning("service_execution_scheduler_error: %s", exc, exc_info=True)
        await asyncio.sleep(interval_seconds)


__all__ = ["reserve_next_service_execution", "run_service_execution_scheduler_loop"]
