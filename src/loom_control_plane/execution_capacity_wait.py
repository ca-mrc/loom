"""Renewable queued execution heads; these records grant no create authority."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import (
    DateTime,
    Select,
    Text,
    case,
    cast,
    exists,
    func,
    literal,
    or_,
    select,
    tuple_,
)
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    Batch,
    ServiceExecutionClass,
    ServiceExecutionLease,
    ServiceExecutionTarget,
    Task,
    TaskImageMaterialization,
    TeamQuota,
    Trial,
    TrialTaskImageMaterialization,
)
from loom.pipeline.keys import canonical_digest
from loom_control_plane.task_image_capacity_wait import WAIT_LEASE_SECONDS, _realizable
from loom_execution_capacity_collector.contracts import ResourceTotals


def execution_wait_fingerprint(row: Any) -> str:
    """Invalidate cached demand when its source inputs or queue identity change."""
    return canonical_digest({key: row[key] for key in (
        "task_id", "task_checksum", "task_config", "task_source_provenance",
        "legacy_separate_verifier_checksum", "trial_config", "batch_runtime_profile",
        "attempt_count", "execution_route_generation", "execution_route_pool_name", "requires_caps",
    )})


def execution_target_fingerprint(target: ServiceExecutionTarget) -> str:
    """A waiting envelope belongs to this target configuration, not only its ID."""
    return canonical_digest({"spec": target.spec_json, "execution_class_id": target.execution_class_id,
                             "region": target.region, "data_residency": target.data_residency,
                             "environment": target.environment, "logical_pool_id": target.logical_pool_id})


def _time(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("invalid wait timestamp")
    result = datetime.fromisoformat(value)
    if result.utcoffset() is None:
        raise ValueError("invalid wait timestamp")
    return result


@dataclass(frozen=True)
class ExecutionCapacityWait:
    trial_id: UUID
    target_id: str
    resources: ResourceTotals
    first_waited_at: datetime

    @property
    def demand_id(self) -> str:
        return f"execution-wait:{self.trial_id}"

    @property
    def order(self) -> tuple[datetime, str]:
        return self.first_waited_at, self.demand_id


def _candidate_rows(exclude_lease_id: UUID | None = None) -> Select[Any]:
    # Read under the capacity lock without taking Trial locks: the scheduler
    # already takes its Trial lock before capacity admission, never the reverse.
    return (select(
        Trial.id, Trial.task_id, Trial.attempt_count, Trial.execution_route_generation,
        Trial.execution_route_pool_name, Trial.requires_caps, Trial.next_attempt_at,
        Trial.scheduling_observation, Task.checksum.label("task_checksum"),
        Task.config.label("task_config"), Task.source_provenance.label("task_source_provenance"),
        Task.legacy_separate_verifier_checksum, Trial.config.label("trial_config"),
        Batch.service_execution_runtime_profile.label("batch_runtime_profile"),
    ).join(Task, Task.id == Trial.task_id).join(Batch, Batch.id == Trial.batch_id)
        .join(TeamQuota, TeamQuota.team_id == Trial.team_id).where(
            Trial.state == "queued", Trial.cancellation_requested_at.is_(None),
            Trial.attempt_count < TeamQuota.max_attempts_ceiling, Trial.family_key.is_(None),
            Batch.backend == "nebius",
            ~exists(select(ServiceExecutionLease.id).where(
                ServiceExecutionLease.trial_id == Trial.id,
                ServiceExecutionLease.execution_role == "attempt",
                or_(ServiceExecutionLease.revoked_at.is_(None), ServiceExecutionLease.cleanup_state != "complete"),
                *(() if exclude_lease_id is None else (ServiceExecutionLease.id != exclude_lease_id,)),
            )),
        ))


async def _eligible(session: AsyncSession, row: Any, payload: Any, now: datetime) -> ExecutionCapacityWait | None:
    try:
        if not isinstance(payload, dict) or payload.get("input_fingerprint") != execution_wait_fingerprint(row):
            return None
        first, renewed, expiry = (_time(payload[key]) for key in ("first_waited_at", "renewed_at", "expires_at"))
        if (not first <= renewed < expiry <= renewed + timedelta(seconds=WAIT_LEASE_SECONDS)
                or renewed > now + timedelta(seconds=60) or expiry <= now
                or _time(payload["retry_at"]) != row["next_attempt_at"]):
            return None
        target_id, pool_id = payload["target_id"], payload["pool_id"]
        if not isinstance(target_id, str) or not isinstance(pool_id, str):
            return None
        if (row["requires_caps"].get("worker_pool") != pool_id
                or row["execution_route_pool_name"] not in (None, pool_id)):
            return None
        resources = ResourceTotals.model_validate(payload["resources"])
        if min(resources.cpu_millis, resources.memory_mib, resources.storage_mib) <= 0:
            return None
        target = await session.get(ServiceExecutionTarget, target_id, populate_existing=True)
        if (target is None or target.observed_state != "ready"
                or target.health_observed_at is None
                or target.health_observed_at + timedelta(seconds=int(target.spec_json["health_stale_after_seconds"])) <= now
                or execution_target_fingerprint(target) != payload["target_fingerprint"]):
            return None
        execution_class = await session.get(ServiceExecutionClass, target.execution_class_id)
        if execution_class is None or not execution_class.enabled:
            return None
        image_id = payload.get("image_materialization_id")
        images = list((await session.scalars(select(TaskImageMaterialization).join(
            TrialTaskImageMaterialization,
            TrialTaskImageMaterialization.materialization_id == TaskImageMaterialization.id,
        ).where(TrialTaskImageMaterialization.trial_id == row["id"],
                TaskImageMaterialization.task_id == row["task_id"],
                TaskImageMaterialization.cpu_arch == "x86_64"))).all())
        if image_id is None:
            if images:
                return None
        elif not any(str(image.id) == image_id and image.state == "ready"
                     and "sha256:" + image.task_checksum == payload["task_revision_sha256"]
                     and image.ready_publication_operation_id is None for image in images):
            return None
        if not await _realizable(session, target_id=target_id, pool_id=pool_id, resources=resources, now=now):
            return None
        return ExecutionCapacityWait(row["id"], target_id, resources, first)
    except (KeyError, TypeError, ValueError, OverflowError):
        # Historical observations and malformed disposable hints cannot reserve
        # resources. The normal scheduler remains responsible for validation.
        return None


async def read_execution_capacity_waits(
    session: AsyncSession, *, now: datetime, claiming_lease_id: UUID | None = None,
    claiming_inputs: dict[str, Any] | None = None,
) -> tuple[list[ExecutionCapacityWait], tuple[datetime, str] | None]:
    claiming = await session.get(ServiceExecutionLease, claiming_lease_id) if claiming_lease_id else None
    own_order = None
    heads: dict[str, ExecutionCapacityWait] = {}
    if claiming is not None:
        row = (await session.execute(_candidate_rows(claiming_lease_id).where(
            Trial.id == claiming.trial_id,
        ))).mappings().one_or_none()
        if row is not None:
            payload = (row["scheduling_observation"] or {}).get("capacity_wait")
            candidate = dict(row)
            if claiming_inputs is not None:
                # Route binding inside this uncommitted claim is expected.
                for key in ("execution_route_generation", "execution_route_pool_name"):
                    candidate[key] = claiming_inputs[key]
            wait = await _eligible(session, candidate, payload, now)
            if (wait is not None and wait.target_id == claiming.target_id
                    and isinstance(payload, dict)
                    and payload.get("runtime_contract_sha256") == claiming.runtime_contract_sha256):
                own_order = wait.order

    hint = Trial.scheduling_observation["capacity_wait"]
    target = hint["target_id"].astext

    def timestamp(key: str) -> Any:
        value = hint[key].astext
        # PostgreSQL 16 is the deployed/tested baseline. Malformed disposable
        # hints must not make a cast abort another workload's admission.
        return case((func.pg_input_is_valid(value, "timestamptz"), cast(value, DateTime(timezone=True))))

    first, expiry = timestamp("first_waited_at"), timestamp("expires_at")
    order_time = func.coalesce(first, cast(literal("-infinity"), DateTime(timezone=True)))
    filters = [Trial.state == "queued", Trial.cancellation_requested_at.is_(None),
               func.jsonb_typeof(hint["target_id"]) == "string",
               target.is_not(None), or_(expiry.is_(None), expiry > now)]
    if claiming is not None:
        filters.append(Trial.id != claiming.trial_id)
    targets = (await session.scalars(select(target).where(*filters).distinct().order_by(target))).all()
    for target_id in targets:
        cursor: tuple[str, UUID] | None = None
        while True:
            query = select(Trial.id, cast(first, Text).label("order_at"), Trial.next_attempt_at,
                *(hint[key].astext.label(key) for key in ("first_waited_at", "renewed_at", "expires_at", "retry_at")),
            ).where(*filters, target == target_id)
            if cursor is not None:
                query = query.where(tuple_(order_time, Trial.id) > tuple_(
                    cast(literal(cursor[0]), DateTime(timezone=True)), literal(cursor[1])))
            page = (await session.execute(query.order_by(order_time, Trial.id).limit(100))).mappings().all()
            if not page:
                break
            plausible = []
            for item in page:
                try:
                    waited, renewed, expires = (_time(item[key]) for key in ("first_waited_at", "renewed_at", "expires_at"))
                    if (waited <= renewed < expires <= renewed + timedelta(seconds=WAIT_LEASE_SECONDS)
                            and renewed <= now + timedelta(seconds=60) and expires > now
                            and _time(item["retry_at"]) == item["next_attempt_at"]):
                        plausible.append(item["id"])
                except (KeyError, TypeError, ValueError, OverflowError):
                    continue
            # Batch invalid-head fallback, rather than a round trip per stale
            # fingerprint. Only this page's plausible rows need rich context.
            rows = (await session.execute(_candidate_rows(claiming_lease_id).where(
                Trial.id.in_(plausible),
            ))).mappings().all() if plausible else []
            contexts = {row["id"]: row for row in rows}
            finished = False
            for item in page:
                row = contexts.get(item["id"])
                if row is None:
                    continue
                payload = (row["scheduling_observation"] or {}).get("capacity_wait")
                wait = await _eligible(session, row, payload, now)
                if wait is None:
                    continue
                previous = heads.get(wait.target_id)
                if previous is None or wait.order < previous.order:
                    heads[wait.target_id] = wait
                # Python accepts some ISO forms PostgreSQL does not. Their
                # null-first prefix must all be compared before stopping at
                # the oldest valid normalized timestamp; never sort by text.
                if item["order_at"] is not None:
                    finished = True
                    break
            if finished:
                break
            last = page[-1]
            cursor = last["order_at"] or "-infinity", last["id"]
    return list(heads.values()), own_order


async def remember_execution_capacity_wait(
    session: AsyncSession, *, row: Any, context: dict[str, Any] | None,
    reason: str, now: datetime, retry_at: datetime,
) -> dict[str, Any]:
    """Called after the rejected reservation savepoint rolled back."""
    from loom_control_plane.execution_capacity import _CAPACITY_ADMISSION_LOCK

    observation: dict[str, Any] = {"reason": reason, "observed_at": now.isoformat()}
    if context is None:
        return observation
    await session.execute(_CAPACITY_ADMISSION_LOCK)
    candidate = (await session.execute(_candidate_rows().where(Trial.id == row["id"]))).mappings().one_or_none()
    if candidate is None or execution_wait_fingerprint(candidate) != execution_wait_fingerprint(row):
        return observation
    payload = {**context, "input_fingerprint": execution_wait_fingerprint(row),
               "first_waited_at": now.isoformat(), "renewed_at": now.isoformat(),
               "expires_at": (now + timedelta(seconds=WAIT_LEASE_SECONDS)).isoformat(),
               "retry_at": retry_at.isoformat()}
    # Eligibility checks the exact scheduling backoff, including while it is
    # future-dated. It is a retry delay, not an unrelated suspension of demand.
    proposed = dict(candidate)
    proposed["next_attempt_at"] = retry_at
    if await _eligible(session, proposed, payload, now) is None:
        return observation
    prior = (candidate["scheduling_observation"] or {}).get("capacity_wait")
    if (isinstance(prior, dict) and await _eligible(session, candidate, prior, now) is not None
            and all(prior.get(key) == payload.get(key) for key in context)):
        payload["first_waited_at"] = prior["first_waited_at"]
    observation["capacity_wait"] = payload
    return observation
