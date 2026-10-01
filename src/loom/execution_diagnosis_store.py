"""Read durable native failure evidence under the caller's Trial authority."""

from typing import Any
from uuid import UUID

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import ServiceExecutionEvent, ServiceExecutionLease, Trial
from loom.execution_failure_diagnosis import execution_failure_diagnosis
from loom.execution_runtime_contract import ExecutionRuntimePlanV1


async def read_execution_failure(
    session: AsyncSession, lease: ServiceExecutionLease,
) -> dict[str, Any] | None:
    if lease.runtime_contract_json is None or not lease.job_uid or not lease.pod_uid:
        return None
    rows = (await session.execute(select(
        ServiceExecutionEvent.ordinal, ServiceExecutionEvent.event_kind, ServiceExecutionEvent.payload_json,
    ).where(
        ServiceExecutionEvent.lease_id == lease.id,
        or_(
            (ServiceExecutionEvent.event_kind == "kubernetes_observed")
            & (ServiceExecutionEvent.payload_json["job_uid"].astext == lease.job_uid)
            & (ServiceExecutionEvent.payload_json["pod_uid"].astext == lease.pod_uid)
            & (ServiceExecutionEvent.payload_json["normalized_state"].astext.in_(
                {"failed", "oom_killed", "evicted", "node_lost", "deadline_exceeded"})),
            ServiceExecutionEvent.event_kind.in_({"failed", "finalized"}),
        ),
    ).order_by(ServiceExecutionEvent.ordinal))).all()
    diagnosis = execution_failure_diagnosis(
        [{"ordinal": ordinal, "payload": payload} for ordinal, kind, payload in rows
         if kind == "kubernetes_observed"],
        plan=ExecutionRuntimePlanV1.model_validate(lease.runtime_contract_json),
        job_uid=lease.job_uid, pod_uid=lease.pod_uid,
    )
    if diagnosis is not None:
        diagnosis["supporting_events"] = [
            {"ordinal": ordinal, "kind": kind,
             "reason": payload.get("reason") or payload.get("error_code") or payload.get("failure_reason"),
             "normalized_state": payload.get("normalized_state")}
            for ordinal, kind, payload in rows
        ]
    return diagnosis


async def read_trial_execution_failure(session: AsyncSession, trial: Trial) -> dict[str, Any] | None:
    lease = await session.scalar(select(ServiceExecutionLease).where(
        ServiceExecutionLease.trial_id == trial.id,
        ServiceExecutionLease.attempt == trial.attempt_count,
        ServiceExecutionLease.execution_role == "attempt",
    ))
    return await read_execution_failure(session, lease) if lease is not None else None


async def execution_failure_groups(
    session: AsyncSession, *, batch_id: UUID | None = None, trial_id: UUID | None = None,
) -> list[dict[str, Any]]:
    """Group confirmed terminations separately from unavailable sampled counters."""
    leases = (await session.scalars(select(ServiceExecutionLease,).join(
        Trial, Trial.id == ServiceExecutionLease.trial_id,
    ).where(
        *((Trial.batch_id == batch_id,) if batch_id is not None else (Trial.id == trial_id,)),
        ServiceExecutionLease.execution_role == "attempt",
        ServiceExecutionLease.error_code.is_not(None),
    ))).all()
    groups: dict[tuple[Any, ...], dict[str, Any]] = {}
    for lease in leases:
        diagnosis = await read_execution_failure(session, lease)
        if diagnosis is None or diagnosis["reason"] != "oom_killed":
            continue
        trial = await session.get(Trial, lease.trial_id)
        assert trial is not None
        limits = diagnosis["limits"] or {}
        key = (trial.task_id, diagnosis["container_role"], *(
            limits.get(field) for field in ("cpu_millis", "memory_mib", "ephemeral_storage_mib")
        ))
        group = groups.setdefault(key, {
            "task_id": trial.task_id, "container_role": diagnosis["container_role"],
            "limits": limits, "confirmed_oom_attempts": 0,
        })
        group["confirmed_oom_attempts"] += 1
    return list(groups.values())
