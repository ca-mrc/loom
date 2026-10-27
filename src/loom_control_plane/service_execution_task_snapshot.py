"""Resolve the task snapshot already authorized by a persisted execution lease."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    ServiceExecutionLease,
    Task,
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
    Trial,
    TrialTaskImageMaterialization,
)
from loom.execution_runtime_contract import ExecutionRuntimePlanV1
from loom.task_image_materialization import TaskImageExecutionGrantV1


class ServiceExecutionTaskSnapshotError(ValueError):
    pass


@dataclass(frozen=True)
class ServiceExecutionTaskSnapshot:
    config: dict[str, Any]
    source: str | None
    source_provenance: dict[str, Any]


async def resolve_service_execution_task_snapshot(
    session: AsyncSession,
    *,
    lease: ServiceExecutionLease,
    trial: Trial | None = None,
) -> ServiceExecutionTaskSnapshot:
    if trial is None:
        trial = await session.get(Trial, lease.trial_id)
    if trial is None or trial.id != lease.trial_id or trial.team_id != lease.team_id:
        raise ServiceExecutionTaskSnapshotError("task_snapshot_trial_mismatch")

    contract = lease.runtime_contract_json or {}
    if contract.get("task_image_materialization_id") is None:
        task = await session.get(Task, trial.task_id)
        if task is None:
            raise ServiceExecutionTaskSnapshotError("task_snapshot_unavailable")
        return ServiceExecutionTaskSnapshot(task.config, task.source, task.source_provenance or {})

    try:
        plan = ExecutionRuntimePlanV1.model_validate(contract)
    except ValueError as exc:
        raise ServiceExecutionTaskSnapshotError("task_snapshot_identity_invalid") from exc
    # Retirement can follow execution; only the immutable snapshot association
    # matters here, not readiness or registry fields used for new claims.
    row = await session.scalar(
        select(TaskImageMaterialization)
        .join(
            TrialTaskImageMaterialization,
            TrialTaskImageMaterialization.materialization_id == TaskImageMaterialization.id,
        )
        .where(
            TrialTaskImageMaterialization.trial_id == trial.id,
            TaskImageMaterialization.id == plan.task_image_materialization_id,
            TaskImageMaterialization.task_id == trial.task_id,
            TaskImageMaterialization.task_checksum == plan.task_revision_sha256.removeprefix("sha256:"),
        )
    )
    if row is None:
        raise ServiceExecutionTaskSnapshotError("task_snapshot_binding_mismatch")
    return ServiceExecutionTaskSnapshot(
        row.task_config, row.task_source, row.task_source_provenance or {},
    )


async def resolve_service_execution_image_grant(
    session: AsyncSession, *, lease: ServiceExecutionLease, cpu_arch: str,
) -> TaskImageExecutionGrantV1:
    """Read an already admitted lease's image receipt without scheduling authority."""
    # This establishes the exact trial/team/revision association. Source admission
    # and its locks belong to scheduling, never to the read-only input broker.
    snapshot = await resolve_service_execution_task_snapshot(session, lease=lease)
    plan = ExecutionRuntimePlanV1.model_validate(lease.runtime_contract_json)
    row = await session.get(TaskImageMaterialization, plan.task_image_materialization_id)
    if row is None or row.cpu_arch != cpu_arch or row.ready_publication_operation_id is not None:
        raise ServiceExecutionTaskSnapshotError("task_image_receipt_binding_mismatch")
    attempt = await session.scalar(select(TaskImageMaterializationAttempt).where(
        TaskImageMaterializationAttempt.materialization_id == row.id,
        TaskImageMaterializationAttempt.lease_epoch == row.lease_epoch,
    ))
    if attempt is None:
        raise ServiceExecutionTaskSnapshotError("task_image_receipt_unavailable")
    receipt = attempt.native_build or {}
    return TaskImageExecutionGrantV1(
        schema_version="loom.task-image-execution-grant.v1", materialization_id=row.id,
        materialization_key=row.materialization_key, cpu_arch=row.cpu_arch,
        task_checksum=row.task_checksum, task_config=snapshot.config,
        task_source=snapshot.source, task_source_provenance=snapshot.source_provenance,
        registry_images=row.registry_images, image_archives=receipt.get("image_archives", {}),
        image_runtime_metadata=receipt.get("image_runtime_metadata", {}),
    )
