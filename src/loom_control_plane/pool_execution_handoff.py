"""Qualify retained SQL evidence before replacing local physical admission.

No network, arbitrary skip flag, or management-database access belongs here.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.nebius_pool_outbox_schema import NebiusPoolExecutionOutbox
from loom.db.schema import ServiceExecutionTarget, Trial
from loom.execution_contract import WorkloadRequirementsV1
from loom.execution_runtime_contract import ExecutionRuntimePlanV1
from loom.nebius_pool_contract import PoolReceiptV1
from loom.nebius_pool_workload import PoolExecutionPrepareV1
from loom.pipeline.keys import canonical_digest


def execution_selection_snapshot(trial: Trial, candidate: Any, target: ServiceExecutionTarget,
                                 runtime: ExecutionRuntimePlanV1) -> dict[str, Any]:
    return {
        "runtime_contract_sha256": canonical_digest(runtime.canonical_payload()),
        "trial": {"id": str(trial.id), "task_id": trial.task_id, "team_id": str(trial.team_id),
            "batch_id": str(trial.batch_id), "attempt_count": trial.attempt_count,
            "config": trial.config, "requires_caps": trial.requires_caps, "origin": trial.pool_origin,
            "route_generation": trial.execution_route_generation, "route_pool": trial.execution_route_pool_name,
            "route": trial.execution_route_json},
        "source": {key: candidate[key] for key in ("task_checksum", "task_config", "task_source_provenance",
            "legacy_separate_verifier_checksum", "batch_runtime_profile")},
        "target": {"id": target.id, "spec": target.spec_json, "environment": target.environment,
            "logical_pool_id": target.logical_pool_id, "execution_class_id": target.execution_class_id},
    }


async def qualify_execution_handoff(session: AsyncSession, *, handoff_id: UUID, request_id: UUID,
    trial: Trial, target: ServiceExecutionTarget, requirements: WorkloadRequirementsV1,
    runtime: ExecutionRuntimePlanV1, deadline_at: datetime, now: datetime,
) -> tuple[UUID, str]:
    from loom_control_plane.service_execution import ServiceExecutionConflict, _execution_identity
    from loom_control_plane.service_execution_scheduler import _SERVICE_TRIAL_BY_ID

    row = await session.get(NebiusPoolExecutionOutbox, handoff_id, with_for_update=True)
    if row is None or row.phase != "grant_pending" or row.receipt_json is None:
        raise ServiceExecutionConflict("global execution handoff unavailable")
    request = PoolExecutionPrepareV1.model_validate(row.request_json)
    receipt = PoolReceiptV1.model_validate(row.receipt_json)
    _, _, _, unit = _execution_identity(trial_id=trial.id, attempt=trial.attempt_count + 1,
        generation=1, execution_role="attempt", namespace_name=str(target.spec_json["namespace_name"]),
        target_id=target.id)
    candidate = (await session.execute(_SERVICE_TRIAL_BY_ID,
        {"trial_id": trial.id, "pool_id": target.logical_pool_id, "now": now})).mappings().one_or_none()
    if (candidate is None or row.trial_id != trial.id or row.lease_id != request_id
            or request.key.local_work_id != row.lease_id or request.key.generation != 1
            or request.execution.lease_generation != 1 or request.key.workload_kind != "trial"
            or request.execution.execution_unit_key != unit
            or request.execution.parent_lease_id is not None or runtime.execution_role != "attempt"
            or request.target_id != target.id or request.deadline_at != deadline_at
            or request.execution.requirements != requirements or request.execution.runtime != runtime
            or request.origin.model_dump(mode="json") != trial.pool_origin
            or row.request_sha256 != canonical_digest(row.request_json).removeprefix("sha256:")
            or receipt.phase != "reserved" or receipt.reservation_id != row.reservation_id
            or receipt.request_key != request.key or receipt.pool_id != request.pool_id
            or receipt.admission_epoch != request.admission_epoch or receipt.request_sha256 != row.request_sha256
            or execution_selection_snapshot(trial, candidate, target, runtime) != row.selection_json):
        raise ServiceExecutionConflict("global execution handoff changed selected work")
    return row.lease_id, "loom-pool-" + receipt.reservation_id.hex
