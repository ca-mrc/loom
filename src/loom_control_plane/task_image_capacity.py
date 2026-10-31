"""Reserve native build resources in the existing execution capacity domain."""

from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    ServiceExecutionTarget,
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
)
from loom_control_plane.execution_capacity import (
    _CAPACITY_ADMISSION_LOCK,
    ExecutionProvisioningBlockedError,
    _utc,
    admit_capacity_resources,
    native_build_resources,
)
from loom_control_plane.metrics import EXECUTION_CAPACITY_LOCK_WAIT_SECONDS


def _bind_io_job(native: dict[str, Any], binding: dict[str, Any]) -> dict[str, Any]:
    """Freeze the placement decision into the original Job before publication.

    Keep the execution target's selector/tolerations and intersect any required
    affinity rather than replacing its scheduling rules. metadata.name affinity
    remains visible to the scheduler/autoscaler; nodeName would bypass both.
    """
    native = deepcopy(native)
    job = native["job"]
    pod = job["spec"]["template"]["spec"]
    if binding["mode"] == "node":
        fields = [{"key": "metadata.name", "operator": "In", "values": [binding["node_name"]]}]
        job["metadata"].setdefault("annotations", {}).update({
            "loom.openai.com/build-node-name": binding["node_name"],
            "loom.openai.com/build-node-uid": binding["node_uid"],
        })
    elif binding["mode"] == "bootstrap":
        excluded = binding["excluded_node_names"]
        # Kubernetes field requirements accept exactly one value each. Multiple
        # exclusions are ANDed within each existing selector term.
        fields = [{"key": "metadata.name", "operator": "NotIn", "values": [name]} for name in excluded]
    else:
        raise ValueError("invalid native I/O binding mode")
    if fields:
        required = pod.setdefault("affinity", {}).setdefault("nodeAffinity", {}).setdefault(
            "requiredDuringSchedulingIgnoredDuringExecution", {"nodeSelectorTerms": [{}]},
        )
        for term in required["nodeSelectorTerms"]:
            term.setdefault("matchFields", []).extend(deepcopy(fields))
    native["io_binding"] = deepcopy(binding)
    return native


async def reserve_native_task_image_capacity(
    session: AsyncSession,
    *,
    attempt_id: UUID,
    now: datetime | None = None,
    revalidate_existing: bool = False,
) -> dict[str, Any]:
    """Persist capacity before Job creation; caller commits the claim together.

    A reservation lasts until explicit UID-fenced cleanup acknowledgement sets
    capacity_released_at, including after lease expiry or a terminal build.
    """
    current_time = _utc(now or datetime.now(UTC), name="now")
    with EXECUTION_CAPACITY_LOCK_WAIT_SECONDS.labels(operation="build").time():
        await session.execute(_CAPACITY_ADMISSION_LOCK)
    attempt = await session.scalar(select(TaskImageMaterializationAttempt).where(
        TaskImageMaterializationAttempt.id == attempt_id,
    ).with_for_update())
    if attempt is None or not attempt.native_build:
        raise ValueError("native build attempt or configuration does not exist")
    native = dict(attempt.native_build)
    if native.get("capacity_released_at") is not None:
        raise ExecutionProvisioningBlockedError("execution_capacity_authorization_released")
    row = await session.get(TaskImageMaterialization, attempt.materialization_id)
    if (row is None or row.state not in {"claimed", "running"}
            or row.claimed_by != attempt.builder_id or row.lease_epoch != attempt.lease_epoch
            or row.lease_expires_at is None or row.lease_expires_at <= current_time):
        raise ExecutionProvisioningBlockedError("execution_capacity_native_lease_stale")
    existing = native.get("capacity_reserved_at") is not None
    if existing and not revalidate_existing:
        return native
    target = await session.get(ServiceExecutionTarget, native.get("target_id"))
    if target is None or target.provider != "nebius":
        raise ValueError("native build requires an existing Nebius execution target")
    try:
        resources = native_build_resources(native)
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("native build resource envelope is invalid") from exc
    decision = await admit_capacity_resources(
        session, target=target,
        demand_id=f"task-image:{attempt.materialization_id}:{attempt.lease_epoch}",
        resources=resources, current_time=current_time,
        already_reserved=existing, exclude_native_attempt_id=attempt.id,
    )
    if not existing:
        if "io_binding" in decision:
            native = _bind_io_job(native, decision["io_binding"])
        native.update(capacity_reserved_at=current_time.isoformat(),
                      capacity_observation_id=decision["observation_id"],
                      capacity_policy_version=decision["policy_version"],
                      capacity_reason=decision["decision_reason"])
        attempt.native_build = native
        await session.flush()
    return native
