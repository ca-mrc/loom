"""On-demand placement projection; no provider calls or cross-team identities."""
from __future__ import annotations

from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    ExecutionCapacityObservation,
    ServiceExecutionLease,
    TaskImageMaterializationAttempt,
    Trial,
    TrialTaskImageMaterialization,
)
from loom_service.trial_progress import wait_message


def placement_response(
    payload: dict[str, Any], visible: dict[str, dict[str, Any]], *, admin: bool,
) -> dict[str, Any]:
    def workloads(pods: list[dict[str, Any]]) -> list[dict[str, Any]]:
        result = []
        for pod in pods:
            key = f"{pod['lease_id']}:{pod['generation']}"
            if key in visible:
                result.append({**visible[key], "requests": pod["requests"]})
        return result

    nodes = []
    for index, node in enumerate(payload.get("nodes", [])):
        pods = node["managed_pods"]
        nodes.append({
            "label": f"Node {index + 1}", "id": node["uid"] if admin else str(index + 1),
            "ready": node["ready"], "draining": node.get("draining"),
            "deleting": node["deleting"], "unschedulable": node["unschedulable"],
            "allocatable": node["allocatable"], "requested": node["requested"],
            "build_pods": sum(p["lease_id"].startswith("task-image:") for p in pods),
            "execution_pods": sum(not p["lease_id"].startswith("task-image:") for p in pods),
            "workloads": workloads(pods),
        })
    pending = payload.get("pending_pods", [])
    return {
        "nodes": nodes, "pending": workloads(pending),
        "build_concurrency_limit": payload.get("build_concurrency_limit"),
        "pending_builds": sum(p["lease_id"].startswith("task-image:") for p in pending),
        "pending_executions": sum(not p["lease_id"].startswith("task-image:") for p in pending),
        "capacity_scope": "shared_target",
        "workload_scope": "authorized_filtered_trials",
    }


async def load_placement(
    session: AsyncSession, *, observation_id: str, scoped_ids: Any, admin: bool,
) -> dict[str, Any]:
    observation = await session.get(ExecutionCapacityObservation, UUID(observation_id))
    payload = (observation.observation_json or {}).get("placement") if observation else None
    if observation is None or not payload:
        return {"available": False, "nodes": [], "pending": []}
    pods = [pod for node in payload.get("nodes", []) for pod in node["managed_pods"]]
    pods.extend(payload.get("pending_pods", []))
    execution_ids: set[UUID] = set()
    image_ids: set[UUID] = set()
    for pod in pods:
        identity = pod["lease_id"]
        try:
            (image_ids if identity.startswith("task-image:") else execution_ids).add(
                UUID(identity.removeprefix("task-image:")))
        except ValueError:
            continue
    # Limit queries to observed Pods, including old attempts still awaiting cleanup.
    leases = (await session.scalars(select(ServiceExecutionLease).where(
        ServiceExecutionLease.trial_id.in_(scoped_ids), ServiceExecutionLease.id.in_(execution_ids),
    ))).all()
    visible: dict[str, dict[str, Any]] = {}
    for lease in leases:
        visible[f"{lease.id}:{lease.resource_generation}"] = {
            "kind": "execution", "trial_id": str(lease.trial_id),
            "label": f"Trial {str(lease.trial_id)[:8]}", "state": lease.observed_state,
            "wait_message": wait_message(lease.error_code),
        }
    references = (await session.execute(
        select(TrialTaskImageMaterialization.materialization_id, Trial.id, Trial.task_id)
        .join(Trial, Trial.id == TrialTaskImageMaterialization.trial_id)
        .where(Trial.id.in_(scoped_ids),
               TrialTaskImageMaterialization.materialization_id.in_(image_ids))
        .order_by(Trial.submitted_at.desc())
    )).all()
    by_image: dict[UUID, tuple[UUID, str]] = {}
    for image_id, trial_id, task_id in references:
        by_image.setdefault(image_id, (trial_id, task_id))
    builds = (await session.scalars(select(TaskImageMaterializationAttempt).where(
        TaskImageMaterializationAttempt.materialization_id.in_(by_image),
        TaskImageMaterializationAttempt.native_build.is_not(None),
        TaskImageMaterializationAttempt.native_capacity_target_id == observation.target_id,
    ))).all() if by_image else []
    for build in builds:
        native = build.native_build or {}
        trial_id, task_id = by_image[build.materialization_id]
        phase = next((p["name"] for p in native.get("phases", [])
                      if "running" in p.get("state", {})), native.get("state", "unknown"))
        visible[f"task-image:{build.materialization_id}:{build.lease_epoch}"] = {
            "kind": "build", "trial_id": str(trial_id), "label": task_id, "state": phase,
            "wait_message": wait_message((native.get("scheduling") or {}).get("reason")),
        }
    return {
        "available": True, **placement_response(payload, visible, admin=admin),
    }
