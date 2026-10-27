"""Read-only progress over existing image and execution authorities.

The SQL stage is shared by filters, counts and row projections. It is not a
second lifecycle or permission to schedule work. Callers authorize the Trial
scope before loading any shared image observations.
"""
from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import and_, case, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from loom.db.schema import (
    Batch,
    ServiceExecutionLease,
    TaskImageCapacityWait,
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
    Trial,
    TrialTaskImageMaterialization,
)
from loom_service.task_image_preparation import preparation_response

STAGES = {
    "image_preparation": "Preparing image",
    "execution_wait": "Waiting for execution",
    "starting": "Starting environment",
    "running": "Running",
    "archiving": "Archiving output",
    "succeeded": "Succeeded",
    "failed": "Failed",
    "cancelled": "Cancelled",
}
TERMINAL = {"succeeded", "failed", "cancelled"}


def latest_execution_id() -> Any:
    lease = aliased(ServiceExecutionLease)
    return (
        select(lease.id).where(lease.trial_id == Trial.id, lease.execution_role == "attempt")
        .order_by(lease.attempt.desc(), lease.created_at.desc(), lease.id.desc()).limit(1)
        .correlate(Trial).scalar_subquery()
    )


def progress_stage_case() -> Any:
    image_wait = (
        select(TrialTaskImageMaterialization.trial_id)
        .join(TaskImageMaterialization,
              TaskImageMaterialization.id == TrialTaskImageMaterialization.materialization_id)
        .where(TrialTaskImageMaterialization.trial_id == Trial.id,
               TaskImageMaterialization.task_id == Trial.task_id,
               TaskImageMaterialization.cpu_arch == "x86_64",
               TaskImageMaterialization.state != "ready")
        .correlate(Trial).exists()
    )
    lease = aliased(ServiceExecutionLease)
    execution = select(case(
        (lease.materialization_state.in_(("pending", "running")), "archiving"),
        (lease.observed_state.in_(("finalizing", "finalized")), "archiving"),
        (lease.output_commit_state == "uploading", "archiving"),
        (lease.observed_state == "running", "running"),
        else_="starting",
    )).where(lease.id == latest_execution_id()).correlate(Trial).scalar_subquery()
    return case(
        (Trial.state.in_(tuple(TERMINAL)), Trial.state),
        (Trial.state == "materializing", "archiving"),
        (and_(Trial.state == "queued", image_wait), "image_preparation"),
        (Trial.state.in_(("queued", "protected-pending")), "execution_wait"),
        else_=func.coalesce(execution, case(
            (Trial.state == "running", "running"), else_="starting",
        )),
    )


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _time(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
        return parsed if parsed.tzinfo is not None else None
    except ValueError:
        return None


def wait_message(reason: object) -> str | None:
    """Only known codes become public text; never forward arbitrary diagnostics."""
    messages = {
        "task_image_preparation_pending": "Waiting for the task image to become ready.",
        "execution_capacity_observation_stale": "Waiting for a fresh capacity observation.",
        "execution_capacity_observation_missing": "Waiting for a capacity observation.",
        "execution_capacity_placement_unavailable": "Waiting for node placement information.",
        "execution_capacity_native_lease_stale": "Waiting for the image build reservation to refresh.",
        "execution_capacity_native_io_feedback_pending": (
            "Waiting for disk I/O measurements from an earlier image build before admitting more work."),
        "execution_capacity_bound_node_unavailable": (
            "A reserved node cannot currently accept this workload. Waiting for capacity recovery."),
        "unschedulable": "Kubernetes has not found a suitable node for this workload.",
        "Unschedulable": "Kubernetes has not found a suitable node for this workload.",
        "execution_target_unavailable": "Waiting for a healthy execution target.",
        "execution_capacity_observation_unavailable": "Waiting for a capacity observation.",
        "execution_capacity_provisioning_delay": "Waiting for provisioned nodes to become ready.",
        "execution_capacity_autoscaler_stalled": "The node autoscaler is stalled.",
        "execution_capacity_autoscaler_unknown": "Waiting for a fresh autoscaler observation.",
        "execution_capacity_physical_capacity_unavailable": "The provider currently has insufficient physical capacity.",
        "execution_capacity_physical_capacity_unknown": "Provider capacity has not been confirmed.",
        "ImagePullBackOff": "The node could not pull an image; Kubernetes will retry.",
        "ErrImagePull": "The node could not pull an image.",
        "image_pull_backoff": "The node could not pull an image; Kubernetes will retry.",
    }
    if not isinstance(reason, str):
        return None
    if reason in messages:
        return messages[reason]
    for resource, label in (("nodes", "node"), ("vcpu", "CPU"), ("memory", "memory"), ("storage", "storage")):
        if reason == f"execution_capacity_provider_quota_{resource}_exceeded":
            return f"Waiting for provider {label} quota to become available."
        if reason == f"execution_capacity_max_{resource}_exceeded":
            return f"Waiting for capacity within the configured {label} limit."
    if reason.startswith("execution_capacity_"):
        return "Waiting for execution capacity; detailed admission diagnostics are unavailable."
    if reason.startswith("execution_admission_"):
        return "Waiting for an execution concurrency reservation."
    return None


def _timeline(trial: Trial, lease: ServiceExecutionLease | None) -> list[dict[str, Any]]:
    # Existing terminal records do not bind the image attempt they consumed.
    # Do not invent historical build duration from mutable shared cache state.
    reserved = lease.created_at if lease else None
    snapshot = trial.scheduling_observation or {}
    ready = _time(snapshot.get("image_ready_at")) if lease and snapshot.get("lease_id") == str(lease.id) else None
    preparation: list[tuple[str, datetime | None, datetime | None]] = [
        ("Preparation and admission (historical timing unavailable)", trial.submitted_at, reserved),
    ]
    if lease and lease.attempt > 1:
        # submitted_at predates previous execution(s); it is not this retry's
        # queue-entry time. Keep unknown instead of counting old runtime as wait.
        preparation = [("Retry preparation/admission (timing unavailable)", None, None)]
    elif ready:
        mode = snapshot.get("image_mode")
        preparation = [
            ("Image ready (reused)" if mode == "reused" else "Prebuilt image" if mode == "prebuilt"
             else "Image preparation", trial.submitted_at, ready),
            ("Execution admission", ready, reserved),
        ]
    rows = [
        *preparation,
        ("Pod scheduling", reserved, lease.pod_scheduled_at if lease else None),
        ("Environment startup", lease.pod_scheduled_at if lease else None,
         lease.pod_started_at if lease else None),
        ("Execution", lease.pod_started_at if lease else None,
         lease.pod_terminated_at if lease else None),
        ("Output archival", lease.pod_terminated_at if lease else None,
         lease.materialization_committed_at if lease else None),
    ]
    return [{
        "label": label, "started_at": _iso(start), "finished_at": _iso(end),
        "seconds": max(0, int((end - start).total_seconds())) if start and end else None,
    } for label, start, end in rows]


def progress_response(
    trial: Trial, stage: str, lease: ServiceExecutionLease | None,
    preparations: list[dict[str, Any]], *, admin: bool, now: datetime,
) -> dict[str, Any]:
    reason = None
    observed = lease.last_reconciled_at if lease else None
    detail = None
    if stage == "image_preparation" and preparations:
        image = preparations[0]
        detail = image.get("stage", image["state"])
        reason = image.get("wait_message") or image.get("message")
        observed = _time(image.get("observed_at"))
    elif stage == "execution_wait":
        observation = trial.scheduling_observation or {}
        observed = _time(observation.get("observed_at"))
        reason = wait_message(observation.get("reason"))
    elif stage == "starting" and lease:
        detail = "pod_scheduling" if lease.pod_scheduled_at is None else "environment_startup"
        reason = wait_message(lease.error_code)
    # Do not let a later cache rebuild or old scheduling observation rewrite a result.
    if stage in TERMINAL:
        reason, observed = None, trial.finished_at
    fresh = observed is not None and observed >= now - timedelta(seconds=120)
    return {
        "stage": stage, "label": STAGES.get(stage, stage), "detail": detail,
        "wait_message": reason, "observed_at": _iso(observed),
        "observation_stale": stage not in TERMINAL and not fresh,
        "node_name": lease.node_name if admin and lease and stage != "execution_wait" else None,
        "timeline": _timeline(trial, lease),
    }


async def load_trial_progress(
    session: AsyncSession, trials: Sequence[Trial], *, admin: bool = False,
) -> dict[UUID, dict[str, Any]]:
    if not trials:
        return {}
    ids = [trial.id for trial in trials]
    stages = {key: value for key, value in (await session.execute(
        select(Trial.id, progress_stage_case()).where(Trial.id.in_(ids))
    )).all()}
    leases = list((await session.scalars(
        select(ServiceExecutionLease).join(Trial, Trial.id == ServiceExecutionLease.trial_id)
        .where(Trial.id.in_(ids), ServiceExecutionLease.id == latest_execution_id())
    )).all())
    by_trial = {lease.trial_id: lease for lease in leases}
    images = (await session.execute(
        select(TrialTaskImageMaterialization.trial_id, TaskImageMaterialization,
               TaskImageMaterializationAttempt, TaskImageCapacityWait)
        .join(TaskImageMaterialization,
              TaskImageMaterialization.id == TrialTaskImageMaterialization.materialization_id)
        .join(Trial, Trial.id == TrialTaskImageMaterialization.trial_id)
        .outerjoin(TaskImageMaterializationAttempt, and_(
            TaskImageMaterializationAttempt.materialization_id == TaskImageMaterialization.id,
            TaskImageMaterializationAttempt.lease_epoch == TaskImageMaterialization.lease_epoch))
        .outerjoin(TaskImageCapacityWait,
                   TaskImageCapacityWait.materialization_id == TaskImageMaterialization.id)
        .where(TrialTaskImageMaterialization.trial_id.in_(ids),
               TaskImageMaterialization.task_id == Trial.task_id,
               TaskImageMaterialization.cpu_arch == "x86_64")
    )).all()
    now = datetime.now(UTC)
    preparations: dict[UUID, list[dict[str, Any]]] = {}
    for trial_id, image, attempt, wait in images:
        item = preparation_response(image, attempt)
        native = attempt.native_build if attempt and attempt.native_build else {}
        item["observed_at"] = native.get("observed_at")
        item["stage"] = image.state
        if native.get("scheduling"):
            item["wait_message"] = wait_message(native["scheduling"].get("reason"))
        for phase in item["phases"]:
            if phase["state"] == "running":
                item["stage"] = phase["name"]
                break
        if wait and wait.lease_epoch == image.lease_epoch and wait.expires_at > now:
            item.update(stage="capacity_wait", wait_message=wait_message(wait.reason),
                        observed_at=_iso(wait.renewed_at))
        preparations.setdefault(trial_id, []).append(item)
    return {
        trial.id: progress_response(
            trial, stages[trial.id], by_trial.get(trial.id),
            preparations.get(trial.id, []), admin=admin, now=now,
        ) for trial in trials
    }


async def progress_summary(session: AsyncSession, scoped_ids: Any) -> dict[str, Any]:
    stage = progress_stage_case()
    rows = (await session.execute(
        select(stage, func.count(), func.min(Trial.submitted_at))
        .where(Trial.id.in_(scoped_ids)).group_by(stage)
    )).all()
    counts = {key: int(value) for key, value, _ in rows}
    now = datetime.now(UTC)
    oldest_wait = {key: max(0, int((now - oldest).total_seconds()))
                   for key, _, oldest in rows
                   if oldest and key in {"image_preparation", "execution_wait"}}
    image_ids = (
        select(TrialTaskImageMaterialization.materialization_id)
        .where(TrialTaskImageMaterialization.trial_id.in_(scoped_ids)).distinct()
    )
    image_counts = {key: int(value) for key, value in (await session.execute(
        select(TaskImageMaterialization.state, func.count())
        .where(TaskImageMaterialization.id.in_(image_ids),
               TaskImageMaterialization.cpu_arch == "x86_64")
        .group_by(TaskImageMaterialization.state)
    )).all()}
    native_count = await session.scalar(select(func.count()).select_from(Trial).where(
        Trial.id.in_(scoped_ids), Trial.state.not_in(tuple(TERMINAL)),
        (Trial.requires_caps["backend"].as_string() == "nebius")
        | Trial.batch_id.in_(select(Batch.id).where(Batch.backend == "nebius"))
        | latest_execution_id().is_not(None),
    ))
    return {
        "native_active_trials": int(native_count or 0),
        "stages": {name: int(counts.get(name, 0)) for name in STAGES},
        "oldest_wait_since_submission_seconds": oldest_wait,
        "trial_count": sum(int(count) for count in counts.values()),
        "images": {"states": image_counts, "image_count": sum(image_counts.values()),
                   "waiting_trials": int(counts.get("image_preparation", 0))},
    }
