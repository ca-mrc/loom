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

from sqlalchemy import String, and_, case, cast, column, func, or_, select, true
from sqlalchemy.dialects.postgresql import JSONB, JSONPATH
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased, load_only

from loom.db.schema import (
    Batch,
    ServiceExecutionLease,
    ServiceExecutionTarget,
    TaskImageCapacityWait,
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
    Trial,
    TrialTaskImageMaterialization,
)
from loom_service.task_image_preparation import preparation_failure_message

STAGES = {
    "image_preparation": "Waiting for image",
    "execution_wait": "Waiting for execution",
    "starting": "Starting environment",
    "running": "Running",
    "verifying": "Verifying",
    "unknown": "Unknown",
    "archiving": "Archiving output",
    "succeeded": "Succeeded",
    "failed": "Failed",
    "cancelled": "Cancelled",
}
TERMINAL = {"succeeded", "failed", "cancelled"}


def latest_execution_id() -> Any:
    """The applicable attempt, never an older attempt left running during a retry."""
    lease = aliased(ServiceExecutionLease)
    return (
        select(lease.id).where(lease.trial_id == Trial.id, lease.execution_role == "attempt",
                               lease.attempt == Trial.attempt_count)
        .limit(1).correlate(Trial).scalar_subquery()
    )


def pipeline_rows(scoped_ids: Any = None) -> Any:
    """One relational projection shared by counts, filters and bounded row loading.

    Join immutable attempt/parent/retry identities once. No per-Trial scan of
    image attempts, native manifests or diagnostic reports enters this query.
    """
    trial = aliased(Trial, name="pipeline_trial")
    parent = aliased(ServiceExecutionLease, name="pipeline_attempt")
    child = aliased(ServiceExecutionLease, name="pipeline_verifier")
    image_wait = (
        select(TrialTaskImageMaterialization.trial_id.label("trial_id"))
        .join(TaskImageMaterialization,
              TaskImageMaterialization.id == TrialTaskImageMaterialization.materialization_id)
        .join(Trial, Trial.id == TrialTaskImageMaterialization.trial_id)
        .where(TaskImageMaterialization.task_id == Trial.task_id,
               TaskImageMaterialization.cpu_arch == "x86_64",
               TaskImageMaterialization.state != "ready")
    )
    cleanup = select(
        ServiceExecutionLease.trial_id.label("trial_id"),
        func.max(case(
            (ServiceExecutionLease.cleanup_state == "blocked", 5),
            (ServiceExecutionLease.cleanup_state == "in_progress", 4),
            (ServiceExecutionLease.cleanup_state == "pending", 3),
            (ServiceExecutionLease.cleanup_state == "not_requested", 2),
            (ServiceExecutionLease.cleanup_state == "complete", 1), else_=0,
        )).label("priority"),
        func.min(ServiceExecutionLease.last_reconciled_at).filter(
            ServiceExecutionLease.cleanup_state.in_(("pending", "in_progress", "blocked")))
        .label("observed_at"),
        func.count().filter(and_(
            ServiceExecutionLease.cleanup_state.in_(("pending", "in_progress", "blocked")),
            ServiceExecutionLease.last_reconciled_at.is_(None))).label("missing_observations"),
    )
    if scoped_ids is not None:
        image_wait = image_wait.where(Trial.id.in_(scoped_ids))
        cleanup = cleanup.where(ServiceExecutionLease.trial_id.in_(scoped_ids))
    waiting = image_wait.distinct().subquery("pipeline_image_wait")
    cleaning = cleanup.group_by(ServiceExecutionLease.trial_id).subquery("pipeline_cleanup")
    handoff = trial.result["verifier_execution"]
    bound_handoff = and_(
        parent.id.is_not(None),
        parent.runtime_contract_json["verifier_execution"].as_string() == "separate_execution",
        handoff["parent_lease_id"].as_string() == cast(parent.id, String),
        handoff["state"].as_string().in_(("pending", "committed", "unavailable")),
    )
    has_child = child.id.is_not(None)
    def effective(field: str) -> Any:
        return case((has_child, getattr(child, field)), else_=getattr(parent, field))
    output = case(
        (and_(effective("output_generation").is_not(None),
              effective("output_generation") != effective("resource_generation")), "unknown"),
        else_=func.coalesce(effective("output_commit_state"), "unknown"),
    )
    observed = effective("observed_state")
    archive = or_(
        effective("materialization_state").in_(("pending", "running")),
        observed.in_(("finalizing", "finalized")), output == "uploading",
    )
    verifying = and_(bound_handoff, handoff["state"].as_string() == "pending")
    stage = case(
        (trial.state.in_(tuple(TERMINAL)), trial.state),
        (trial.state == "materializing", "archiving"),
        (and_(verifying, has_child, archive), "archiving"),
        (verifying, "verifying"),
        (and_(trial.state == "queued", waiting.c.trial_id.is_not(None)), "image_preparation"),
        (trial.state.in_(("queued", "protected-pending")), "execution_wait"),
        (and_(parent.id.is_not(None), archive), "archiving"),
        (and_(parent.id.is_not(None), parent.revoked_at.is_(None), observed == "running"), "running"),
        (and_(parent.id.is_not(None), parent.revoked_at.is_(None),
              observed.in_(("reserved", "creating", "not_created", "pending", "starting", "unknown"))), "starting"),
        else_="unknown",
    )
    cleanup_state = case(
        (cleaning.c.priority == 5, "blocked"), (cleaning.c.priority == 4, "in_progress"),
        (cleaning.c.priority == 3, "pending"), (cleaning.c.priority == 2, "not_requested"),
        (cleaning.c.priority == 1, "complete"), else_="unknown",
    )
    stmt = (
        select(trial.id.label("trial_id"), stage.label("stage"),
               parent.id.label("parent_id"), child.id.label("verifier_id"),
               output.label("output_state"), cleanup_state.label("cleanup_state"),
               case((cleaning.c.missing_observations > 0, None), else_=cleaning.c.observed_at)
               .label("cleanup_observed_at"),
               trial.submitted_at.label("submitted_at"), trial.state.label("trial_state"),
               or_(parent.id.is_not(None), trial.requires_caps["backend"].as_string() == "nebius",
                   trial.batch_id.in_(select(Batch.id).where(Batch.backend == "nebius")))
               .label("native"))
        .select_from(trial)
        .outerjoin(parent, and_(parent.trial_id == trial.id, parent.execution_role == "attempt",
                                parent.attempt == trial.attempt_count))
        .outerjoin(child, and_(child.trial_id == trial.id, child.execution_role == "verifier",
                              child.parent_lease_id == parent.id, child.attempt == parent.attempt,
                              bound_handoff,
                              or_(handoff["state"].as_string() != "pending", child.revoked_at.is_(None)),
                              or_(and_(handoff["state"].as_string() != "pending",
                                       handoff["lease_id"].as_string() == cast(child.id, String)),
                                  and_(handoff["state"].as_string() == "pending",
                                       cast(child.verifier_retry, String)
                                       == func.coalesce(handoff["retries"].as_string(), "0")))))
        .outerjoin(waiting, waiting.c.trial_id == trial.id)
        .outerjoin(cleaning, cleaning.c.trial_id == trial.id)
    )
    if scoped_ids is not None:
        stmt = stmt.where(trial.id.in_(scoped_ids))
    return stmt.subquery("pipeline")


def progress_stage_case() -> Any:
    rows = pipeline_rows()
    return select(rows.c.stage).where(rows.c.trial_id == Trial.id).correlate(Trial).scalar_subquery()


def progress_state_filter(kind: str, values: list[str], scoped_ids: Any = None) -> Any:
    rows = pipeline_rows(scoped_ids)
    field = {"cleanup": rows.c.cleanup_state, "output": rows.c.output_state, "stage": rows.c.stage}[kind]
    return Trial.id.in_(select(rows.c.trial_id).where(field.in_(values)))


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


def _seconds(start: datetime | None, now: datetime) -> int | None:
    return max(0, int((now - start).total_seconds())) if start else None


def progress_response(
    trial: Trial, stage: str, lease: ServiceExecutionLease | None,
    preparations: list[dict[str, Any]], *, admin: bool, now: datetime,
    parent: ServiceExecutionLease | None = None, region: str | None = None,
    output_state: str | None = None, cleanup_state: str | None = None,
    cleanup_observed_at: datetime | None = None,
) -> dict[str, Any]:
    reason = None
    observed = lease.last_reconciled_at if lease else None
    detail = None
    started = None
    if stage == "image_preparation" and preparations:
        image = preparations[0]
        detail = image.get("stage", image["state"])
        reason = image.get("wait_message") or image.get("message")
        observed = _time(image.get("observed_at"))
        # This Trial may have joined an older shared build. Its image-wait
        # entry timestamp is not recorded; image phase clocks stay on image rows.
        started = None
        region = image.get("region")
    elif stage == "execution_wait":
        observation = trial.scheduling_observation or {}
        observed = _time(observation.get("observed_at"))
        reason = wait_message(observation.get("reason"))
        # The observation time is a refreshed sample, not entry into this stage.
    elif stage == "starting" and lease:
        detail = "pod_scheduling" if lease.pod_scheduled_at is None else "environment_startup"
        reason = wait_message(lease.error_code)
        started = lease.created_at
    elif stage == "running" and lease:
        started = lease.pod_started_at
    elif stage == "verifying":
        if lease and getattr(lease, "execution_role", None) == "verifier":
            detail = "running" if lease.observed_state == "running" else "starting"
            started = parent.finalized_at if parent else None
            reason = wait_message(lease.error_code)
        else:
            detail = "execution_wait"
            reason = "Waiting for independent verifier capacity after the agent output was committed."
            started = parent.finalized_at if parent else None
            observed = parent.last_reconciled_at if parent else None
    elif stage == "archiving" and lease:
        started = lease.pod_terminated_at
    # Later shared cache observations cannot alter retained result or its clock.
    if stage in TERMINAL:
        reason, observed, started = None, trial.finished_at, trial.finished_at
    fresh = observed is not None and observed >= now - timedelta(seconds=120)
    if output_state is None:
        output_state = getattr(lease, "output_commit_state", None) or "unknown"
    if cleanup_state is None:
        cleanup_state = getattr(lease, "cleanup_state", None) or "unknown"
    timeline = _timeline(trial, parent or lease)
    if parent and lease and lease.id != parent.id:
        for item in _timeline(trial, lease)[-4:]:
            timeline.append({**item, "label": "Verifier " + item["label"].lower()})
    return {
        "stage": stage, "label": STAGES.get(stage, stage), "detail": detail,
        "wait_message": reason, "observed_at": _iso(observed),
        "observation_stale": stage not in TERMINAL and not fresh,
        "observation_missing": observed is None,
        "stage_started_at": _iso(started),
        "stage_seconds": _seconds(started, now) if stage not in TERMINAL else None,
        "region": region, "output_state": output_state, "cleanup_state": cleanup_state,
        "cleanup_observed_at": _iso(cleanup_observed_at),
        "cleanup_observation_stale": cleanup_state in {"pending", "in_progress", "blocked"}
        and (cleanup_observed_at is None or cleanup_observed_at < now - timedelta(seconds=120)),
        "node_name": lease.node_name if admin and lease and stage != "execution_wait" else None,
        "timeline": timeline,
    }


_IMAGE_LABELS = {
    "queued": "Queued for image preparation", "retry_wait": "Waiting to retry image preparation",
    "capacity_wait": "Waiting for image build resources", "prepare": "Preparing build context",
    "build": "Build container active", "publish": "Publishing image", "cleanup": "Releasing build resources",
    "ready": "Image ready", "failed": "Image preparation failed", "unknown": "Image stage unknown",
}


def image_rows(scoped_ids: Any) -> Any:
    """Unique authorized materializations with only small public observation inputs."""
    image = TaskImageMaterialization
    attempt = TaskImageMaterializationAttempt
    wait = TaskImageCapacityWait
    links = (
        select(TrialTaskImageMaterialization.materialization_id.label("id"),
               func.count(func.distinct(Trial.id)).label("affected_trials"),
               func.array_agg(func.distinct(Trial.id)).label("trial_ids"),
               func.count(func.distinct(Trial.id)).filter(
                   Trial.state == "queued").label("waiting_trials"))
        .join(Trial, Trial.id == TrialTaskImageMaterialization.trial_id)
        .join(image, and_(image.id == TrialTaskImageMaterialization.materialization_id,
                          image.task_id == Trial.task_id, image.cpu_arch == "x86_64"))
        .where(Trial.id.in_(scoped_ids)).group_by(TrialTaskImageMaterialization.materialization_id)
        .cte("pipeline_image_links")
    )
    # A retry can advance the current epoch before the former build releases
    # its reservation. The claim guard waits on every such attempt; expose the
    # oldest outstanding cleanup using indexed scalar projections, not manifests.
    prior = attempt.__table__.alias("pipeline_prior_attempt")
    old_cleanup = (
        select(prior.c.materialization_id.label("id"),
               prior.c.native_capacity_target_id.label("target_id"),
               prior.c.native_build["observed_at"].as_string().label("observed_at"))
        .join(image, image.id == prior.c.materialization_id)
        .where(prior.c.materialization_id.in_(select(links.c.id)),
               prior.c.lease_epoch < image.lease_epoch,
               prior.c.native_capacity_reserved_at.is_not(None),
               prior.c.native_capacity_released_at.is_(None))
        .distinct(prior.c.materialization_id)
        .order_by(prior.c.materialization_id, prior.c.lease_epoch)
        .subquery("pipeline_old_cleanup")
    )
    prior_cleanup = old_cleanup.c.id.is_not(None)
    # Decompose the TOASTed native receipt once per unique image. Repeated
    # JSON subscripts otherwise decompress large private manifests per field.
    native = func.jsonb_to_record(case(
        (func.jsonb_typeof(attempt.native_build) == "object", attempt.native_build),
        else_=cast({}, JSONB),
    )).table_valued(*[column(name, kind) for name, kind in (
        ("state", String), ("phases", JSONB), ("observed_at", String),
        ("capacity_reserved_at", String), ("capacity_released_at", String),
        ("target_id", String), ("scheduling", JSONB),
    )]).render_derived(name="pipeline_native", with_types=True).lateral()
    # Project only observation fields; never fetch full Job/configmap/claim JSON.
    phase = {name: func.jsonb_path_query_first(native.c.phases,
             cast('$[*] ? (@.name == "' + name + '")', JSONPATH), type_=JSONB) for name in ("prepare", "build", "publish")}
    # SQL NULL and JSON null differ. Container-state objects must actually exist.
    running_phase = case(*[(func.jsonb_typeof(phase[name]["state"]["running"]) == "object", name)
                          for name in ("prepare", "build", "publish")], else_="unknown")
    released = native.c.capacity_released_at
    reserved = native.c.capacity_reserved_at
    valid_wait = and_(wait.lease_epoch == image.lease_epoch, wait.expires_at > func.now())
    needs_cleanup = and_(reserved.is_not(None), released.is_(None), image.state.in_(("ready", "failed")))
    stage = case(
        (or_(prior_cleanup, needs_cleanup, and_(native.c.state == "cleaning", released.is_(None))), "cleanup"),
        (image.state == "ready", "ready"), (image.state == "failed", "failed"),
        (valid_wait, "capacity_wait"),
        (and_(image.state == "queued", image.failure_reason.is_not(None)), "retry_wait"),
        (image.state == "queued", "queued"),
        (image.state.in_(("claimed", "running")), running_phase), else_="unknown",
    )
    return (
        select(image.id, image.task_id, image.state, image.failure_reason, image.created_at,
               image.claimed_at, image.started_at, image.finished_at, image.ready_at,
               image.attempt_count, image.lease_epoch, image.next_attempt_at, links.c.affected_trials, links.c.trial_ids,
               case((image.state != "ready", links.c.waiting_trials), else_=0).label("waiting_trials"),
               stage.label("stage"), prior_cleanup.label("prior_cleanup"),
               old_cleanup.c.observed_at.label("cleanup_observed_at"),
               case((valid_wait, wait.renewed_at), else_=None).label("wait_observed_at"),
               case((valid_wait, wait.first_waited_at), else_=None).label("wait_started_at"),
               case((valid_wait, wait.reason), else_=None).label("wait_reason"),
               native.c.observed_at.label("native_observed_at"),
               native.c.scheduling["reason"].as_string().label("scheduling_reason"),
               reserved.label("reserved_at"), released.label("released_at"),
               *[phase[name]["state"]["running"]["startedAt"].as_string().label(name + "_started_at")
                 for name in phase],
               ServiceExecutionTarget.region.label("region"))
        .select_from(links).join(image, image.id == links.c.id)
        .outerjoin(attempt, and_(attempt.materialization_id == image.id,
                                attempt.lease_epoch == image.lease_epoch))
        .outerjoin(native, true())
        .outerjoin(old_cleanup, old_cleanup.c.id == image.id)
        .outerjoin(wait, wait.materialization_id == image.id)
        .outerjoin(ServiceExecutionTarget,
                   ServiceExecutionTarget.id == case((prior_cleanup, old_cleanup.c.target_id),
                                                     (valid_wait, wait.target_id),
                                                     else_=native.c.target_id))
        .subquery("pipeline_images")
    )


def image_response(row: Any, *, now: datetime) -> dict[str, Any]:
    stage = row["stage"]
    observed = (_time(row["cleanup_observed_at"]) if row["prior_cleanup"]
                else row["wait_observed_at"] or _time(row["native_observed_at"]))
    started = row["wait_started_at"] if stage == "capacity_wait" else None
    if stage in {"prepare", "build", "publish"}:
        started = _time(row[stage + "_started_at"])
    elif stage == "queued" and row["attempt_count"] == 0 and row["lease_epoch"] == 0:
        started = row["created_at"]
    elif stage == "cleanup" and not row["prior_cleanup"]:
        started = row["finished_at"] or row["ready_at"]
    # Keep any failed attempt visible while waiting to retry; no raw code/text.
    safe_failure = preparation_failure_message(row["failure_reason"])
    reason = safe_failure or wait_message(row["wait_reason"] or row["scheduling_reason"])
    if row["prior_cleanup"]:
        reason = "Previous image attempt is releasing build resources before a replacement can start."
    if stage == "build":
        note = "Build container is active; its build and scratch-cleanup substeps are not separately observed."
        reason = f"{reason} {note}" if reason else note
    fresh = observed is not None and observed >= now - timedelta(seconds=120)
    return {
        "id": str(row["id"]), "task_id": row["task_id"], "stage": stage,
        "label": _IMAGE_LABELS[stage], "state": row["state"],
        "affected_trials": int(row["affected_trials"]), "waiting_trials": int(row["waiting_trials"]),
        "wait_message": reason, "stage_started_at": _iso(started),
        "stage_seconds": _seconds(started, now) if stage not in {"ready", "failed"} else None,
        "region": row["region"], "observed_at": _iso(observed),
        "observation_stale": stage not in {"ready", "failed"} and not fresh,
        "observation_missing": observed is None,
        "resources_released": False if row["prior_cleanup"] else bool(row["released_at"]) if row["reserved_at"] else None,
    }


async def load_trial_progress(
    session: AsyncSession, trials: Sequence[Trial], *, admin: bool = False,
) -> dict[UUID, dict[str, Any]]:
    if not trials:
        return {}
    ids = [trial.id for trial in trials]
    projection = pipeline_rows(select(Trial.id).where(Trial.id.in_(ids)))
    rows = {row["trial_id"]: row for row in (await session.execute(select(projection))).mappings()}
    lease_ids = {row[key] for row in rows.values() for key in ("parent_id", "verifier_id") if row[key]}
    # Load only bounded lifecycle fields, never frozen contracts/manifests.
    lease_rows = (await session.execute(
        select(ServiceExecutionLease, ServiceExecutionTarget.region)
        .outerjoin(ServiceExecutionTarget, ServiceExecutionTarget.id == ServiceExecutionLease.target_id)
        .where(ServiceExecutionLease.id.in_(lease_ids))
        .options(load_only(*[getattr(ServiceExecutionLease, name) for name in (
            "id", "trial_id", "attempt", "execution_role", "created_at", "pod_scheduled_at",
            "pod_started_at", "pod_terminated_at", "materialization_committed_at", "last_reconciled_at",
            "node_name", "error_code", "observed_state", "finalized_at", "output_commit_state",
            "cleanup_state", "target_id",
        )]))
    )).all() if lease_ids else []
    by_id = {lease.id: lease for lease, _ in lease_rows}
    regions = {lease.id: region for lease, region in lease_rows}
    images = image_rows(select(Trial.id).where(Trial.id.in_(ids)))
    preparations: dict[UUID, list[dict[str, Any]]] = {}
    for row in (await session.execute(select(images))).mappings():
        item = image_response(row, now=datetime.now(UTC))
        for trial_id in row["trial_ids"]:
            preparations.setdefault(trial_id, []).append(item)
    now = datetime.now(UTC)
    result = {}
    for trial in trials:
        row = rows[trial.id]
        parent = by_id.get(row["parent_id"])
        lease = by_id.get(row["verifier_id"]) or parent
        result[trial.id] = progress_response(
            trial, row["stage"], lease, preparations.get(trial.id, []), admin=admin, now=now,
            parent=parent, region=regions.get(lease.id) if lease else None,
            output_state=row["output_state"], cleanup_state=row["cleanup_state"],
            cleanup_observed_at=row["cleanup_observed_at"],
        )
    return result


async def progress_summary(session: AsyncSession, scoped_ids: Any) -> dict[str, Any]:
    projection = pipeline_rows(scoped_ids)
    rows = (await session.execute(select(
        projection.c.stage, projection.c.output_state, projection.c.cleanup_state,
        func.count().label("count"), func.min(projection.c.submitted_at).label("oldest"),
        func.count().filter(and_(projection.c.native,
                                 projection.c.trial_state.not_in(tuple(TERMINAL)))).label("native"),
    ).group_by(projection.c.stage, projection.c.output_state, projection.c.cleanup_state))).mappings().all()
    counts: dict[str, int] = {}
    outputs: dict[str, int] = {}
    cleanups: dict[str, int] = {}
    oldest: dict[str, datetime] = {}
    native_count = 0
    for row in rows:
        for field, target in (("stage", counts), ("output_state", outputs), ("cleanup_state", cleanups)):
            target[row[field]] = target.get(row[field], 0) + row["count"]
        if row["oldest"] and row["stage"] in {"image_preparation", "execution_wait"}:
            oldest[row["stage"]] = min(oldest.get(row["stage"], row["oldest"]), row["oldest"])
        native_count += row["native"]
    images = image_rows(scoped_ids)
    image_stats = (await session.execute(select(images.c.state, images.c.stage, func.count().label("count"))
                  .group_by(images.c.state, images.c.stage))).mappings().all()
    image_counts: dict[str, int] = {}
    image_stages: dict[str, int] = {}
    for row in image_stats:
        image_counts[row["state"]] = image_counts.get(row["state"], 0) + row["count"]
        image_stages[row["stage"]] = image_stages.get(row["stage"], 0) + row["count"]
    now = datetime.now(UTC)
    return {
        "native_active_trials": native_count,
        "stages": {name: counts.get(name, 0) for name in STAGES},
        "output_states": outputs, "cleanup_states": cleanups,
        "oldest_wait_since_submission_seconds": {key: _seconds(value, now) for key, value in oldest.items()},
        "trial_count": sum(counts.values()),
        "images": {"states": image_counts, "stages": image_stages,
                   "image_count": sum(image_counts.values()),
                   "waiting_trials": counts.get("image_preparation", 0)},
    }
