"""Monitor summary API.

The SPA Monitor page lists paginated rows, but release/debug workflows also
need URL-scoped state counters and worker capacity at a glance.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Any, Literal
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Response
from sqlalchemy import case, func, select

from loom.db.schema import ArtifactUploadSession, Batch, ServiceExecutionLease, Trial
from loom.resource_pools import get_resource_pool_summary
from loom_control_plane.execution_capacity import fetch_execution_capacity_status
from loom_control_plane.execution_resource_calibration import (
    fetch_execution_resource_profile_status,
)
from loom_service import wire_responses as wire
from loom_service.auth_guards import is_admin, require_scope
from loom_service.dependencies import SessionAndCtx
from loom_service.monitor_filters import (
    apply_batch_monitor_filters,
    apply_trial_monitor_filters,
    resolve_monitor_team_filter,
)
from loom_service.monitor_placement import load_placement
from loom_service.service_execution_status import (
    SERVICE_EXECUTION_LIFECYCLE_STAGES,
    service_execution_lifecycle_case,
)
from loom_service.trial_progress import latest_execution_id, progress_summary
from loom_service.worker_backends import (
    _HEARTBEAT_FRESHNESS_SEC,
    get_active_backends,
    get_active_worker_count,
)

router = APIRouter()

View = Literal["batches", "trials"]

_BATCH_STATES = ("submitted", "running", "finished", "cancelled")
_TRIAL_STATES = (
    "queued",
    "protected-pending",
    "claimed",
    "running",
    "materializing",
    "succeeded",
    "failed",
    "cancelled",
)

_CAPACITY_POLICY_FIELDS = (
    "enabled",
    "max_nodes",
    "max_vcpu_millis",
    "max_memory_mib",
    "max_storage_mib",
    "node_cpu_millis",
    "node_memory_mib",
    "node_storage_mib",
    "max_pending_jobs",
    "max_unschedulable_jobs",
    "max_image_pull_backoff_jobs",
    "observation_max_age_seconds",
)
_CAPACITY_OBSERVATION_FIELDS = (
    "observed_at",
    "fresh_until",
    "is_fresh",
    "provider_capacity_state",
    "provider_capacity_reason",
    "autoscaler_state",
    "autoscaler_reason",
    "provider_quota_nodes",
    "provider_used_nodes",
    "provider_quota_nodes_headroom",
    "provider_quota_vcpu_millis",
    "provider_used_vcpu_millis",
    "provider_quota_vcpu_millis_headroom",
    "active_nodes",
    "occupied_nodes",
    "draining_nodes",
    "node_states",
    "policy_nodes_headroom",
    "provisioned_vcpu_millis",
    "policy_vcpu_millis_headroom",
    "allocatable_cpu_millis",
    "requested_cpu_millis",
    "allocatable_cpu_millis_free",
    "pending_jobs",
    "unschedulable_jobs",
    "image_pull_backoff_jobs",
    "pending_reasons",
)
_RESOURCE_PROFILE_FIELDS = (
    "forecast_is_fresh",
    "observed_fit_slots",
    "immediate_executable_slots",
    "configured_additional_nodes",
    "configured_slots_per_node",
    "configured_scale_headroom_slots",
    "configured_total_fit_slots",
    "blockers",
)


def _select_fields(value: object, fields: tuple[str, ...]) -> dict[str, object] | None:
    if not isinstance(value, dict):
        return None
    return {field: value.get(field) for field in fields}


def _public_execution_capacity(
    capacity: dict[str, object],
    profiles: dict[str, object],
    *,
    admin: bool,
) -> list[dict[str, object]]:
    profile_rows = profiles.get("targets")
    capacity_rows = capacity.get("targets")
    profile_by_target = {
        str(row.get("target_id")): row
        for row in profile_rows
        if isinstance(row, dict)
    } if isinstance(profile_rows, list) else {}
    out: list[dict[str, object]] = []
    if not isinstance(capacity_rows, list):
        return out
    for row in capacity_rows:
        if not isinstance(row, dict):
            continue
        target_id = str(row.get("target_id"))
        profile = profile_by_target.get(target_id, {})
        public_profile = _select_fields(profile, _RESOURCE_PROFILE_FIELDS)
        # Admission uses conservative zeroes for unavailable forecasts. Those
        # zeroes are not measurements of the user's executable capacity.
        observation = row.get("observation") or {}
        forecast_unavailable = (
            not (profile.get("binding") or {}).get("enabled")
            or not (profile.get("calibration") or {}).get("eligible")
            or not observation.get("is_fresh")
        )
        if (
            public_profile is not None
            and not profile.get("forecast_is_fresh")
            and forecast_unavailable
        ):
            for field in _RESOURCE_PROFILE_FIELDS:
                if field not in {"forecast_is_fresh", "blockers"}:
                    public_profile[field] = None
        public: dict[str, object] = {
            "target_id": row.get("target_id"),
            **{
                field: row[field]
                for field in ("execution_class_id", "capacity_owner_target_id")
                if field in row
            },
            "provider": "nebius",
            "pool_id": row.get("pool_id"),
            "environment": row.get("environment"),
            "region": row.get("region"),
            "desired_state": row.get("desired_state"),
            "health_status": row.get("health_status"),
            "policy": _select_fields(row.get("policy"), _CAPACITY_POLICY_FIELDS),
            "observation": _select_fields(
                row.get("observation"), _CAPACITY_OBSERVATION_FIELDS
            ),
            "command_backlog": int(row.get("command_backlog") or 0),
            "blockers": list(row.get("blockers") or []),
            "resource_profile": public_profile,
        }
        out.append(public)
    return out


async def _service_execution_activity(
    session: Any,
    *,
    target_team: UUID | None,
    filters: dict[str, Any],
) -> dict[str, object]:
    def scoped(stmt: Any, *, latest: bool = True) -> Any:
        stmt = stmt.join(Trial, Trial.id == ServiceExecutionLease.trial_id).where(
            ServiceExecutionLease.execution_role == "attempt"
        )
        if latest:
            stmt = stmt.where(ServiceExecutionLease.id == latest_execution_id())
        return apply_trial_monitor_filters(
            stmt,
            target_team=target_team,
            **filters,
        )

    execution_rows = (
        await session.execute(
            scoped(
                select(ServiceExecutionLease.observed_state, func.count())
                .select_from(ServiceExecutionLease)
            ).group_by(ServiceExecutionLease.observed_state)
        )
    ).all()
    lifecycle_expr = service_execution_lifecycle_case()
    lifecycle_rows = (
        await session.execute(
            scoped(
                select(lifecycle_expr.label("lifecycle_stage"), func.count())
                .select_from(ServiceExecutionLease)
            ).group_by(lifecycle_expr)
        )
    ).all()
    materialization_rows = (
        await session.execute(
            scoped(
                select(ServiceExecutionLease.materialization_state, func.count())
                .select_from(ServiceExecutionLease)
            ).group_by(ServiceExecutionLease.materialization_state)
        )
    ).all()
    cleanup_rows = (
        await session.execute(
            scoped(
                select(ServiceExecutionLease.source_cleanup_state, func.count())
                .select_from(ServiceExecutionLease), latest=False,
            ).group_by(ServiceExecutionLease.source_cleanup_state)
        )
    ).all()
    pending_since = func.coalesce(
        ServiceExecutionLease.output_committed_at,
        ServiceExecutionLease.created_at,
    )
    totals = (
        await session.execute(
            scoped(
                select(
                    func.count(ServiceExecutionLease.id).label("lease_count"),
                    func.coalesce(
                        func.sum(
                            case(
                                (
                                    ServiceExecutionLease.materialization_attempts > 0,
                                    ServiceExecutionLease.materialization_attempts - 1,
                                ),
                                else_=0,
                            )
                        ),
                        0,
                    ).label("retry_attempts"),
                    func.min(ServiceExecutionLease.materialization_next_attempt_at)
                    .filter(
                        ServiceExecutionLease.materialization_state.in_(("pending", "running"))
                    )
                    .label("oldest_next_attempt_at"),
                    func.min(pending_since)
                    .filter(
                        ServiceExecutionLease.materialization_state.in_(("pending", "running"))
                    )
                    .label("oldest_pending_at"),
                    func.max(ServiceExecutionLease.materialization_committed_at).label(
                        "last_committed_at"
                    ),
                )
                .select_from(ServiceExecutionLease)
            )
        )
    ).one()
    source_totals = (
        await session.execute(
            scoped(
                select(
                    func.coalesce(
                        func.sum(ArtifactUploadSession.actual_total_bytes).filter(
                            ServiceExecutionLease.materialization_state.in_(
                                ("pending", "running")
                            )
                        ),
                        0,
                    ).label("pending_bytes"),
                    func.coalesce(
                        func.sum(ArtifactUploadSession.actual_total_bytes).filter(
                            ServiceExecutionLease.source_cleanup_state != "complete"
                        ),
                        0,
                    ).label("retained_bytes"),
                )
                .select_from(ServiceExecutionLease)
                .outerjoin(
                    ArtifactUploadSession,
                    ArtifactUploadSession.id
                    == ServiceExecutionLease.output_upload_session_id,
                ), latest=False,
            )
        )
    ).one()
    materialization = {
        state: 0
        for state in ("not_started", "pending", "running", "committed", "unavailable")
    }
    for state, count in materialization_rows:
        if state in materialization:
            materialization[str(state)] = int(count)
    execution = {str(state): int(count) for state, count in execution_rows}
    lifecycle = {state: 0 for state in SERVICE_EXECUTION_LIFECYCLE_STAGES}
    for state, count in lifecycle_rows:
        if state in lifecycle:
            lifecycle[str(state)] = int(count)
    cleanup = {state: 0 for state in ("not_ready", "retained", "running", "complete")}
    for state, count in cleanup_rows:
        if state in cleanup:
            cleanup[str(state)] = int(count)
    oldest_pending_at = totals.oldest_pending_at
    oldest_pending_age_seconds = None
    if oldest_pending_at is not None:
        if oldest_pending_at.tzinfo is None:
            oldest_pending_at = oldest_pending_at.replace(tzinfo=UTC)
        oldest_pending_age_seconds = max(
            0, int((datetime.now(UTC) - oldest_pending_at).total_seconds())
        )
    return {
        "lease_count": int(totals.lease_count or 0),
        "execution_states": execution,
        "lifecycle_stages": lifecycle,
        "materialization": {
            "states": materialization,
            "backlog": materialization["pending"] + materialization["running"],
            "retry_attempts": int(totals.retry_attempts or 0),
            "oldest_next_attempt_at": (
                totals.oldest_next_attempt_at.isoformat()
                if totals.oldest_next_attempt_at is not None
                else None
            ),
            "oldest_pending_at": (
                oldest_pending_at.isoformat() if oldest_pending_at is not None else None
            ),
            "oldest_pending_age_seconds": oldest_pending_age_seconds,
            "last_committed_at": (
                totals.last_committed_at.isoformat()
                if totals.last_committed_at is not None
                else None
            ),
            "pending_bytes": int(source_totals.pending_bytes or 0),
            "source_retained_bytes": int(source_totals.retained_bytes or 0),
        },
        "source_cleanup_states": cleanup,
    }


async def _counts(
    session: Any,
    stmt: Any,
    *,
    state_col: Any,
    states: tuple[str, ...],
) -> dict[str, int]:
    counts = {state: 0 for state in states}
    rows = await session.execute(stmt.group_by(state_col))
    for state, count in rows.all():
        if state in counts:
            counts[str(state)] = int(count)
    return counts


def _queue_status(
    *,
    queued: int,
    claimed: int,
    running: int,
    active_workers: int,
    native_work: bool = False,
) -> str:
    waiting = queued + claimed
    if not native_work and active_workers <= 0 and (waiting > 0 or running > 0):
        return "blocked"
    if waiting > 0:
        return "waiting"
    if running > 0:
        return "running"
    return "idle"


def _resource_trials_stmt() -> Any:
    return (
        select(
            Trial.state,
            Trial.worker_id,
            Trial.requires_caps,
            Trial.claimed_at,
            Trial.pre_start_heartbeat_at,
        )
        .select_from(Trial)
        .where(
            Trial.state.in_(("queued", "protected-pending", "claimed", "running"))
        )
    )


@router.get("/monitor/summary", response_model=wire.MonitorSummary, response_model_exclude_unset=True)
async def get_monitor_summary(
    response: Response,
    sc: SessionAndCtx,
    view: Annotated[View, Query()] = "batches",
    team_id: Annotated[UUID | None, Query()] = None,
    q: Annotated[str | None, Query()] = None,
    benchmark_id: Annotated[str | None, Query()] = None,
    agent_name: Annotated[str | None, Query()] = None,
    agent: Annotated[str | None, Query()] = None,
    model_provider: Annotated[str | None, Query()] = None,
    model_name: Annotated[str | None, Query()] = None,
    model: Annotated[str | None, Query()] = None,
    provider_connection_id: Annotated[UUID | None, Query()] = None,
    provider_model_id: Annotated[str | None, Query()] = None,
    batch_id: Annotated[UUID | None, Query()] = None,
    state: Annotated[
        str | None,
        Query(description="selected table state; summary still counts all states"),
    ] = None,
) -> dict[str, Any]:
    # Capacity and queue state are heartbeat-derived. A cached response can
    # otherwise make offline workers appear claimable long after they stop.
    response.headers["Cache-Control"] = "no-store"
    session, ctx = sc
    require_scope(ctx, "read:own")
    target_team = resolve_monitor_team_filter(ctx, team_id)
    agent_value = agent_name or agent
    model_value = model_name or model
    q_value = q.strip() if isinstance(q, str) and q.strip() else None

    batch_counts_stmt = select(Batch.state, func.count()).select_from(Batch)
    batch_counts_stmt = apply_batch_monitor_filters(
        batch_counts_stmt,
        target_team=target_team,
        q=q_value if view == "batches" else None,
        benchmark_id=benchmark_id,
        agent_name=agent_value,
        model_provider=model_provider,
        model_name=model_value,
        provider_connection_id=provider_connection_id,
        provider_model_id=provider_model_id,
        state=None,
    )
    if batch_id is not None:
        batch_counts_stmt = batch_counts_stmt.where(Batch.id == batch_id)
    trial_counts_stmt = select(Trial.state, func.count()).select_from(Trial)
    trial_counts_stmt = apply_trial_monitor_filters(
        trial_counts_stmt,
        target_team=target_team,
        q=q_value if view == "batches" else None,
        trial_q=q_value if view == "trials" else None,
        batch_id=batch_id,
        benchmark_id=benchmark_id,
        agent_name=agent_value,
        model_provider=model_provider,
        model_name=model_value,
        provider_connection_id=provider_connection_id,
        provider_model_id=provider_model_id,
        state=None,
    )
    resource_trials_stmt = _resource_trials_stmt()
    resource_trials_stmt = apply_trial_monitor_filters(
        resource_trials_stmt,
        target_team=target_team,
        q=q_value if view == "batches" else None,
        trial_q=q_value if view == "trials" else None,
        batch_id=batch_id,
        benchmark_id=benchmark_id,
        agent_name=agent_value,
        model_provider=model_provider,
        model_name=model_value,
        provider_connection_id=provider_connection_id,
        provider_model_id=provider_model_id,
        state=None,
    )
    service_filters: dict[str, Any] = {
        "q": q_value if view == "batches" else None,
        "trial_q": q_value if view == "trials" else None,
        "batch_id": batch_id,
        "benchmark_id": benchmark_id,
        "agent_name": agent_value,
        "model_provider": model_provider,
        "model_name": model_value,
        "provider_connection_id": provider_connection_id,
        "provider_model_id": provider_model_id,
        "state": None,
    }
    batch_counts = await _counts(
        session,
        batch_counts_stmt,
        state_col=Batch.state,
        states=_BATCH_STATES,
    )
    trial_counts = await _counts(
        session,
        trial_counts_stmt,
        state_col=Trial.state,
        states=_TRIAL_STATES,
    )

    active_backends = sorted(await get_active_backends(session))
    active_workers = await get_active_worker_count(session)
    resources = await get_resource_pool_summary(
        session,
        freshness_sec=_HEARTBEAT_FRESHNESS_SEC,
        trial_stmt=resource_trials_stmt,
    )
    capacity = await fetch_execution_capacity_status(session)
    profiles = await fetch_execution_resource_profile_status(session)
    service_execution = {
        "targets": _public_execution_capacity(
            capacity,
            profiles,
            admin=is_admin(ctx),
        ),
        "activity": await _service_execution_activity(
            session,
            target_team=target_team,
            filters=service_filters,
        ),
    }
    progress = await progress_summary(session, apply_trial_monitor_filters(
        select(Trial.id), target_team=target_team, **service_filters,
    ))
    queued = trial_counts["queued"]
    protected_pending = trial_counts["protected-pending"]
    claimed = trial_counts["claimed"]
    running = trial_counts["running"]
    return {
        "scope": {
            "view": view,
            "team_id": str(target_team) if target_team else None,
            "q": q_value,
            "benchmark_id": benchmark_id,
            "agent_name": agent_value,
            "model_provider": model_provider,
            "model_name": model_value,
            "provider_connection_id": (
                str(provider_connection_id) if provider_connection_id else None
            ),
            "provider_model_id": provider_model_id,
            "batch_id": str(batch_id) if batch_id else None,
            "state": state,
        },
        "progress": progress,
        "state_counts": {
            "batches": batch_counts,
            "trials": trial_counts,
        },
        "queue": {
            "queued": queued,
            "protected_pending": protected_pending,
            "claimed": claimed,
            "running": running,
            "waiting": queued + protected_pending + claimed,
            "active_workers": active_workers,
            "available_backends": active_backends,
            "has_default_backend": "docker" in active_backends,
            "status": _queue_status(
                queued=queued + protected_pending,
                claimed=claimed,
                running=running,
                active_workers=active_workers,
                native_work=progress["native_active_trials"] > 0,
            ),
        },
        "resources": resources,
        "service_execution": service_execution,
    }


@router.get("/monitor/placement", response_model=wire.MonitorPlacement, response_model_exclude_unset=True)
async def get_monitor_placement(
    response: Response, sc: SessionAndCtx, target_id: str,
    team_id: UUID | None = None, batch_id: UUID | None = None,
    q: str | None = None, benchmark_id: str | None = None,
    view: str = "batches",
    agent_name: str | None = None, model_provider: str | None = None,
    model_name: str | None = None, provider_connection_id: UUID | None = None,
    provider_model_id: str | None = None,
) -> dict[str, Any]:
    session, ctx = sc
    require_scope(ctx, "read:own")
    response.headers["Cache-Control"] = "no-store"
    ids = apply_trial_monitor_filters(
        select(Trial.id), target_team=resolve_monitor_team_filter(ctx, team_id),
        batch_id=batch_id, q=q if view != "trials" else None,
        trial_q=q if view == "trials" else None,
        benchmark_id=benchmark_id, agent_name=agent_name,
        model_provider=model_provider, model_name=model_name,
        provider_connection_id=provider_connection_id, provider_model_id=provider_model_id,
    )
    capacity = await fetch_execution_capacity_status(session)
    rows = capacity.get("targets")
    target = next((row for row in rows if isinstance(row, dict) and row["target_id"] == target_id), None) if isinstance(rows, list) else None
    if target is None:
        raise HTTPException(status_code=404, detail="execution target not found")
    observation = target.get("observation")
    if observation is None:
        return {"available": False, "nodes": [], "pending": [], "is_fresh": False}
    return {
        **await load_placement(session, observation_id=observation["id"], scoped_ids=ids,
                               admin=is_admin(ctx)),
        "observed_at": observation["observed_at"], "is_fresh": observation["is_fresh"],
    }
