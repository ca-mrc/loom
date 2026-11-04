"""Role-aware Home overview summary (#455).

This endpoint intentionally returns an already-aggregated first-screen
payload for the SPA. Keeping the readiness math service-side prevents the
Home page from stitching together five different resources and drifting
from the submit/runtime rules.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Response
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.auth import AuthContext
from loom.db.schema import Batch, Benchmark, ProviderConnection, Team, Trial
from loom_control_plane.execution_capacity import fetch_execution_capacity_status
from loom_service import wire_responses as wire
from loom_service.auth_guards import is_admin, require_scope
from loom_service.dependencies import SessionAndCtx
from loom_service.routes.benchmarks import (
    _aliases_by_profile,
    _bench_row,
    _benchmark_rows_with_readiness,
    _visible_benchmarks_statement,
)
from loom_service.worker_backends import (
    get_active_backends,
    get_active_worker_count,
)

router = APIRouter()

_BATCH_STATES = ("submitted", "running", "cancelling", "finished", "cancelled")
_TRIAL_STATES = (
    "queued",
    "protected-pending",
    "claimed",
    "running",
    "succeeded",
    "failed",
    "cancelled",
)


def _target_team_id(ctx: AuthContext) -> UUID | None:
    """Scope overview to the selected browser team when present.

    A singleton admin bearer has no team and therefore gets aggregate
    platform counts. Browser platform admins still carry a selected
    current team, so their Home page stays focused on that team.
    """
    return ctx.team_id


async def _team_context(
    session: AsyncSession,
    ctx: AuthContext,
) -> dict[str, Any]:
    team: Team | None = None
    if ctx.team_id is not None:
        team = await session.get(Team, ctx.team_id)
    return {
        "team_id": str(ctx.team_id) if ctx.team_id else None,
        "team_name": team.name if team else None,
        "role": ctx.role or ("platform_admin" if is_admin(ctx) else None),
        "scopes": list(ctx.scopes),
        "is_platform_admin": is_admin(ctx),
        "submissions_paused": (team.submissions_paused_at is not None if team else False),
    }


def _capabilities(ctx: AuthContext) -> dict[str, bool]:
    admin = is_admin(ctx)
    scopes = set(ctx.scopes)
    has_team = ctx.team_id is not None
    has_submitter = ctx.user_id is not None
    return {
        "can_read": True,
        "can_submit": has_team and has_submitter and (admin or "submit" in scopes),
        "can_manage_providers": (has_team and (admin or "providers:manage" in scopes)),
        "can_manage_team": has_team and (admin or "team:manage" in scopes),
    }


def _scope_statement(stmt: Any, column: Any, team_id: UUID | None) -> Any:
    if team_id is None:
        return stmt
    return stmt.where(column == team_id)


async def _provider_health(
    session: AsyncSession,
    team_id: UUID | None,
) -> dict[str, Any]:
    stmt = (
        select(ProviderConnection)
        .where(
            ProviderConnection.deleted_at.is_(None),
        )
        .order_by(ProviderConnection.display_name)
    )
    rows = list(
        (
            await session.scalars(
                _scope_statement(stmt, ProviderConnection.team_id, team_id),
            )
        ).all()
    )
    ready = sum(1 for row in rows if row.status == "valid")
    needs_attention = sum(1 for row in rows if row.status == "invalid")
    untested = len(rows) - ready - needs_attention
    return {
        "total": len(rows),
        "ready": ready,
        "needs_attention": needs_attention,
        "untested": untested,
        "latest": [
            {
                "id": str(row.id),
                "name": row.display_name,
                "type": row.provider_type,
                "status": row.status,
                "last_validated_at": (
                    row.last_validated_at.isoformat() if row.last_validated_at else None
                ),
                "last_validation_error": row.last_validation_error,
            }
            for row in rows[:5]
        ],
    }


async def _benchmark_readiness(session: AsyncSession) -> dict[str, Any]:
    benchmarks = list(
        (
            await session.scalars(
                _visible_benchmarks_statement(include_historical=False).order_by(
                    Benchmark.display_name,
                ),
            )
        ).all(),
    )
    rows = await _benchmark_rows_with_readiness(session, benchmarks)
    aliases_by_profile = await _aliases_by_profile(
        session,
        [benchmark.id for benchmark, _readiness in rows],
    )
    projected = [
        _bench_row(
            benchmark,
            readiness,
            aliases=aliases_by_profile.get(benchmark.id, []),
            public_selector=aliases_by_profile.get(benchmark.id, [benchmark.id])[0],
        )
        for benchmark, readiness in rows
    ]
    runnable = sum(1 for row in projected if row["readiness_state"] == "runnable")
    blocked = [
        {
            "id": row["id"],
            "display_name": row["display_name"],
            "readiness_state": row["readiness_state"],
            "readiness_label": row["readiness_label"],
            "blocker_reason": row["blocker_reason"],
            "task_count": row["task_count"],
        }
        for row in projected
        if row["readiness_state"] != "runnable"
    ]
    return {
        "total": len(projected),
        "runnable": runnable,
        "needs_attention": len(projected) - runnable,
        "blocked": blocked[:5],
    }


async def _state_counts(
    session: AsyncSession,
    *,
    model: Any,
    state_col: Any,
    team_col: Any,
    states: tuple[str, ...],
    team_id: UUID | None,
) -> dict[str, int]:
    stmt = (
        select(state_col, func.count())
        .select_from(model)
        .group_by(
            state_col,
        )
    )
    stmt = _scope_statement(stmt, team_col, team_id)
    counts = {state: 0 for state in states}
    for state, count in (await session.execute(stmt)).all():
        if state in counts:
            counts[str(state)] = int(count)
    return counts


def _latest_batch_payload(batch: Batch | None) -> dict[str, Any] | None:
    if batch is None:
        return None
    return {
        "id": str(batch.id),
        "name": batch.name,
        "state": batch.state,
        "result_status": batch.result_status,
        "expected_trial_count": batch.expected_trial_count,
        "created_at": batch.created_at.isoformat(),
    }


async def _run_activity(
    session: AsyncSession,
    team_id: UUID | None,
) -> dict[str, Any]:
    latest_stmt = (
        select(Batch)
        .order_by(
            Batch.created_at.desc(),
            Batch.id.desc(),
        )
        .limit(1)
    )
    latest = (
        await session.scalars(
            _scope_statement(latest_stmt, Batch.team_id, team_id),
        )
    ).first()
    return {
        "batches": await _state_counts(
            session,
            model=Batch,
            state_col=Batch.state,
            team_col=Batch.team_id,
            states=_BATCH_STATES,
            team_id=team_id,
        ),
        "trials": await _state_counts(
            session,
            model=Trial,
            state_col=Trial.state,
            team_col=Trial.team_id,
            states=_TRIAL_STATES,
            team_id=team_id,
        ),
        "latest_batch": _latest_batch_payload(latest),
    }


def _execution_health(capacity: dict[str, Any]) -> dict[str, Any]:
    """Describe configured Nebius execution without predicting per-task admission.

    Zero running nodes is normal for autoscaling. Configuration permits intake;
    fresh observations describe runtime conditions, not a second admission gate.
    """
    targets = [
        target for target in capacity["targets"]
        if target["desired_state"] == "active"
        and (target.get("policy") or {}).get("enabled")
    ]
    if not targets:
        status = "not_configured"
    elif any(not (target.get("observation") or {}).get("is_fresh") for target in targets):
        status = "unknown"
    elif any(target["blockers"] for target in targets):
        status = "needs_attention"
    else:
        status = "observed"
    return {"configured_targets": len(targets), "status": status}


def _next_actions(
    *,
    capabilities: dict[str, bool],
    provider_health: dict[str, Any],
    benchmark_readiness: dict[str, Any],
    execution_health: dict[str, Any],
) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    if (
        capabilities["can_submit"]
        and provider_health["ready"] > 0
        and benchmark_readiness["runnable"] > 0
        and execution_health["configured_targets"] > 0
    ):
        actions.append(
            {
                "id": "create_batch",
                "label": "Create a batch",
                "to": "/batches/new",
                "kind": "user",
                "priority": 10,
            }
        )
    if capabilities["can_manage_providers"] and provider_health["total"] == 0:
        actions.append(
            {
                "id": "create_provider",
                "label": "Create provider connection",
                "to": "/providers/new",
                "kind": "user",
                "priority": 20,
            }
        )
    if capabilities["can_manage_providers"] and provider_health["needs_attention"] > 0:
        actions.append(
            {
                "id": "repair_provider",
                "label": "Repair provider connection",
                "to": "/providers",
                "kind": "user",
                "priority": 30,
            }
        )
    if benchmark_readiness["runnable"] == 0:
        actions.append(
            {
                "id": "publish_benchmarks",
                "label": "Publish benchmark tasks",
                "to": "/benchmarks",
                "kind": "operator",
                "priority": 40,
            }
        )
    if execution_health["status"] == "not_configured":
        actions.append(
            {
                "id": "configure_execution",
                "label": "Configure Nebius execution",
                "to": "/monitor",
                "kind": "operator",
                "priority": 50,
            }
        )
    elif execution_health["status"] != "observed":
        actions.append({
            "id": "inspect_execution", "label": "Check Nebius capacity in Monitor",
            "to": "/monitor", "kind": "operator", "priority": 50,
        })
    return sorted(actions, key=lambda item: item["priority"])


def _status(
    *,
    team_context: dict[str, Any],
    capabilities: dict[str, bool],
    provider_health: dict[str, Any],
    benchmark_readiness: dict[str, Any],
    execution_health: dict[str, Any],
) -> str:
    if team_context["submissions_paused"]:
        return "blocked"
    if (
        capabilities["can_submit"]
        and provider_health["ready"] > 0
        and benchmark_readiness["runnable"] > 0
        and execution_health["configured_targets"] > 0
    ):
        return "ready"
    return "needs_setup"


def _summary(status: str) -> str:
    if status == "ready":
        return "This team can submit evaluations. Execution may wait for capacity or node startup."
    if status == "blocked":
        return "Team submissions are currently paused."
    return "Finish the setup items below before launching evaluations."


@router.get("/overview", response_model=wire.OverviewSummary, response_model_exclude_unset=True)
async def get_overview(response: Response, sc: SessionAndCtx) -> dict[str, Any]:
    # Execution observations can change within seconds. Keep
    # browsers and intermediary caches from replaying an old readiness result.
    response.headers["Cache-Control"] = "no-store"
    session, ctx = sc
    require_scope(ctx, "read:own")
    team_id = _target_team_id(ctx)
    team_context = await _team_context(session, ctx)
    capabilities = _capabilities(ctx)
    provider_health = await _provider_health(session, team_id)
    benchmark_readiness = await _benchmark_readiness(session)
    active_backends = sorted(await get_active_backends(session))
    worker_health = {
        "active": await get_active_worker_count(session),
        "available_backends": active_backends,
        "has_default_backend": "docker" in active_backends,
    }
    execution_health = _execution_health(await fetch_execution_capacity_status(session))
    run_activity = await _run_activity(session, team_id)
    status = _status(
        team_context=team_context,
        capabilities=capabilities,
        provider_health=provider_health,
        benchmark_readiness=benchmark_readiness,
        execution_health=execution_health,
    )
    return {
        "status": status,
        "summary": _summary(status),
        "team_context": team_context,
        "capabilities": capabilities,
        "provider_health": provider_health,
        "benchmark_readiness": benchmark_readiness,
        "worker_health": worker_health,  # Legacy API compatibility; not launch readiness.
        "execution_health": execution_health,
        "run_activity": run_activity,
        "next_actions": _next_actions(
            capabilities=capabilities,
            provider_health=provider_health,
            benchmark_readiness=benchmark_readiness,
            execution_health=execution_health,
        ),
    }
