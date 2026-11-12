"""Shared Monitor filter helpers.

The Monitor table routes and summary route need the same team, benchmark,
agent, model, batch, and state semantics so URL-backed views do not drift.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from sqlalchemy import String, and_, cast, func, or_, select

from loom.auth import AuthContext
from loom.db.schema import (
    Batch,
    Task,
    TaskImageMaterialization,
    Team,
    Trial,
    TrialTaskImageMaterialization,
    User,
)
from loom_service.auth_guards import is_admin, require_team_or_admin
from loom_service.trial_progress import progress_state_filter


def resolve_monitor_team_filter(
    ctx: AuthContext,
    team_id: UUID | None,
) -> UUID | None:
    """Resolve optional Monitor team filters against caller permissions."""
    if team_id is not None:
        require_team_or_admin(ctx, team_id)
        return team_id
    if not is_admin(ctx):
        return ctx.team_id
    return None


def split_state_filter(state: str | None) -> list[str]:
    if not state:
        return []
    return [value.strip() for value in state.split(",") if value.strip()]


def apply_batch_monitor_filters(
    stmt: Any,
    *,
    target_team: UUID | None,
    q: str | None = None,
    benchmark_id: str | None = None,
    agent_name: str | None = None,
    model_provider: str | None = None,
    model_name: str | None = None,
    provider_connection_id: UUID | None = None,
    provider_model_id: str | None = None,
    state: str | None = None,
) -> Any:
    if target_team is not None:
        stmt = stmt.where(Batch.team_id == target_team)
    if q:
        pattern = f"%{q.strip()}%"
        if q.strip():
            stmt = stmt.where(or_(
                Batch.name.ilike(pattern),
                Batch.description.ilike(pattern),
                cast(Batch.id, String).ilike(pattern),
            ))
    if benchmark_id:
        stmt = stmt.where(or_(
            Batch.task_filter.contains({"benchmark_id": benchmark_id}),
            Batch.task_filter.contains({"benchmark_ids": [benchmark_id]}),
        ))
    if agent_name:
        stmt = stmt.where(or_(
            Batch.trial_config.contains({"agent_name": agent_name}),
            Batch.trial_config.contains({"agent": {"name": agent_name}}),
            Batch.combinations.contains([{"agent_name": agent_name}]),
        ))
    if model_provider:
        stmt = stmt.where(or_(
            Batch.trial_config.contains({
                "agent_model": {"provider": model_provider},
            }),
            Batch.trial_config.contains({
                "agent": {"model": {"provider": model_provider}},
            }),
            Batch.combinations.contains([
                {"agent_model": {"provider": model_provider}},
            ]),
        ))
    if model_name:
        stmt = stmt.where(or_(
            Batch.trial_config.contains({"agent_model": {"name": model_name}}),
            Batch.trial_config.contains({
                "agent": {"model": {"name": model_name}},
            }),
            Batch.combinations.contains([{"agent_model": {"name": model_name}}]),
        ))
    if provider_connection_id is not None:
        stmt = stmt.where(or_(
            Batch.provider_connection_id == provider_connection_id,
            Batch.combinations.contains([
                {"provider_connection_id": str(provider_connection_id)},
            ]),
        ))
    if provider_model_id:
        stmt = stmt.where(or_(
            Batch.provider_model_id == provider_model_id,
            Batch.combinations.contains([
                {"provider_model_id": provider_model_id},
            ]),
        ))
    wanted = split_state_filter(state)
    if wanted:
        stmt = stmt.where(Batch.state.in_(wanted))
    return stmt


def apply_trial_monitor_filters(
    stmt: Any,
    *,
    target_team: UUID | None,
    q: str | None = None,
    trial_q: str | None = None,
    task_id: str | None = None,
    image_id: UUID | None = None,
    batch_id: UUID | None = None,
    benchmark_id: str | None = None,
    agent_name: str | None = None,
    agent: str | None = None,
    model_provider: str | None = None,
    model_name: str | None = None,
    model: str | None = None,
    provider_connection_id: UUID | None = None,
    provider_model_id: str | None = None,
    state: str | None = None,
) -> Any:
    if target_team is not None:
        stmt = stmt.where(Trial.team_id == target_team)
    if q:
        needle = q.strip()
        if needle:
            pattern = f"%{needle}%"
            stmt = stmt.join(Batch, Batch.id == Trial.batch_id).where(or_(
                Batch.name.ilike(pattern),
                Batch.description.ilike(pattern),
                cast(Batch.id, String).ilike(pattern),
            ))
    if trial_q and trial_q.strip():
        # Apply the literal text predicate before cursor/limit and retain tenant scope.
        needle = trial_q.strip().replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        pattern = f"%{needle}%"
        stmt = stmt.where(or_(
            cast(Trial.id, String).ilike(pattern, escape="\\"),
            Trial.task_id.ilike(pattern, escape="\\"),
            Trial.team_id.in_(select(Team.id).where(Team.name.ilike(pattern, escape="\\"))),
            Trial.submitted_by_user_id.in_(select(User.id).where(User.username.ilike(pattern, escape="\\"))),
        ))
    if image_id is not None:
        stmt = stmt.where(Trial.id.in_(select(TrialTaskImageMaterialization.trial_id)
            .join(TaskImageMaterialization,
                  TaskImageMaterialization.id == TrialTaskImageMaterialization.materialization_id)
            .where(TaskImageMaterialization.id == image_id,
                   TaskImageMaterialization.task_id == Trial.task_id,
                   TaskImageMaterialization.cpu_arch == "x86_64").correlate(Trial)))
    if task_id is not None:
        stmt = stmt.where(Trial.task_id == task_id)
    if benchmark_id is not None:
        stmt = stmt.join(Task, Task.id == Trial.task_id).where(
            Task.benchmark_id == benchmark_id,
        )
    if batch_id is not None:
        stmt = stmt.where(Trial.batch_id == batch_id)
    agent_value = agent_name or agent
    if agent_value:
        stmt = stmt.where(or_(
            func.jsonb_extract_path_text(Trial.config, "agent_name") == agent_value,
            func.jsonb_extract_path_text(Trial.config, "agent", "name")
            == agent_value,
        ))
    if model_provider:
        stmt = stmt.where(or_(
            func.jsonb_extract_path_text(
                Trial.config, "agent_model", "provider",
            ) == model_provider,
            func.jsonb_extract_path_text(
                Trial.config, "agent", "model", "provider",
            ) == model_provider,
        ))
    model_value = model_name or model
    if model_value:
        stmt = stmt.where(or_(
            func.jsonb_extract_path_text(
                Trial.config, "agent_model", "name",
            ) == model_value,
            func.jsonb_extract_path_text(
                Trial.config, "agent", "model", "name",
            ) == model_value,
        ))
    if provider_connection_id is not None:
        stmt = stmt.where(Trial.provider_connection_id == provider_connection_id)
    if provider_model_id:
        stmt = stmt.where(Trial.provider_model_id == provider_model_id)
    wanted = split_state_filter(state)
    if wanted:
        stages = [value.removeprefix("stage:") for value in wanted if value.startswith("stage:")]
        cleanups = [value.removeprefix("cleanup:") for value in wanted if value.startswith("cleanup:")]
        outputs = [value.removeprefix("output:") for value in wanted if value.startswith("output:")]
        failures = [
            value.removeprefix("failure:")
            for value in wanted
            if value.startswith("failure:") and value.removeprefix("failure:")
        ]
        states = [value for value in wanted if ":" not in value]
        # Carry authorized non-state predicates into every projection aggregate;
        # an outer tenant filter alone must not scan unrelated historical work.
        scope = stmt.with_only_columns(Trial.id, maintain_column_froms=True).order_by(None)
        predicates: list[Any] = [Trial.state.in_(states)]
        if stages:
            predicates.append(progress_state_filter("stage", stages, scope))
        if cleanups:
            predicates.append(progress_state_filter("cleanup", cleanups, scope))
        if outputs:
            predicates.append(progress_state_filter("output", outputs, scope))
        if failures:
            unknown = "unknown_failure" in failures
            named = [name for name in failures if name != "unknown_failure"]
            reason_match: list[Any] = []
            if named:
                reason_match.append(Trial.failure_reason.in_(named))
            if unknown:
                reason_match.append(Trial.failure_reason.is_(None))
            predicates.append(and_(Trial.state == "failed", or_(*reason_match)))
        stmt = stmt.where(or_(*predicates))
    return stmt
