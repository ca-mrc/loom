from __future__ import annotations

from uuid import UUID, uuid4

import pytest
from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.dialects import postgresql

from loom.auth import AuthContext, role_scopes
from loom.db.schema import Batch, Trial
from loom_service.monitor_filters import (
    apply_batch_monitor_filters,
    apply_trial_monitor_filters,
    resolve_monitor_team_filter,
    split_state_filter,
)


def _ctx(
    *,
    type_: str,
    team_id: UUID | None,
    scopes: list[str] | None = None,
    role: str | None = None,
) -> AuthContext:
    return AuthContext(
        token_hash=b"\x00" * 32,
        type=type_,
        scopes=scopes or [],
        team_id=team_id,
        expires_at=None,
        role=role,
    )


def _sql(statement: object) -> str:
    return str(statement.compile(dialect=postgresql.dialect()))


def test_resolve_monitor_team_filter_scopes_non_admin_to_own_team() -> None:
    team_id = uuid4()
    ctx = _ctx(
        type_="user",
        scopes=role_scopes("viewer"),
        team_id=team_id,
        role="viewer",
    )

    assert resolve_monitor_team_filter(ctx, None) == team_id
    assert resolve_monitor_team_filter(ctx, team_id) == team_id

    with pytest.raises(HTTPException):
        resolve_monitor_team_filter(ctx, uuid4())


def test_resolve_monitor_team_filter_leaves_admin_unscoped_by_default() -> None:
    ctx = _ctx(
        type_="user",
        scopes=role_scopes("platform_admin"),
        team_id=None,
        role="platform_admin",
    )
    target_team = uuid4()

    assert resolve_monitor_team_filter(ctx, None) is None
    assert resolve_monitor_team_filter(ctx, target_team) == target_team


def test_split_state_filter_trims_empty_values() -> None:
    assert split_state_filter(None) == []
    assert split_state_filter(" queued, , done ") == ["queued", "done"]


def test_apply_batch_monitor_filters_adds_structured_predicates() -> None:
    statement = apply_batch_monitor_filters(
        select(Batch),
        target_team=uuid4(),
        q=" canary ",
        benchmark_id="humaneval",
        agent_name="litellm",
        model_provider="openai",
        model_name="gpt-4o-mini",
        provider_connection_id=uuid4(),
        provider_model_id="openai:gpt-4o-mini",
        state="submitted, running",
    )

    sql = _sql(statement)

    assert "batches.team_id" in sql
    assert "batches.name ILIKE" in sql
    assert "batches.description ILIKE" in sql
    assert "CAST(batches.id AS VARCHAR) ILIKE" in sql
    assert "batches.task_filter" in sql
    assert "batches.trial_config" in sql
    assert "batches.combinations" in sql
    assert "batches.provider_connection_id" in sql
    assert "batches.provider_model_id" in sql
    assert "batches.state IN" in sql


def test_apply_batch_monitor_filters_ignores_blank_free_text() -> None:
    statement = apply_batch_monitor_filters(
        select(Batch),
        target_team=None,
        q="   ",
        state="",
    )

    assert " ILIKE " not in _sql(statement)


def test_apply_trial_monitor_filters_adds_structured_predicates() -> None:
    statement = apply_trial_monitor_filters(
        select(Trial),
        target_team=uuid4(),
        task_id="task-1",
        batch_id=uuid4(),
        benchmark_id="mbpp",
        agent_name="codex",
        agent="ignored-legacy-agent",
        model_provider="yibuapi",
        model_name="qwen3.6-35b-a3b",
        model="ignored-legacy-model",
        provider_connection_id=uuid4(),
        provider_model_id="yibu:qwen3.6-35b-a3b",
        state="queued,failed",
    )

    sql = _sql(statement)

    assert "JOIN tasks ON tasks.id = trials.task_id" in sql
    assert "trials.team_id" in sql
    assert "trials.task_id" in sql
    assert "trials.batch_id" in sql
    assert "tasks.benchmark_id" in sql
    assert "jsonb_extract_path_text" in sql
    assert "trials.provider_connection_id" in sql
    assert "trials.provider_model_id" in sql
    assert "trials.state IN" in sql


def test_apply_trial_monitor_filters_failure_reason_prefix() -> None:
    named = _sql(
        apply_trial_monitor_filters(
            select(Trial),
            target_team=None,
            state="failure:timed_out",
        )
    )
    assert "trials.failure_reason IN" in named
    assert "trials.state" in named

    unknown = _sql(
        apply_trial_monitor_filters(
            select(Trial),
            target_team=None,
            state="failure:unknown_failure",
        )
    )
    assert "IS NULL" in unknown


def test_apply_trial_monitor_filters_keeps_legacy_agent_and_model_aliases() -> None:
    statement = apply_trial_monitor_filters(
        select(Trial),
        target_team=None,
        agent="aider",
        model="claude-3-5-sonnet",
        state=" ",
    )

    sql = _sql(statement)

    assert "jsonb_extract_path_text" in sql
    assert "trials.state IN" not in sql
