"""Batch detail reads only a few fields of `trials.result` / `trials.config`
(#2771). The slim projection must give the same response as the full columns
for every shape those readers handle."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import create_engine, insert
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Trial
from loom_service.routes import batches as batches_routes
from tests.integration.test_service_batches_crud import camp_setup  # noqa: F401

_RUNTIME = {
    "schema_version": "loom.execution-runtime-result.v1",
    "execution_role": "attempt",
    "started_at": "2026-11-01T10:00:00+00:00",
    "finished_at": "2026-11-01T10:05:00+00:00",
    # Bulk the readers never look at.
    "phases": [{"role": "agent", "stdout": {"path": "01-agent.stdout"}}] * 20,
    "outputs": [{"kind": "task_artifact", "sha256": "sha256:" + "0" * 64}] * 20,
}
_HIGH_DEMAND = (
    "codex exited rc=1: We're currently experiencing high demand, "
    "which may cause temporary errors"
)

# (state, failure_reason, started_at column, result, config)
_CASES: list[tuple[str, str | None, datetime | None, Any, Any]] = [
    ("running", None, None, None, {}),
    ("cancelled", None, None, None, {"agent_model": {"name": "m", "provider": "p"}}),
    ("succeeded", None, None, {"aggregate_reward": 1.0, "runtime_result": _RUNTIME}, {}),
    ("succeeded", None, None, {"reward": {"a": 1, "b": 0, "c": "x"}}, {}),
    ("succeeded", None, None, {"reward": "0.5", "verifier_output": "x" * 2000}, {}),
    ("succeeded", None, None, {"aggregate_reward": None, "reward": 0.0}, {}),
    ("succeeded", None, None, {}, {}),
    ("succeeded", None, None, {"unrelated": [1, 2, 3]}, {"agent": "terminus-2"}),
    (
        "failed", "agent_error", None,
        {"steps": [
            {"error": {"phase": "agent", "message": _HIGH_DEMAND, "detail": "y" * 500}},
            "not-a-step",
            {"error": "not-an-object"},
            {"name": "main", "trajectory": ["z"] * 50},
            {"error": {"phase": "verifier", "message": "verifier failed"}},
        ]},
        {"agent_model": {"name": "gpt-4.1-mini", "provider": "openai"}},
    ),
    (
        "failed", "agent_error", None,
        {"steps": {"error": {"phase": "agent", "message": _HIGH_DEMAND}}},
        {"agent": {"model": {"name": "legacy", "provider": "openai"}}},
    ),
    (
        "failed", "agent_error", None,
        {"steps": "not-a-list"},
        {"agent_model": {}, "agent": {"model": {"name": "legacy"}}},
    ),
    (
        "failed", "task_image_build_failed", None,
        {"task_image_preparation_failure": {"reason": "build_bound_node_unavailable", "log": "b" * 300}},
        {"agent_model": None, "agent": {"name": "codex"}},
    ),
    (
        "failed", "task_image_build_failed", None,
        {"task_image_preparation_failure": "not-an-object"}, {},
    ),
    (
        "failed", "verifier_error", None,
        {"runtime_result": {**_RUNTIME, "execution_role": "verifier"}}, {},
    ),
    ("failed", "verifier_error", None, {"runtime_result": "not-an-object"}, {}),
    (
        "failed", "verifier_error", None,
        {"runtime_result": {**_RUNTIME, "finished_at": "2026-11-01T09:00:00+00:00"}}, {},
    ),
    (
        "succeeded", None, datetime(2026, 11, 2, tzinfo=UTC),
        {"aggregate_reward": 0.25, "runtime_result": _RUNTIME}, {},
    ),
]


async def _create_batch(app: FastAPI, raw: str) -> UUID:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://svc",
    ) as ac:
        created = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "slim-projection",
                "purpose": "evaluation",
                "task_filter": {"license": "MIT"},
                "trial_config": {},
            },
        )
    assert created.status_code in {200, 201}, created.text
    return UUID(created.json()["batch_id"])


def _seed(postgres_url: str, team_id: UUID, batch_id: UUID) -> None:
    engine = create_engine(postgres_url)
    with sessionmaker(engine)() as s:
        for i, (state, reason, started_at, result, config) in enumerate(_CASES):
            s.execute(
                insert(Trial).values(
                    id=uuid4(),
                    task_id=f"local/mit-{i % 3}",
                    sample_idx=i,
                    team_id=team_id,
                    state=state,
                    failure_reason=reason,
                    failure_message=None if reason is None else f"{reason} happened",
                    started_at=started_at,
                    config=config,
                    requires_caps={},
                    submitted_at=datetime.now(UTC),
                    batch_id=batch_id,
                    result=result,
                )
            )
        s.commit()
    engine.dispose()


async def test_slim_projection_gives_the_same_batch_detail(
    camp_setup: tuple[FastAPI, str, UUID],  # noqa: F811
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, raw, team_id = camp_setup
    headers = {"Authorization": f"Bearer {raw}"}
    batch_id = await _create_batch(app, raw)
    _seed(postgres_url, team_id, batch_id)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://svc",
    ) as ac:
        slim = await ac.get(f"/api/v1/batches/{batch_id}", headers=headers)

        projections = batches_routes._trial_projections_for_batch_ids
        calls: list[bool] = []

        async def full_projection(
            session: Any, batch_ids: Any, *, slim: bool = False,
        ) -> Any:
            calls.append(slim)
            return await projections(session, batch_ids, slim=False)

        monkeypatch.setattr(
            batches_routes, "_trial_projections_for_batch_ids", full_projection,
        )
        full = await ac.get(f"/api/v1/batches/{batch_id}", headers=headers)

    assert slim.status_code == full.status_code == 200
    assert calls == [True]  # the detail read asks for the slim projection
    assert slim.json() == full.json()
    # Guard against a vacuous comparison: the seeded shapes reach the readers.
    body = slim.json()
    assert body["no_call_trial_count"] > 0
    assert body["aggregate_reward"] is not None


async def test_slim_projection_matches_full_readers_per_trial(
    camp_setup: tuple[FastAPI, str, UUID],  # noqa: F811
    postgres_url: str,
) -> None:
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from loom_service.failure_taxonomy import aggregate_reward, classify_trial_outcome
    from loom_service.usage_accounting import no_call_evidence_reason, trial_requires_llm

    app, raw, team_id = camp_setup
    batch_id = await _create_batch(app, raw)
    _seed(postgres_url, team_id, batch_id)

    engine = create_async_engine(postgres_url)
    async with async_sessionmaker(engine)() as session:
        full = await batches_routes._trial_projections_for_batch_ids(session, [batch_id])
        slim = await batches_routes._trial_projections_for_batch_ids(
            session, [batch_id], slim=True,
        )
    await engine.dispose()

    def readers(trial: Any) -> tuple[Any, ...]:
        return (
            trial.started_at,
            aggregate_reward(trial.result),
            batches_routes._rollup_from_result(trial.result),
            classify_trial_outcome(trial),
            trial_requires_llm(trial),
            no_call_evidence_reason(trial),
            trial.result is None,
        )

    full_by_id = {trial.id: trial for trial in full}
    assert len(slim) == len(full) >= len(_CASES)
    for trial in slim:
        assert readers(trial) == readers(full_by_id[trial.id]), trial.result
    # The bulk is gone.
    heavy = next(t for t in slim if isinstance(t.result, dict) and "runtime_result" in t.result)
    assert "phases" not in heavy.result["runtime_result"]
