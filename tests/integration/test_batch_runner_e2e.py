"""run_once fans a batch into per-task trial submissions, with
idempotency_key carrying the batch+task identity (Plan 19 Task 5).

The Control Plane is mocked via httpx.MockTransport — the handler
mirrors the real CP's INSERT into the trials table so the test
exercises the round trip: service runner → /trials → DB row →
service re-reads via Trial.batch_id.
"""

from __future__ import annotations

import hashlib
import json as _json
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import create_engine, delete, insert, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.db.schema import (
    Batch,
    Benchmark,
    LlmCall,
    ProviderConnection,
    Task,
    Team,
    TeamQuota,
    Token,
    Trial,
    Worker,
)
from loom.models.task import TaskConfig, normalize_steps
from loom_control_plane.scheduler.claim import claim_one
from loom_control_plane.scheduler.requires_caps import derive_requires_caps
from loom_service.batch_runner import (
    _idempotency_key,
    next_batch_state,
    run_once,
)


def _valid_task_config(
    task_id: str,
    *,
    cpu_arch: str = "x86_64",
) -> dict[str, object]:
    return {
        "schema_version": "1",
        "task": {"id": task_id, "name": task_id},
        "environment": {
            "os": "linux",
            "cpu_arch": cpu_arch,
            "docker_image": "alpine",
        },
        "agent": {"name": "oracle"},
        "verifier": {"name": "pytest"},
        "steps": [{"name": "main"}],
    }


@pytest.fixture
async def runner_setup(
    postgres_url: str,
) -> AsyncIterator[tuple[
    async_sessionmaker, httpx.AsyncClient, UUID, list[str], list[dict],
]]:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    team_id = uuid4()
    task_ids = [f"local/runner-{i}" for i in range(5)]
    raw = f"loom_team_{uuid4().hex}"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
        s.execute(insert(TeamQuota).values(team_id=team_id))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw.encode()).digest(),
            type="team", scopes=["submit"], team_id=team_id,
            issued_at=datetime.now(UTC),
        ))
        s.execute(insert(Benchmark).values(
            id="runner-benchmark",
            display_name="Runner Benchmark",
            # Runner/snapshot tests start with an already admitted catalog.
            execution_state="runnable",
            upstream_kind="fixture",
            upstream_locator="local",
            upstream_revision="",
            license_spdx="MIT",
            license_url="https://example/license",
            splits=["test"],
        ))
        for tid in task_ids:
            s.execute(insert(Task).values(
                id=tid, checksum="x" * 64,
                config=_valid_task_config(tid),
                source="local", license="MIT",
                benchmark_id="runner-benchmark",
            ))
        s.commit()
    sync_engine.dispose()

    # Mock CP: handler that mirrors the real INSERT (with idempotency
    # ON CONFLICT semantics — checks for existing key, returns its id).
    captured: list[dict] = []

    def cp_handler(req: httpx.Request) -> httpx.Response:
        if req.method == "POST" and req.url.path.endswith("/cancel"):
            trial_id = UUID(req.url.path.split("/")[-2])
            captured.append({"cancel_trial_id": str(trial_id)})
            e = create_engine(postgres_url)
            local = sessionmaker(e)
            try:
                with local() as s:
                    trial = s.get(Trial, trial_id)
                    if trial is None:
                        return httpx.Response(404)
                    if trial.state in {"succeeded", "failed", "cancelled"}:
                        return httpx.Response(409)
                    trial.state = "cancelled"
                    trial.cancellation_requested_at = datetime.now(UTC)
                    trial.finished_at = datetime.now(UTC)
                    s.commit()
                return httpx.Response(200, json={"state": "cancelled"})
            finally:
                e.dispose()
        if req.url.path != "/trials" or req.method != "POST":
            return httpx.Response(404)
        body = _json.loads(req.content.decode())
        captured.append(body)
        e = create_engine(postgres_url)
        local = sessionmaker(e)
        try:
            with local() as s:
                idem = body.get("idempotency_key")
                if idem is not None:
                    existing = s.execute(
                        select(Trial).where(
                            Trial.idempotency_key == idem,
                        ),
                    ).scalar_one_or_none()
                    if existing is not None:
                        return httpx.Response(
                            201,
                            json={
                                "trial_id": str(existing.id),
                                "state": existing.state,
                                "submitted_at": (
                                    existing.submitted_at.isoformat()
                                ),
                            },
                        )
                task_row = s.execute(
                    select(Task).where(Task.id == body["task_id"]),
                ).scalar_one()
                requires_caps = derive_requires_caps(
                    normalize_steps(TaskConfig.model_validate(task_row.config)),
                ).model_dump(mode="json")
                required_pool = body.get("required_worker_pool")
                if required_pool:
                    requires_caps["worker_pool"] = str(required_pool).strip()
                new_id = uuid4()
                s.execute(insert(Trial).values(
                    id=new_id,
                    task_id=body["task_id"],
                    team_id=team_id,
                    state="queued",
                    config=body["config"],
                    requires_caps=requires_caps,
                    submitted_at=datetime.now(UTC),
                    batch_id=(
                        UUID(body["batch_id"])
                        if body.get("batch_id") else None
                    ),
                    idempotency_key=idem,
                    sample_idx=int(body.get("sample_idx") or 0),
                    combination_idx=int(body.get("combination_idx") or 0),
                    provider_connection_id=(
                        UUID(body["provider_connection_id"])
                        if body.get("provider_connection_id") else None
                    ),
                    provider_model_id=body.get("provider_model_id"),
                ))
                s.commit()
            return httpx.Response(
                201,
                json={
                    "trial_id": str(new_id),
                    "state": "queued",
                    "submitted_at": datetime.now(UTC).isoformat(),
                },
            )
        finally:
            e.dispose()

    http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(cp_handler),
        base_url="http://cp",
    )

    try:
        yield session_factory, http_client, team_id, task_ids, captured
    finally:
        await http_client.aclose()
        await engine.dispose()
        sync_engine = create_engine(postgres_url)
        sl = sessionmaker(sync_engine)
        with sl() as s:
            s.execute(delete(Trial))
            s.execute(delete(Worker))
            s.execute(delete(Token))
            s.execute(delete(Batch))
            s.execute(delete(ProviderConnection))
            s.execute(delete(Task))
            s.execute(delete(Benchmark))
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.commit()
        sync_engine.dispose()


def _insert_pool_worker(
    postgres_url: str, *, pool_name: str, cpu_arch: str,
) -> None:
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Worker).values(
            id=uuid4(), hostname="local-capability-worker", version="test",
            pool_name=pool_name,
            capabilities=[{"backend": "docker", "cpu_arch": cpu_arch}],
            registered_at=datetime.now(UTC), last_seen_at=datetime.now(UTC),
            status="active",
        ))
        s.commit()
    sync_engine.dispose()


async def test_runner_fans_out_5_trials(
    runner_setup: tuple[
        async_sessionmaker, httpx.AsyncClient, UUID, list[str], list[dict],
    ],
    postgres_url: str,
) -> None:
    session_factory, http_client, team_id, task_ids, _captured = runner_setup
    async with session_factory() as s:
        c = Batch(
            team_id=team_id, name="C",
            task_filter={"license": "MIT"},
            trial_config={"agent": {"name": "fake"}},
            state="submitted",
            created_by_token_prefix="abcdef12",
            expected_trial_count=5,
        )
        s.add(c)
        await s.commit()
        await s.refresh(c)
        cid = c.id

    await run_once(
        session_factory=session_factory,
        http_client=http_client,
        batch_size=10,
        submit_rate_per_sec=100,
    )

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        trials = s.execute(
            select(Trial)
            .where(Trial.batch_id == cid)
            .order_by(Trial.task_id.asc()),
        ).scalars().all()
        batch_row = s.execute(
            select(Batch).where(Batch.id == cid),
        ).scalar_one()
    sync_engine.dispose()

    assert len(trials) == 5
    assert {t.task_id for t in trials} == set(task_ids)
    # Each trial carries the deterministic idempotency_key (sample 0).
    assert all(
        t.idempotency_key == _idempotency_key(cid, t.task_id, 0)
        for t in trials
    )
    assert all(t.sample_idx == 0 for t in trials)
    # State transitioned: submitted → running (trials in queued state).
    assert batch_row.state == "running"


async def test_runner_fans_out_required_worker_pool_coverage_units(
    runner_setup: tuple[
        async_sessionmaker, httpx.AsyncClient, UUID, list[str], list[dict],
    ],
    postgres_url: str,
) -> None:
    session_factory, http_client, team_id, task_ids, captured = runner_setup
    async with session_factory() as s:
        c = Batch(
            team_id=team_id,
            name="coverage",
            task_filter={
                "benchmark_ids": ["runner-benchmark"],
                "subset_kind": "first_n",
                "n": 1,
            },
            trial_config={},
            state="submitted",
            created_by_token_prefix="abcdef12",
            expected_trial_count=3,
            required_worker_pools=["local_x86", "k8s-worker"],
        )
        s.add(c)
        await s.commit()
        await s.refresh(c)
        cid = c.id

    await run_once(
        session_factory=session_factory,
        http_client=http_client,
        batch_size=10,
        submit_rate_per_sec=100,
    )

    assert [body.get("required_worker_pool") for body in captured] == [
        None,
        "local_x86",
        "k8s-worker",
    ]
    assert [body["idempotency_key"] for body in captured] == [
        _idempotency_key(cid, task_ids[0], 0),
        _idempotency_key(
            cid,
            task_ids[0],
            1,
            required_worker_pool="local_x86",
        ),
        _idempotency_key(
            cid,
            task_ids[0],
            2,
            required_worker_pool="k8s-worker",
        ),
    ]

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        trials = s.execute(
            select(Trial)
            .where(Trial.batch_id == cid)
            .order_by(Trial.sample_idx.asc()),
        ).scalars().all()
    sync_engine.dispose()

    assert len(trials) == 3
    assert [t.sample_idx for t in trials] == [0, 1, 2]


async def test_runner_rejects_required_pool_coverage_when_tasks_mismatch_pool_arch(
    runner_setup: tuple[
        async_sessionmaker, httpx.AsyncClient, UUID, list[str], list[dict],
    ],
    postgres_url: str,
) -> None:
    session_factory, http_client, team_id, task_ids, captured = runner_setup
    _insert_pool_worker(postgres_url, pool_name="local_arm", cpu_arch="arm64")
    async with session_factory() as s:
        c = Batch(
            team_id=team_id,
            name="x86-only coverage",
            task_filter={
                "task_ids": [task_ids[0]],
                "subset_kind": "explicit",
            },
            trial_config={},
            state="submitted",
            created_by_token_prefix="abcdef12",
            expected_trial_count=2,
            required_worker_pools=["local_arm"],
        )
        s.add(c)
        await s.commit()
        await s.refresh(c)
        cid = c.id

    await run_once(
        session_factory=session_factory,
        http_client=http_client,
        batch_size=10,
        submit_rate_per_sec=100,
    )

    assert [body.get("required_worker_pool") for body in captured] == [None]

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        batch_row = s.execute(
            select(Batch).where(Batch.id == cid),
        ).scalar_one()
        trials = s.execute(
            select(Trial).where(Trial.batch_id == cid),
        ).scalars().all()
    sync_engine.dispose()

    assert len(trials) == 1
    assert batch_row.expected_trial_count == 1
    assert batch_row.result_status == "partial_failed"
    assert batch_row.fanout_errors
    error = batch_row.fanout_errors[0]
    assert error["reason"] == "required_worker_pool_incompatible"
    assert error["required_worker_pool"] == "local_arm"
    assert error["pool_cpu_arches"] == ["arm64"]
    assert error["task_cpu_arches"] == {"x86_64": [task_ids[0]]}


async def test_runner_selects_claimable_task_for_x86_required_pool_coverage(
    runner_setup: tuple[
        async_sessionmaker, httpx.AsyncClient, UUID, list[str], list[dict],
    ],
    postgres_url: str,
) -> None:
    session_factory, http_client, team_id, task_ids, captured = runner_setup
    _insert_pool_worker(postgres_url, pool_name="x86-test-pool", cpu_arch="x86_64")
    async with session_factory() as s:
        c = Batch(
            team_id=team_id,
            name="x86 coverage",
            task_filter={
                "task_ids": [task_ids[0], task_ids[1]],
                "subset_kind": "explicit",
            },
            trial_config={},
            state="submitted",
            created_by_token_prefix="abcdef12",
            expected_trial_count=3,
            required_worker_pools=["x86-test-pool"],
        )
        s.add(c)
        await s.commit()
        await s.refresh(c)
        cid = c.id

    await run_once(
        session_factory=session_factory,
        http_client=http_client,
        batch_size=10,
        submit_rate_per_sec=100,
    )

    coverage_key = _idempotency_key(
        cid,
        task_ids[0],
        1,
        required_worker_pool="x86-test-pool",
    )
    assert captured[-1]["task_id"] == task_ids[0]
    assert captured[-1]["required_worker_pool"] == "x86-test-pool"
    assert captured[-1]["idempotency_key"] == coverage_key

    x86_worker = uuid4()
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Worker).values(
            id=x86_worker,
            hostname="x86-test-worker",
            version="v",
            pool_name="x86-test-pool",
            capabilities=[{
                "os": "linux",
                "cpu_arch": "x86_64",
                "gpu_vendor": "none",
                "network_policies": ["public"],
                "dynamic_network_policy": True,
                "mounted_fs": True,
                "resource_modes": ["auto"],
            }],
            registered_at=datetime.now(UTC),
            last_seen_at=datetime.now(UTC),
            status="active",
        ))
        coverage_trial = s.execute(
            select(Trial).where(Trial.idempotency_key == coverage_key),
        ).scalar_one()
        assert coverage_trial.requires_caps == {
            "backend": "docker",
            "os": "linux",
            "cpu_arch": "x86_64",
            "gpu_vendor": "none",
            "network_policies": ["public"],
            "terminus2_model_switch": False,
            "worker_pool": "x86-test-pool",
        }
        coverage_trial_id = coverage_trial.id
        s.commit()
    sync_engine.dispose()

    claimed: list[UUID] = []
    for _ in range(3):
        async with session_factory() as session:
            row = await claim_one(
                session,
                worker_id=x86_worker,
                worker_os=["linux"],
                worker_gpu_vendors=["none"],
                worker_network_policies=["public"],
                worker_cpu_arches=["x86_64"],
            )
            await session.commit()
            assert row is not None
            claimed.append(row["id"])

    assert coverage_trial_id in claimed


async def test_runner_is_idempotent(
    runner_setup: tuple[
        async_sessionmaker, httpx.AsyncClient, UUID, list[str], list[dict],
    ],
    postgres_url: str,
) -> None:
    """Running the runner twice produces exactly 5 trials, not 10."""
    session_factory, http_client, team_id, _task_ids, _captured = runner_setup
    async with session_factory() as s:
        c = Batch(
            team_id=team_id, name="C",
            task_filter={"license": "MIT"},
            trial_config={},
            state="submitted",
            created_by_token_prefix="abcdef12",
            expected_trial_count=5,
        )
        s.add(c)
        await s.commit()
        await s.refresh(c)
        cid = c.id

    for _ in range(2):
        await run_once(
            session_factory=session_factory,
            http_client=http_client,
            batch_size=10,
            submit_rate_per_sec=100,
        )

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        trials = s.execute(
            select(Trial)
            .where(Trial.batch_id == cid)
            .order_by(Trial.task_id.asc()),
        ).scalars().all()
    sync_engine.dispose()
    assert len(trials) == 5


async def test_runner_hard_cancels_batch_when_live_usage_exceeds_budget(
    runner_setup: tuple[
        async_sessionmaker, httpx.AsyncClient, UUID, list[str], list[dict],
    ],
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_factory, http_client, team_id, task_ids, captured = runner_setup
    batch_id = uuid4()
    trial_id = uuid4()
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            insert(Batch).values(
                id=batch_id,
                team_id=team_id,
                name="budget stop",
                task_filter={"license": "MIT"},
                trial_config={},
                state="running",
                created_by_token_prefix="abcdef12",
                expected_trial_count=5,
                budget_usd=0.01,
                budget_policy="hard",
            )
        )
        s.execute(
            insert(Trial).values(
                id=trial_id,
                task_id=task_ids[0],
                team_id=team_id,
                state="running",
                config={},
                requires_caps={},
                submitted_at=datetime.now(UTC),
                batch_id=batch_id,
                sample_idx=0,
                combination_idx=0,
            )
        )
        s.execute(
            insert(LlmCall).values(
                id=uuid4(),
                team_id=team_id,
                trial_id=trial_id,
                step_id="main",
                model="glm-5.1-thinking",
                dialect="openai",
                input_tokens=1000,
                output_tokens=100,
                provider_extras={},
                cost_usd=0.02,
                rate_card_hash="facade:operator-supplied",
            )
        )
        s.commit()
    sync_engine.dispose()

    original_request = http_client.request
    unavailable = True

    async def inspect_budget_fence(*args, **kwargs):  # type: ignore[no-untyped-def]
        nonlocal unavailable
        async with session_factory() as session:
            batch = (await session.execute(
                select(Batch).where(Batch.id == batch_id).with_for_update(nowait=True),
            )).scalar_one()
            assert batch.state == "cancelled"
        if unavailable:
            unavailable = False
            raise httpx.ConnectError("CP temporarily unavailable")
        return await original_request(*args, **kwargs)

    monkeypatch.setattr(http_client, "request", inspect_budget_fence)
    await run_once(
        session_factory=session_factory,
        http_client=http_client,
        batch_size=10,
        submit_rate_per_sec=100,
    )
    assert captured == []
    await run_once(
        session_factory=session_factory,
        http_client=http_client,
        batch_size=10,
        submit_rate_per_sec=100,
    )

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        batch_row = s.execute(
            select(Batch).where(Batch.id == batch_id),
        ).scalar_one()
        trial_row = s.execute(
            select(Trial).where(Trial.id == trial_id),
        ).scalar_one()
    sync_engine.dispose()

    assert captured == [{"cancel_trial_id": str(trial_id)}]
    assert batch_row.state == "cancelled"
    assert batch_row.result_status == "cancelled"
    assert trial_row.state == "cancelled"
    assert trial_row.cancellation_requested_at is not None
    assert batch_row.budget_diagnostics
    assert len(batch_row.budget_diagnostics) == 1
    diagnostic = batch_row.budget_diagnostics[-1]
    assert diagnostic["reason"] == "budget_hard_limit_exceeded"
    assert diagnostic["estimated_cost_usd"] == pytest.approx(0.02)
    assert diagnostic["budget_usd"] == pytest.approx(0.01)


async def test_runner_uses_rerun_targets_instead_of_original_filter(
    runner_setup: tuple[
        async_sessionmaker, httpx.AsyncClient, UUID, list[str], list[dict],
    ],
    postgres_url: str,
) -> None:
    session_factory, http_client, team_id, task_ids, captured = runner_setup
    targets = [
        {
            "task_id": task_ids[1],
            "sample_idx": 2,
            "combination_idx": 0,
            "original_trial_id": str(uuid4()),
            "failure_reason": "gateway_error",
        },
        {
            "task_id": task_ids[3],
            "sample_idx": 1,
            "combination_idx": 0,
            "original_trial_id": str(uuid4()),
            "failure_reason": "gateway_error",
        },
    ]
    async with session_factory() as s:
        c = Batch(
            team_id=team_id,
            name="rerun",
            task_filter={"license": "MIT"},
            trial_config={"agent": {"name": "fake"}},
            state="submitted",
            created_by_token_prefix="abcdef12",
            expected_trial_count=2,
            n_per_task=99,
            rerun_targets=targets,
        )
        s.add(c)
        await s.commit()
        await s.refresh(c)
        cid = c.id

    await run_once(
        session_factory=session_factory,
        http_client=http_client,
        batch_size=10,
        submit_rate_per_sec=100,
    )

    assert [body["task_id"] for body in captured] == [task_ids[1], task_ids[3]]
    assert [body["sample_idx"] for body in captured] == [2, 1]
    assert [body["combination_idx"] for body in captured] == [0, 0]
    assert [body["idempotency_key"] for body in captured] == [
        _idempotency_key(cid, task_ids[1], 2, combination_idx=0),
        _idempotency_key(cid, task_ids[3], 1, combination_idx=0),
    ]

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        trials = s.execute(
            select(Trial)
            .where(Trial.batch_id == cid)
            .order_by(Trial.task_id.asc()),
        ).scalars().all()
    sync_engine.dispose()
    assert [(t.task_id, t.sample_idx, t.combination_idx) for t in trials] == [
        (task_ids[1], 2, 0),
        (task_ids[3], 1, 0),
    ]


async def test_runner_advances_to_finished_when_all_terminal(
    runner_setup: tuple[
        async_sessionmaker, httpx.AsyncClient, UUID, list[str], list[dict],
    ],
    postgres_url: str,
) -> None:
    """After the runner submits, externally mark every trial succeeded
    and run again — the runner should transition the batch to
    finished."""
    session_factory, http_client, team_id, _task_ids, _captured = runner_setup
    async with session_factory() as s:
        c = Batch(
            team_id=team_id, name="C",
            task_filter={"license": "MIT"},
            trial_config={},
            state="submitted",
            created_by_token_prefix="abcdef12",
            expected_trial_count=5,
        )
        s.add(c)
        await s.commit()
        await s.refresh(c)
        cid = c.id

    await run_once(
        session_factory=session_factory, http_client=http_client,
        batch_size=10, submit_rate_per_sec=100,
    )

    # Mark every trial succeeded.
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            update(Trial)
            .where(Trial.batch_id == cid)
            .values(
                state="succeeded",
                result={"aggregate_reward": 1.0},
                finished_at=datetime.now(UTC),
            ),
        )
        s.commit()
    sync_engine.dispose()

    # Tick once more — no new trials to submit; state should advance.
    await run_once(
        session_factory=session_factory, http_client=http_client,
        batch_size=10, submit_rate_per_sec=100,
    )

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        batch_row = s.execute(
            select(Batch).where(Batch.id == cid),
        ).scalar_one()
    sync_engine.dispose()
    assert batch_row.state == "finished"
    assert batch_row.result_status == "succeeded"
    assert batch_row.finished_at is not None


async def test_runner_fans_out_n_samples_per_task(
    runner_setup: tuple[
        async_sessionmaker, httpx.AsyncClient, UUID, list[str], list[dict],
    ],
    postgres_url: str,
) -> None:
    """Plan 23: batch.n_per_task=3 produces 3 trials per matched
    task, each with a distinct sample_idx 0..2, each carrying the
    sample-aware idempotency key. Re-running the runner is still
    idempotent — no duplicates."""
    session_factory, http_client, team_id, task_ids, _captured = runner_setup
    async with session_factory() as s:
        c = Batch(
            team_id=team_id, name="C",
            task_filter={"license": "MIT"},
            trial_config={},
            state="submitted",
            created_by_token_prefix="abcdef12",
            expected_trial_count=15,
            n_per_task=3,
        )
        s.add(c)
        await s.commit()
        await s.refresh(c)
        cid = c.id

    for _ in range(2):  # 2nd tick must NOT duplicate
        await run_once(
            session_factory=session_factory, http_client=http_client,
            batch_size=10, submit_rate_per_sec=100,
        )

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        trials = s.execute(
            select(Trial).where(Trial.batch_id == cid),
        ).scalars().all()
    sync_engine.dispose()

    assert len(trials) == 15
    # Every (task_id, sample_idx) pair shows up exactly once.
    pairs = {(t.task_id, t.sample_idx) for t in trials}
    assert pairs == {(tid, s_idx) for tid in task_ids for s_idx in range(3)}
    # Idempotency keys include sample_idx and are unique.
    keys = {t.idempotency_key for t in trials}
    assert len(keys) == 15
    assert all(
        t.idempotency_key
        == _idempotency_key(cid, t.task_id, t.sample_idx)
        for t in trials
    )


async def test_runner_honors_benchmark_ids_first_n_filter(
    runner_setup: tuple[
        async_sessionmaker, httpx.AsyncClient, UUID, list[str], list[dict],
    ],
    postgres_url: str,
) -> None:
    session_factory, http_client, team_id, task_ids, _captured = runner_setup
    async with session_factory() as s:
        c = Batch(
            team_id=team_id,
            name="C",
            task_filter={
                "benchmark_ids": ["runner-benchmark"],
                "subset_kind": "first_n",
                "n": 1,
            },
            trial_config={},
            state="submitted",
            created_by_token_prefix="abcdef12",
            expected_trial_count=1,
        )
        s.add(c)
        await s.commit()
        await s.refresh(c)
        cid = c.id

    await run_once(
        session_factory=session_factory,
        http_client=http_client,
        batch_size=10,
        submit_rate_per_sec=100,
    )

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        trials = s.execute(
            select(Trial)
            .where(Trial.batch_id == cid)
            .order_by(Trial.task_id.asc()),
        ).scalars().all()
    sync_engine.dispose()

    assert [t.task_id for t in trials] == [task_ids[0]]


async def test_runner_honors_subset_filter_variants(
    runner_setup: tuple[
        async_sessionmaker, httpx.AsyncClient, UUID, list[str], list[dict],
    ],
    postgres_url: str,
) -> None:
    session_factory, http_client, team_id, task_ids, _captured = runner_setup
    cases = [
        (
            "last",
            {
                "benchmark_ids": ["runner-benchmark"],
                "subset_kind": "last_n",
                "n": 2,
            },
            task_ids[-2:],
        ),
        (
            "random",
            {
                "benchmark_ids": ["runner-benchmark"],
                "subset_kind": "random_n",
                "n": 2,
                "seed": 42,
            },
            ["local/runner-0", "local/runner-4"],
        ),
        (
            "explicit",
            {
                "task_ids": [task_ids[3], task_ids[1]],
                "subset_kind": "explicit",
            },
            [task_ids[1], task_ids[3]],
        ),
    ]

    created: list[tuple[UUID, list[str]]] = []
    async with session_factory() as s:
        for name, task_filter, expected in cases:
            c = Batch(
                team_id=team_id,
                name=name,
                task_filter=task_filter,
                trial_config={},
                state="submitted",
                created_by_token_prefix="abcdef12",
                expected_trial_count=len(expected),
            )
            s.add(c)
            await s.flush()
            created.append((c.id, expected))
        await s.commit()

    await run_once(
        session_factory=session_factory,
        http_client=http_client,
        batch_size=10,
        submit_rate_per_sec=100,
    )

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        for batch_id, expected in created:
            trials = s.execute(
                select(Trial)
                .where(Trial.batch_id == batch_id)
                .order_by(Trial.task_id.asc()),
            ).scalars().all()
            assert [t.task_id for t in trials] == expected
    sync_engine.dispose()


async def test_runner_honors_empty_benchmark_ids_filter(
    runner_setup: tuple[
        async_sessionmaker, httpx.AsyncClient, UUID, list[str], list[dict],
    ],
    postgres_url: str,
) -> None:
    session_factory, http_client, team_id, _task_ids, _captured = runner_setup
    async with session_factory() as s:
        c = Batch(
            team_id=team_id,
            name="empty",
            task_filter={"benchmark_ids": []},
            trial_config={},
            state="submitted",
            created_by_token_prefix="abcdef12",
            expected_trial_count=0,
        )
        s.add(c)
        await s.commit()
        await s.refresh(c)
        cid = c.id

    await run_once(
        session_factory=session_factory,
        http_client=http_client,
        batch_size=10,
        submit_rate_per_sec=100,
    )

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        trials = s.execute(
            select(Trial).where(Trial.batch_id == cid),
        ).scalars().all()
    sync_engine.dispose()

    assert trials == []


async def test_runner_forwards_batch_provider_connection_fields(
    runner_setup: tuple[
        async_sessionmaker, httpx.AsyncClient, UUID, list[str], list[dict],
    ],
    postgres_url: str,
) -> None:
    session_factory, http_client, team_id, _task_ids, captured = runner_setup
    conn_id = uuid4()
    async with session_factory() as s:
        s.add(ProviderConnection(
            id=conn_id,
            team_id=team_id,
            provider_type="openai-compatible",
            display_name="Lab vLLM",
            base_url="https://api.openai.com/v1",
            upstream_host="api.openai.com",
            resolved_egress_ips=["104.18.0.1"],
            encrypted_api_key_ref="test://runner",
            status="valid",
            pricing_source="tokens-only",
            rate_card_provider="openai",
            created_by="test:runner",
        ))
        await s.flush()
        c = Batch(
            team_id=team_id,
            name="C",
            task_filter={"license": "MIT"},
            trial_config={"agent_name": "litellm"},
            state="submitted",
            created_by_token_prefix="abcdef12",
            expected_trial_count=5,
            provider_connection_id=conn_id,
            provider_model_id="deepseek-chat",
        )
        s.add(c)
        await s.commit()
        await s.refresh(c)
        cid = c.id

    await run_once(
        session_factory=session_factory,
        http_client=http_client,
        batch_size=10,
        submit_rate_per_sec=100,
    )

    assert captured
    assert {body["provider_connection_id"] for body in captured} == {
        str(conn_id),
    }
    assert {body["provider_model_id"] for body in captured} == {
        "deepseek-chat",
    }

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        trials = s.execute(
            select(Trial).where(Trial.batch_id == cid),
        ).scalars().all()
    sync_engine.dispose()
    assert {t.provider_connection_id for t in trials} == {conn_id}
    assert {t.provider_model_id for t in trials} == {"deepseek-chat"}


async def test_runner_forwards_combination_provider_connection_fields(
    runner_setup: tuple[
        async_sessionmaker, httpx.AsyncClient, UUID, list[str], list[dict],
    ],
    postgres_url: str,
) -> None:
    session_factory, http_client, team_id, _task_ids, captured = runner_setup
    conn_a = uuid4()
    conn_b = uuid4()
    async with session_factory() as s:
        for conn_id, display_name in (
            (conn_a, "Combo provider A"),
            (conn_b, "Combo provider B"),
        ):
            s.add(ProviderConnection(
                id=conn_id,
                team_id=team_id,
                provider_type="openai-compatible",
                display_name=display_name,
                base_url="https://api.openai.com/v1",
                upstream_host="api.openai.com",
                resolved_egress_ips=["104.18.0.1"],
                encrypted_api_key_ref=f"test://{conn_id}",
                status="valid",
                pricing_source="tokens-only",
                rate_card_provider="openai",
                created_by="test:runner",
            ))
        await s.flush()
        c = Batch(
            team_id=team_id,
            name="combo-providers",
            task_filter={"license": "MIT", "subset_kind": "first_n", "n": 1},
            trial_config={},
            combinations=[
                {
                    "agent_name": "litellm",
                    "agent_model": {"provider": "openai", "name": "glm-5.1-thinking"},
                    "provider_connection_id": str(conn_a),
                    "provider_model_id": "glm-5.1-thinking",
                    "n_per_task": 1,
                    "label": "glm",
                },
                {
                    "agent_name": "litellm",
                    "agent_model": {"provider": "openai", "name": "qwen3.6-35b-a3b"},
                    "provider_connection_id": str(conn_b),
                    "provider_model_id": "qwen3.6-35b-a3b",
                    "n_per_task": 1,
                    "label": "qwen",
                },
            ],
            state="submitted",
            created_by_token_prefix="abcdef12",
            expected_trial_count=2,
            provider_connection_id=None,
            provider_model_id=None,
        )
        s.add(c)
        await s.commit()
        await s.refresh(c)
        cid = c.id

    await run_once(
        session_factory=session_factory,
        http_client=http_client,
        batch_size=10,
        submit_rate_per_sec=100,
    )

    assert captured
    payloads_by_combo = {body["combination_idx"]: body for body in captured}
    assert payloads_by_combo[0]["provider_connection_id"] == str(conn_a)
    assert payloads_by_combo[0]["provider_model_id"] == "glm-5.1-thinking"
    assert payloads_by_combo[1]["provider_connection_id"] == str(conn_b)
    assert payloads_by_combo[1]["provider_model_id"] == "qwen3.6-35b-a3b"

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        trials = s.execute(
            select(Trial)
            .where(Trial.batch_id == cid)
            .order_by(Trial.combination_idx.asc()),
        ).scalars().all()
    sync_engine.dispose()
    assert [(t.combination_idx, t.provider_connection_id, t.provider_model_id) for t in trials] == [
        (0, conn_a, "glm-5.1-thinking"),
        (1, conn_b, "qwen3.6-35b-a3b"),
    ]


async def test_runner_skips_invalid_task_configs_and_adjusts_expected(
    runner_setup: tuple[
        async_sessionmaker, httpx.AsyncClient, UUID, list[str], list[dict],
    ],
    postgres_url: str,
) -> None:
    session_factory, http_client, team_id, task_ids, captured = runner_setup
    bad_task_id = "local/runner-broken"
    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(insert(Task).values(
            id=bad_task_id,
            checksum="b" * 64,
            config={},
            source="local",
            license="MIT",
        ))
    sync_engine.dispose()

    async with session_factory() as s:
        c = Batch(
            team_id=team_id,
            name="C",
            task_filter={"license": "MIT"},
            trial_config={},
            state="submitted",
            created_by_token_prefix="abcdef12",
            expected_trial_count=6,
        )
        s.add(c)
        await s.commit()
        await s.refresh(c)
        cid = c.id

    await run_once(
        session_factory=session_factory,
        http_client=http_client,
        batch_size=10,
        submit_rate_per_sec=100,
    )

    submitted_task_ids = {body["task_id"] for body in captured}
    assert submitted_task_ids == set(task_ids)
    assert bad_task_id not in submitted_task_ids

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        batch_row = s.execute(
            select(Batch).where(Batch.id == cid),
        ).scalar_one()
    sync_engine.dispose()
    assert batch_row.expected_trial_count == 5
    assert batch_row.result_status == "partial_failed"
    assert batch_row.state == "running"


async def test_runner_finishes_batch_when_all_task_configs_are_invalid(
    runner_setup: tuple[
        async_sessionmaker, httpx.AsyncClient, UUID, list[str], list[dict],
    ],
    postgres_url: str,
) -> None:
    session_factory, http_client, team_id, _task_ids, captured = runner_setup
    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        for idx in range(2):
            conn.execute(insert(Task).values(
                id=f"local/all-broken-{idx}",
                checksum=str(idx) * 64,
                config={},
                source="local",
                license="BrokenOnly",
            ))
    sync_engine.dispose()

    async with session_factory() as s:
        c = Batch(
            team_id=team_id,
            name="C",
            task_filter={"license": "BrokenOnly"},
            trial_config={},
            state="submitted",
            created_by_token_prefix="abcdef12",
            expected_trial_count=2,
        )
        s.add(c)
        await s.commit()
        await s.refresh(c)
        cid = c.id

    await run_once(
        session_factory=session_factory,
        http_client=http_client,
        batch_size=10,
        submit_rate_per_sec=100,
    )

    assert captured == []
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        batch_row = s.execute(
            select(Batch).where(Batch.id == cid),
        ).scalar_one()
    sync_engine.dispose()
    assert batch_row.expected_trial_count == 0
    assert batch_row.state == "finished"
    assert batch_row.result_status == "all_failed"
    assert batch_row.finished_at is not None


async def test_runner_finishes_batch_when_trial_submit_policy_rejects_task(
    runner_setup: tuple[
        async_sessionmaker, httpx.AsyncClient, UUID, list[str], list[dict],
    ],
    postgres_url: str,
) -> None:
    session_factory, _http_client, team_id, task_ids, _captured = runner_setup

    async with session_factory() as s:
        c = Batch(
            team_id=team_id,
            name="policy-blocked",
            task_filter={
                "benchmark_ids": ["runner-benchmark"],
                "subset_kind": "first_n",
                "n": 1,
            },
            trial_config={},
            state="submitted",
            created_by_token_prefix="abcdef12",
            expected_trial_count=1,
        )
        s.add(c)
        await s.commit()
        await s.refresh(c)
        cid = c.id

    rejected: list[dict] = []

    def rejecting_cp_handler(req: httpx.Request) -> httpx.Response:
        if req.url.path != "/trials" or req.method != "POST":
            return httpx.Response(404)
        body = _json.loads(req.content.decode())
        rejected.append(body)
        return httpx.Response(
            403,
            json={
                "detail": "task license proprietary-MAA not in team allowlist",
            },
        )

    rejecting_client = httpx.AsyncClient(
        transport=httpx.MockTransport(rejecting_cp_handler),
        base_url="http://cp",
    )
    try:
        await run_once(
            session_factory=session_factory,
            http_client=rejecting_client,
            batch_size=10,
            submit_rate_per_sec=100,
        )
        await run_once(
            session_factory=session_factory,
            http_client=rejecting_client,
            batch_size=10,
            submit_rate_per_sec=100,
        )
    finally:
        await rejecting_client.aclose()

    assert [body["task_id"] for body in rejected] == [task_ids[0]]

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        trials = s.execute(
            select(Trial).where(Trial.batch_id == cid),
        ).scalars().all()
        batch_row = s.execute(
            select(Batch).where(Batch.id == cid),
        ).scalar_one()
    sync_engine.dispose()

    assert trials == []
    assert batch_row.expected_trial_count == 0
    assert batch_row.state == "finished"
    assert batch_row.result_status == "all_failed"
    assert batch_row.failure_reason == "fanout_submit_failed"
    assert "proprietary-MAA" in (batch_row.failure_message or "")
    assert batch_row.finished_at is not None


# Sanity import to keep `next_batch_state` referenced.
_ = next_batch_state
