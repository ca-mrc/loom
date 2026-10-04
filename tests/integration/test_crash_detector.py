from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import delete, insert, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import LlmCall, Task, Team, TeamQuota, Trial, TrialEvent, Worker
from loom_control_plane.retry_exhausted_sweeper import sweep_retry_exhausted
from loom_control_plane.scheduler.crash_detector import (
    reclaim_expired_workers,
    reclaim_stale_running_trials,
)


@pytest.fixture(autouse=True)
async def _cleanup_db(postgres_url: str):  # type: ignore[no-untyped-def]
    yield
    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as s:
        await s.execute(delete(TrialEvent))
        await s.execute(delete(LlmCall))
        await s.execute(delete(Trial))
        await s.execute(delete(Worker))
        await s.execute(delete(TeamQuota))
        await s.execute(delete(Team))
        await s.execute(delete(Task))
        await s.commit()
    await engine.dispose()


async def test_reclaim_expired_workers_moves_trials_to_queued(postgres_url: str):
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    team_id = uuid4()
    worker_id = uuid4()
    trial_id = uuid4()

    async with session_factory() as s:
        await s.execute(insert(Team).values(id=team_id, name=f"x-{team_id}"))
        await s.execute(
            insert(Worker).values(
                id=worker_id,
                hostname="h",
                version="v",
                capabilities=[],
                registered_at=datetime.now(UTC),
                last_seen_at=datetime.now(UTC) - timedelta(seconds=60),
                status="active",
            )
        )
        await s.execute(insert(Task).values(id="t", checksum="0" * 64, config={}))
        await s.execute(
            insert(Trial).values(
                id=trial_id,
                team_id=team_id,
                task_id="t",
                config={},
                requires_caps={},
                state="running",
                worker_id=worker_id,
            )
        )
        await s.commit()

    async with session_factory() as s:
        n = await reclaim_expired_workers(s, expiry_sec=15)
        await s.commit()
    assert n == 1

    async with session_factory() as s:
        row = (
            await s.execute(
                select(Trial).where(Trial.id == trial_id),
            )
        ).scalar_one()
        assert row.state == "queued"
        assert row.worker_id is None
        assert row.next_attempt_at is not None

    await engine.dispose()


@pytest.mark.parametrize("state", ["claimed", "running", "queued"])
async def test_reclaim_started_trial_keeps_worker_death_diagnostic(
    postgres_url: str, state: str,
):
    """A started-but-queued legacy row also must lose its dead owner (#19)."""
    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    team_id, worker_id, trial_id = uuid4(), uuid4(), uuid4()
    task_id = f"worker-death-{trial_id}"
    now = datetime.now(UTC)
    started = now - timedelta(seconds=120)
    try:
        async with factory() as session, session.begin():
            session.add(Team(id=team_id, name=f"worker-death-{team_id}"))
            await session.flush()
            session.add(TeamQuota(team_id=team_id, max_attempts_ceiling=1))
            session.add(Worker(id=worker_id, hostname="dead-local-worker", version="test",
                               capabilities=[], registered_at=started,
                               last_seen_at=now - timedelta(seconds=60), status="active"))
            session.add(Task(id=task_id, checksum="0" * 64, config={}))
            await session.flush()
            session.add(Trial(id=trial_id, team_id=team_id, task_id=task_id, config={},
                              requires_caps={}, state=state, worker_id=worker_id,
                              claimed_at=started, started_at=started, attempt_count=1))
        async with factory() as session, session.begin():
            assert await reclaim_expired_workers(session, expiry_sec=15) == 1
        async with factory() as session:
            trial = await session.get(Trial, trial_id)
            assert trial.state == "queued"
            assert trial.worker_id is None
            assert trial.failure_reason == "worker_lost_claim"
            assert "worker_heartbeat_expired_reclaimed" in trial.failure_message
            assert str(worker_id) in trial.failure_message
            assert f"previous_state={state}" in trial.failure_message
            assert trial.started_at == started
            assert trial.next_attempt_at > now
            diagnostic = trial.failure_message
        async with factory() as session, session.begin():
            assert await reclaim_expired_workers(session, expiry_sec=15) == 0
            assert await sweep_retry_exhausted(session) == [trial_id]
        async with factory() as session:
            trial = await session.get(Trial, trial_id)
            assert trial.state == "failed"
            assert trial.failure_reason == "retry_exhausted"
            assert trial.failure_message == diagnostic
            assert trial.finished_at is not None
    finally:
        await engine.dispose()


async def test_reclaim_preserves_prod_pressure_retry_diagnostic(postgres_url: str):
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    team_id = uuid4()
    worker_id = uuid4()
    trial_id = uuid4()
    task_id = f"prod-pressure-{trial_id.hex}"
    message = (
        "preemptible staging trial selected for worker drain after production "
        "capacity pressure grace period elapsed; crash reclaim returns it to "
        "queued with retry backoff"
    )

    async with session_factory() as s:
        await s.execute(insert(Team).values(id=team_id, name=f"pp-{team_id}"))
        await s.execute(
            insert(Worker).values(
                id=worker_id,
                hostname="h-pressure",
                version="v",
                capabilities=[],
                registered_at=datetime.now(UTC),
                last_seen_at=datetime.now(UTC) - timedelta(seconds=60),
                status="active",
            )
        )
        await s.execute(insert(Task).values(id=task_id, checksum="0" * 64, config={}))
        await s.execute(
            insert(Trial).values(
                id=trial_id,
                team_id=team_id,
                task_id=task_id,
                config={},
                requires_caps={},
                state="claimed",
                worker_id=worker_id,
                claimed_at=datetime.now(UTC) - timedelta(seconds=60),
                failure_reason="prod_capacity_pressure",
                failure_message=message,
            )
        )
        await s.commit()

    async with session_factory() as s:
        assert await reclaim_expired_workers(s, expiry_sec=15) == 1
        await s.commit()

    async with session_factory() as s:
        row = await s.get(Trial, trial_id)
        assert row is not None
        assert row.state == "queued"
        assert row.worker_id is None
        assert row.failure_reason == "prod_capacity_pressure"
        assert row.failure_message == message
        assert row.next_attempt_at is not None

    await engine.dispose()


async def test_reclaim_leaves_fresh_workers_alone(postgres_url: str):
    """A worker with a recent heartbeat is NOT reclaimed."""
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    team_id = uuid4()
    worker_id = uuid4()
    trial_id = uuid4()

    async with session_factory() as s:
        await s.execute(insert(Team).values(id=team_id, name=f"a-{team_id}"))
        await s.execute(
            insert(Worker).values(
                id=worker_id,
                hostname="h",
                version="v",
                capabilities=[],
                registered_at=datetime.now(UTC),
                last_seen_at=datetime.now(UTC),  # very fresh
                status="active",
            )
        )
        await s.execute(insert(Task).values(id="t", checksum="0" * 64, config={}))
        await s.execute(
            insert(Trial).values(
                id=trial_id,
                team_id=team_id,
                task_id="t",
                config={},
                requires_caps={},
                state="running",
                worker_id=worker_id,
            )
        )
        await s.commit()

    async with session_factory() as s:
        n = await reclaim_expired_workers(s, expiry_sec=15)
        await s.commit()
    assert n == 0

    await engine.dispose()


async def test_reclaim_stale_running_trial_on_fresh_worker_records_timeout_diagnostic(
    postgres_url: str,
):
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    team_id = uuid4()
    worker_id = uuid4()
    trial_id = uuid4()
    task_id = f"stale-running-{trial_id.hex}"

    async with session_factory() as s:
        await s.execute(insert(Team).values(id=team_id, name=f"sr-{team_id}"))
        await s.execute(
            insert(Worker).values(
                id=worker_id,
                hostname="trt-local_gpu-4",
                version="v",
                capabilities=[],
                registered_at=now - timedelta(hours=2),
                last_seen_at=now - timedelta(seconds=5),
                status="active",
                pool_name="local_gpu",
            )
        )
        await s.execute(
            insert(Task).values(
                id=task_id,
                checksum="0" * 64,
                config={
                    "task": {"id": task_id},
                    "agent": {"name": "opencode", "timeout_sec": 10.0},
                },
            )
        )
        await s.execute(
            insert(Trial).values(
                id=trial_id,
                team_id=team_id,
                task_id=task_id,
                config={
                    "schema_version": "1",
                    "agent_name": "opencode",
                    "agent_model": {"provider": "openai", "name": "glm"},
                    "agent_timeout_multiplier": 1.0,
                },
                requires_caps={},
                state="running",
                worker_id=worker_id,
                claimed_at=now - timedelta(seconds=100),
                started_at=now - timedelta(seconds=100),
            )
        )
        await s.execute(
            insert(TrialEvent).values(
                trial_id=trial_id,
                seq=1,
                kind="thought",
                source="worker",
                created_at=now - timedelta(seconds=80),
                payload={
                    "kind": "thought",
                    "emitted_at": (now - timedelta(seconds=80)).isoformat(),
                    "trial_id": str(trial_id),
                    "step_id": "main",
                    "seq": 1,
                    "content": "working",
                },
            )
        )
        await s.execute(
            insert(LlmCall).values(
                id=uuid4(),
                team_id=team_id,
                trial_id=trial_id,
                step_id="main",
                model="glm",
                dialect="openai_facade",
                input_tokens=10,
                output_tokens=5,
                provider_extras={},
                cost_usd=Decimal("0.001"),
                rate_card_hash="rate",
                captured_at=now - timedelta(seconds=75),
            )
        )
        await s.commit()

    async with session_factory() as s:
        n = await reclaim_stale_running_trials(
            s,
            now=now,
            worker_heartbeat_expiry_sec=120,
            timeout_multiplier=2.0,
            grace_sec=10.0,
            silence_sec=10.0,
        )
        await s.commit()
    assert n == 1

    async with session_factory() as s:
        row = (
            await s.execute(select(Trial).where(Trial.id == trial_id))
        ).scalar_one()
        assert row.state == "failed"
        assert row.worker_id == worker_id
        assert row.failure_reason == "agent_timeout"
        assert row.finished_at is not None
        assert row.failure_message is not None
        assert "stale_running_reclaimed" in row.failure_message
        assert "runtime_sec=100" in row.failure_message
        assert "agent_timeout_sec=10" in row.failure_message
        assert "worker_heartbeat_status=fresh" in row.failure_message
        assert "last_event_at=" in row.failure_message
        assert "last_llm_call_at=" in row.failure_message

    await engine.dispose()


async def test_reclaim_stale_running_trial_skips_recent_activity(
    postgres_url: str,
):
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    team_id = uuid4()
    worker_id = uuid4()
    trial_id = uuid4()
    task_id = f"recent-running-{trial_id.hex}"

    async with session_factory() as s:
        await s.execute(insert(Team).values(id=team_id, name=f"ra-{team_id}"))
        await s.execute(
            insert(Worker).values(
                id=worker_id,
                hostname="trt-local_gpu-8",
                version="v",
                capabilities=[],
                registered_at=now - timedelta(hours=2),
                last_seen_at=now - timedelta(seconds=5),
                status="active",
                pool_name="local_gpu",
            )
        )
        await s.execute(
            insert(Task).values(
                id=task_id,
                checksum="0" * 64,
                config={
                    "task": {"id": task_id},
                    "agent": {"name": "opencode", "timeout_sec": 10.0},
                },
            )
        )
        await s.execute(
            insert(Trial).values(
                id=trial_id,
                team_id=team_id,
                task_id=task_id,
                config={"agent_timeout_multiplier": 1.0},
                requires_caps={},
                state="running",
                worker_id=worker_id,
                claimed_at=now - timedelta(seconds=100),
                started_at=now - timedelta(seconds=100),
            )
        )
        await s.execute(
            insert(TrialEvent).values(
                trial_id=trial_id,
                seq=1,
                kind="thought",
                source="worker",
                created_at=now - timedelta(seconds=3),
                payload={
                    "kind": "thought",
                    "emitted_at": (now - timedelta(seconds=3)).isoformat(),
                    "trial_id": str(trial_id),
                    "step_id": "main",
                    "seq": 1,
                    "content": "still active",
                },
            )
        )
        await s.commit()

    async with session_factory() as s:
        n = await reclaim_stale_running_trials(
            s,
            now=now,
            worker_heartbeat_expiry_sec=120,
            timeout_multiplier=2.0,
            grace_sec=10.0,
            silence_sec=10.0,
        )
        await s.commit()
    assert n == 0

    async with session_factory() as s:
        row = (
            await s.execute(select(Trial).where(Trial.id == trial_id))
        ).scalar_one()
        assert row.state == "running"
        assert row.failure_reason is None

    await engine.dispose()


async def test_reclaim_worker_exit_before_started_records_diagnostic(
    postgres_url: str,
):
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    team_id = uuid4()
    worker_id = uuid4()
    trial_id = uuid4()
    task_id = f"t-{trial_id.hex}"

    async with session_factory() as s:
        await s.execute(insert(Team).values(id=team_id, name=f"e-{team_id}"))
        await s.execute(
            insert(Worker).values(
                id=worker_id,
                hostname="exited",
                version="v",
                capabilities=[],
                registered_at=datetime.now(UTC),
                last_seen_at=datetime.now(UTC) - timedelta(seconds=600),
                status="active",
            )
        )
        await s.execute(insert(Task).values(id=task_id, checksum="0" * 64, config={}))
        await s.execute(
            insert(Trial).values(
                id=trial_id,
                team_id=team_id,
                task_id=task_id,
                config={},
                requires_caps={},
                state="claimed",
                worker_id=worker_id,
                claimed_at=datetime.now(UTC) - timedelta(seconds=120),
                started_at=None,
            )
        )
        await s.commit()

    async with session_factory() as s:
        n = await reclaim_expired_workers(s, expiry_sec=300)
        await s.commit()
    assert n == 1

    async with session_factory() as s:
        row = (
            await s.execute(
                select(Trial).where(Trial.id == trial_id),
            )
        ).scalar_one()
        assert row.state == "queued"
        assert row.worker_id is None
        assert row.failure_reason == "worker_lost_claim"
        assert row.failure_message is not None
        assert "claimed_without_started_reclaimed" in row.failure_message
        assert str(worker_id) in row.failure_message
        assert str(trial_id) in row.failure_message
        assert "started_at=NULL" in row.failure_message
        assert row.next_attempt_at is not None

    await engine.dispose()


async def test_reclaim_stale_claimed_without_started_at_even_when_worker_is_fresh(
    postgres_url: str,
):
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    team_id = uuid4()
    worker_id = uuid4()
    trial_id = uuid4()
    task_id = f"t-{trial_id.hex}"

    async with session_factory() as s:
        await s.execute(insert(Team).values(id=team_id, name=f"s-{team_id}"))
        await s.execute(
            insert(Worker).values(
                id=worker_id,
                hostname="fresh",
                version="v",
                capabilities=[],
                registered_at=datetime.now(UTC),
                last_seen_at=datetime.now(UTC),
                status="active",
            )
        )
        await s.execute(insert(Task).values(id=task_id, checksum="0" * 64, config={}))
        await s.execute(
            insert(Trial).values(
                id=trial_id,
                team_id=team_id,
                task_id=task_id,
                config={},
                requires_caps={},
                state="claimed",
                worker_id=worker_id,
                claimed_at=datetime.now(UTC) - timedelta(seconds=600),
                started_at=None,
            )
        )
        await s.commit()

    async with session_factory() as s:
        n = await reclaim_expired_workers(
            s,
            expiry_sec=15,
            claimed_without_start_expiry_sec=300,
        )
        await s.commit()
    assert n == 1

    async with session_factory() as s:
        row = (
            await s.execute(
                select(Trial).where(Trial.id == trial_id),
            )
        ).scalar_one()
        assert row.state == "queued"
        assert row.worker_id is None
        assert row.failure_reason == "worker_lost_claim"
        assert row.failure_message is not None
        assert "claimed_without_started_reclaimed" in row.failure_message
        assert str(worker_id) in row.failure_message
        assert str(trial_id) in row.failure_message
        assert "started_at=NULL" in row.failure_message
        assert row.next_attempt_at is not None

    await engine.dispose()


async def test_recent_pre_start_heartbeat_keeps_fresh_worker_claim(
    postgres_url: str,
):
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    team_id = uuid4()
    worker_id = uuid4()
    trial_id = uuid4()
    task_id = f"t-{trial_id.hex}"
    now = datetime.now(UTC)

    async with session_factory() as s:
        await s.execute(insert(Team).values(id=team_id, name=f"prestart-{team_id}"))
        await s.execute(
            insert(Worker).values(
                id=worker_id,
                hostname="fresh-prestart",
                version="v",
                capabilities=[],
                registered_at=now,
                last_seen_at=now,
                status="active",
            )
        )
        await s.execute(insert(Task).values(id=task_id, checksum="0" * 64, config={}))
        await s.execute(
            insert(Trial).values(
                id=trial_id,
                team_id=team_id,
                task_id=task_id,
                config={},
                requires_caps={},
                state="claimed",
                worker_id=worker_id,
                claimed_at=now - timedelta(seconds=1800),
                started_at=None,
                pre_start_heartbeat_at=now - timedelta(seconds=60),
            )
        )
        await s.commit()

    async with session_factory() as s:
        n = await reclaim_expired_workers(
            s,
            expiry_sec=15,
            claimed_without_start_expiry_sec=300,
            now=now,
        )
        await s.commit()
    assert n == 0

    async with session_factory() as s:
        row = (
            await s.execute(
                select(Trial).where(Trial.id == trial_id),
            )
        ).scalar_one()
        assert row.state == "claimed"
        assert row.worker_id == worker_id
        assert row.started_at is None
        assert row.pre_start_heartbeat_at == now - timedelta(seconds=60)

    await engine.dispose()


async def test_final_pre_start_reclaim_keeps_retry_exhausted_diagnostic(
    postgres_url: str,
):
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    team_id = uuid4()
    worker_id = uuid4()
    trial_id = uuid4()
    task_id = f"t-{trial_id.hex}"

    async with session_factory() as s:
        await s.execute(insert(Team).values(id=team_id, name=f"f-{team_id}"))
        await s.execute(insert(TeamQuota).values(team_id=team_id, max_attempts_ceiling=3))
        await s.execute(
            insert(Worker).values(
                id=worker_id,
                hostname="fresh-final",
                version="v",
                capabilities=[],
                registered_at=datetime.now(UTC),
                last_seen_at=datetime.now(UTC),
                status="active",
            )
        )
        await s.execute(insert(Task).values(id=task_id, checksum="0" * 64, config={}))
        await s.execute(
            insert(Trial).values(
                id=trial_id,
                team_id=team_id,
                task_id=task_id,
                config={},
                requires_caps={},
                state="claimed",
                worker_id=worker_id,
                claimed_at=datetime.now(UTC) - timedelta(seconds=600),
                started_at=None,
                attempt_count=3,
            )
        )
        await s.commit()

    async with session_factory() as s:
        reclaimed = await reclaim_expired_workers(
            s,
            expiry_sec=15,
            claimed_without_start_expiry_sec=300,
        )
        await s.commit()
    assert reclaimed == 1

    async with session_factory() as s:
        swept = await sweep_retry_exhausted(s)
        await s.commit()
    assert swept == [trial_id]

    async with session_factory() as s:
        row = (
            await s.execute(
                select(Trial).where(Trial.id == trial_id),
            )
        ).scalar_one()
        assert row.state == "failed"
        assert row.worker_id is None
        assert row.failure_reason == "retry_exhausted"
        assert row.failure_message is not None
        assert "claimed_without_started_reclaimed" in row.failure_message
        assert str(worker_id) in row.failure_message
        assert str(trial_id) in row.failure_message
        assert row.started_at is None

    await engine.dispose()


async def test_reclaim_stale_claimed_keeps_started_trial_on_fresh_worker(
    postgres_url: str,
):
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    team_id = uuid4()
    worker_id = uuid4()
    trial_id = uuid4()
    task_id = f"t-{trial_id.hex}"

    async with session_factory() as s:
        await s.execute(insert(Team).values(id=team_id, name=f"r-{team_id}"))
        await s.execute(
            insert(Worker).values(
                id=worker_id,
                hostname="fresh-running",
                version="v",
                capabilities=[],
                registered_at=datetime.now(UTC),
                last_seen_at=datetime.now(UTC),
                status="active",
            )
        )
        await s.execute(insert(Task).values(id=task_id, checksum="0" * 64, config={}))
        await s.execute(
            insert(Trial).values(
                id=trial_id,
                team_id=team_id,
                task_id=task_id,
                config={},
                requires_caps={},
                state="claimed",
                worker_id=worker_id,
                claimed_at=datetime.now(UTC) - timedelta(seconds=600),
                started_at=datetime.now(UTC) - timedelta(seconds=590),
            )
        )
        await s.commit()

    async with session_factory() as s:
        n = await reclaim_expired_workers(
            s,
            expiry_sec=15,
            claimed_without_start_expiry_sec=300,
        )
        await s.commit()
    assert n == 0

    async with session_factory() as s:
        row = (
            await s.execute(
                select(Trial).where(Trial.id == trial_id),
            )
        ).scalar_one()
        assert row.state == "claimed"
        assert row.worker_id == worker_id

    await engine.dispose()


# ──────────────────────────────────────────────────────────────────────
# #193 fault-injection: production-race reproductions
#
# The above tests set up static DB state and run one sweep. The three
# tests below stress the actual production races that motivated #193:
# reclaim happening concurrent with a worker's belated state PATCH, a
# single dead worker holding many stuck claims (the "6 stuck trials on
# staging-b453057" evidence), and multi-cycle reclaim attribution
# through retry_exhausted.
# ──────────────────────────────────────────────────────────────────────


# Copy of the fencing predicate from state.py — kept local to the test
# so a future rewrite of the PATCH SQL is caught here by a compilation
# failure rather than a silent regression.
_PATCH_FENCE_CHECK_SQL = text("""
UPDATE trials
   SET state = 'running',
       started_at = CASE WHEN started_at IS NULL THEN NOW() ELSE started_at END
 WHERE id = (:trial_id)::uuid
   AND worker_id = (:worker_id)::uuid
   AND state = 'claimed'
 RETURNING id;
""")


async def test_reclaim_then_worker_patch_returns_zero_rows(
    postgres_url: str,
):
    """Production race from #193 evidence
    (`PATCH /trials/.../state` → 409 Conflict after the CP reclaim
    already cleared `worker_id`). Worker A claims trial, never marks
    it started, CP reclaims because worker heartbeat expired, then
    worker A wakes up and tries to PATCH state=running with its own
    worker_id. The fencing predicate (`worker_id = :worker_id`) must
    match zero rows so the route returns 409 without silently reviving
    the trial under the dead worker.
    """
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    team_id = uuid4()
    worker_a_id = uuid4()
    trial_id = uuid4()
    task_id = f"t-{trial_id.hex}"

    async with session_factory() as s:
        await s.execute(insert(Team).values(id=team_id, name=f"race-{team_id}"))
        await s.execute(
            insert(Worker).values(
                id=worker_a_id,
                hostname="worker-a",
                version="v",
                capabilities=[],
                registered_at=datetime.now(UTC),
                # Heartbeat lapsed → crash detector will reclaim.
                last_seen_at=datetime.now(UTC) - timedelta(seconds=120),
                status="active",
            )
        )
        await s.execute(insert(Task).values(id=task_id, checksum="0" * 64, config={}))
        await s.execute(
            insert(Trial).values(
                id=trial_id,
                team_id=team_id,
                task_id=task_id,
                config={},
                requires_caps={},
                state="claimed",
                worker_id=worker_a_id,
                claimed_at=datetime.now(UTC) - timedelta(seconds=60),
                started_at=None,
            )
        )
        await s.commit()

    async with session_factory() as s:
        reclaimed = await reclaim_expired_workers(
            s,
            expiry_sec=15,
            claimed_without_start_expiry_sec=300,
        )
        await s.commit()
    assert reclaimed == 1

    # Simulate worker A's belated PATCH state=running. Must match zero
    # rows because reclaim cleared worker_id AND changed state to
    # queued — both halves of the fence trip.
    async with session_factory() as s:
        rows = (
            await s.execute(
                _PATCH_FENCE_CHECK_SQL,
                {"trial_id": trial_id, "worker_id": worker_a_id},
            )
        ).all()
        await s.commit()
    assert rows == [], (
        "Worker A's belated PATCH after reclaim should match zero rows "
        "(fencing predicate). If this fails, the fencing has silently "
        "regressed and a dead worker could revive a reclaimed trial."
    )

    # Verify canonical state: reclaimed trial is queued with the
    # #193 diagnostic message and no worker attribution.
    async with session_factory() as s:
        row = (
            await s.execute(select(Trial).where(Trial.id == trial_id))
        ).scalar_one()
    assert row.state == "queued"
    assert row.worker_id is None
    assert row.failure_reason == "worker_lost_claim"
    assert "claimed_without_started_reclaimed" in (row.failure_message or "")
    assert row.started_at is None

    await engine.dispose()


async def test_batch_reclaim_of_multiple_stuck_claims_from_same_worker(
    postgres_url: str,
):
    """Reproduces the six-stuck-trials evidence from #193 (staging-
    b453057 batch `19e825da-...`). One dead worker held six claimed
    trials with `started_at=NULL`; a single crash-detector sweep must
    reclaim ALL six with distinct diagnostic messages tagging each
    trial and the same dead worker.
    """
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    team_id = uuid4()
    dead_worker_id = uuid4()
    trial_ids = [uuid4() for _ in range(6)]

    async with session_factory() as s:
        await s.execute(insert(Team).values(id=team_id, name=f"stuck-{team_id}"))
        await s.execute(
            insert(Worker).values(
                id=dead_worker_id,
                hostname="loom-worker-5b749f7f69-dead",
                version="v",
                capabilities=[],
                registered_at=datetime.now(UTC),
                last_seen_at=datetime.now(UTC) - timedelta(seconds=600),
                status="active",
            )
        )
        for i, trial_id in enumerate(trial_ids):
            task_id = f"stuck-{trial_id.hex}"
            await s.execute(
                insert(Task).values(id=task_id, checksum="0" * 64, config={})
            )
            await s.execute(
                insert(Trial).values(
                    id=trial_id,
                    team_id=team_id,
                    task_id=task_id,
                    config={},
                    requires_caps={},
                    state="claimed",
                    worker_id=dead_worker_id,
                    claimed_at=datetime.now(UTC) - timedelta(seconds=300 + i),
                    started_at=None,
                )
            )
        await s.commit()

    async with session_factory() as s:
        reclaimed = await reclaim_expired_workers(
            s,
            expiry_sec=15,
            claimed_without_start_expiry_sec=60,
        )
        await s.commit()
    # Single sweep must reclaim ALL six trials — batch semantics, not
    # one-per-tick. This is the guarantee that avoids the "some trials
    # stuck for another 30 s per tick" pathology.
    assert reclaimed == 6

    async with session_factory() as s:
        rows = (
            await s.execute(
                select(Trial).where(Trial.id.in_(trial_ids))
            )
        ).scalars().all()

    assert len(rows) == 6
    for row in rows:
        assert row.state == "queued", (
            f"trial {row.id} was left in state {row.state!r} instead "
            f"of queued after reclaim"
        )
        assert row.worker_id is None
        assert row.failure_reason == "worker_lost_claim"
        # Each diagnostic must tag its OWN trial_id and the dead
        # worker_id so operator triage can attribute the loss.
        message = row.failure_message or ""
        assert "claimed_without_started_reclaimed" in message
        assert str(row.id) in message
        assert str(dead_worker_id) in message
        assert row.started_at is None

    await engine.dispose()


async def test_reclaim_diagnostic_preserved_across_two_reclaim_cycles(
    postgres_url: str,
):
    """Multi-cycle attribution: a trial can be claimed → reclaimed →
    reclaimed → retry_exhausted. Each reclaim must overwrite the
    failure_message with a *new* attribution (current worker_id at
    reclaim time), and the final retry_exhausted row must preserve
    the LAST reclaim's message. Guards against a bug where the
    diagnostic could freeze on the first reclaim and mis-attribute
    later work to a stale worker.
    """
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    team_id = uuid4()
    worker_a_id = uuid4()
    worker_b_id = uuid4()
    trial_id = uuid4()
    task_id = f"t-{trial_id.hex}"

    async with session_factory() as s:
        await s.execute(insert(Team).values(id=team_id, name=f"multi-{team_id}"))
        # attempt_count 2 so the second reclaim tips into retry_exhausted.
        await s.execute(insert(TeamQuota).values(team_id=team_id, max_attempts_ceiling=3))
        await s.execute(
            insert(Worker).values(
                id=worker_a_id,
                hostname="worker-a-dead",
                version="v",
                capabilities=[],
                registered_at=datetime.now(UTC),
                last_seen_at=datetime.now(UTC) - timedelta(seconds=120),
                status="active",
            )
        )
        await s.execute(insert(Task).values(id=task_id, checksum="0" * 64, config={}))
        await s.execute(
            insert(Trial).values(
                id=trial_id,
                team_id=team_id,
                task_id=task_id,
                config={},
                requires_caps={},
                state="claimed",
                worker_id=worker_a_id,
                claimed_at=datetime.now(UTC) - timedelta(seconds=60),
                started_at=None,
                attempt_count=1,
            )
        )
        await s.commit()

    # First reclaim: worker A dies, trial goes back to queued.
    async with session_factory() as s:
        assert await reclaim_expired_workers(
            s,
            expiry_sec=15,
            claimed_without_start_expiry_sec=300,
        ) == 1
        await s.commit()

    async with session_factory() as s:
        first = (
            await s.execute(select(Trial).where(Trial.id == trial_id))
        ).scalar_one()
    assert first.state == "queued"
    assert str(worker_a_id) in (first.failure_message or "")

    # Simulate scheduler re-dispatch to worker B, worker B also stalls
    # (never sets started_at), heartbeat expires. Bump attempt_count as
    # the claim path would.
    async with session_factory() as s:
        await s.execute(
            insert(Worker).values(
                id=worker_b_id,
                hostname="worker-b-dead",
                version="v",
                capabilities=[],
                registered_at=datetime.now(UTC),
                last_seen_at=datetime.now(UTC) - timedelta(seconds=120),
                status="active",
            )
        )
        await s.execute(
            text(
                "UPDATE trials SET state='claimed', worker_id=:w, "
                "claimed_at=NOW() - INTERVAL '60 seconds', "
                "started_at=NULL, attempt_count=2 "
                "WHERE id=:t"
            ),
            {"w": worker_b_id, "t": trial_id},
        )
        await s.commit()

    # Second reclaim: overwrites the message with worker B attribution.
    async with session_factory() as s:
        assert await reclaim_expired_workers(
            s,
            expiry_sec=15,
            claimed_without_start_expiry_sec=300,
        ) == 1
        await s.commit()

    async with session_factory() as s:
        second = (
            await s.execute(select(Trial).where(Trial.id == trial_id))
        ).scalar_one()
    assert second.state == "queued"
    assert second.failure_reason == "worker_lost_claim"
    message = second.failure_message or ""
    assert "claimed_without_started_reclaimed" in message
    assert str(worker_b_id) in message, (
        "second reclaim should attribute to worker B (the current "
        "claimant), not stay frozen on worker A's attribution"
    )
    assert str(worker_a_id) not in message

    # Now retry_exhausted: attempt_count already 2, budget 3 — bump to
    # 3 to simulate a final failed claim and let the sweeper flip
    # state=failed while preserving the last reclaim diagnostic.
    async with session_factory() as s:
        await s.execute(
            text("UPDATE trials SET attempt_count = 3 WHERE id=:t"),
            {"t": trial_id},
        )
        await s.commit()

    async with session_factory() as s:
        swept = await sweep_retry_exhausted(s)
        await s.commit()
    assert swept == [trial_id]

    async with session_factory() as s:
        final = (
            await s.execute(select(Trial).where(Trial.id == trial_id))
        ).scalar_one()
    assert final.state == "failed"
    assert final.failure_reason == "retry_exhausted"
    # The retry_exhausted sweeper must preserve the LAST reclaim's
    # attribution — not blank it out.
    final_msg = final.failure_message or ""
    assert "claimed_without_started_reclaimed" in final_msg
    assert str(worker_b_id) in final_msg

    await engine.dispose()
